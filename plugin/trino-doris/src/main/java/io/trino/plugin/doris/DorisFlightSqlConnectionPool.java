/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.doris;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.units.Duration;
import io.trino.plugin.base.audit.RemoteOperationAuditContext;
import jakarta.annotation.PreDestroy;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.google.common.cache.RemovalCause.EXPLICIT;
import static java.util.Objects.requireNonNull;

/**
 * Connection pool for Flight SQL connections that survive across queries.
 * Connections are keyed by (feHost, flightSqlPort) and reused when available.
 */
public class DorisFlightSqlConnectionPool
        implements AutoCloseable
{
    private final AdbcDorisFlightSqlClient.FlightSqlStreamOpenerFactory streamOpenerFactory;
    private final Cache<ConnectionKey, PooledConnection> connectionCache;
    private final AtomicBoolean closed = new AtomicBoolean();

    public DorisFlightSqlConnectionPool(
            AdbcDorisFlightSqlClient.FlightSqlStreamOpenerFactory streamOpenerFactory,
            int maxConnectionsPerEndpoint,
            Duration connectionIdleTimeout)
    {
        this.streamOpenerFactory = requireNonNull(streamOpenerFactory, "streamOpenerFactory is null");
        this.connectionCache = createCache(maxConnectionsPerEndpoint, connectionIdleTimeout);
    }

    @SuppressModernizer // This pool transfers ownership explicitly and never uses a cache loader.
    private static Cache<ConnectionKey, PooledConnection> createCache(int maxConnectionsPerEndpoint, Duration connectionIdleTimeout)
    {
        return CacheBuilder.newBuilder()
                .maximumSize(maxConnectionsPerEndpoint * 10L) // Assume max 10 endpoints
                .expireAfterWrite(connectionIdleTimeout.toJavaTime())
                .<ConnectionKey, PooledConnection>removalListener(notification -> {
                    if (notification.getCause() != EXPLICIT) {
                        requireNonNull(notification.getValue(), "connection is null").forceClose();
                    }
                })
                .build();
    }

    /**
     * Try to acquire a connection from the pool. Returns empty if no connection is available.
     */
    public synchronized Optional<PooledConnection> tryAcquire(String feHost, int flightSqlPort)
    {
        if (closed.get()) {
            return Optional.empty();
        }

        ConnectionKey key = new ConnectionKey(feHost, flightSqlPort);
        PooledConnection connection = connectionCache.asMap().remove(key);

        if (connection != null && connection.tryAcquire()) {
            return Optional.of(connection);
        }

        return Optional.empty();
    }

    /**
     * Create a new pooled connection.
     */
    public PooledConnection createConnection(String feHost, int flightSqlPort)
    {
        if (closed.get()) {
            throw new IllegalStateException("Connection pool is closed");
        }

        AdbcDorisFlightSqlClient.FlightSqlStreamOpener opener = streamOpenerFactory.create(feHost, flightSqlPort);
        ConnectionKey key = new ConnectionKey(feHost, flightSqlPort);
        PooledConnection connection = new PooledConnection(key, opener, this::returnConnection);

        return connection;
    }

    private synchronized void returnConnection(PooledConnection connection)
    {
        if (closed.get() || connection.isClosed()) {
            connection.forceClose();
            return;
        }

        // Return to pool
        connectionCache.put(connection.getKey(), connection);
    }

    @PreDestroy
    @Override
    public synchronized void close()
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        connectionCache.asMap().values().forEach(PooledConnection::forceClose);
        connectionCache.invalidateAll();
        connectionCache.cleanUp();
    }

    public static class PooledConnection
            implements AutoCloseable
    {
        private final ConnectionKey key;
        private final AdbcDorisFlightSqlClient.FlightSqlStreamOpener opener;
        private final Consumer<PooledConnection> returnToPool;
        private final AtomicBoolean inUse = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private PooledConnection(
                ConnectionKey key,
                AdbcDorisFlightSqlClient.FlightSqlStreamOpener opener,
                Consumer<PooledConnection> returnToPool)
        {
            this.key = requireNonNull(key, "key is null");
            this.opener = requireNonNull(opener, "opener is null");
            this.returnToPool = requireNonNull(returnToPool, "returnToPool is null");
        }

        public ConnectionKey getKey()
        {
            return key;
        }

        public boolean tryAcquire()
        {
            return !closed.get() && inUse.compareAndSet(false, true);
        }

        public DorisFlightSqlResult openStream(String sql)
        {
            return openStream(sql, null);
        }

        public DorisFlightSqlResult openStream(String sql, RemoteOperationAuditContext auditContext)
        {
            if (closed.get()) {
                throw new IllegalStateException("Connection is closed");
            }
            if (!inUse.get()) {
                throw new IllegalStateException("Connection is not acquired");
            }

            return opener.openStream(sql, auditContext);
        }

        public boolean isClosed()
        {
            return closed.get();
        }

        @Override
        public void close()
        {
            if (!inUse.compareAndSet(true, false)) {
                return;
            }

            returnToPool.accept(this);
        }

        public void forceClose()
        {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            inUse.set(false);
            opener.close();
        }
    }

    public record ConnectionKey(String feHost, int flightSqlPort) {}
}
