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
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import jakarta.annotation.PreDestroy;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
        this.connectionCache = EvictableCacheBuilder.newBuilder()
                .maximumSize(maxConnectionsPerEndpoint * 10) // Assume max 10 endpoints
                .expireAfterWrite(connectionIdleTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .shareNothingWhenDisabled()
                .build();
    }

    /**
     * Try to acquire a connection from the pool. Returns empty if no connection is available.
     */
    public Optional<PooledConnection> tryAcquire(String feHost, int flightSqlPort)
    {
        if (closed.get()) {
            return Optional.empty();
        }

        ConnectionKey key = new ConnectionKey(feHost, flightSqlPort);
        PooledConnection connection = connectionCache.getIfPresent(key);

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

    private void returnConnection(PooledConnection connection)
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
    public void close()
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        connectionCache.invalidateAll();
        connectionCache.cleanUp();
    }

    public static class PooledConnection
            implements AutoCloseable
    {
        private final ConnectionKey key;
        private final AdbcDorisFlightSqlClient.FlightSqlStreamOpener opener;
        private final java.util.function.Consumer<PooledConnection> returnToPool;
        private final AtomicBoolean inUse = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private PooledConnection(
                ConnectionKey key,
                AdbcDorisFlightSqlClient.FlightSqlStreamOpener opener,
                java.util.function.Consumer<PooledConnection> returnToPool)
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
            if (closed.get()) {
                throw new IllegalStateException("Connection is closed");
            }
            if (!inUse.get()) {
                throw new IllegalStateException("Connection is not acquired");
            }

            return opener.openStream(sql);
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
