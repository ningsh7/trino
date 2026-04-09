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

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import jakarta.annotation.PreDestroy;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class AdbcDorisFlightSqlClient
        implements DorisFlightSqlClient, DorisQueryEventListener
{
    private static final String APPLICATION_NAME_PREFIX = "/* ApplicationName=Trino Doris Flight SQL Query */ ";

    private final DorisConfig config;
    private final DorisQueryBuilder queryBuilder;
    private final DorisFlightSqlPortResolver portResolver;
    private final FlightSqlStreamOpenerFactory streamOpenerFactory;
    private final Supplier<List<String>> feHostsSupplier;
    private final Set<String> activeQueries = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, QueryResources> queryResources = new ConcurrentHashMap<>();
    private volatile List<String> cachedFeHosts;
    private volatile String preferredFeHost;

    @Inject
    public AdbcDorisFlightSqlClient(DorisConfig config, DorisQueryBuilder queryBuilder, DorisFlightSqlPortResolver portResolver)
    {
        this(
                config,
                queryBuilder,
                portResolver,
                new AdbcFlightSqlStreamOpenerFactory(config),
                () -> DorisFeEndpoints.getHosts(config));
    }

    AdbcDorisFlightSqlClient(
            DorisConfig config,
            DorisQueryBuilder queryBuilder,
            DorisFlightSqlPortResolver portResolver,
            FlightSqlStreamOpenerFactory streamOpenerFactory,
            Supplier<List<String>> feHostsSupplier)
    {
        this.config = requireNonNull(config, "config is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.portResolver = requireNonNull(portResolver, "portResolver is null");
        this.streamOpenerFactory = requireNonNull(streamOpenerFactory, "streamOpenerFactory is null");
        this.feHostsSupplier = requireNonNull(feHostsSupplier, "feHostsSupplier is null");
    }

    @Override
    public DorisFlightSqlResult openStream(DorisTableHandle tableHandle, DorisSplit split, List<DorisColumnHandle> columns)
    {
        return openStream(null, tableHandle, split, columns);
    }

    @Override
    public DorisFlightSqlResult openStream(ConnectorSession session, DorisTableHandle tableHandle, DorisSplit split, List<DorisColumnHandle> columns)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        List<DorisColumnHandle> queryColumns = tableHandle.aggregations().isPresent()
                ? columns
                : tableHandle.projectedColumns().orElse(columns);

        int flightSqlPort = portResolver.resolveFlightSqlPort(session);
        String sql = APPLICATION_NAME_PREFIX + queryBuilder.buildSelectSql(
                tableHandle,
                queryColumns.stream()
                        .map(DorisColumnHandle::columnName)
                        .toList(),
                split.tabletIds());

        QueryResources reusableQueryResources = getQueryResources(session);
        List<String> failures = new ArrayList<>();
        for (String feHost : prioritizedFeHosts()) {
            try {
                DorisFlightSqlResult result = openStream(reusableQueryResources, feHost, flightSqlPort, sql);
                preferredFeHost = feHost;
                return result;
            }
            catch (RuntimeException e) {
                failures.add("%s:%s -> %s".formatted(feHost, flightSqlPort, e.getMessage()));
            }
        }

        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open Doris Flight SQL stream: " + failures);
    }

    private List<String> prioritizedFeHosts()
    {
        List<String> hosts = cachedFeHosts;
        if (hosts == null) {
            hosts = feHostsSupplier.get();
            cachedFeHosts = hosts;
        }

        String preferred = preferredFeHost;
        if (preferred == null || !hosts.contains(preferred)) {
            return hosts;
        }

        List<String> prioritized = new ArrayList<>(hosts.size());
        prioritized.add(preferred);
        for (String host : hosts) {
            if (!host.equals(preferred)) {
                prioritized.add(host);
            }
        }
        return List.copyOf(prioritized);
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        String queryId = session.getQueryId();
        activeQueries.add(queryId);
        queryResources.computeIfAbsent(queryId, ignored -> new QueryResources());
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        String queryId = session.getQueryId();
        activeQueries.remove(queryId);
        closeQuietly(queryResources.remove(queryId));
    }

    @PreDestroy
    public void close()
    {
        activeQueries.clear();
        queryResources.values().forEach(AdbcDorisFlightSqlClient::closeQuietly);
        queryResources.clear();
    }

    private QueryResources getQueryResources(ConnectorSession session)
    {
        if (session == null) {
            return null;
        }

        String queryId = session.getQueryId();
        if (!activeQueries.contains(queryId)) {
            return null;
        }
        return queryResources.computeIfAbsent(queryId, ignored -> new QueryResources());
    }

    private DorisFlightSqlResult openStream(QueryResources reusableQueryResources, String feHost, int flightSqlPort, String sql)
    {
        if (reusableQueryResources == null) {
            return openStandaloneStream(feHost, flightSqlPort, sql);
        }
        return reusableQueryResources.openStream(feHost, flightSqlPort, Thread.currentThread().threadId(), sql);
    }

    private DorisFlightSqlResult openStandaloneStream(String feHost, int flightSqlPort, String sql)
    {
        FlightSqlStreamOpener opener = streamOpenerFactory.create(feHost, flightSqlPort);

        try {
            return new ManagedDorisFlightSqlResult(opener.openStream(sql), opener::close);
        }
        catch (RuntimeException e) {
            closeQuietly(opener);
            throw e;
        }
    }

    private static void closeQuietly(AutoCloseable closeable)
    {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        }
        catch (Exception ignored) {
        }
    }

    private static Exception closeResource(Exception failure, AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception e) {
            if (failure == null) {
                return e;
            }
            failure.addSuppressed(e);
        }
        return failure;
    }

    interface FlightSqlStreamOpenerFactory
    {
        FlightSqlStreamOpener create(String feHost, int flightSqlPort);
    }

    interface FlightSqlStreamOpener
            extends AutoCloseable
    {
        DorisFlightSqlResult openStream(String sql);

        long getMemoryUsage();

        @Override
        void close();
    }

    private final class QueryResources
            implements AutoCloseable
    {
        private final ConcurrentMap<ReusableExecutorKey, ReusableFlightSqlExecutor> executors = new ConcurrentHashMap<>();
        private final AtomicBoolean closed = new AtomicBoolean();

        public DorisFlightSqlResult openStream(String feHost, int flightSqlPort, long threadId, String sql)
        {
            if (closed.get()) {
                return openStandaloneStream(feHost, flightSqlPort, sql);
            }

            ReusableExecutorKey key = new ReusableExecutorKey(threadId, feHost, flightSqlPort);
            while (true) {
                if (closed.get()) {
                    return openStandaloneStream(feHost, flightSqlPort, sql);
                }

                ReusableFlightSqlExecutor executor = executors.get(key);
                if (executor == null) {
                    ReusableFlightSqlExecutor createdExecutor = new ReusableFlightSqlExecutor(streamOpenerFactory.create(feHost, flightSqlPort));
                    if (closed.get()) {
                        closeQuietly(createdExecutor);
                        return openStandaloneStream(feHost, flightSqlPort, sql);
                    }

                    ReusableFlightSqlExecutor existingExecutor = executors.putIfAbsent(key, createdExecutor);
                    executor = existingExecutor == null ? createdExecutor : existingExecutor;
                    if (existingExecutor != null) {
                        closeQuietly(createdExecutor);
                    }
                }

                Optional<DorisFlightSqlResult> reusableResult = executor.tryOpenStream(sql);
                if (reusableResult.isPresent()) {
                    return reusableResult.orElseThrow();
                }

                if (executor.isClosed()) {
                    executors.remove(key, executor);
                    continue;
                }

                return openStandaloneStream(feHost, flightSqlPort, sql);
            }
        }

        @Override
        public void close()
        {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            executors.values().forEach(AdbcDorisFlightSqlClient::closeQuietly);
            executors.clear();
        }
    }

    private record ReusableExecutorKey(long threadId, String feHost, int flightSqlPort) {}

    private static final class ReusableFlightSqlExecutor
            implements AutoCloseable
    {
        private final FlightSqlStreamOpener opener;
        private final AtomicBoolean inUse = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private ReusableFlightSqlExecutor(FlightSqlStreamOpener opener)
        {
            this.opener = requireNonNull(opener, "opener is null");
        }

        public Optional<DorisFlightSqlResult> tryOpenStream(String sql)
        {
            if (!inUse.compareAndSet(false, true)) {
                return Optional.empty();
            }

            if (closed.get()) {
                inUse.set(false);
                return Optional.empty();
            }

            try {
                return Optional.of(new ManagedDorisFlightSqlResult(opener.openStream(sql), this::release));
            }
            catch (RuntimeException e) {
                inUse.set(false);
                close();
                throw e;
            }
        }

        public boolean isClosed()
        {
            return closed.get();
        }

        @Override
        public void close()
        {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            opener.close();
        }

        private void release()
        {
            inUse.set(false);
        }
    }

    private static final class ManagedDorisFlightSqlResult
            implements DorisFlightSqlResult
    {
        private final DorisFlightSqlResult delegate;
        private final Runnable afterClose;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ManagedDorisFlightSqlResult(DorisFlightSqlResult delegate, Runnable afterClose)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.afterClose = requireNonNull(afterClose, "afterClose is null");
        }

        @Override
        public boolean loadNextBatch()
        {
            return delegate.loadNextBatch();
        }

        @Override
        public VectorSchemaRoot getVectorSchemaRoot()
        {
            return delegate.getVectorSchemaRoot();
        }

        @Override
        public long getMemoryUsage()
        {
            try {
                return delegate.getMemoryUsage();
            }
            catch (RuntimeException ignored) {
                return 0;
            }
        }

        @Override
        public void close()
        {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            RuntimeException failure = null;
            try {
                delegate.close();
            }
            catch (RuntimeException e) {
                failure = e;
            }

            try {
                afterClose.run();
            }
            catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }

            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class AdbcFlightSqlStreamOpenerFactory
            implements FlightSqlStreamOpenerFactory
    {
        private final DorisConfig config;

        private AdbcFlightSqlStreamOpenerFactory(DorisConfig config)
        {
            this.config = requireNonNull(config, "config is null");
        }

        @Override
        public FlightSqlStreamOpener create(String feHost, int flightSqlPort)
        {
            return new AdbcFlightSqlStreamOpener(config, feHost, flightSqlPort);
        }
    }

    private static final class AdbcFlightSqlStreamOpener
            implements FlightSqlStreamOpener
    {
        private final BufferAllocator allocator;
        private final AdbcDatabase database;
        private final AdbcConnection connection;
        private final AtomicBoolean closed = new AtomicBoolean();

        private AdbcFlightSqlStreamOpener(DorisConfig config, String feHost, int flightSqlPort)
        {
            requireNonNull(config, "config is null");

            BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            AdbcDatabase database = null;
            AdbcConnection connection = null;

            try {
                FlightSqlDriver driver = new FlightSqlDriver(allocator);
                Map<String, Object> parameters = new HashMap<>();
                AdbcDriver.PARAM_URI.set(parameters, Location.forGrpcInsecure(feHost, flightSqlPort).getUri().toString());
                config.getUsername().ifPresent(value -> AdbcDriver.PARAM_USERNAME.set(parameters, value));
                config.getPassword().ifPresent(value -> AdbcDriver.PARAM_PASSWORD.set(parameters, value));

                database = driver.open(parameters);
                connection = database.connect();

                this.allocator = allocator;
                this.database = database;
                this.connection = connection;
            }
            catch (AdbcException | RuntimeException e) {
                closeQuietly(connection);
                closeQuietly(database);
                closeQuietly(allocator);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to initialize Doris Flight SQL executor", e);
            }
        }

        @Override
        public DorisFlightSqlResult openStream(String sql)
        {
            if (closed.get()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Doris Flight SQL executor is closed");
            }

            AdbcStatement statement = null;
            AdbcStatement.QueryResult queryResult = null;

            try {
                statement = connection.createStatement();
                statement.setSqlQuery(sql);
                queryResult = statement.executeQuery();
                ArrowReader reader = queryResult.getReader();
                return new AdbcStreamResult(this, statement, queryResult, reader);
            }
            catch (AdbcException | RuntimeException e) {
                closeQuietly(queryResult);
                closeQuietly(statement);
                close();
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to execute Doris Flight SQL query", e);
            }
        }

        @Override
        public long getMemoryUsage()
        {
            if (closed.get()) {
                return 0;
            }

            try {
                return allocator.getAllocatedMemory();
            }
            catch (RuntimeException ignored) {
                return 0;
            }
        }

        @Override
        public void close()
        {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            Exception failure = null;
            failure = closeResource(failure, connection);
            failure = closeResource(failure, database);
            failure = closeResource(failure, allocator);
            if (failure != null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to close Doris Flight SQL executor", failure);
            }
        }
    }

    private static final class AdbcStreamResult
            implements DorisFlightSqlResult
    {
        private final FlightSqlStreamOpener opener;
        private final AdbcStatement statement;
        private final AdbcStatement.QueryResult queryResult;
        private final ArrowReader reader;

        private AdbcStreamResult(
                FlightSqlStreamOpener opener,
                AdbcStatement statement,
                AdbcStatement.QueryResult queryResult,
                ArrowReader reader)
        {
            this.opener = requireNonNull(opener, "opener is null");
            this.statement = requireNonNull(statement, "statement is null");
            this.queryResult = requireNonNull(queryResult, "queryResult is null");
            this.reader = requireNonNull(reader, "reader is null");
        }

        @Override
        public boolean loadNextBatch()
        {
            try {
                return reader.loadNextBatch();
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to load Doris Flight SQL batch", e);
            }
        }

        @Override
        public org.apache.arrow.vector.VectorSchemaRoot getVectorSchemaRoot()
        {
            try {
                return reader.getVectorSchemaRoot();
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to access Doris Flight SQL batch schema root", e);
            }
        }

        @Override
        public long getMemoryUsage()
        {
            return opener.getMemoryUsage();
        }

        @Override
        public void close()
        {
            Exception failure = null;
            failure = close(failure, reader);
            failure = close(failure, queryResult);
            failure = close(failure, statement);

            if (failure != null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to close Doris Flight SQL resources", failure);
            }
        }

        private static Exception close(Exception failure, AutoCloseable closeable)
        {
            try {
                closeable.close();
            }
            catch (Exception e) {
                if (failure == null) {
                    return e;
                }
                failure.addSuppressed(e);
            }
            return failure;
        }
    }
}
