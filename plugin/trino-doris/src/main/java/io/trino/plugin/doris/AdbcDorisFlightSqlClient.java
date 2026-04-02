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
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class AdbcDorisFlightSqlClient
        implements DorisFlightSqlClient
{
    private static final String APPLICATION_NAME_PREFIX = "/* ApplicationName=Trino Doris Flight SQL Query */ ";

    private final DorisConfig config;
    private final DorisQueryBuilder queryBuilder;
    private final DorisFlightSqlPortResolver portResolver;

    @Inject
    public AdbcDorisFlightSqlClient(DorisConfig config, DorisQueryBuilder queryBuilder, DorisFlightSqlPortResolver portResolver)
    {
        this.config = requireNonNull(config, "config is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.portResolver = requireNonNull(portResolver, "portResolver is null");
    }

    @Override
    public DorisFlightSqlResult openStream(DorisTableHandle tableHandle, DorisSplit split, List<DorisColumnHandle> columns)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        List<DorisColumnHandle> queryColumns = tableHandle.aggregations().isPresent()
                ? columns
                : tableHandle.projectedColumns().orElse(columns);

        int flightSqlPort = portResolver.resolveFlightSqlPort();
        String sql = APPLICATION_NAME_PREFIX + queryBuilder.buildSelectSql(
                tableHandle,
                queryColumns.stream()
                        .map(DorisColumnHandle::columnName)
                        .toList(),
                split.tabletIds());

        List<String> failures = new ArrayList<>();
        for (String feHost : DorisFeEndpoints.getHosts(config)) {
            try {
                return openStream(feHost, flightSqlPort, sql);
            }
            catch (RuntimeException e) {
                failures.add("%s:%s -> %s".formatted(feHost, flightSqlPort, e.getMessage()));
            }
        }

        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open Doris Flight SQL stream: " + failures);
    }

    private DorisFlightSqlResult openStream(String feHost, int flightSqlPort, String sql)
    {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FlightSqlDriver driver = null;
        AdbcDatabase database = null;
        AdbcConnection connection = null;
        AdbcStatement statement = null;
        AdbcStatement.QueryResult queryResult = null;

        try {
            driver = new FlightSqlDriver(allocator);

            Map<String, Object> parameters = new HashMap<>();
            AdbcDriver.PARAM_URI.set(parameters, Location.forGrpcInsecure(feHost, flightSqlPort).getUri().toString());
            config.getUsername().ifPresent(value -> AdbcDriver.PARAM_USERNAME.set(parameters, value));
            config.getPassword().ifPresent(value -> AdbcDriver.PARAM_PASSWORD.set(parameters, value));

            database = driver.open(parameters);
            connection = database.connect();
            statement = connection.createStatement();
            statement.setSqlQuery(sql);
            queryResult = statement.executeQuery();
            ArrowReader reader = queryResult.getReader();

            return new AdbcDorisFlightSqlResult(allocator, database, connection, statement, queryResult, reader);
        }
        catch (AdbcException | RuntimeException e) {
            closeQuietly(queryResult);
            closeQuietly(statement);
            closeQuietly(connection);
            closeQuietly(database);
            closeQuietly(allocator);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to execute Doris Flight SQL query", e);
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

    private static final class AdbcDorisFlightSqlResult
            implements DorisFlightSqlResult
    {
        private final BufferAllocator allocator;
        private final AdbcDatabase database;
        private final AdbcConnection connection;
        private final AdbcStatement statement;
        private final AdbcStatement.QueryResult queryResult;
        private final ArrowReader reader;

        private AdbcDorisFlightSqlResult(
                BufferAllocator allocator,
                AdbcDatabase database,
                AdbcConnection connection,
                AdbcStatement statement,
                AdbcStatement.QueryResult queryResult,
                ArrowReader reader)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.database = requireNonNull(database, "database is null");
            this.connection = requireNonNull(connection, "connection is null");
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
            return allocator.getAllocatedMemory();
        }

        @Override
        public void close()
        {
            Exception failure = null;
            failure = close(failure, reader);
            failure = close(failure, queryResult);
            failure = close(failure, statement);
            failure = close(failure, connection);
            failure = close(failure, database);
            failure = close(failure, allocator);

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
