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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class DorisJdbcConnectionFactory
        implements DorisQueryEventListener
{
    private final Driver driver;
    private final String jdbcUrl;
    private final Properties connectionProperties;
    private final Map<String, Connection> queryConnections = new ConcurrentHashMap<>();
    private final java.util.Set<String> activeQueries = ConcurrentHashMap.newKeySet();

    @Inject
    public DorisJdbcConnectionFactory(DorisConfig config)
    {
        this(
                createDriver(),
                requireNonNull(config, "config is null").getJdbcUrl()
                        .orElseThrow(() -> new IllegalArgumentException("doris.jdbc-url must be set for Doris FE JDBC access")),
                createConnectionProperties(config));
    }

    DorisJdbcConnectionFactory(Driver driver, String jdbcUrl, Properties connectionProperties)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.jdbcUrl = requireNonNull(jdbcUrl, "jdbcUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "connectionProperties is null"));
    }

    public Connection openConnection()
            throws SQLException
    {
        return openPhysicalConnection();
    }

    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        if (isNull(session)) {
            return openConnection();
        }

        String queryId = session.getQueryId();
        activeQueries.add(queryId);

        Connection cachedConnection = queryConnections.remove(queryId);
        if (cachedConnection != null) {
            if (!cachedConnection.isClosed()) {
                return new CachedConnection(queryId, cachedConnection);
            }
            closeQuietly(cachedConnection);
        }

        return new CachedConnection(queryId, openPhysicalConnection());
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        activeQueries.add(session.getQueryId());
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        String queryId = session.getQueryId();
        activeQueries.remove(queryId);
        closeQuietly(queryConnections.remove(queryId));
    }

    @PreDestroy
    public void close()
    {
        activeQueries.clear();
        queryConnections.values().forEach(DorisJdbcConnectionFactory::closeQuietly);
        queryConnections.clear();
    }

    private Connection openPhysicalConnection()
            throws SQLException
    {
        // Using the driver directly avoids plugin classloader registration issues with DriverManager.
        Properties properties = new Properties();
        properties.putAll(connectionProperties);

        Connection connection = driver.connect(jdbcUrl, properties);
        if (connection == null) {
            throw new SQLException("MySQL driver refused Doris JDBC URL: " + jdbcUrl);
        }
        connection.setReadOnly(true);
        return connection;
    }

    private static Driver createDriver()
    {
        try {
            return new com.mysql.cj.jdbc.Driver();
        }
        catch (SQLException e) {
            throw jdbcOperationFailed("Failed to initialize MySQL driver for Doris FE JDBC access", e);
        }
    }

    static TrinoException jdbcOperationFailed(String message, SQLException cause)
    {
        return new TrinoException(GENERIC_INTERNAL_ERROR, message, cause);
    }

    private static Properties createConnectionProperties(DorisConfig config)
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("useInformationSchema", "true");
        config.getUsername().ifPresent(value -> connectionProperties.setProperty("user", value));
        config.getPassword().ifPresent(value -> connectionProperties.setProperty("password", value));
        return connectionProperties;
    }

    private static void closeQuietly(Connection connection)
    {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        }
        catch (SQLException | RuntimeException ignored) {
        }
    }

    final class CachedConnection
            extends ForwardingConnection
    {
        private final String queryId;
        private final Connection delegate;
        private volatile boolean closed;
        private volatile boolean dirty;

        private CachedConnection(String queryId, Connection delegate)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        protected Connection delegate()
        {
            if (closed) {
                throw new IllegalStateException("Connection is already closed");
            }
            return delegate;
        }

        @Override
        public void setAutoCommit(boolean autoCommit)
                throws SQLException
        {
            dirty = true;
            super.setAutoCommit(autoCommit);
        }

        @Override
        public void setReadOnly(boolean readOnly)
                throws SQLException
        {
            dirty = true;
            super.setReadOnly(readOnly);
        }

        @Override
        public boolean isClosed()
                throws SQLException
        {
            return closed || delegate.isClosed();
        }

        @Override
        public void close()
                throws SQLException
        {
            if (closed) {
                return;
            }
            closed = true;

            if (dirty || delegate.isClosed() || !activeQueries.contains(queryId)) {
                delegate.close();
                return;
            }

            Connection previousConnection = queryConnections.putIfAbsent(queryId, delegate);
            if (previousConnection != null) {
                delegate.close();
            }
        }
    }
}
