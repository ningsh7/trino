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
import com.mysql.cj.jdbc.Driver;
import io.trino.spi.TrinoException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class DorisJdbcConnectionFactory
{
    private final Driver driver;
    private final String jdbcUrl;
    private final Properties connectionProperties;

    @Inject
    public DorisJdbcConnectionFactory(DorisConfig config)
    {
        requireNonNull(config, "config is null");

        this.driver = createDriver();
        this.jdbcUrl = config.getJdbcUrl()
                .orElseThrow(() -> new IllegalArgumentException("doris.jdbc-url must be set for Doris FE JDBC access"));
        this.connectionProperties = new Properties();
        connectionProperties.setProperty("useInformationSchema", "true");
        config.getUsername().ifPresent(value -> connectionProperties.setProperty("user", value));
        config.getPassword().ifPresent(value -> connectionProperties.setProperty("password", value));
    }

    public Connection openConnection()
            throws SQLException
    {
        // Using the driver directly avoids plugin classloader registration issues with DriverManager.
        Properties properties = new Properties();
        properties.putAll(connectionProperties);

        Connection connection = driver.connect(jdbcUrl, properties);
        if (connection == null) {
            throw new SQLException("MySQL driver refused Doris JDBC URL: " + jdbcUrl);
        }
        return connection;
    }

    private static Driver createDriver()
    {
        try {
            return new Driver();
        }
        catch (SQLException e) {
            throw jdbcOperationFailed("Failed to initialize MySQL driver for Doris FE JDBC access", e);
        }
    }

    static TrinoException jdbcOperationFailed(String message, SQLException cause)
    {
        return new TrinoException(GENERIC_INTERNAL_ERROR, message, cause);
    }
}
