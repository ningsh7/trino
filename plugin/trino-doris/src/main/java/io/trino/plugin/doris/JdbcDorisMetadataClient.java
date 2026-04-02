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
import io.trino.spi.connector.SchemaTableName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class JdbcDorisMetadataClient
        implements DorisMetadataClient
{
    private static final String LIST_SCHEMAS_SQL = """
            SELECT SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME <> 'information_schema'
            ORDER BY SCHEMA_NAME
            """;
    private static final String LIST_ALL_TABLES_SQL = """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA <> 'information_schema'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
    private static final String LIST_TABLES_IN_SCHEMA_SQL = """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ?
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
    private static final String LIST_COLUMNS_SQL = """
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_SIZE, DECIMAL_DIGITS, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """;
    private static final String TABLE_ROW_COUNT_SQL = """
            SELECT TABLE_ROWS
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """;

    private final DorisJdbcConnectionFactory connectionFactory;

    @Inject
    public JdbcDorisMetadataClient(DorisJdbcConnectionFactory connectionFactory)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
    }

    @Override
    public List<String> listSchemaNames()
    {
        try (Connection connection = connectionFactory.openConnection();
                PreparedStatement statement = connection.prepareStatement(LIST_SCHEMAS_SQL);
                ResultSet resultSet = statement.executeQuery()) {
            List<String> schemas = new ArrayList<>();
            while (resultSet.next()) {
                schemas.add(resultSet.getString("SCHEMA_NAME"));
            }
            return List.copyOf(schemas);
        }
        catch (SQLException e) {
            throw DorisJdbcConnectionFactory.jdbcOperationFailed("Failed to list Doris schemas", e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(Optional<String> schemaName)
    {
        try (Connection connection = connectionFactory.openConnection();
                PreparedStatement statement = connection.prepareStatement(schemaName.isPresent() ? LIST_TABLES_IN_SCHEMA_SQL : LIST_ALL_TABLES_SQL)) {
            if (schemaName.isPresent()) {
                statement.setString(1, schemaName.get());
            }

            try (ResultSet resultSet = statement.executeQuery()) {
                List<SchemaTableName> tables = new ArrayList<>();
                while (resultSet.next()) {
                    tables.add(new SchemaTableName(
                            resultSet.getString("TABLE_SCHEMA"),
                            resultSet.getString("TABLE_NAME")));
                }
                return List.copyOf(tables);
            }
        }
        catch (SQLException e) {
            throw DorisJdbcConnectionFactory.jdbcOperationFailed("Failed to list Doris tables", e);
        }
    }

    @Override
    public Optional<DorisRemoteTable> getTable(SchemaTableName tableName)
    {
        List<DorisRemoteColumn> columns = loadColumns(tableName);
        if (columns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new DorisRemoteTable(tableName, columns));
    }

    @Override
    public OptionalLong getTableRowCount(SchemaTableName tableName)
    {
        try (Connection connection = connectionFactory.openConnection();
                PreparedStatement statement = connection.prepareStatement(TABLE_ROW_COUNT_SQL)) {
            statement.setString(1, tableName.getSchemaName());
            statement.setString(2, tableName.getTableName());

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return OptionalLong.empty();
                }

                long rowCount = resultSet.getLong("TABLE_ROWS");
                if (resultSet.wasNull()) {
                    return OptionalLong.empty();
                }
                return OptionalLong.of(Math.max(0L, rowCount));
            }
        }
        catch (SQLException e) {
            throw DorisJdbcConnectionFactory.jdbcOperationFailed("Failed to load Doris table row count for table '%s'".formatted(tableName), e);
        }
    }

    private List<DorisRemoteColumn> loadColumns(SchemaTableName tableName)
    {
        try (Connection connection = connectionFactory.openConnection();
                PreparedStatement statement = connection.prepareStatement(LIST_COLUMNS_SQL)) {
            statement.setString(1, tableName.getSchemaName());
            statement.setString(2, tableName.getTableName());

            try (ResultSet resultSet = statement.executeQuery()) {
                List<DorisRemoteColumn> columns = new ArrayList<>();
                while (resultSet.next()) {
                    columns.add(new DorisRemoteColumn(
                            resultSet.getString("COLUMN_NAME"),
                            resultSet.getString("DATA_TYPE"),
                            getOptionalInt(resultSet, "COLUMN_SIZE"),
                            getOptionalInt(resultSet, "DECIMAL_DIGITS"),
                            resultSet.getInt("ORDINAL_POSITION")));
                }
                return List.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw DorisJdbcConnectionFactory.jdbcOperationFailed("Failed to load Doris columns for table '%s'".formatted(tableName), e);
        }
    }

    private static Optional<Integer> getOptionalInt(ResultSet resultSet, String columnLabel)
            throws SQLException
    {
        int value = resultSet.getInt(columnLabel);
        if (resultSet.wasNull()) {
            return Optional.empty();
        }
        return Optional.of(value);
    }
}

