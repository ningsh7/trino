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
package io.trino.plugin.xcloud;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.Type;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.isNonTransactionalInsert;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.stream.Collectors.joining;

public class XCloudClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(XCloudClient.class);

    private static final Pattern DECIMAL_DECLARATION = Pattern.compile("(NUMBER|DECIMAL|NUMERIC)\\s*\\(\\s*([0-9]{1,2}|\\*)\\s*(?:,\\s*([+-]?[0-9]{1,3})\\s*)?\\)");
    private static final Pattern TIMESTAMP_DECLARATION = Pattern.compile("TIMESTAMP\\s*\\(\\s*([0-9])\\s*\\)");

    @Inject
    public XCloudClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, IdentifierMapping identifierMapping, RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        try (PreparedStatement statement = connection.prepareStatement("SELECT schema_name FROM v$user_schemas");
                ResultSet result = statement.executeQuery()) {
            java.util.ArrayList<String> schemas = new java.util.ArrayList<>();
            while (result.next()) {
                String schema = result.getString(1);
                if (filterRemoteSchema(schema)) {
                    schemas.add(schema);
                }
            }
            return List.copyOf(schemas);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schema, Optional<String> table)
            throws SQLException
    {
        // A single URL selects one database. Normalize TABLE_CAT in both metadata queries.
        String sql = "SELECT 'Empty' AS TABLE_CAT, schema_name AS TABLE_SCHEM, table_name AS TABLE_NAME, table_type AS TABLE_TYPE, table_comment AS REMARKS FROM v$user_tables WHERE 1=1";
        if (schema.isPresent()) {
            sql += " AND schema_name = ?";
        }
        if (table.isPresent()) {
            sql += " AND table_name = ?";
        }
        return metadata(connection, sql, java.util.stream.Stream.of(schema, table).flatMap(Optional::stream).toList());
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, SchemaTableName name, RemoteTableName table)
    {
        try (Connection connection = connectionFactory.openConnection(session);
                ResultSet rows = metadata(
                        connection,
                        "SELECT 'Empty' AS TABLE_CAT, schema_name AS TABLE_SCHEM, table_name AS TABLE_NAME, column_name AS COLUMN_NAME, jdbc_data_type AS DATA_TYPE, data_type AS TYPE_NAME, data_size AS COLUMN_SIZE, nullable AS NULLABLE, column_comment AS REMARKS, ordinal_position AS ORDINAL_POSITION FROM v$user_tab_columns WHERE schema_name = ? AND table_name = ? ORDER BY ordinal_position",
                        List.of(table.getSchemaName().orElseThrow(), table.getTableName()))) {
            java.util.ArrayList<JdbcColumnHandle> columns = new java.util.ArrayList<>();
            while (rows.next()) {
                JdbcTypeHandle handle = catalogType(rows.getInt("DATA_TYPE"), rows.getString("TYPE_NAME"), getInteger(rows, "COLUMN_SIZE"));
                Optional<ColumnMapping> mapping = toColumnMapping(session, connection, handle);
                if (mapping.isPresent()) {
                    columns.add(JdbcColumnHandle.builder().setColumnName(rows.getString("COLUMN_NAME")).setJdbcTypeHandle(handle)
                            .setColumnType(mapping.orElseThrow().getType()).setNullable(rows.getInt("NULLABLE") != java.sql.DatabaseMetaData.columnNoNulls)
                            .setComment(Optional.ofNullable(rows.getString("REMARKS")).filter(comment -> !comment.isEmpty())).build());
                }
            }
            if (columns.isEmpty()) {
                throw new TableNotFoundException(name, "Table has no supported columns");
            }
            return List.copyOf(columns);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    // The catalog reports storage bytes and a placeholder scale. Precision is part of the native declaration.
    static JdbcTypeHandle catalogType(int jdbcType, String declaration, Optional<Integer> storageSize)
    {
        String name = declaration == null ? "" : declaration.strip().toUpperCase(Locale.ROOT);
        Optional<Integer> size = storageSize;
        Optional<Integer> scale = Optional.empty();
        Matcher decimal = DECIMAL_DECLARATION.matcher(name);
        Matcher timestamp = TIMESTAMP_DECLARATION.matcher(name);
        if (decimal.matches()) {
            name = decimal.group(1);
            size = Optional.of(decimal.group(2).equals("*") ? 38 : Integer.parseInt(decimal.group(2)));
            scale = Optional.of(decimal.group(3) == null ? 0 : Integer.parseInt(decimal.group(3)));
        }
        else if (timestamp.matches()) {
            name = "TIMESTAMP";
            scale = Optional.of(Integer.parseInt(timestamp.group(1)));
        }
        return new JdbcTypeHandle(jdbcType, name.isEmpty() ? Optional.empty() : Optional.of(name), size, scale, Optional.empty(), Optional.empty());
    }

    private static CachedRowSet metadata(Connection connection, String sql, List<String> parameters)
            throws SQLException
    {
        // Only metadata is buffered. Disconnected rows give the caller normal ResultSet ownership without leaking statements.
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < parameters.size(); index++) {
                statement.setString(index + 1, parameters.get(index));
            }
            try (ResultSet result = statement.executeQuery()) {
                CachedRowSet rows = RowSetProvider.newFactory().createCachedRowSet();
                // CachedRowSet.populate calls optional metadata APIs absent from this driver.
                // Copy only metadata strings through implemented getters; getInt on the disconnected rows parses numeric fields.
                java.sql.ResultSetMetaData resultMetadata = result.getMetaData();
                javax.sql.rowset.RowSetMetaDataImpl columns = new javax.sql.rowset.RowSetMetaDataImpl();
                int count = resultMetadata.getColumnCount();
                columns.setColumnCount(count);
                for (int index = 1; index <= count; index++) {
                    columns.setColumnName(index, resultMetadata.getColumnLabel(index));
                    columns.setColumnLabel(index, resultMetadata.getColumnLabel(index));
                    columns.setColumnType(index, java.sql.Types.VARCHAR);
                }
                rows.setMetaData(columns);
                while (result.next()) {
                    rows.moveToInsertRow();
                    for (int index = 1; index <= count; index++) {
                        String value = result.getString(index);
                        if (value == null) {
                            rows.updateNull(index);
                        }
                        else {
                            rows.updateString(index, value);
                        }
                    }
                    rows.insertRow();
                    rows.moveToCurrentRow();
                    rows.last();
                }
                rows.beforeFirst();
                return rows;
            }
        }
    }

    @Override
    protected String quoted(String catalog, String schema, String table)
    {
        return (schema == null || schema.isEmpty() ? "" : relationIdentifier(schema) + ".") + relationIdentifier(table);
    }

    static String relationIdentifier(String name)
    {
        // XCloud accepts quoted columns, but rejects quoted schema/table names in SELECT and DDL.
        // Keep IdentifierMapping's remote spelling; reject punctuation instead of interpolating unsafe SQL.
        if (!name.matches("[\\p{L}_][\\p{L}\\p{N}_#]*")) {
            throw new TrinoException(NOT_SUPPORTED, "XCloud schema/table name cannot be expressed without quotes: " + name);
        }
        return name;
    }

    @Override
    protected OptionalInt getMaxColumnNameLengthFromDatabaseMetaData(ConnectorSession session)
    {
        // The server enforces the 64 BYTE limit in its configured encoding, not a Java character count.
        return OptionalInt.empty();
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle handle)
    {
        return getForcedMappingToVarchar(handle).or(() -> XCloudTypeMappings.columnMapping(session, handle));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return XCloudTypeMappings.writeMapping(type);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalog, String schema, String table, String newSchema, String newTable)
            throws SQLException
    {
        if (!Objects.equals(schema, newSchema)) {
            throw new TrinoException(NOT_SUPPORTED, "XCloud does not support moving tables between schemas");
        }
        execute(session, connection, "ALTER TABLE " + quoted(catalog, schema, table) + " RENAME TO " + relationIdentifier(newTable));
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        if (isNonTransactionalInsert(session)) {
            return;
        }
        checkArgument(handle.getPageSinkIdColumnName().isEmpty(), "XCloud does not support write retries");
        RemoteTableName temporary = new RemoteTableName(handle.getRemoteTableName().getCatalogName(), handle.getRemoteTableName().getSchemaName(), handle.getTemporaryTableName().orElseThrow());
        RuntimeException failure = null;
        try (Connection connection = getConnection(session, handle)) {
            String columns = handle.getColumnNames().stream().map(this::quoted).collect(joining(", "));
            log.debug("queryId=%s phase=final-copy source=%s target=%s", session.getQueryId(), temporary, handle.getRemoteTableName());
            execute(session, connection, "INSERT INTO " + quoted(handle.getRemoteTableName()) + " (" + columns + ") SELECT " + columns + " FROM " + quoted(temporary));
        }
        catch (SQLException e) {
            failure = new TrinoException(JDBC_ERROR, "XCloud final INSERT failed; outcome must be checked before any manual replay", e);
            throw failure;
        }
        catch (RuntimeException e) {
            failure = e;
            throw e;
        }
        finally {
            try {
                dropTable(session, temporary, true);
            }
            catch (RuntimeException cleanup) {
                log.warn("queryId=%s phase=cleanup table=%s exception=%s", session.getQueryId(), temporary, cleanup.getClass().getName());
                if (failure != null) {
                    failure.addSuppressed(cleanup);
                }
                else {
                    throw cleanup;
                }
            }
        }
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        return modify(session, handle, false);
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        return modify(session, handle, true);
    }

    private OptionalLong modify(ConnectorSession session, JdbcTableHandle handle, boolean update)
    {
        checkArgument(handle.isNamedRelation() && handle.getLimit().isEmpty() && handle.getSortOrder().isEmpty() && handle.getAuthorization().isEmpty(), "Invalid XCloud DML handle");
        checkArgument(update != handle.getUpdateAssignments().isEmpty(), "Invalid XCloud DML assignments");
        try (Connection connection = connectionFactory.openConnection(session)) {
            PreparedQuery query = update
                    ? queryBuilder.prepareUpdateQuery(this, session, connection, handle.getRequiredNamedRelation(), handle.getConstraint(), getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()), handle.getUpdateAssignments())
                    : queryBuilder.prepareDeleteQuery(this, session, connection, handle.getRequiredNamedRelation(), handle.getConstraint(), getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()));
            try (PreparedStatement statement = queryBuilder.prepareStatement(this, session, connection, query, Optional.empty())) {
                return updateCount(statement.executeUpdate());
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "XCloud DML failed; outcome must be checked before any manual replay", e);
        }
    }

    static OptionalLong updateCount(int count)
    {
        // executeUpdate does not consume getUpdateCount's segmented counter. A negative count means unknown, never a negative affected-row total.
        return count < 0 ? OptionalLong.empty() : OptionalLong.of(count);
    }
}
