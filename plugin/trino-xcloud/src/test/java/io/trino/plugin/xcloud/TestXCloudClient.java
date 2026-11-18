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

import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.JdbcAssignmentItem;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestXCloudClient
{
    private static final ConnectorSession SESSION = testSessionBuilder().build().toConnectorSession();

    private XCloudClient client(Connection connection)
    {
        return new XCloudClient(new BaseJdbcConfig(), _ -> connection, new XCloudQueryBuilder(RemoteQueryModifier.NONE), mock(IdentifierMapping.class), RemoteQueryModifier.NONE);
    }

    @Test
    public void testSqlAndCapabilities()
    {
        XCloudClient client = client(mock(Connection.class));
        assertThat(client.quoted(new RemoteTableName(Optional.of("Empty"), Optional.of("SCHEMA_1"), "TABLE#1"))).isEqualTo("SCHEMA_1.TABLE#1");
        assertThat(client.quoted("a\"b")).isEqualTo("\"a\"\"b\"");
        assertThat(client.quoted(new RemoteTableName(Optional.empty(), Optional.of("中文模式"), "中文表"))).isEqualTo("中文模式.中文表");
        for (String name : List.of("a.b", "a;DROP TABLE x", "a\"b", "a b", "%", "1table")) {
            assertThatThrownBy(() -> client.quoted(new RemoteTableName(Optional.empty(), Optional.of("s"), name))).hasMessageContaining("cannot be expressed without quotes");
        }
        assertThat(client.limitFunction().orElseThrow().apply("SELECT 1", 7L)).isEqualTo("SELECT 1 LIMIT 7");
        assertThat(client.supportsRetries()).isFalse();
        assertThat(client.supportsMerge()).isFalse();
        for (int count : new int[] {0, 1, Integer.MAX_VALUE}) {
            assertThat(XCloudClient.updateCount(count)).hasValue(count);
        }
        for (int count : new int[] {-1, -2, -3}) {
            assertThat(XCloudClient.updateCount(count)).isEmpty();
        }
    }

    @Test
    public void testCatalogColumnDiscoveryWithNativeDeclarations()
            throws Exception
    {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet result = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(result);
        when(result.getMetaData()).thenReturn(metadata);
        List<String> labels = List.of("COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "NULLABLE", "REMARKS");
        when(metadata.getColumnCount()).thenReturn(labels.size());
        for (int index = 0; index < labels.size(); index++) {
            when(metadata.getColumnLabel(index + 1)).thenReturn(labels.get(index));
        }
        // Native 2.14.7 catalog rows: NUMBER has JDBC code 0; sizes are bytes and DECIMAL_DIGITS is 0.
        when(result.next()).thenReturn(true, true, true, true, true, false);
        when(result.getString(1)).thenReturn("I", "N", "D", "T", "V");
        when(result.getString(2)).thenReturn("4", "0", "93", "93", "0");
        when(result.getString(3)).thenReturn("INT", "NUMBER(38,9)", "TIMESTAMP(0)", "TIMESTAMP(9)", "CLOB");
        when(result.getString(4)).thenReturn("4", "24", "12", "12", "4136");
        when(result.getString(5)).thenReturn("1");

        List<JdbcColumnHandle> columns = client(connection).getColumns(SESSION, new SchemaTableName("s", "t"), new RemoteTableName(Optional.of("Empty"), Optional.of("S"), "T"));
        assertThat(columns).extracting(JdbcColumnHandle::getColumnName).containsExactly("I", "N", "D", "T", "V");
        assertThat(columns).extracting(column -> column.getColumnType().getDisplayName())
                .containsExactly("integer", "decimal(38,9)", "timestamp(0)", "timestamp(9)", "varchar");
        verify(statement).setString(1, "S");
        verify(statement).setString(2, "T");
    }

    @Test
    public void testMetadataExactBindingsAndOwnership()
            throws Exception
    {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet result = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(result);
        when(result.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnLabel(1)).thenReturn("TABLE_CAT");
        when(result.next()).thenReturn(true, false);
        when(result.getString(1)).thenReturn("Empty");
        try (ResultSet rows = client(connection).getTables(connection, Optional.of("s%_'中文"), Optional.of("t_\""))) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString("TABLE_CAT")).isEqualTo("Empty");
        }
        verify(statement).setString(1, "s%_'中文");
        verify(statement).setString(2, "t_\"");
        verify(statement).close();
        verify(result).close();
        verify(connection, never()).getMetaData();
    }

    @Test
    public void testNullUpdateUsesLiteral()
            throws Exception
    {
        XCloudClient client = client(mock(Connection.class));
        JdbcColumnHandle column = JdbcColumnHandle.builder().setColumnName("v").setJdbcTypeHandle(TestXCloudTypes.handle("INT", 10, 0)).setColumnType(IntegerType.INTEGER).build();
        PreparedQuery query = new XCloudQueryBuilder(RemoteQueryModifier.NONE).prepareUpdateQuery(
                client,
                SESSION,
                mock(Connection.class),
                new JdbcNamedRelationHandle(new io.trino.spi.connector.SchemaTableName("s", "t"), new RemoteTableName(Optional.of("Empty"), Optional.of("s"), "t"), Optional.empty()),
                TupleDomain.<ColumnHandle>all(),
                Optional.empty(),
                List.of(new JdbcAssignmentItem(column, new QueryParameter(IntegerType.INTEGER, Optional.empty()))));
        assertThat(query.query()).isEqualTo("UPDATE s.t SET \"v\" = NULL");
        assertThat(query.parameters()).isEmpty();
    }

    @Test
    public void testTemporalUpdateBinding()
            throws Exception
    {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        XCloudClient client = client(connection);
        XCloudQueryBuilder builder = new XCloudQueryBuilder(RemoteQueryModifier.NONE);
        io.trino.spi.type.TimestampType type = io.trino.spi.type.TimestampType.createTimestampType(9);
        java.time.LocalDateTime local = java.time.LocalDateTime.parse("1969-12-31T23:59:59.123456789");
        JdbcColumnHandle timestamp = JdbcColumnHandle.builder().setColumnName("T").setJdbcTypeHandle(TestXCloudTypes.handle("TIMESTAMP", 29, 9)).setColumnType(type).build();
        JdbcColumnHandle date = JdbcColumnHandle.builder().setColumnName("D").setJdbcTypeHandle(TestXCloudTypes.handle("DATE", 7, 0)).setColumnType(io.trino.spi.type.TimestampType.createTimestampType(0)).build();
        PreparedQuery query = builder.prepareUpdateQuery(
                client,
                SESSION,
                connection,
                new JdbcNamedRelationHandle(new io.trino.spi.connector.SchemaTableName("s", "t"), new RemoteTableName(Optional.of("Empty"), Optional.of("S"), "T"), Optional.empty()),
                TupleDomain.<ColumnHandle>all(),
                Optional.empty(),
                List.of(
                        new JdbcAssignmentItem(timestamp, new QueryParameter(type, Optional.of(io.trino.plugin.jdbc.StandardColumnMappings.toLongTrinoTimestamp(type, local)))),
                        new JdbcAssignmentItem(date, new QueryParameter(date.getColumnType(), Optional.of(123000000L)))));
        assertThat(query.query()).isEqualTo("UPDATE S.T SET \"T\" = CAST(? AS TIMESTAMP(9)), \"D\" = ?");
        try (PreparedStatement ignored = builder.prepareStatement(client, SESSION, connection, query, Optional.empty())) {
            verify(statement).setString(1, "1969-12-31 23:59:59.123456789");
            verify(statement).setString(2, "1970-01-01 00:02:03");
            verify(statement, never()).setTimestamp(org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.any());
        }
    }

    @Test
    public void testFinalCopyPreservesOriginalFailure()
            throws Exception
    {
        Connection connection = mock(Connection.class);
        java.sql.Statement statement = mock(java.sql.Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        when(connection.getAutoCommit()).thenReturn(true);
        when(statement.execute(anyString())).thenThrow(new SQLException("original-copy-failure"), new SQLException("cleanup-failure"));
        XCloudClient client = client(connection);
        JdbcOutputTableHandle handle = new JdbcOutputTableHandle(new RemoteTableName(Optional.of("Empty"), Optional.of("s"), "t"), List.of("i"), List.of(IntegerType.INTEGER), Optional.empty(), Optional.of("tmp_trino_test"), Optional.empty());
        // Use a session with the framework's write properties rather than a fake database.
        ConnectorSession session = mock(ConnectorSession.class);
        when(session.getProperty("non_transactional_insert", Boolean.class)).thenReturn(false);
        when(session.getQueryId()).thenReturn("test_query");
        assertThatThrownBy(() -> client.finishInsertTable(session, handle, Set.of())).hasMessageContaining("outcome must be checked").hasRootCauseMessage("original-copy-failure")
                .satisfies(error -> assertThat(error.getSuppressed()).hasSize(1));
        verify(statement, times(2)).execute(anyString());
    }
}
