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
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcPageSink;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ConnectorSession;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestXCloudPageSink
{
    @Test
    public void testBatchFinishAndAbort()
            throws Exception
    {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        JdbcPageSink sink = sink(connection);
        sink.appendPage(page());
        verify(statement, times(2)).executeBatch();
        sink.finish().join();
        verify(statement, times(3)).executeBatch();
        verify(connection, times(3)).commit();
        verify(statement).close();
        verify(connection).close();
        reset(connection, statement);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        sink(connection).abort();
        // The framework calls rollback, but XCloud's implementation supplies no remote undo.
        verify(connection).rollback();
        verify(connection).close();
    }

    @Test
    public void testBatchFailureNeverReplayed()
            throws Exception
    {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeBatch()).thenThrow(new SQLException("batch-failure", "08006"));
        JdbcPageSink sink = sink(connection);
        assertThatThrownBy(() -> sink.appendPage(page())).hasRootCauseMessage("batch-failure");
        verify(statement, times(1)).executeBatch();
        verify(connection, never()).commit();
        sink.abort();
        verify(connection).close();
    }

    private static Page page()
    {
        return new Page(new LongArrayBlock(5, Optional.empty(), new long[] {1, 2, 3, 4, 5}));
    }

    private static JdbcPageSink sink(Connection connection)
    {
        ConnectorSession session = mock(ConnectorSession.class);
        when(session.getProperty("write_batch_size", Integer.class)).thenReturn(2);
        XCloudClient client = new XCloudClient(new BaseJdbcConfig(), _ -> connection, new XCloudQueryBuilder(RemoteQueryModifier.NONE), mock(IdentifierMapping.class), RemoteQueryModifier.NONE);
        JdbcOutputTableHandle handle = new JdbcOutputTableHandle(new RemoteTableName(Optional.of("Empty"), Optional.of("s"), "t"), List.of("v"), List.of(BIGINT), Optional.empty(), Optional.of("tmp_trino_test"), Optional.empty());
        return new JdbcPageSink(session, handle, client, () -> 1, RemoteQueryModifier.NONE, JdbcClient::buildInsertSql);
    }
}
