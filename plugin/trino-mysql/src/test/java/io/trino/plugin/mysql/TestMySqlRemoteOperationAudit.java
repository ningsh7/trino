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
package io.trino.plugin.mysql;

import io.trino.plugin.base.audit.RemoteOperationAuditConfig;
import io.trino.plugin.base.audit.RemoteOperationAuditContext;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent;
import io.trino.plugin.base.audit.RemoteOperationAuditStats;
import io.trino.plugin.base.audit.RemoteOperationAuditor;
import io.trino.plugin.base.audit.jdbc.AuditingJdbc;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.audit.RemoteOperationAuditEvent.Operation.COMMIT;
import static io.trino.plugin.base.audit.RemoteOperationAuditEvent.StatementKind.CALLABLE_TEMPLATE;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestMySqlRemoteOperationAudit
{
    @Test
    void testStreamingPreparedBatchAndProcedure()
            throws Exception
    {
        List<RemoteOperationAuditEvent> events = new ArrayList<>();
        var auditor = new RemoteOperationAuditor(
                new RemoteOperationAuditConfig().setEnabled(true),
                events::add,
                new RemoteOperationAuditStats(),
                "company_mysql",
                "mysql",
                "worker");
        MySqlClient client = new MySqlClient(
                new BaseJdbcConfig(),
                new JdbcStatisticsConfig(),
                _ -> { throw new UnsupportedOperationException(); },
                new DefaultQueryBuilder(RemoteQueryModifier.NONE),
                TESTING_TYPE_MANAGER,
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE);

        try (TestingMySqlServer server = new TestingMySqlServer();
                Connection physical = server.createConnection();
                Connection connection = AuditingJdbc.wrap(physical, new RemoteOperationAuditContext("mysql_query", "trace", "alice", "web"), auditor)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE audit_test (id INTEGER)");
                statement.execute("CREATE PROCEDURE audit_procedure() SELECT 17");
            }
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO audit_test VALUES (?)")) {
                statement.setInt(1, 1234567);
                statement.addBatch();
                assertThat(statement.executeBatch()).containsExactly(1);
            }
            connection.commit();
            try (PreparedStatement statement = client.getPreparedStatement(connection, "SELECT id FROM audit_test", Optional.empty());
                    ResultSet rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(1234567);
                assertThat(rows.next()).isFalse();
            }
            try (var statement = connection.prepareCall("{call audit_procedure()}");
                    var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(17);
            }
            assertThat(events).hasSize(6);
            assertThat(events).allSatisfy(event -> {
                assertThat(event.executionUser()).startsWith(server.getUsername());
                assertThat(event.trinoUser()).isEqualTo("alice");
                assertThat(event.queryId()).isEqualTo("mysql_query");
                assertThat(event.toString()).doesNotContain("1234567");
            });
            assertThat(events.get(3).operation()).isEqualTo(COMMIT);
            assertThat(events.getLast().statementKind()).isEqualTo(CALLABLE_TEMPLATE);
        }
    }
}
