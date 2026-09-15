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
package io.trino.plugin.base.audit;

import io.trino.plugin.base.audit.RemoteOperationAuditEvent.Operation;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.Origin;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.Protocol;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.Result;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.StatementKind;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.base.audit.RemoteOperationAuditConfig.StatementMode.HASH;
import static io.trino.plugin.base.audit.RemoteOperationAuditConfig.StatementMode.NONE;
import static io.trino.plugin.base.audit.RemoteOperationAuditConfig.StatementMode.TEMPLATE;
import static org.assertj.core.api.Assertions.assertThat;

class TestRemoteOperationAuditor
{
    private static final RemoteOperationAuditContext CONTEXT = new RemoteOperationAuditContext("query-1", "trace-1", "alice", "web");
    private static final RemoteOperationAuditTarget TARGET = new RemoteOperationAuditTarget(Protocol.JDBC, "connection-1", "service", "db", "schema", null, false);

    @Test
    void testCorrelatableJsonAndSingleCompletion()
    {
        List<String> lines = new ArrayList<>();
        var codec = jsonCodec(RemoteOperationAuditEvent.class);
        var stats = new RemoteOperationAuditStats();
        var auditor = new RemoteOperationAuditor(
                new RemoteOperationAuditConfig().setEnabled(true),
                new JsonLogRemoteOperationAuditSink(codec, lines::add),
                stats,
                "company_mysql",
                "mysql",
                "worker-1");
        var scope = auditor.begin(CONTEXT, () -> TARGET, Operation.EXECUTE_UPDATE, StatementKind.PREPARED_TEMPLATE, "UPDATE t\nSET a = ?", null);
        scope.complete(2L, null, null);
        scope.failed(new SQLException("password=hidden"));
        assertThat(lines).hasSize(1);
        assertThat(lines.getFirst()).doesNotContain("\n", "\r", "hidden");
        var event = codec.fromJson(lines.getFirst());
        assertThat(event.queryId()).isEqualTo("query-1");
        assertThat(event.trinoUser()).isEqualTo("alice");
        assertThat(event.executionUser()).isEqualTo("service");
        assertThat(event.traceToken()).isEqualTo("trace-1");
        assertThat(event.catalog()).isEqualTo("company_mysql");
        assertThat(event.origin()).isEqualTo(Origin.QUERY);
        assertThat(event.result()).isEqualTo(Result.SUCCESS);
        assertThat(event.remoteStatement()).isEqualTo("UPDATE t SET a = ?");
        assertThat(event.affectedRows()).isEqualTo(2L);
        assertThat(event.autoCommit()).isFalse();
        assertThat(stats.getSinkSuccesses()).isEqualTo(1);
    }

    @Test
    void testFailureIsolationAndBackground()
    {
        var stats = new RemoteOperationAuditStats();
        var auditor = new RemoteOperationAuditor(
                new RemoteOperationAuditConfig().setEnabled(true),
                _ -> { throw new IllegalStateException("secret"); },
                stats,
                "catalog",
                "jdbc",
                "node");
        auditor.begin(CONTEXT, () -> TARGET, Operation.EXECUTE, StatementKind.SQL, "SELECT 1", null).succeeded();
        assertThat(stats.getOperationSuccesses()).isEqualTo(1);
        assertThat(stats.getSinkFailures()).isEqualTo(1);
        assertThat(stats.getDroppedEvents()).isEqualTo(1);
        assertThat(stats.getLastFailureTime()).isPositive();

        List<RemoteOperationAuditEvent> events = new ArrayList<>();
        auditor = new RemoteOperationAuditor(new RemoteOperationAuditConfig().setEnabled(true), events::add, stats, "catalog", "jdbc", "node");
        auditor.begin(auditor.context(null), () -> TARGET, Operation.EXECUTE, StatementKind.SQL, "SELECT 1", null)
                .failed(new SQLException("bound value: highly-private"));
        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.queryId()).isNull();
            assertThat(event.trinoUser()).isNull();
            assertThat(event.origin()).isEqualTo(Origin.BACKGROUND);
            assertThat(event.errorMessage()).doesNotContain("highly-private");
            assertThat(event.errorType()).isEqualTo(SQLException.class.getName());
        });
        auditor.begin(CONTEXT, () -> { throw new IllegalStateException(); }, Operation.EXECUTE, StatementKind.SQL, "SELECT 1", null).succeeded();
        assertThat(stats.getContextFailures()).isEqualTo(1);
    }

    @Test
    void testReadFilteringDoesNotFilterCallableExecutionOrWrites()
    {
        List<RemoteOperationAuditEvent> events = new ArrayList<>();
        var stats = new RemoteOperationAuditStats();
        var auditor = new RemoteOperationAuditor(new RemoteOperationAuditConfig().setEnabled(true).setReadEnabled(false), events::add, stats, "catalog", "jdbc", "node");
        auditor.begin(CONTEXT, () -> TARGET, Operation.EXECUTE_QUERY, StatementKind.SQL, "SELECT 1", null).succeeded();
        auditor.begin(CONTEXT, () -> TARGET, Operation.METADATA, StatementKind.METADATA_METHOD, "getTables", null).succeeded();
        auditor.begin(CONTEXT, () -> TARGET, Operation.EXECUTE, StatementKind.CALLABLE_TEMPLATE, "CALL p(?)", null).succeeded();
        auditor.begin(CONTEXT, () -> TARGET, Operation.EXECUTE_QUERY, StatementKind.CALLABLE_TEMPLATE, "CALL p(?)", null).succeeded();
        auditor.begin(CONTEXT, () -> TARGET, Operation.EXECUTE_QUERY, StatementKind.PREPARED_TEMPLATE, "INSERT INTO t VALUES (?) RETURNING id", null).succeeded();
        auditor.begin(CONTEXT, () -> TARGET, Operation.COMMIT, StatementKind.NONE, null, null).succeeded();
        assertThat(events).extracting(RemoteOperationAuditEvent::operation).containsExactly(Operation.EXECUTE, Operation.EXECUTE_QUERY, Operation.EXECUTE_QUERY, Operation.COMMIT);
        assertThat(stats.getFilteredEvents()).isEqualTo(2);
    }

    @Test
    void testSanitization()
    {
        String sql = "SELECT '\nhttps://alice:secret@host/path?token=hidden#fragment'";
        assertThat(RemoteStatementSanitizer.statement(sql, TEMPLATE, 1000)).doesNotContain("secret", "hidden", "\n");
        assertThat(RemoteStatementSanitizer.statement("ALTER USER alice IDENTIFIED BY 'secret'", TEMPLATE, 1000)).doesNotContain("secret");
        assertThat(RemoteStatementSanitizer.statement("SELECT 'https://alice:secret@host/path?x=private'", TEMPLATE, 1000))
                .isEqualTo("SELECT 'https://host/path'");
        assertThat(RemoteStatementSanitizer.statement("SELECT 123", TEMPLATE, 6)).isEqualTo("SELECT");
        assertThat(RemoteStatementSanitizer.statement("SELECT 123", NONE, 6)).isNull();
        assertThat(RemoteStatementSanitizer.statement("SELECT 123", HASH, 6)).matches("sha256:[a-f0-9]{64}");
        assertThat(RemoteStatementSanitizer.statement("SELECT 123", HASH, 6))
                .isEqualTo(RemoteStatementSanitizer.statement("SELECT 123", HASH, 100));
        assertThat(RemoteStatementSanitizer.endpoint("https://user:secret@host:80/path?token=hidden#f")).isEqualTo("https://host:80/path");
        assertThat(RemoteStatementSanitizer.endpoint("not a valid URI")).isNull();
    }

    @Test
    void testSerializationFailure()
    {
        var stats = new RemoteOperationAuditStats();
        var auditor = new RemoteOperationAuditor(
                new RemoteOperationAuditConfig().setEnabled(true),
                _ -> { throw new JsonLogRemoteOperationAuditSink.SerializationException(new IllegalArgumentException()); },
                stats,
                "catalog",
                "jdbc",
                "node");
        auditor.begin(CONTEXT, () -> TARGET, Operation.EXECUTE, StatementKind.SQL, "SELECT 1", null).succeeded();
        assertThat(stats.getSerializationFailures()).isEqualTo(1);
        assertThat(stats.getSinkFailures()).isEqualTo(1);
    }
}
