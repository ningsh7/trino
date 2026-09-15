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
package io.trino.plugin.base.audit.jdbc;

import io.trino.plugin.base.audit.RemoteOperationAuditConfig;
import io.trino.plugin.base.audit.RemoteOperationAuditContext;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.Operation;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.Result;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.StatementKind;
import io.trino.plugin.base.audit.RemoteOperationAuditStats;
import io.trino.plugin.base.audit.RemoteOperationAuditor;
import org.h2.Driver;
import org.h2.jdbc.JdbcConnection;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAuditingJdbc
{
    private static final RemoteOperationAuditContext CONTEXT = new RemoteOperationAuditContext("query-1", "trace", "alice", "test");

    @Test
    void testPreparedBatchAndTransactions()
            throws Exception
    {
        List<RemoteOperationAuditEvent> events = new ArrayList<>();
        try (Connection physical = connection();
                Connection audited = AuditingJdbc.wrap(physical, CONTEXT, auditor(events))) {
            audited.setAutoCommit(false);
            try (Statement statement = audited.createStatement()) {
                statement.execute("CREATE TABLE t (id INTEGER, payload VARCHAR)");
            }
            try (PreparedStatement statement = audited.prepareStatement("INSERT INTO t VALUES (?, ?)")) {
                statement.setInt(1, 1);
                statement.setString(2, "private-bound-value");
                statement.addBatch();
                statement.setInt(1, 2);
                statement.addBatch();
                assertThat(statement.executeBatch()).containsExactly(1, 1);
                statement.setInt(1, 3);
                statement.addBatch();
                assertThat(statement.executeLargeBatch()).containsExactly(1L);
                statement.clearBatch();
            }
            audited.rollback();
            try (Statement statement = audited.createStatement();
                    ResultSet rows = statement.executeQuery("SELECT count(*) FROM t")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isZero();
            }
            audited.commit();
        }
        assertThat(events).extracting(RemoteOperationAuditEvent::operation).containsExactly(
                Operation.EXECUTE,
                Operation.EXECUTE_BATCH,
                Operation.EXECUTE_LARGE_BATCH,
                Operation.ROLLBACK,
                Operation.EXECUTE_QUERY,
                Operation.COMMIT);
        assertThat(events.get(1).batchSize()).isEqualTo(2);
        assertThat(events.get(2).batchSize()).isEqualTo(1);
        assertThat(events.get(1).remoteStatement()).isEqualTo("INSERT INTO t VALUES (?, ?)");
        assertThat(events).allSatisfy(event -> {
            assertThat(event.trinoUser()).isEqualTo("alice");
            assertThat(event.executionUser()).isEqualTo("SERVICE_USER");
            assertThat(event.connectionId()).isEqualTo(events.getFirst().connectionId());
            assertThat(event.result()).isEqualTo(Result.SUCCESS);
            assertThat(event.autoCommit()).isFalse();
            assertThat(event.toString()).doesNotContain("private-bound-value");
        });
    }

    @Test
    void testStatementCreationOverloadsAndDriverUnwrap()
            throws Exception
    {
        List<RemoteOperationAuditEvent> events = new ArrayList<>();
        try (Connection physical = connection();
                Connection audited = AuditingJdbc.wrap(physical, CONTEXT, auditor(events))) {
            assertThat(audited.unwrap(JdbcConnection.class)).isSameAs(physical);
            assertThat(audited.isWrapperFor(JdbcConnection.class)).isTrue();
            List<Statement> statements = List.of(
                    audited.createStatement(),
                    audited.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY),
                    audited.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT),
                    audited.prepareStatement("SELECT 1"),
                    audited.prepareStatement("SELECT 1", RETURN_GENERATED_KEYS),
                    audited.prepareStatement("SELECT 1", new int[] {1}),
                    audited.prepareStatement("SELECT 1", new String[] {"X"}),
                    audited.prepareStatement("SELECT 1", TYPE_FORWARD_ONLY, CONCUR_READ_ONLY),
                    audited.prepareStatement("SELECT 1", TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT),
                    audited.prepareCall("CALL ABS(-1)"),
                    audited.prepareCall("CALL ABS(-1)", TYPE_FORWARD_ONLY, CONCUR_READ_ONLY),
                    audited.prepareCall("CALL ABS(-1)", TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT));
            for (Statement statement : statements) {
                try (statement) {
                    assertThat(statement.getConnection()).isSameAs(audited);
                    assertThat(statement.isWrapperFor(statement.unwrap(Statement.class).getClass())).isTrue();
                    try (ResultSet result = statement instanceof PreparedStatement prepared ? prepared.executeQuery() : statement.executeQuery("SELECT 1")) {
                        assertThat(result.next()).isTrue();
                        assertThat(result.getInt(1)).isEqualTo(1);
                    }
                }
            }
            assertThat(events).hasSize(statements.size());
            assertThat(events.subList(9, 12)).allSatisfy(event -> assertThat(event.statementKind()).isEqualTo(StatementKind.CALLABLE_TEMPLATE));
        }
    }

    @Test
    void testUpdateDdlAndBatchSemantics()
            throws Exception
    {
        List<RemoteOperationAuditEvent> events = new ArrayList<>();
        try (Connection physical = connection();
                Connection audited = AuditingJdbc.wrap(physical, CONTEXT, auditor(events));
                Statement statement = audited.createStatement()) {
            statement.executeUpdate("CREATE TABLE t (id INTEGER)");
            assertThat(statement.executeUpdate("INSERT INTO t VALUES (1)", RETURN_GENERATED_KEYS)).isEqualTo(1);
            assertThat(statement.executeLargeUpdate("UPDATE t SET id = 2", new int[] {1})).isEqualTo(1);
            statement.addBatch("DELETE FROM t");
            statement.clearBatch();
            statement.addBatch("INSERT INTO t VALUES (3)");
            assertThat(statement.executeBatch()).containsExactly(1);
            assertThat(events.get(3).batchSize()).isEqualTo(1);
            assertThat(events.get(3).statementKind()).isEqualTo(StatementKind.BATCH);
            assertThat(events.get(1).affectedRows()).isEqualTo(1L);
            assertThat(events.get(2).affectedRows()).isEqualTo(1L);
            assertThatThrownBy(() -> statement.executeQuery("SELECT missing_column FROM t")).isInstanceOf(SQLException.class);
            assertThat(events.getLast().result()).isEqualTo(Result.FAILURE);
        }
    }

    @Test
    void testSinkFailurePreservesResultAndExactDriverException()
            throws Exception
    {
        var stats = new RemoteOperationAuditStats();
        var auditor = new RemoteOperationAuditor(
                new RemoteOperationAuditConfig().setEnabled(true),
                _ -> { throw new IllegalStateException("broken sink"); },
                stats,
                "catalog",
                "h2",
                "node");
        SQLException original = new SQLException("private-bound-value");
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[] {Statement.class},
                (_, _, _) -> { throw original; });
        Connection physical = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (_, method, _) -> {
                    if (method.getName().equals("createStatement")) {
                        return statement;
                    }
                    throw new SQLException("unsupported metadata");
                });
        Connection audited = AuditingJdbc.wrap(physical, CONTEXT, auditor);
        assertThatThrownBy(() -> audited.createStatement().execute("SELECT 1")).isSameAs(original);
        assertThat(stats.getOperationFailures()).isEqualTo(1);
        assertThat(stats.getSinkFailures()).isEqualTo(1);

        try (Connection database = connection();
                Connection wrapped = AuditingJdbc.wrap(database, CONTEXT, auditor);
                Statement query = wrapped.createStatement();
                ResultSet rows = query.executeQuery("SELECT 7")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(7);
        }
        assertThat(stats.getSinkFailures()).isEqualTo(2);
    }

    @Test
    void testDisabledAuditAndMetadataMethods()
            throws Exception
    {
        List<RemoteOperationAuditEvent> events = new ArrayList<>();
        var stats = new RemoteOperationAuditStats();
        var auditor = new RemoteOperationAuditor(
                new RemoteOperationAuditConfig().setEnabled(true).setMetadataEnabled(true),
                events::add,
                stats,
                "catalog",
                "h2",
                "node");
        try (Connection physical = connection()) {
            assertThat(AuditingJdbc.wrap(physical, CONTEXT, RemoteOperationAuditor.noop())).isSameAs(physical);
            Connection audited = AuditingJdbc.wrap(physical, CONTEXT, auditor);
            try (ResultSet ignored = audited.getMetaData().getTables(null, null, null, null)) {
                assertThat(events).singleElement().satisfies(event -> {
                    assertThat(event.operation()).isEqualTo(Operation.METADATA);
                    assertThat(event.remoteStatement()).isEqualTo("getTables");
                    assertThat(event.statementKind()).isEqualTo(StatementKind.METADATA_METHOD);
                });
            }
            assertThat(audited.getMetaData().getConnection()).isSameAs(audited);
        }
    }

    @Test
    void testResultSetIsNotWrapped()
            throws Exception
    {
        List<RemoteOperationAuditEvent> events = new ArrayList<>();
        try (Connection physical = connection();
                Statement statement = physical.createStatement();
                ResultSet result = statement.executeQuery("SELECT 1")) {
            Statement fakeStatement = (Statement) Proxy.newProxyInstance(
                    Statement.class.getClassLoader(),
                    new Class<?>[] {Statement.class},
                    (_, _, _) -> result);
            Connection fakeConnection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                    (_, method, args) -> {
                        if (method.getName().equals("createStatement")) {
                            return fakeStatement;
                        }
                        try {
                            return method.invoke(physical, args);
                        }
                        catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
            assertThat(AuditingJdbc.wrap(fakeConnection, CONTEXT, auditor(events)).createStatement().executeQuery("SELECT 1")).isSameAs(result);
        }
    }

    private static Connection connection()
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", "service_user");
        return new Driver().connect("jdbc:h2:mem:" + UUID.randomUUID(), properties);
    }

    private static RemoteOperationAuditor auditor(List<RemoteOperationAuditEvent> events)
    {
        return new RemoteOperationAuditor(new RemoteOperationAuditConfig().setEnabled(true), events::add, new RemoteOperationAuditStats(), "catalog", "h2", "node");
    }
}
