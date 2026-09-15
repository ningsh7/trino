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

import com.sun.net.httpserver.HttpServer;
import io.trino.plugin.base.audit.RemoteOperationAuditConfig;
import io.trino.plugin.base.audit.RemoteOperationAuditContext;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.Operation;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.Protocol;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.Result;
import io.trino.plugin.base.audit.RemoteOperationAuditStats;
import io.trino.plugin.base.audit.RemoteOperationAuditTarget;
import io.trino.plugin.base.audit.RemoteOperationAuditor;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDorisRemoteOperationAudit
{
    @Test
    void testHttpRetriesProduceDistinctCorrelatableEvents()
            throws Exception
    {
        List<String> requests = new CopyOnWriteArrayList<>();
        HttpServer failed = server(503, "unavailable", requests);
        HttpServer successful = server(200, "{\"status\":200,\"opaqued_query_plan\":\"plan\",\"partitions\":{}}", requests);
        try {
            List<RemoteOperationAuditEvent> events = new ArrayList<>();
            DorisConfig config = new DorisConfig()
                    .setFenodes("127.0.0.1:" + failed.getAddress().getPort() + ",127.0.0.1:" + successful.getAddress().getPort())
                    .setUsername("source_user").setPassword("private-password");
            FeDorisSplitPlanner planner = new FeDorisSplitPlanner(config, new DorisQueryBuilder(), auditor(events));
            var session = TestingConnectorSession.builder().setIdentity(ConnectorIdentity.ofUser("alice")).setTraceToken("trace").build();
            assertThat(planner.planSplits(session, new DorisTableHandle("sales", "orders"))).isEmpty();
            assertThat(requests).hasSize(2).allSatisfy(body -> assertThat(body).contains("sales", "orders"));
            assertThat(events).hasSize(2);
            assertThat(events.getFirst().result()).isEqualTo(Result.FAILURE);
            assertThat(events.getFirst().httpStatus()).isEqualTo(503);
            assertThat(events.getLast().result()).isEqualTo(Result.SUCCESS);
            assertThat(events.getLast().httpStatus()).isEqualTo(200);
            assertThat(events.getFirst().operationId()).isNotEqualTo(events.getLast().operationId());
            assertThat(events).allSatisfy(event -> {
                assertThat(event.protocol()).isEqualTo(Protocol.DORIS_FE_HTTP);
                assertThat(event.operation()).isEqualTo(Operation.PLAN_SPLITS);
                assertThat(event.executionUser()).isEqualTo("source_user");
                assertThat(event.trinoUser()).isEqualTo("alice");
                assertThat(event.queryId()).isEqualTo(session.getQueryId());
                assertThat(event.traceToken()).isEqualTo("trace");
                assertThat(event.remoteStatement()).contains("sales", "orders");
                assertThat(event.toString()).doesNotContain("private-password", "Basic");
            });
        }
        finally {
            failed.stop(0);
            successful.stop(0);
        }
    }

    @Test
    void testFlightExecuteBoundaryPreservesResultAndFailure()
            throws Exception
    {
        List<RemoteOperationAuditEvent> events = new ArrayList<>();
        var auditor = auditor(events);
        var context = new RemoteOperationAuditContext("query", "trace", "alice", "web");
        var target = new RemoteOperationAuditTarget(Protocol.DORIS_FLIGHT_SQL, null, "source_user", null, null, "grpc://fe:9401", null);
        var result = new AdbcStatement.QueryResult(-1, null);
        AdbcStatement successful = (AdbcStatement) Proxy.newProxyInstance(
                AdbcStatement.class.getClassLoader(),
                new Class<?>[] {AdbcStatement.class},
                (_, _, _) -> result);
        assertThat(AdbcDorisFlightSqlClient.executeQuery(successful, "SELECT 1", context, target, auditor)).isSameAs(result);
        AdbcException original = AdbcException.notImplemented("private-response");
        AdbcStatement failed = (AdbcStatement) Proxy.newProxyInstance(
                AdbcStatement.class.getClassLoader(),
                new Class<?>[] {AdbcStatement.class},
                (_, _, _) -> { throw original; });
        assertThatThrownBy(() -> AdbcDorisFlightSqlClient.executeQuery(failed, "SELECT 2", context, target, auditor)).isSameAs(original);
        assertThat(events).extracting(RemoteOperationAuditEvent::result).containsExactly(Result.SUCCESS, Result.FAILURE);
        assertThat(events).allSatisfy(event -> {
            assertThat(event.protocol()).isEqualTo(Protocol.DORIS_FLIGHT_SQL);
            assertThat(event.operation()).isEqualTo(Operation.OPEN_READ);
            assertThat(event.toString()).doesNotContain("private-response");
        });

        var brokenSink = new RemoteOperationAuditor(
                new RemoteOperationAuditConfig().setEnabled(true),
                _ -> { throw new IllegalStateException(); },
                new RemoteOperationAuditStats(),
                "doris",
                "doris",
                "worker");
        assertThat(AdbcDorisFlightSqlClient.executeQuery(successful, "SELECT 1", context, target, brokenSink)).isSameAs(result);
        assertThatThrownBy(() -> AdbcDorisFlightSqlClient.executeQuery(failed, "SELECT 2", context, target, brokenSink)).isSameAs(original);
    }

    private static RemoteOperationAuditor auditor(List<RemoteOperationAuditEvent> events)
    {
        return new RemoteOperationAuditor(
                new RemoteOperationAuditConfig().setEnabled(true),
                events::add,
                new RemoteOperationAuditStats(),
                "company_doris",
                "doris",
                "worker");
    }

    private static HttpServer server(int status, String body, List<String> requests)
            throws Exception
    {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                requests.add(new String(exchange.getRequestBody().readAllBytes(), UTF_8));
                byte[] response = body.getBytes(UTF_8);
                exchange.sendResponseHeaders(status, response.length);
                exchange.getResponseBody().write(response);
            }
        });
        server.start();
        return server;
    }
}
