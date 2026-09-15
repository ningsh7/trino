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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent;
import io.trino.plugin.base.audit.RemoteOperationAuditSink;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.plugin.base.audit.RemoteOperationAuditEvent.Operation.EXECUTE_QUERY;
import static org.assertj.core.api.Assertions.assertThat;

class TestJdbcRemoteOperationAudit
{
    @Test
    void testFactoryWiringWithAndWithoutConnectionReuse()
            throws Exception
    {
        for (boolean reuse : List.of(false, true)) {
            List<RemoteOperationAuditEvent> events = new ArrayList<>();
            var injector = new Bootstrap(
                    new ConnectorContextModule("company_h2", new TestingConnectorContext()),
                    new JdbcModule("h2"),
                    new CredentialProviderModule(),
                    new TestingH2JdbcModule(),
                    binder -> newOptionalBinder(binder, RemoteOperationAuditSink.class).setBinding().toInstance(events::add))
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                            .putAll(TestingH2JdbcModule.createProperties())
                            .put("remote-operation-audit.enabled", "true")
                            .put("query.reuse-connection", Boolean.toString(reuse))
                            .put("connection-user", "source_user")
                            .buildOrThrow())
                    .initialize();
            try {
                ConnectionFactory factory = injector.getInstance(ConnectionFactory.class);
                assertThat(injector.getInstance(MBeanServer.class).queryNames(new ObjectName("io.trino.plugin.base.audit:*"), null))
                        .anySatisfy(name -> assertThat(name.getKeyProperty("name")).isEqualTo("company_h2"));
                var alice = TestingConnectorSession.builder().setIdentity(ConnectorIdentity.ofUser("alice")).setSource("web").setTraceToken("trace").build();
                var bob = TestingConnectorSession.builder().setIdentity(ConnectorIdentity.ofUser("bob")).build();
                try (Connection unused = factory.openConnection(alice)) {
                    assertThat(events).isEmpty();
                }
                for (var session : List.of(alice, alice, bob)) {
                    try (Connection connection = factory.openConnection(session);
                            Statement statement = connection.createStatement();
                            ResultSet result = statement.executeQuery("SELECT 42")) {
                        assertThat(result.next()).isTrue();
                        assertThat(result.getInt(1)).isEqualTo(42);
                    }
                }
                assertThat(events).hasSize(3);
                assertThat(events).extracting(RemoteOperationAuditEvent::trinoUser).containsExactly("alice", "alice", "bob");
                assertThat(events).allSatisfy(event -> {
                    assertThat(event.executionUser()).isEqualTo("SOURCE_USER");
                    assertThat(event.catalog()).isEqualTo("company_h2");
                    assertThat(event.connector()).isEqualTo("h2");
                    assertThat(event.operation()).isEqualTo(EXECUTE_QUERY);
                });
                if (reuse) {
                    assertThat(events.getFirst().connectionId()).isEqualTo(events.get(1).connectionId());
                }
                assertThat(events.getLast().connectionId()).isNotEqualTo(events.getFirst().connectionId());
                assertThat(events.getFirst().queryId()).isEqualTo(alice.getQueryId());
                assertThat(events.getLast().queryId()).isEqualTo(bob.getQueryId());
            }
            finally {
                injector.getInstance(LifeCycleManager.class).stop();
            }
        }
    }
}
