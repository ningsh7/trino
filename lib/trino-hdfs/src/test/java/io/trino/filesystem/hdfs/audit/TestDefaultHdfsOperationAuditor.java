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
package io.trino.filesystem.hdfs.audit;

import io.airlift.testing.TestingTicker;
import io.trino.filesystem.FileSystemContext;
import io.trino.filesystem.Location;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.HdfsAuthentication;
import io.trino.hdfs.authentication.HdfsExecutionIdentity;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.trino.filesystem.hdfs.audit.HdfsOperation.DELETE_FILE;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.LIST_FILES;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditOrigin.BACKGROUND;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditOrigin.QUERY;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditResult.FAILURE;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditResult.SUCCESS;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditSinkException.Type.SERIALIZATION;
import static io.trino.hdfs.authentication.HdfsExecutionIdentity.Mode.FIXED_SERVICE_USER;
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDefaultHdfsOperationAuditor
{
    private static final Instant EVENT_TIME = Instant.parse("2026-09-04T08:00:00Z");
    private static final UUID OPERATION_ID = UUID.fromString("2a27aa9f-ef72-49cf-8e4c-67687b056001");
    private static final Location LOCATION = Location.of("hdfs://user:password@namenode:8020/warehouse/table?token=secret");

    @Test
    public void testQuerySuccessEvent()
            throws IOException
    {
        List<HdfsOperationAuditEvent> events = new ArrayList<>();
        HdfsOperationAuditStats stats = new HdfsOperationAuditStats();
        TestingTicker ticker = new TestingTicker();
        DefaultHdfsOperationAuditor auditor = createAuditor(new HdfsOperationAuditConfig().setEnabled(true), events::add, stats, ticker);
        HdfsContext context = new HdfsContext(new FileSystemContext(
                ConnectorIdentity.ofUser("alice"),
                Optional.of("20260904_000001_00001_abcd"),
                Optional.of("trace-token"),
                Optional.of("web")));

        String result = auditor.audit(context, DELETE_FILE, LOCATION, () -> {
            ticker.increment(12, MILLISECONDS);
            return "done";
        });

        assertThat(result).isEqualTo("done");
        assertThat(events).hasSize(1);
        HdfsOperationAuditEvent event = events.getFirst();
        assertThat(event.schemaVersion()).isEqualTo(1);
        assertThat(event.operationId()).isEqualTo(OPERATION_ID.toString());
        assertThat(event.timestamp()).isEqualTo(EVENT_TIME);
        assertThat(event.queryId()).contains("20260904_000001_00001_abcd");
        assertThat(event.traceToken()).contains("trace-token");
        assertThat(event.trinoUser()).isEqualTo("alice");
        assertThat(event.source()).contains("web");
        assertThat(event.catalog()).isEqualTo("company_a_hive");
        assertThat(event.executionUser()).isEqualTo("hive_service");
        assertThat(event.identityMode()).isEqualTo(FIXED_SERVICE_USER);
        assertThat(event.stronglyAuthenticated()).isFalse();
        assertThat(event.operation()).isEqualTo(DELETE_FILE);
        assertThat(event.sourcePath()).isEqualTo("hdfs://namenode:8020/warehouse/table");
        assertThat(event.targetPath()).isEmpty();
        assertThat(event.batchId()).isEmpty();
        assertThat(event.batchSize()).isEmpty();
        assertThat(event.nodeId()).isEqualTo("worker-1");
        assertThat(event.result()).isEqualTo(SUCCESS);
        assertThat(event.durationMillis()).isEqualTo(12);
        assertThat(event.bytes()).isEmpty();
        assertThat(event.errorType()).isEmpty();
        assertThat(event.errorMessage()).isEmpty();
        assertThat(event.origin()).isEqualTo(QUERY);
        assertThat(stats.getOperationSuccesses().getTotalCount()).isEqualTo(1);
        assertThat(stats.getOperationCount(DELETE_FILE)).isEqualTo(1);
        assertThat(stats.getSinkSuccesses().getTotalCount()).isEqualTo(1);
    }

    @Test
    public void testBackgroundFailureEventPreservesException()
    {
        List<HdfsOperationAuditEvent> events = new ArrayList<>();
        HdfsOperationAuditStats stats = new HdfsOperationAuditStats();
        DefaultHdfsOperationAuditor auditor = createAuditor(
                new HdfsOperationAuditConfig().setEnabled(true),
                events::add,
                stats,
                new TestingTicker());
        HdfsContext context = new HdfsContext(ConnectorIdentity.ofUser("cleanup-service"));
        IOException failure = new IOException("Delete " + LOCATION + " failed\nwith secret");

        assertThatThrownBy(() -> auditor.audit(context, DELETE_FILE, LOCATION, () -> {
            throw failure;
        }))
                .isSameAs(failure);

        assertThat(events).hasSize(1);
        HdfsOperationAuditEvent event = events.getFirst();
        assertThat(event.queryId()).isEmpty();
        assertThat(event.origin()).isEqualTo(BACKGROUND);
        assertThat(event.result()).isEqualTo(FAILURE);
        assertThat(event.errorType()).contains(IOException.class.getName());
        assertThat(event.errorMessage().orElseThrow())
                .doesNotContain("user", "password", "token", "\n")
                .contains("hdfs://namenode:8020/warehouse/table");
        assertThat(stats.getOperationFailures().getTotalCount()).isEqualTo(1);
    }

    @Test
    public void testSinkFailureDoesNotChangeOperationResult()
            throws IOException
    {
        HdfsOperationAuditStats stats = new HdfsOperationAuditStats();
        DefaultHdfsOperationAuditor auditor = createAuditor(
                new HdfsOperationAuditConfig().setEnabled(true),
                _ -> {
                    throw new RuntimeException("sink unavailable");
                },
                stats,
                new TestingTicker());

        assertThat(auditor.audit(new HdfsContext(ConnectorIdentity.ofUser("background")), DELETE_FILE, LOCATION, () -> "done"))
                .isEqualTo("done");
        assertThat(stats.getSinkFailures().getTotalCount()).isEqualTo(1);
        assertThat(stats.getDroppedEvents().getTotalCount()).isEqualTo(1);
        assertThat(stats.getLastOutputErrorTime()).isEqualTo(EVENT_TIME.toEpochMilli());
    }

    @Test
    public void testSerializationFailureIsClassifiedAndDoesNotChangeOperationResult()
            throws IOException
    {
        HdfsOperationAuditStats stats = new HdfsOperationAuditStats();
        DefaultHdfsOperationAuditor auditor = createAuditor(
                new HdfsOperationAuditConfig().setEnabled(true),
                _ -> {
                    throw new HdfsOperationAuditSinkException(SERIALIZATION, new IllegalArgumentException("serialization failed"));
                },
                stats,
                new TestingTicker());

        assertThat(auditor.audit(new HdfsContext(ConnectorIdentity.ofUser("background")), DELETE_FILE, LOCATION, () -> "done"))
                .isEqualTo("done");
        assertThat(stats.getSerializationFailures().getTotalCount()).isEqualTo(1);
        assertThat(stats.getSinkFailures().getTotalCount()).isZero();
        assertThat(stats.getDroppedEvents().getTotalCount()).isEqualTo(1);
    }

    @Test
    public void testDisabledAndFilteredOperationsDoNotEmitEvents()
            throws IOException
    {
        List<HdfsOperationAuditEvent> events = new ArrayList<>();
        HdfsOperationAuditStats stats = new HdfsOperationAuditStats();
        DefaultHdfsOperationAuditor disabledAuditor = createAuditor(
                new HdfsOperationAuditConfig(),
                events::add,
                stats,
                new TestingTicker());
        DefaultHdfsOperationAuditor listDisabledAuditor = createAuditor(
                new HdfsOperationAuditConfig().setEnabled(true),
                events::add,
                stats,
                new TestingTicker());

        assertThat(disabledAuditor.audit(new HdfsContext(ConnectorIdentity.ofUser("user")), DELETE_FILE, LOCATION, () -> 1)).isEqualTo(1);
        assertThat(listDisabledAuditor.audit(new HdfsContext(ConnectorIdentity.ofUser("user")), LIST_FILES, LOCATION, () -> 2)).isEqualTo(2);

        assertThat(events).isEmpty();
        assertThat(stats.getFilteredEvents().getTotalCount()).isEqualTo(1);
    }

    private static DefaultHdfsOperationAuditor createAuditor(
            HdfsOperationAuditConfig config,
            HdfsOperationAuditSink sink,
            HdfsOperationAuditStats stats,
            TestingTicker ticker)
    {
        HdfsAuthentication authentication = new HdfsAuthentication()
        {
            @Override
            public HdfsExecutionIdentity getExecutionIdentity(ConnectorIdentity identity)
            {
                return new HdfsExecutionIdentity("hive_service", FIXED_SERVICE_USER, false);
            }

            @Override
            public <T> T doAs(ConnectorIdentity identity, ExceptionAction<T> action)
                    throws IOException
            {
                return action.run();
            }
        };
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsEnvironment environment = new HdfsEnvironment(
                new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), emptySet()),
                hdfsConfig,
                authentication);
        return new DefaultHdfsOperationAuditor(
                environment,
                config,
                new HdfsAuditPathSanitizer(config),
                sink,
                stats,
                "company_a_hive",
                "worker-1",
                Clock.fixed(EVENT_TIME, ZoneOffset.UTC),
                ticker,
                () -> OPERATION_ID);
    }
}
