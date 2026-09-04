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

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.filesystem.hdfs.audit.HdfsOperation.DELETE_FILE;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditEvent.SCHEMA_VERSION;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditOrigin.QUERY;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditResult.FAILURE;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditSinkException.Type.OUTPUT;
import static io.trino.hdfs.authentication.HdfsExecutionIdentity.Mode.FIXED_SERVICE_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJsonLogHdfsOperationAuditSink
{
    private static final JsonCodec<HdfsOperationAuditEvent> EVENT_CODEC = new JsonCodecFactory().jsonCodec(HdfsOperationAuditEvent.class);

    @Test
    public void testSingleLineJson()
    {
        List<String> output = new ArrayList<>();
        JsonLogHdfsOperationAuditSink sink = new JsonLogHdfsOperationAuditSink(EVENT_CODEC, output::add);
        HdfsOperationAuditEvent event = event();

        sink.audit(event);

        assertThat(output).hasSize(1);
        assertThat(output.getFirst())
                .doesNotContain("\n", "\r")
                .contains("\"schemaVersion\":1")
                .contains("\"queryId\":\"query-id\"")
                .contains("\"trinoUser\":\"alice\"")
                .contains("\"executionUser\":\"hive_service\"")
                .contains("\"sourcePath\":\"hdfs://namenode/warehouse/file\"")
                .contains("escaped\\nmessage");
        assertThat(EVENT_CODEC.fromJson(output.getFirst())).isEqualTo(event);
    }

    @Test
    public void testOutputFailureIsClassified()
    {
        JsonLogHdfsOperationAuditSink sink = new JsonLogHdfsOperationAuditSink(EVENT_CODEC, _ -> {
            throw new RuntimeException("logger unavailable");
        });

        assertThatThrownBy(() -> sink.audit(event()))
                .isInstanceOfSatisfying(HdfsOperationAuditSinkException.class, exception ->
                        assertThat(exception.getType()).isEqualTo(OUTPUT));
    }

    private static HdfsOperationAuditEvent event()
    {
        return new HdfsOperationAuditEvent(
                SCHEMA_VERSION,
                "2a27aa9f-ef72-49cf-8e4c-67687b056001",
                Instant.parse("2026-09-04T08:00:00Z"),
                Optional.of("query-id"),
                Optional.empty(),
                "alice",
                Optional.of("web"),
                "company_a_hive",
                "hive_service",
                FIXED_SERVICE_USER,
                false,
                DELETE_FILE,
                "hdfs://namenode/warehouse/file",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                "worker-1",
                FAILURE,
                12,
                Optional.empty(),
                Optional.of("java.io.IOException"),
                Optional.of("escaped\nmessage"),
                QUERY);
    }
}
