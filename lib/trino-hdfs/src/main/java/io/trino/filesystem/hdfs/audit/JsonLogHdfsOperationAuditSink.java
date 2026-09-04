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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import java.util.function.Consumer;

import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditSinkException.Type.OUTPUT;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditSinkException.Type.SERIALIZATION;
import static java.util.Objects.requireNonNull;

public class JsonLogHdfsOperationAuditSink
        implements HdfsOperationAuditSink
{
    private static final Logger AUDIT_LOG = Logger.get("io.trino.filesystem.hdfs.audit");

    private final JsonCodec<HdfsOperationAuditEvent> eventCodec;
    private final Consumer<String> output;

    @Inject
    public JsonLogHdfsOperationAuditSink(JsonCodec<HdfsOperationAuditEvent> eventCodec)
    {
        this(eventCodec, JsonLogHdfsOperationAuditSink::writeToAuditLog);
    }

    JsonLogHdfsOperationAuditSink(JsonCodec<HdfsOperationAuditEvent> eventCodec, Consumer<String> output)
    {
        this.eventCodec = requireNonNull(eventCodec, "eventCodec is null");
        this.output = requireNonNull(output, "output is null");
    }

    @Override
    public void audit(HdfsOperationAuditEvent event)
    {
        String json;
        try {
            json = eventCodec.toJson(requireNonNull(event, "event is null"))
                    .replace("\r", "")
                    .replace("\n", "");
        }
        catch (RuntimeException e) {
            throw new HdfsOperationAuditSinkException(SERIALIZATION, e);
        }

        try {
            output.accept(json);
        }
        catch (RuntimeException e) {
            throw new HdfsOperationAuditSinkException(OUTPUT, e);
        }
    }

    private static void writeToAuditLog(String message)
    {
        if (!AUDIT_LOG.isInfoEnabled()) {
            throw new IllegalStateException("HDFS audit logger is disabled");
        }
        AUDIT_LOG.info(message);
    }
}
