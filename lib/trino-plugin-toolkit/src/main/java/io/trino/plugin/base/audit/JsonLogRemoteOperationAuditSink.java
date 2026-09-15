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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class JsonLogRemoteOperationAuditSink
        implements RemoteOperationAuditSink
{
    private static final Logger AUDIT_LOG = Logger.get("io.trino.plugin.base.audit.RemoteOperationAudit");

    private final JsonCodec<RemoteOperationAuditEvent> codec;
    private final Consumer<String> output;

    @Inject
    public JsonLogRemoteOperationAuditSink(JsonCodec<RemoteOperationAuditEvent> codec)
    {
        this(codec, message -> {
            if (!AUDIT_LOG.isInfoEnabled()) {
                throw new IllegalStateException("Remote operation audit logger is disabled");
            }
            AUDIT_LOG.info(message);
        });
    }

    JsonLogRemoteOperationAuditSink(JsonCodec<RemoteOperationAuditEvent> codec, Consumer<String> output)
    {
        this.codec = requireNonNull(codec, "codec is null");
        this.output = requireNonNull(output, "output is null");
    }

    @Override
    public void audit(RemoteOperationAuditEvent event)
    {
        String json;
        try {
            json = codec.toJson(event).replace("\r", "").replace("\n", "");
        }
        catch (RuntimeException e) {
            throw new SerializationException(e);
        }
        output.accept(json);
    }

    public static class SerializationException
            extends RuntimeException
    {
        public SerializationException(Throwable cause)
        {
            super(cause);
        }
    }
}
