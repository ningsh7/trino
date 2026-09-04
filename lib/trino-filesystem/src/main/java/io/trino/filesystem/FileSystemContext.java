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
package io.trino.filesystem;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record FileSystemContext(
        ConnectorIdentity identity,
        Optional<String> queryId,
        Optional<String> traceToken,
        Optional<String> source)
{
    public FileSystemContext
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(traceToken, "traceToken is null");
        requireNonNull(source, "source is null");
    }

    public static FileSystemContext of(ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        return new FileSystemContext(
                session.getIdentity(),
                Optional.of(session.getQueryId()),
                session.getTraceToken(),
                session.getSource());
    }

    public static FileSystemContext of(ConnectorIdentity identity)
    {
        return new FileSystemContext(identity, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public FileSystemContext withIdentity(ConnectorIdentity identity)
    {
        return new FileSystemContext(identity, queryId, traceToken, source);
    }
}
