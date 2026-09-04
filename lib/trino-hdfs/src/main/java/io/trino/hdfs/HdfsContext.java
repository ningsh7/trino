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
package io.trino.hdfs;

import io.trino.filesystem.FileSystemContext;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HdfsContext
{
    private final FileSystemContext fileSystemContext;

    public HdfsContext(ConnectorIdentity identity)
    {
        this(FileSystemContext.of(identity));
    }

    public HdfsContext(ConnectorSession session)
    {
        this(FileSystemContext.of(session));
    }

    public HdfsContext(FileSystemContext fileSystemContext)
    {
        this.fileSystemContext = requireNonNull(fileSystemContext, "fileSystemContext is null");
    }

    public ConnectorIdentity getIdentity()
    {
        return fileSystemContext.identity();
    }

    public Optional<String> getQueryId()
    {
        return fileSystemContext.queryId();
    }

    public Optional<String> getTraceToken()
    {
        return fileSystemContext.traceToken();
    }

    public Optional<String> getSource()
    {
        return fileSystemContext.source();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("user", getIdentity())
                .add("queryId", getQueryId().orElse(null))
                .add("source", getSource().orElse(null))
                .toString();
    }
}
