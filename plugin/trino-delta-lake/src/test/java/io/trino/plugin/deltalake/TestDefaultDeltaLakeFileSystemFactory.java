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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.FileSystemContext;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDefaultDeltaLakeFileSystemFactory
{
    @Test
    public void testContextPreservedWithoutVendedCredentials()
    {
        RecordingFileSystemFactory delegate = new RecordingFileSystemFactory();
        ConnectorSession session = testingSession();
        DefaultDeltaLakeFileSystemFactory factory = new DefaultDeltaLakeFileSystemFactory(delegate, new NoOpTableCredentialsProvider());

        factory.create(session, Optional.empty());

        assertQueryContext(delegate.getContext(), session);
        assertThat(delegate.getContext().identity()).isEqualTo(session.getIdentity());
    }

    @Test
    public void testContextPreservedWithVendedCredentials()
    {
        RecordingFileSystemFactory delegate = new RecordingFileSystemFactory();
        ConnectorSession session = testingSession();
        DefaultDeltaLakeFileSystemFactory factory = new DefaultDeltaLakeFileSystemFactory(delegate, new NoOpTableCredentialsProvider());
        FileSystemCredentials credentials = new FileSystemCredentials()
        {
            @Override
            public ImmutableMap<String, String> asExtraCredentials()
            {
                return ImmutableMap.of("storage_token", "secret");
            }

            @Override
            public boolean isValid()
            {
                return true;
            }
        };
        DeltaLakeTableCredentials tableCredentials = new DeltaLakeTableCredentials(
                VendedCredentialsHandle.empty("s3://bucket/table"),
                credentials);

        factory.create(session, Optional.of(tableCredentials));

        assertQueryContext(delegate.getContext(), session);
        assertThat(delegate.getContext().identity().getUser()).isEqualTo("ldap_user");
        assertThat(delegate.getContext().identity().getExtraCredentials()).containsExactlyEntriesOf(ImmutableMap.of("storage_token", "secret"));
    }

    private static ConnectorSession testingSession()
    {
        return TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("ldap_user"))
                .setSource("test_source")
                .setTraceToken("test_trace_token")
                .build();
    }

    private static void assertQueryContext(FileSystemContext context, ConnectorSession session)
    {
        assertThat(context.queryId()).contains(session.getQueryId());
        assertThat(context.source()).contains("test_source");
        assertThat(context.traceToken()).contains("test_trace_token");
    }

    private static class RecordingFileSystemFactory
            implements TrinoFileSystemFactory
    {
        private FileSystemContext context;

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            throw new AssertionError("FileSystemContext was not propagated");
        }

        @Override
        public TrinoFileSystem create(FileSystemContext context)
        {
            this.context = context;
            return new MemoryFileSystem();
        }

        public FileSystemContext getContext()
        {
            return context;
        }
    }
}
