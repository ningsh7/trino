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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.cache.CacheFileSystemFactory;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.filesystem.switching.SwitchingFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.filesystem.tracking.TrackingFileSystemFactory;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFileSystemContext
{
    private static final Location LOCATION = Location.of("memory:///test");

    @Test
    public void testCreatesContextFromSession()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("ldap_user"))
                .setSource("test_source")
                .setTraceToken("test_trace_token")
                .build();

        FileSystemContext context = FileSystemContext.of(session);

        assertThat(context.identity()).isEqualTo(session.getIdentity());
        assertThat(context.queryId()).contains(session.getQueryId());
        assertThat(context.source()).contains("test_source");
        assertThat(context.traceToken()).contains("test_trace_token");
    }

    @Test
    public void testIdentityOnlyContextHasNoQueryInformation()
    {
        ConnectorIdentity identity = ConnectorIdentity.ofUser("background_user");

        FileSystemContext context = FileSystemContext.of(identity);

        assertThat(context.identity()).isEqualTo(identity);
        assertThat(context.queryId()).isEmpty();
        assertThat(context.source()).isEmpty();
        assertThat(context.traceToken()).isEmpty();
    }

    @Test
    public void testContextPropagatesThroughFileSystemFactoryWrappers()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("ldap_user"))
                .setSource("test_source")
                .setTraceToken("test_trace_token")
                .build();
        RecordingFileSystemFactory delegate = new RecordingFileSystemFactory();

        TrinoFileSystemFactory factory = wrappedFactory(delegate);
        factory.create(session).newInputFile(LOCATION);

        assertThat(delegate.getContext()).isEqualTo(FileSystemContext.of(session));
    }

    @Test
    public void testReplacingIdentityPreservesQueryInformation()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("ldap_user"))
                .setSource("test_source")
                .setTraceToken("test_trace_token")
                .build();
        FileSystemContext queryContext = FileSystemContext.of(session);
        ConnectorIdentity storageIdentity = ConnectorIdentity.ofUser("storage_user");
        RecordingFileSystemFactory delegate = new RecordingFileSystemFactory();

        wrappedFactory(delegate).create(queryContext.withIdentity(storageIdentity)).newInputFile(LOCATION);

        assertThat(delegate.getContext())
                .isEqualTo(new FileSystemContext(
                        storageIdentity,
                        queryContext.queryId(),
                        queryContext.traceToken(),
                        queryContext.source()));
    }

    private static TrinoFileSystemFactory wrappedFactory(TrinoFileSystemFactory delegate)
    {
        Tracer tracer = OpenTelemetry.noop().getTracer("test");
        return new TracingFileSystemFactory(
                tracer,
                new TrackingFileSystemFactory(
                        new CacheFileSystemFactory(
                                tracer,
                                new SwitchingFileSystemFactory(_ -> delegate),
                                new NoopBlobCache(),
                                new DefaultCacheKeyProvider())));
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

    private static class NoopBlobCache
            implements BlobCache
    {
        @Override
        public Blob get(CacheKey key, BlobSource source)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void tryInvalidate(CacheKey prefix) {}
    }
}
