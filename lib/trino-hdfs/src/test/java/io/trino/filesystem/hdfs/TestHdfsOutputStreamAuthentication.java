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
package io.trino.filesystem.hdfs;

import io.trino.filesystem.Location;
import io.trino.filesystem.hdfs.audit.HdfsOperationAuditor.OperationAudit;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.HdfsAuthentication;
import io.trino.hdfs.authentication.HdfsExecutionIdentity;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.hdfs.authentication.HdfsExecutionIdentity.Mode.PROCESS_USER;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHdfsOutputStreamAuthentication
{
    @Test
    public void testWritesRunThroughHdfsEnvironmentAuthentication()
            throws IOException
    {
        CountingHdfsAuthentication authentication = new CountingHdfsAuthentication();
        HdfsEnvironment environment = new HdfsEnvironment(
                new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), emptySet()),
                new HdfsConfig(),
                authentication);

        ByteArrayOutputStream delegate = new ByteArrayOutputStream();
        RecordingOperationAudit operationAudit = new RecordingOperationAudit();
        try (HdfsOutputStream output = new HdfsOutputStream(
                Location.of("hdfs://hadoop-master/test"),
                new FSDataOutputStream(delegate, null),
                environment,
                new HdfsContext(ConnectorIdentity.ofUser("test")),
                operationAudit)) {
            output.write(1);
            output.write(new byte[] {2, 3, 4}, 1, 2);
        }

        assertThat(delegate.toByteArray()).containsExactly(1, 3, 4);
        assertThat(authentication.getInvocations()).isEqualTo(2);
        assertThat(operationAudit.isSucceeded()).isTrue();
        assertThat(operationAudit.getBytes()).hasValue(3);
    }

    @Test
    public void testCloseFailureIsAuditedAndPreserved()
            throws IOException
    {
        IOException closeFailure = new IOException("close failed");
        ByteArrayOutputStream delegate = new ByteArrayOutputStream()
        {
            @Override
            public void close()
                    throws IOException
            {
                throw closeFailure;
            }
        };
        RecordingOperationAudit operationAudit = new RecordingOperationAudit();
        HdfsOutputStream output = new HdfsOutputStream(
                Location.of("hdfs://hadoop-master/test"),
                new FSDataOutputStream(delegate, null),
                createEnvironment(new CountingHdfsAuthentication()),
                new HdfsContext(ConnectorIdentity.ofUser("test")),
                operationAudit);
        output.write(new byte[] {1, 2, 3});

        assertThatThrownBy(output::close).isSameAs(closeFailure);
        assertThat(operationAudit.getFailure()).isSameAs(closeFailure);
        assertThat(operationAudit.getBytes()).hasValue(3);
    }

    @Test
    public void testWriteFailureIsAuditedOnClose()
            throws IOException
    {
        IOException writeFailure = new IOException("write failed");
        ByteArrayOutputStream delegate = new ByteArrayOutputStream()
        {
            @Override
            public synchronized void write(int value)
            {
                throw new RuntimeException(writeFailure);
            }
        };
        RecordingOperationAudit operationAudit = new RecordingOperationAudit();
        HdfsOutputStream output = new HdfsOutputStream(
                Location.of("hdfs://hadoop-master/test"),
                new FSDataOutputStream(delegate, null),
                createEnvironment(new CountingHdfsAuthentication()),
                new HdfsContext(ConnectorIdentity.ofUser("test")),
                operationAudit);

        assertThatThrownBy(() -> output.write(1))
                .isInstanceOf(RuntimeException.class)
                .hasCause(writeFailure);
        output.close();

        assertThat(operationAudit.getFailure())
                .isInstanceOf(RuntimeException.class)
                .hasCause(writeFailure);
        assertThat(operationAudit.getBytes()).hasValue(0);
    }

    private static HdfsEnvironment createEnvironment(HdfsAuthentication authentication)
    {
        return new HdfsEnvironment(
                new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), emptySet()),
                new HdfsConfig(),
                authentication);
    }

    private static class CountingHdfsAuthentication
            implements HdfsAuthentication
    {
        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public HdfsExecutionIdentity getExecutionIdentity(ConnectorIdentity identity)
        {
            return new HdfsExecutionIdentity(identity.getUser(), PROCESS_USER, false);
        }

        @Override
        public <T> T doAs(ConnectorIdentity identity, ExceptionAction<T> action)
                throws IOException
        {
            invocations.incrementAndGet();
            return action.run();
        }

        public int getInvocations()
        {
            return invocations.get();
        }
    }

    private static class RecordingOperationAudit
            implements OperationAudit
    {
        private boolean succeeded;
        private Throwable failure;
        private OptionalLong bytes = OptionalLong.empty();

        @Override
        public void succeeded(OptionalLong bytes)
        {
            succeeded = true;
            this.bytes = bytes;
        }

        @Override
        public void failed(Throwable failure, OptionalLong bytes)
        {
            this.failure = failure;
            this.bytes = bytes;
        }

        public boolean isSucceeded()
        {
            return succeeded;
        }

        public Throwable getFailure()
        {
            return failure;
        }

        public OptionalLong getBytes()
        {
            return bytes;
        }
    }
}
