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
import io.airlift.testing.TestingTicker;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.FileSystemContext;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicConfigurationProvider;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static io.trino.filesystem.hdfs.audit.HdfsOperation.CREATE_DIRECTORY;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.CREATE_FILE;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.DELETE_DIRECTORY;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.DELETE_FILE;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.DELETE_FILES;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.LIST_DIRECTORIES;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.LIST_FILES;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.OPEN_READ;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.RENAME_DIRECTORY;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.RENAME_FILE;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditResult.FAILURE;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditResult.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHdfsFileSystemAudit
{
    @TempDir
    private Path tempDirectory;

    private final List<HdfsOperationAuditEvent> events = new ArrayList<>();
    private HdfsEnvironment environment;
    private FileSystemContext fileSystemContext;
    private TrinoFileSystem fileSystem;
    private Location root;

    @BeforeAll
    public static void beforeAll()
    {
        RawLocalFileSystem.useStatIfAvailable();
    }

    @BeforeEach
    public void setUp()
    {
        String mountTable = "audit" + UUID.randomUUID().toString().replace("-", "");
        root = Location.of("viewfs://" + mountTable + "/");
        DynamicConfigurationProvider viewFs = (config, _, _) ->
                config.set("fs.viewfs.mounttable.%s.linkFallback".formatted(mountTable), tempDirectory.toAbsolutePath().toUri().toString());
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), Set.of(viewFs));
        environment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        HdfsOperationAuditConfig auditConfig = new HdfsOperationAuditConfig()
                .setEnabled(true)
                .setListEnabled(true);
        DefaultHdfsOperationAuditor auditor = new DefaultHdfsOperationAuditor(
                environment,
                auditConfig,
                new HdfsAuditPathSanitizer(auditConfig),
                events::add,
                new HdfsOperationAuditStats(),
                "test_catalog",
                "worker-1",
                Clock.fixed(Instant.parse("2026-09-04T08:00:00Z"), ZoneOffset.UTC),
                new TestingTicker(),
                UUID::randomUUID);
        fileSystemContext = new FileSystemContext(
                ConnectorIdentity.ofUser("alice"),
                Optional.of("query-id"),
                Optional.of("trace-token"),
                Optional.of("test"));
        fileSystem = new HdfsFileSystemFactory(environment, new TrinoHdfsFileSystemStats(), auditor).create(fileSystemContext);
    }

    @Test
    public void testDirectoryOperations()
            throws IOException
    {
        Location source = root.appendPath("source-directory");
        Location target = root.appendPath("target-directory");

        fileSystem.createDirectory(source);
        fileSystem.renameDirectory(source, target);
        fileSystem.deleteDirectory(target);

        assertThat(events)
                .extracting(HdfsOperationAuditEvent::operation)
                .containsExactly(CREATE_DIRECTORY, RENAME_DIRECTORY, DELETE_DIRECTORY);
        assertThat(events).allSatisfy(event -> assertThat(event.result()).isEqualTo(SUCCESS));
        assertThat(events.get(1).sourcePath()).isEqualTo(root.appendPath("source-directory").toString());
        assertThat(events.get(1).targetPath()).contains(root.appendPath("target-directory").toString());

        Location temporaryDirectory = fileSystem.createTemporaryDirectory(root, ".tmp", ".tmp").orElseThrow();
        assertThat(events.getLast().operation()).isEqualTo(CREATE_DIRECTORY);
        assertThat(events.getLast().sourcePath()).isEqualTo(temporaryDirectory.toString());
    }

    @Test
    public void testReadAndWriteOperations()
            throws IOException
    {
        Location location = root.appendPath("data-file");
        OutputStream output = fileSystem.newOutputFile(location).create();
        assertThat(events).isEmpty();

        output.write(new byte[] {1, 2, 3, 4});
        output.close();

        try (var _ = fileSystem.newInputFile(location).newStream()) {
            // Opening the stream is the audited read boundary.
        }
        try (var _ = fileSystem.newInputFile(location).newInput()) {
            // Opening random access input is also one audited read operation.
        }
        assertThatThrownBy(() -> fileSystem.newInputFile(root.appendPath("missing-file")).newStream())
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> {
            try (OutputStream _ = fileSystem.newOutputFile(location).create()) {
                // Existing file must fail before an output stream is returned.
            }
        })
                .isInstanceOf(IOException.class);

        assertThat(events)
                .extracting(HdfsOperationAuditEvent::operation)
                .containsExactly(CREATE_FILE, OPEN_READ, OPEN_READ, OPEN_READ, CREATE_FILE);
        assertThat(events)
                .extracting(HdfsOperationAuditEvent::result)
                .containsExactly(SUCCESS, SUCCESS, SUCCESS, FAILURE, FAILURE);
        assertThat(events.getFirst().bytes()).contains(4L);
        assertThat(events.getFirst().trinoUser()).isEqualTo("alice");
        assertThat(events.getFirst().queryId()).contains("query-id");
        assertThat(events.getFirst().sourcePath()).isEqualTo(location.toString());
    }

    @Test
    public void testFileOperationsProduceCorrelatableJson()
            throws IOException
    {
        JsonCodec<HdfsOperationAuditEvent> eventCodec = new JsonCodecFactory().jsonCodec(HdfsOperationAuditEvent.class);
        List<String> jsonEvents = new ArrayList<>();
        HdfsOperationAuditConfig auditConfig = new HdfsOperationAuditConfig().setEnabled(true);
        DefaultHdfsOperationAuditor auditor = new DefaultHdfsOperationAuditor(
                environment,
                auditConfig,
                new HdfsAuditPathSanitizer(auditConfig),
                new JsonLogHdfsOperationAuditSink(eventCodec, jsonEvents::add),
                new HdfsOperationAuditStats(),
                "test_catalog",
                "worker-1",
                Clock.fixed(Instant.parse("2026-09-04T08:00:00Z"), ZoneOffset.UTC),
                new TestingTicker(),
                UUID::randomUUID);
        TrinoFileSystem jsonFileSystem = new HdfsFileSystemFactory(environment, new TrinoHdfsFileSystemStats(), auditor).create(fileSystemContext);
        Location location = root.appendPath("correlatable-file");

        jsonFileSystem.newOutputFile(location).createOrOverwrite(new byte[] {1, 2, 3});
        try (var _ = jsonFileSystem.newInputFile(location).newStream()) {
            // Closing the input is not a separate audit event.
        }

        assertThat(jsonEvents).hasSize(2).allSatisfy(json -> assertThat(json).doesNotContain("\n", "\r"));
        List<HdfsOperationAuditEvent> decodedEvents = jsonEvents.stream()
                .map(eventCodec::fromJson)
                .toList();
        assertThat(decodedEvents)
                .extracting(HdfsOperationAuditEvent::operation)
                .containsExactly(CREATE_FILE, OPEN_READ);
        String executionUser = environment.getExecutionIdentity(fileSystemContext.identity()).user();
        assertThat(decodedEvents).allSatisfy(event -> {
            assertThat(event.trinoUser()).isEqualTo("alice");
            assertThat(event.executionUser()).isEqualTo(executionUser);
            assertThat(event.queryId()).contains("query-id");
            assertThat(event.traceToken()).contains("trace-token");
            assertThat(event.source()).contains("test");
            assertThat(event.sourcePath()).isEqualTo(location.toString());
            assertThat(event.result()).isEqualTo(SUCCESS);
        });
    }

    @Test
    public void testListOperations()
            throws IOException
    {
        Location directory = root.appendPath("list-directory");
        fileSystem.createDirectory(directory);
        fileSystem.newOutputFile(directory.appendPath("file")).createOrOverwrite(new byte[] {1});
        events.clear();

        FileIterator files = fileSystem.listFiles(root);
        while (files.hasNext()) {
            files.next();
        }
        fileSystem.listDirectories(root);

        assertThat(events)
                .extracting(HdfsOperationAuditEvent::operation)
                .containsExactly(LIST_FILES, LIST_DIRECTORIES);
        assertThat(events).allSatisfy(event -> {
            assertThat(event.result()).isEqualTo(SUCCESS);
            assertThat(event.sourcePath()).isEqualTo(root.toString());
        });
    }

    @Test
    public void testFileOperationsAndFailure()
            throws IOException
    {
        Location source = root.appendPath("source-file");
        Location target = root.appendPath("target-file");
        fileSystem.newOutputFile(source).createOrOverwrite(new byte[] {1, 2, 3});
        events.clear();

        fileSystem.renameFile(source, target);
        fileSystem.deleteFile(target);
        assertThatThrownBy(() -> fileSystem.renameFile(source, target))
                .isInstanceOf(IOException.class);

        assertThat(events)
                .extracting(HdfsOperationAuditEvent::operation)
                .containsExactly(RENAME_FILE, DELETE_FILE, RENAME_FILE);
        assertThat(events)
                .extracting(HdfsOperationAuditEvent::result)
                .containsExactly(SUCCESS, SUCCESS, FAILURE);
    }

    @Test
    public void testBatchDeleteProducesOneEventPerPath()
            throws IOException
    {
        Location first = root.appendPath("first-file");
        Location second = root.appendPath("second-file");
        fileSystem.newOutputFile(first).createOrOverwrite(new byte[] {1});
        fileSystem.newOutputFile(second).createOrOverwrite(new byte[] {2});
        events.clear();

        fileSystem.deleteFiles(List.of(first, second));

        assertThat(events).hasSize(2);
        assertThat(events).allSatisfy(event -> {
            assertThat(event.operation()).isEqualTo(DELETE_FILES);
            assertThat(event.result()).isEqualTo(SUCCESS);
            assertThat(event.batchId()).isPresent();
            assertThat(event.batchSize()).contains(2);
        });
        assertThat(events)
                .extracting(HdfsOperationAuditEvent::sourcePath)
                .containsExactlyInAnyOrder(first.toString(), second.toString());
        assertThat(events)
                .extracting(event -> event.batchId().orElseThrow())
                .containsOnly(events.getFirst().batchId().orElseThrow());
        assertThat(events)
                .extracting(HdfsOperationAuditEvent::operationId)
                .doesNotHaveDuplicates();
    }

    @Test
    public void testBatchDeleteStopsAfterFailedPath()
    {
        IOException deleteFailure = new IOException("delete failed");
        FailingDeleteFileSystem rawFileSystem = new FailingDeleteFileSystem("second-file", deleteFailure);
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), Set.of());
        HdfsEnvironment environment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication())
        {
            @Override
            public FileSystem getFileSystem(HdfsContext context, org.apache.hadoop.fs.Path path)
            {
                return rawFileSystem;
            }
        };
        HdfsOperationAuditConfig auditConfig = new HdfsOperationAuditConfig().setEnabled(true);
        DefaultHdfsOperationAuditor auditor = new DefaultHdfsOperationAuditor(
                environment,
                auditConfig,
                new HdfsAuditPathSanitizer(auditConfig),
                events::add,
                new HdfsOperationAuditStats(),
                "test_catalog",
                "worker-1",
                Clock.fixed(Instant.parse("2026-09-04T08:00:00Z"), ZoneOffset.UTC),
                new TestingTicker(),
                UUID::randomUUID);
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(environment, new TrinoHdfsFileSystemStats(), auditor)
                .create(new FileSystemContext(
                        ConnectorIdentity.ofUser("alice"),
                        Optional.of("query-id"),
                        Optional.empty(),
                        Optional.empty()));
        Location first = Location.of("hdfs://namenode/warehouse/first-file");
        Location second = Location.of("hdfs://namenode/warehouse/second-file");
        Location unattempted = Location.of("hdfs://namenode/warehouse/unattempted-file");

        assertThatThrownBy(() -> fileSystem.deleteFiles(List.of(first, second, unattempted)))
                .isSameAs(deleteFailure);

        assertThat(rawFileSystem.getAttemptedPaths())
                .containsExactly(first.toString(), second.toString());
        assertThat(events)
                .extracting(HdfsOperationAuditEvent::result)
                .containsExactly(SUCCESS, FAILURE);
        assertThat(events)
                .extracting(HdfsOperationAuditEvent::sourcePath)
                .containsExactly(first.toString(), second.toString());
        assertThat(events).allSatisfy(event -> {
            assertThat(event.operation()).isEqualTo(DELETE_FILES);
            assertThat(event.batchSize()).contains(3);
            assertThat(event.batchId()).contains(events.getFirst().batchId().orElseThrow());
        });
    }

    private static class FailingDeleteFileSystem
            extends RawLocalFileSystem
    {
        private final String failingFileName;
        private final IOException failure;
        private final List<String> attemptedPaths = new ArrayList<>();

        private FailingDeleteFileSystem(String failingFileName, IOException failure)
        {
            this.failingFileName = failingFileName;
            this.failure = failure;
        }

        @Override
        public boolean delete(org.apache.hadoop.fs.Path path, boolean recursive)
                throws IOException
        {
            attemptedPaths.add(path.toString());
            if (path.getName().equals(failingFileName)) {
                throw failure;
            }
            return true;
        }

        public List<String> getAttemptedPaths()
        {
            return List.copyOf(attemptedPaths);
        }
    }
}
