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

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TestHdfsFileSystemManager
{
    @Test
    void testFixedUserConfiguration()
    {
        HdfsFileSystemManager manager = new HdfsFileSystemManager(
                ImmutableMap.<String, String>builder()
                        .put("hive.hdfs.authentication.type", "NONE")
                        .put("hive.hdfs.identity.mode", "FIXED")
                        .put("hive.hdfs.fixed-user", "hive_service")
                        .buildOrThrow(),
                "test",
                new TestingConnectorContext());

        try {
            assertThat(manager.configure().keySet()).containsExactlyInAnyOrder(
                    "hive.hdfs.authentication.type",
                    "hive.hdfs.identity.mode",
                    "hive.hdfs.fixed-user");
            assertThat(manager.create()).isNotNull();
        }
        finally {
            manager.stop();
        }
    }

    @Test
    void testManager()
            throws IOException
    {
        HdfsFileSystemManager manager = new HdfsFileSystemManager(
                ImmutableMap.<String, String>builder()
                        .put("unused-property", "ignored")
                        .put("hive.dfs.verify-checksum", "false")
                        .put("hive.s3.region", "us-west-1")
                        .buildOrThrow(),
                "test",
                new TestingConnectorContext());

        assertThat(manager.configure().keySet()).containsExactly("hive.dfs.verify-checksum");

        TrinoFileSystemFactory factory = manager.create();
        TrinoFileSystem fileSystem = factory.create(ConnectorIdentity.ofUser("test"));

        Location location = Location.of("/tmp/" + UUID.randomUUID());
        assertThat(fileSystem.newInputFile(location).exists()).isFalse();

        manager.stop();
    }

    @Test
    void testAuditConfiguration()
    {
        HdfsFileSystemManager manager = new HdfsFileSystemManager(
                ImmutableMap.<String, String>builder()
                        .put("hive.hdfs.audit.enabled", "true")
                        .put("hive.hdfs.audit.read-enabled", "true")
                        .put("hive.hdfs.audit.list-enabled", "false")
                        .put("hive.hdfs.audit.path-mode", "HASH")
                        .put("hive.hdfs.audit.error-message-max-length", "512")
                        .buildOrThrow(),
                "test",
                new TestingConnectorContext());

        try {
            assertThat(manager.configure().keySet()).containsExactlyInAnyOrder(
                    "hive.hdfs.audit.enabled",
                    "hive.hdfs.audit.read-enabled",
                    "hive.hdfs.audit.list-enabled",
                    "hive.hdfs.audit.path-mode",
                    "hive.hdfs.audit.error-message-max-length");
            assertThat(manager.create()).isNotNull();
        }
        finally {
            manager.stop();
        }
    }
}
