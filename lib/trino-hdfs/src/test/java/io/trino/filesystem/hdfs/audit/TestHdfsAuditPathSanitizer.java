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

import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditPathMode.HASH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsAuditPathSanitizer
{
    @Test
    public void testFullPathRemovesCredentialsAndParameters()
    {
        HdfsAuditPathSanitizer sanitizer = new HdfsAuditPathSanitizer(new HdfsOperationAuditConfig());

        assertThat(sanitizer.sanitize(Location.of("hdfs://user:password@namenode:8020/warehouse/sales?token=secret#fragment")))
                .isEqualTo("hdfs://namenode:8020/warehouse/sales");
        assertThat(sanitizer.sanitize(Location.of("/warehouse/sales?token=secret")))
                .isEqualTo("/warehouse/sales");
        assertThat(sanitizer.sanitize(Location.of("viewfs://cluster/warehouse/line\nfeed")))
                .isEqualTo("viewfs://cluster/warehouse/line feed");
    }

    @Test
    public void testHashUsesSanitizedCanonicalPath()
    {
        HdfsAuditPathSanitizer sanitizer = new HdfsAuditPathSanitizer(new HdfsOperationAuditConfig().setPathMode(HASH));

        String first = sanitizer.sanitize(Location.of("hdfs://alice:one@namenode:8020/warehouse/sales?token=one"));
        String second = sanitizer.sanitize(Location.of("hdfs://bob:two@namenode:8020/warehouse/sales?token=two"));

        assertThat(first)
                .startsWith("sha256:")
                .hasSize(71)
                .doesNotContain("warehouse", "alice", "one");
        assertThat(second).isEqualTo(first);
    }

    @Test
    public void testErrorMessageIsSanitizedAndTruncated()
    {
        HdfsOperationAuditConfig config = new HdfsOperationAuditConfig().setErrorMessageMaxLength(130);
        HdfsAuditPathSanitizer sanitizer = new HdfsAuditPathSanitizer(config);
        Location source = Location.of("hdfs://alice:password@namenode:8020/warehouse/source?token=secret");
        Location target = Location.of("hdfs://bob:password@namenode:8020/warehouse/target?token=secret");
        IOException failure = new IOException("Rename " + source + " to " + target + " failed\nsecondary hdfs://carol:secret@other/path?key=value " + "x".repeat(200));

        String message = sanitizer.sanitizeErrorMessage(failure, source, Optional.of(target)).orElseThrow();

        assertThat(message)
                .hasSize(130)
                .doesNotContain("alice", "bob", "carol", "password", "token", "secret", "key=value", "\n", "\r")
                .contains("hdfs://namenode:8020/warehouse/source")
                .contains("hdfs://namenode:8020/warehouse/target");
    }

    @Test
    public void testErrorMessageCanBeDisabled()
    {
        HdfsAuditPathSanitizer sanitizer = new HdfsAuditPathSanitizer(new HdfsOperationAuditConfig().setErrorMessageMaxLength(0));

        assertThat(sanitizer.sanitizeErrorMessage(
                new IOException("failure"),
                Location.of("hdfs://namenode/path"),
                Optional.empty()))
                .isEmpty();
    }

    @Test
    public void testHashModeDoesNotExposePathsFromErrorMessages()
    {
        HdfsOperationAuditConfig config = new HdfsOperationAuditConfig().setPathMode(HASH);
        HdfsAuditPathSanitizer sanitizer = new HdfsAuditPathSanitizer(config);
        Location source = Location.of("hdfs://alice:password@namenode/warehouse/source?token=secret");

        String message = sanitizer.sanitizeErrorMessage(
                new IOException("Read " + source + " failed via hdfs://bob:password@other/warehouse/other?token=secret"),
                source,
                Optional.empty()).orElseThrow();

        assertThat(message)
                .contains("sha256:", "[uri-redacted]")
                .doesNotContain("warehouse", "password", "token", "secret", "alice", "bob");
    }
}
