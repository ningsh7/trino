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

import com.google.common.collect.ImmutableMap;
import jakarta.validation.constraints.Min;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditPathMode.FULL;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditPathMode.HASH;

public class TestHdfsOperationAuditConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HdfsOperationAuditConfig.class)
                .setEnabled(false)
                .setReadEnabled(true)
                .setListEnabled(false)
                .setPathMode(FULL)
                .setErrorMessageMaxLength(2048));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.hdfs.audit.enabled", "true")
                .put("hive.hdfs.audit.read-enabled", "false")
                .put("hive.hdfs.audit.list-enabled", "true")
                .put("hive.hdfs.audit.path-mode", "HASH")
                .put("hive.hdfs.audit.error-message-max-length", "512")
                .buildOrThrow();

        HdfsOperationAuditConfig expected = new HdfsOperationAuditConfig()
                .setEnabled(true)
                .setReadEnabled(false)
                .setListEnabled(true)
                .setPathMode(HASH)
                .setErrorMessageMaxLength(512);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testErrorMessageLengthValidation()
    {
        assertFailsValidation(
                new HdfsOperationAuditConfig().setErrorMessageMaxLength(-1),
                "errorMessageMaxLength",
                "must be greater than or equal to 0",
                Min.class);
    }
}
