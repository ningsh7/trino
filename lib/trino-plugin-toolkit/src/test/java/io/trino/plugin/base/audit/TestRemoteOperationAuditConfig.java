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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.base.audit.RemoteOperationAuditConfig.StatementMode.HASH;
import static io.trino.plugin.base.audit.RemoteOperationAuditConfig.StatementMode.TEMPLATE;

class TestRemoteOperationAuditConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RemoteOperationAuditConfig.class)
                .setEnabled(false)
                .setReadEnabled(true)
                .setMetadataEnabled(false)
                .setStatementMode(TEMPLATE)
                .setStatementMaxLength(16384)
                .setErrorMessageMaxLength(2048));
    }

    @Test
    void testMapping()
    {
        assertFullMapping(Map.of(
                        "remote-operation-audit.enabled", "true",
                        "remote-operation-audit.read-enabled", "false",
                        "remote-operation-audit.metadata-enabled", "true",
                        "remote-operation-audit.statement-mode", "HASH",
                        "remote-operation-audit.statement-max-length", "100",
                        "remote-operation-audit.error-message-max-length", "200"),
                new RemoteOperationAuditConfig()
                        .setEnabled(true).setReadEnabled(false).setMetadataEnabled(true)
                        .setStatementMode(HASH).setStatementMaxLength(100).setErrorMessageMaxLength(200));
    }
}
