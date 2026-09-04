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
package io.trino.hdfs.authentication;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.hdfs.authentication.HdfsAuthenticationConfig.HdfsAuthenticationType;
import io.trino.hdfs.authentication.HdfsAuthenticationConfig.HdfsIdentityMode;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsAuthenticationConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HdfsAuthenticationConfig.class)
                .setHdfsAuthenticationType(HdfsAuthenticationType.NONE)
                .setHdfsIdentityMode(HdfsIdentityMode.PROCESS)
                .setHdfsFixedUser(null)
                .setHdfsImpersonationEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.hdfs.authentication.type", "KERBEROS")
                .put("hive.hdfs.impersonation.enabled", "true")
                .buildOrThrow();

        HdfsAuthenticationConfig expected = new HdfsAuthenticationConfig()
                .setHdfsAuthenticationType(HdfsAuthenticationType.KERBEROS)
                .setHdfsImpersonationEnabled(true);

        assertFullMapping(properties, expected, ImmutableSet.of(
                "hive.hdfs.fixed-user",
                "hive.hdfs.identity.mode"));
        assertThat(expected.getEffectiveHdfsIdentityMode()).isEqualTo(HdfsIdentityMode.IMPERSONATE);
    }

    @Test
    public void testFixedUserPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.hdfs.identity.mode", "FIXED")
                .put("hive.hdfs.fixed-user", "hive_service")
                .buildOrThrow();

        HdfsAuthenticationConfig expected = new HdfsAuthenticationConfig()
                .setHdfsAuthenticationType(HdfsAuthenticationType.NONE)
                .setHdfsIdentityMode(HdfsIdentityMode.FIXED)
                .setHdfsFixedUser("hive_service")
                .setHdfsImpersonationEnabled(false);

        assertFullMapping(properties, expected, ImmutableSet.of(
                "hive.hdfs.authentication.type",
                "hive.hdfs.impersonation.enabled"));
        assertThat(expected.getEffectiveHdfsIdentityMode()).isEqualTo(HdfsIdentityMode.FIXED);
    }

    @Test
    public void testFixedUserValidation()
    {
        assertFailsValidation(
                new HdfsAuthenticationConfig()
                        .setHdfsIdentityMode(HdfsIdentityMode.FIXED),
                "fixedUserConfigValid",
                "hive.hdfs.fixed-user must be configured with a non-blank value if and only if hive.hdfs.identity.mode is FIXED",
                AssertTrue.class);

        assertFailsValidation(
                new HdfsAuthenticationConfig()
                        .setHdfsIdentityMode(HdfsIdentityMode.FIXED)
                        .setHdfsFixedUser("  "),
                "fixedUserConfigValid",
                "hive.hdfs.fixed-user must be configured with a non-blank value if and only if hive.hdfs.identity.mode is FIXED",
                AssertTrue.class);

        assertFailsValidation(
                new HdfsAuthenticationConfig()
                        .setHdfsFixedUser("hive_service"),
                "fixedUserConfigValid",
                "hive.hdfs.fixed-user must be configured with a non-blank value if and only if hive.hdfs.identity.mode is FIXED",
                AssertTrue.class);
    }

    @Test
    public void testFixedUserAuthenticationTypeValidation()
    {
        assertFailsValidation(
                new HdfsAuthenticationConfig()
                        .setHdfsAuthenticationType(HdfsAuthenticationType.KERBEROS)
                        .setHdfsIdentityMode(HdfsIdentityMode.FIXED)
                        .setHdfsFixedUser("hive_service"),
                "fixedUserAuthenticationTypeValid",
                "hive.hdfs.identity.mode=FIXED is only supported with hive.hdfs.authentication.type=NONE",
                AssertTrue.class);
    }

    @Test
    public void testFixedUserImpersonationValidation()
    {
        assertFailsValidation(
                new HdfsAuthenticationConfig()
                        .setHdfsIdentityMode(HdfsIdentityMode.FIXED)
                        .setHdfsFixedUser("hive_service")
                        .setHdfsImpersonationEnabled(true),
                "impersonationConfigValid",
                "hive.hdfs.impersonation.enabled cannot be enabled when hive.hdfs.identity.mode is FIXED",
                AssertTrue.class);
    }
}
