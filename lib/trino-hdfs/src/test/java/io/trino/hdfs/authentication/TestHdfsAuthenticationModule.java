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
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHdfsAuthenticationModule
{
    @Test
    public void testProcessIdentityModeByDefault()
    {
        assertThat(createHdfsAuthentication(ImmutableMap.of()))
                .isInstanceOf(NoHdfsAuthentication.class);
    }

    @Test
    public void testFixedIdentityMode()
            throws Exception
    {
        HdfsAuthentication authentication = createHdfsAuthentication(ImmutableMap.of(
                "hive.hdfs.identity.mode", "FIXED",
                "hive.hdfs.fixed-user", "hive_service"));

        assertThat(authentication)
                .isInstanceOf(FixedUserHdfsAuthentication.class);
        assertThat(authentication.doAs(ConnectorIdentity.ofUser("ldap_user"), () -> getCurrentUser().getUserName()))
                .isEqualTo("hive_service");
    }

    @Test
    public void testExplicitImpersonationIdentityMode()
    {
        assertThat(createHdfsAuthentication(ImmutableMap.of(
                "hive.hdfs.identity.mode", "IMPERSONATE")))
                .isInstanceOf(ImpersonatingHdfsAuthentication.class);
    }

    @Test
    public void testLegacyImpersonationConfiguration()
    {
        assertThat(createHdfsAuthentication(ImmutableMap.of(
                "hive.hdfs.impersonation.enabled", "true")))
                .isInstanceOf(ImpersonatingHdfsAuthentication.class);
    }

    @Test
    public void testFixedIdentityModeRequiresFixedUserAtBootstrap()
    {
        assertThatThrownBy(() -> createHdfsAuthentication(ImmutableMap.of(
                "hive.hdfs.identity.mode", "FIXED")))
                .hasMessageContaining("hive.hdfs.fixed-user must be configured with a non-blank value");
    }

    private static HdfsAuthentication createHdfsAuthentication(Map<String, String> properties)
    {
        return new Bootstrap(new HdfsAuthenticationModule())
                .doNotInitializeLogging()
                .quiet()
                .setRequiredConfigurationProperties(properties)
                .initialize()
                .getInstance(HdfsAuthentication.class);
    }
}
