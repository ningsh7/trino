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

import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import static io.trino.hdfs.authentication.HdfsExecutionIdentity.Mode.END_USER_IMPERSONATION;
import static io.trino.hdfs.authentication.HdfsExecutionIdentity.Mode.FIXED_SERVICE_USER;
import static io.trino.hdfs.authentication.HdfsExecutionIdentity.Mode.PROCESS_USER;
import static io.trino.plugin.base.security.UserNameProvider.SIMPLE_USER_NAME_PROVIDER;
import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsExecutionIdentity
{
    @Test
    public void testProcessUser()
            throws Exception
    {
        ConnectorIdentity identity = ConnectorIdentity.ofUser("ldap_user");

        assertThat(new NoHdfsAuthentication().getExecutionIdentity(identity))
                .isEqualTo(new HdfsExecutionIdentity(getCurrentUser().getUserName(), PROCESS_USER, false));
    }

    @Test
    public void testFixedServiceUser()
            throws Exception
    {
        ConnectorIdentity identity = ConnectorIdentity.ofUser("ldap_user");

        assertThat(new FixedUserHdfsAuthentication("hive_service").getExecutionIdentity(identity))
                .isEqualTo(new HdfsExecutionIdentity("hive_service", FIXED_SERVICE_USER, false));
    }

    @Test
    public void testSimpleEndUserImpersonation()
            throws Exception
    {
        ConnectorIdentity identity = ConnectorIdentity.ofUser("ldap_user");
        HdfsAuthentication authentication = new ImpersonatingHdfsAuthentication(
                new SimpleHadoopAuthentication(),
                SIMPLE_USER_NAME_PROVIDER);

        assertThat(authentication.getExecutionIdentity(identity))
                .isEqualTo(new HdfsExecutionIdentity("ldap_user", END_USER_IMPERSONATION, false));
    }
}
