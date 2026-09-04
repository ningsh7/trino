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

import java.io.IOException;

import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFixedUserHdfsAuthentication
{
    @Test
    public void testUsesFixedUserForDifferentConnectorIdentities()
            throws Exception
    {
        HdfsAuthentication authentication = new FixedUserHdfsAuthentication("hive_service");
        ConnectorIdentity firstIdentity = ConnectorIdentity.ofUser("ldap_zhangsan");
        ConnectorIdentity secondIdentity = ConnectorIdentity.ofUser("ldap_lisi");

        assertThat(authentication.doAs(firstIdentity, () -> getCurrentUser().getUserName()))
                .isEqualTo("hive_service");
        assertThat(authentication.doAs(secondIdentity, () -> getCurrentUser().getUserName()))
                .isEqualTo("hive_service");
        assertThat(firstIdentity.getUser()).isEqualTo("ldap_zhangsan");
        assertThat(secondIdentity.getUser()).isEqualTo("ldap_lisi");
    }

    @Test
    public void testRestoresCallingUser()
            throws Exception
    {
        HdfsAuthentication authentication = new FixedUserHdfsAuthentication("hive_service");
        String callingUser = getCurrentUser().getUserName();

        authentication.doAs(ConnectorIdentity.ofUser("ldap_user"), () -> {
            assertThat(getCurrentUser().getUserName()).isEqualTo("hive_service");
            return null;
        });

        assertThat(getCurrentUser().getUserName()).isEqualTo(callingUser);
    }

    @Test
    public void testPropagatesIOException()
    {
        HdfsAuthentication authentication = new FixedUserHdfsAuthentication("hive_service");
        IOException failure = new IOException("test failure");

        assertThatThrownBy(() -> authentication.doAs(ConnectorIdentity.ofUser("ldap_user"), () -> {
            throw failure;
        })).isSameAs(failure);
    }

    @Test
    public void testRejectsBlankFixedUser()
    {
        assertThatThrownBy(() -> new FixedUserHdfsAuthentication("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fixedUser is blank");
    }
}
