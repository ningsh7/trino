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
package io.trino.hdfs;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsContext
{
    @Test
    public void testSessionContextPreservesQueryInformation()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("ldap_user"))
                .setSource("test_source")
                .setTraceToken("test_trace_token")
                .build();

        HdfsContext context = new HdfsContext(session);

        assertThat(context.getIdentity()).isEqualTo(session.getIdentity());
        assertThat(context.getQueryId()).contains(session.getQueryId());
        assertThat(context.getSource()).contains("test_source");
        assertThat(context.getTraceToken()).contains("test_trace_token");
    }

    @Test
    public void testIdentityContextIsMarkedAsBackgroundByMissingQueryId()
    {
        HdfsContext context = new HdfsContext(ConnectorIdentity.ofUser("background_user"));

        assertThat(context.getQueryId()).isEmpty();
        assertThat(context.getSource()).isEmpty();
        assertThat(context.getTraceToken()).isEmpty();
    }
}
