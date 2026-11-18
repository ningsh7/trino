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
package io.trino.plugin.xcloud;

import com.bonc.xcloud.jdbc.XCloudDriver;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestXCloudPlugin
{
    @Test
    public void testFactory()
    {
        var factory = getOnlyElement(new XCloudPlugin().getConnectorFactories());
        assertThat(factory.getName()).isEqualTo("xcloud");
        factory.create("xcloud", Map.of("connection-url", "jdbc:xcloud:@localhost:1/db", "bootstrap.quiet", "true"), new TestingConnectorContext()).shutdown();
        assertThatThrownBy(() -> factory.create("bad", Map.of("connection-url", "jdbc:mysql://localhost"), new TestingConnectorContext())).hasMessageContaining("XCloud JDBC URL");
    }

    @Test
    public void testUrlValidation()
    {
        for (String url : java.util.List.of("jdbc:xcloud:@", "jdbc:xcloud:@host/", "jdbc:xcloud:@host/?x=y", "jdbc:mysql://host/db")) {
            XCloudJdbcConfig config = new XCloudJdbcConfig();
            config.setConnectionUrl(url);
            assertThat(config.isUrlValid()).isFalse();
        }
        XCloudJdbcConfig config = new XCloudJdbcConfig();
        config.setConnectionUrl("jdbc:xcloud:@host:1/db@other:2/db?connectDirect=false");
        assertThat(config.isUrlValid()).isTrue();
    }

    @Test
    public void testRealDriverLinkage()
            throws Exception
    {
        assertThat(new XCloudDriver().acceptsURL("jdbc:xcloud:@localhost:1/db")).isTrue();
        for (String name : java.util.List.of("com.bonc.xcloud.jdbc.XCloudConnection", "com.bonc.xcloud.jdbc.XCloudPreparedStatement", "com.bonc.xcloud.jdbc.XCloudInsertPreparedStatement", "com.bonc.xcloud.jdbc.XCloudDatabaseMetaData", "com.bonc.xcloud.jdbc.XCloudResultSet", "com.bonc.xcloud.sql.parser.ParserSQL")) {
            Class<?> type = Class.forName(name);
            assertThat(type.getDeclaredMethods()).isNotEmpty();
        }
        // Native methods with unconditional failure can be exercised without a connection.
        assertThatThrownBy(() -> new com.bonc.xcloud.jdbc.XCloudDatabaseMetaData(null, 2, 14, 7).getSearchStringEscape()).isInstanceOf(RuntimeException.class).hasMessageContaining("Not Implemented");
    }
}
