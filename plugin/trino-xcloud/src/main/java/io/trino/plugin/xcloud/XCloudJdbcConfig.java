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

import io.trino.plugin.jdbc.BaseJdbcConfig;
import jakarta.validation.constraints.AssertTrue;

public class XCloudJdbcConfig
        extends BaseJdbcConfig
{
    @AssertTrue(message = "XCloud JDBC URL must start with jdbc:xcloud:@ and include a database")
    public boolean isUrlValid()
    {
        String url = getConnectionUrl();
        if (url == null || !url.startsWith("jdbc:xcloud:@")) {
            return false;
        }
        int separator = url.indexOf('/', "jdbc:xcloud:@".length());
        return separator > "jdbc:xcloud:@".length() && separator + 1 < url.length() && url.charAt(separator + 1) != '?';
    }
}
