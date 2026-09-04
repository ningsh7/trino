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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record HdfsExecutionIdentity(String user, Mode mode, boolean stronglyAuthenticated)
{
    public HdfsExecutionIdentity
    {
        requireNonNull(user, "user is null");
        checkArgument(!user.isBlank(), "user is blank");
        requireNonNull(mode, "mode is null");
    }

    public enum Mode
    {
        PROCESS_USER,
        END_USER_IMPERSONATION,
        FIXED_SERVICE_USER,
        KERBEROS_SERVICE_PRINCIPAL,
        KERBEROS_END_USER_IMPERSONATION,
    }
}
