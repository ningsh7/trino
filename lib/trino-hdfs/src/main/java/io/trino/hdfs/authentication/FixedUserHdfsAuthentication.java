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
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.InterruptedIOException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hdfs.authentication.HdfsExecutionIdentity.Mode.FIXED_SERVICE_USER;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.security.UserGroupInformation.createRemoteUser;

public class FixedUserHdfsAuthentication
        implements HdfsAuthentication
{
    private final UserGroupInformation userGroupInformation;

    public FixedUserHdfsAuthentication(String fixedUser)
    {
        requireNonNull(fixedUser, "fixedUser is null");
        checkArgument(!fixedUser.isBlank(), "fixedUser is blank");
        this.userGroupInformation = createRemoteUser(fixedUser);
    }

    @Override
    public HdfsExecutionIdentity getExecutionIdentity(ConnectorIdentity identity)
    {
        requireNonNull(identity, "identity is null");
        return new HdfsExecutionIdentity(userGroupInformation.getUserName(), FIXED_SERVICE_USER, false);
    }

    @Override
    public <T> T doAs(ConnectorIdentity identity, ExceptionAction<T> action)
            throws IOException
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(action, "action is null");

        try {
            return userGroupInformation.callAs(action::run);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
    }
}
