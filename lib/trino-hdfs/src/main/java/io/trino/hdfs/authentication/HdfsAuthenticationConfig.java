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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class HdfsAuthenticationConfig
{
    private HdfsAuthenticationType hdfsAuthenticationType = HdfsAuthenticationType.NONE;
    private HdfsIdentityMode hdfsIdentityMode = HdfsIdentityMode.PROCESS;
    private String hdfsFixedUser;
    private boolean hdfsImpersonationEnabled;

    public enum HdfsAuthenticationType
    {
        NONE,
        KERBEROS,
    }

    public enum HdfsIdentityMode
    {
        PROCESS,
        FIXED,
        IMPERSONATE,
    }

    @NotNull
    public HdfsAuthenticationType getHdfsAuthenticationType()
    {
        return hdfsAuthenticationType;
    }

    @Config("hive.hdfs.authentication.type")
    @ConfigDescription("HDFS authentication type")
    public HdfsAuthenticationConfig setHdfsAuthenticationType(HdfsAuthenticationType hdfsAuthenticationType)
    {
        this.hdfsAuthenticationType = hdfsAuthenticationType;
        return this;
    }

    @NotNull
    public HdfsIdentityMode getHdfsIdentityMode()
    {
        return hdfsIdentityMode;
    }

    @Config("hive.hdfs.identity.mode")
    @ConfigDescription("Identity selection mode for HDFS operations")
    public HdfsAuthenticationConfig setHdfsIdentityMode(HdfsIdentityMode hdfsIdentityMode)
    {
        this.hdfsIdentityMode = hdfsIdentityMode;
        return this;
    }

    @NotNull
    public Optional<String> getHdfsFixedUser()
    {
        return Optional.ofNullable(hdfsFixedUser);
    }

    @Config("hive.hdfs.fixed-user")
    @ConfigDescription("User name for HDFS operations when hive.hdfs.identity.mode is FIXED")
    public HdfsAuthenticationConfig setHdfsFixedUser(String hdfsFixedUser)
    {
        this.hdfsFixedUser = hdfsFixedUser;
        return this;
    }

    public boolean isHdfsImpersonationEnabled()
    {
        return hdfsImpersonationEnabled;
    }

    @Config("hive.hdfs.impersonation.enabled")
    @ConfigDescription("Should Trino user be impersonated when communicating with HDFS")
    public HdfsAuthenticationConfig setHdfsImpersonationEnabled(boolean hdfsImpersonationEnabled)
    {
        this.hdfsImpersonationEnabled = hdfsImpersonationEnabled;
        return this;
    }

    public HdfsIdentityMode getEffectiveHdfsIdentityMode()
    {
        if (hdfsIdentityMode == HdfsIdentityMode.PROCESS && hdfsImpersonationEnabled) {
            return HdfsIdentityMode.IMPERSONATE;
        }
        return hdfsIdentityMode;
    }

    @AssertTrue(message = "hive.hdfs.fixed-user must be configured with a non-blank value if and only if hive.hdfs.identity.mode is FIXED")
    public boolean isFixedUserConfigValid()
    {
        boolean fixedUserConfigured = getHdfsFixedUser()
                .map(user -> !user.isBlank())
                .orElse(false);
        return (hdfsIdentityMode == HdfsIdentityMode.FIXED) == fixedUserConfigured;
    }

    @AssertTrue(message = "hive.hdfs.identity.mode=FIXED is only supported with hive.hdfs.authentication.type=NONE")
    public boolean isFixedUserAuthenticationTypeValid()
    {
        return hdfsIdentityMode != HdfsIdentityMode.FIXED || hdfsAuthenticationType == HdfsAuthenticationType.NONE;
    }

    @AssertTrue(message = "hive.hdfs.impersonation.enabled cannot be enabled when hive.hdfs.identity.mode is FIXED")
    public boolean isImpersonationConfigValid()
    {
        return hdfsIdentityMode != HdfsIdentityMode.FIXED || !hdfsImpersonationEnabled;
    }
}
