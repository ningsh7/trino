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
package io.trino.filesystem.hdfs.audit;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;

public class HdfsOperationAuditConfig
{
    private boolean enabled;
    private boolean readEnabled = true;
    private boolean listEnabled;
    private HdfsOperationAuditPathMode pathMode = HdfsOperationAuditPathMode.FULL;
    private int errorMessageMaxLength = 2048;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("hive.hdfs.audit.enabled")
    @ConfigDescription("Enable HDFS file operation audit events")
    public HdfsOperationAuditConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public boolean isReadEnabled()
    {
        return readEnabled;
    }

    @Config("hive.hdfs.audit.read-enabled")
    @ConfigDescription("Include file open operations in HDFS audit events")
    public HdfsOperationAuditConfig setReadEnabled(boolean readEnabled)
    {
        this.readEnabled = readEnabled;
        return this;
    }

    public boolean isListEnabled()
    {
        return listEnabled;
    }

    @Config("hive.hdfs.audit.list-enabled")
    @ConfigDescription("Include file and directory listing operations in HDFS audit events")
    public HdfsOperationAuditConfig setListEnabled(boolean listEnabled)
    {
        this.listEnabled = listEnabled;
        return this;
    }

    @NotNull
    public HdfsOperationAuditPathMode getPathMode()
    {
        return pathMode;
    }

    @Config("hive.hdfs.audit.path-mode")
    @ConfigDescription("Representation of paths in HDFS audit events")
    public HdfsOperationAuditConfig setPathMode(HdfsOperationAuditPathMode pathMode)
    {
        this.pathMode = requireNonNull(pathMode, "pathMode is null");
        return this;
    }

    @Min(0)
    public int getErrorMessageMaxLength()
    {
        return errorMessageMaxLength;
    }

    @Config("hive.hdfs.audit.error-message-max-length")
    @ConfigDescription("Maximum number of characters recorded from an HDFS operation error message")
    public HdfsOperationAuditConfig setErrorMessageMaxLength(int errorMessageMaxLength)
    {
        this.errorMessageMaxLength = errorMessageMaxLength;
        return this;
    }
}
