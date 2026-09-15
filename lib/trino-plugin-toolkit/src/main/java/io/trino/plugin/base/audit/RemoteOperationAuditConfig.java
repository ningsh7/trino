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
package io.trino.plugin.base.audit;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public class RemoteOperationAuditConfig
{
    public enum StatementMode
    {
        TEMPLATE, HASH, NONE
    }

    private boolean enabled;
    private boolean readEnabled = true;
    private boolean metadataEnabled;
    private StatementMode statementMode = StatementMode.TEMPLATE;
    private int statementMaxLength = 16384;
    private int errorMessageMaxLength = 2048;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("remote-operation-audit.enabled")
    public RemoteOperationAuditConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public boolean isReadEnabled()
    {
        return readEnabled;
    }

    @Config("remote-operation-audit.read-enabled")
    public RemoteOperationAuditConfig setReadEnabled(boolean readEnabled)
    {
        this.readEnabled = readEnabled;
        return this;
    }

    public boolean isMetadataEnabled()
    {
        return metadataEnabled;
    }

    @Config("remote-operation-audit.metadata-enabled")
    public RemoteOperationAuditConfig setMetadataEnabled(boolean metadataEnabled)
    {
        this.metadataEnabled = metadataEnabled;
        return this;
    }

    @NotNull
    public StatementMode getStatementMode()
    {
        return statementMode;
    }

    @Config("remote-operation-audit.statement-mode")
    public RemoteOperationAuditConfig setStatementMode(StatementMode statementMode)
    {
        this.statementMode = statementMode;
        return this;
    }

    @Min(0)
    public int getStatementMaxLength()
    {
        return statementMaxLength;
    }

    @Config("remote-operation-audit.statement-max-length")
    public RemoteOperationAuditConfig setStatementMaxLength(int statementMaxLength)
    {
        this.statementMaxLength = statementMaxLength;
        return this;
    }

    @Min(0)
    public int getErrorMessageMaxLength()
    {
        return errorMessageMaxLength;
    }

    @Config("remote-operation-audit.error-message-max-length")
    public RemoteOperationAuditConfig setErrorMessageMaxLength(int errorMessageMaxLength)
    {
        this.errorMessageMaxLength = errorMessageMaxLength;
        return this;
    }
}
