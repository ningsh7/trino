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

import jakarta.annotation.Nullable;

import java.time.Instant;

public record RemoteOperationAuditEvent(
        int schemaVersion,
        String operationId,
        @Nullable String connectionId,
        Instant timestamp,
        @Nullable String queryId,
        @Nullable String traceToken,
        @Nullable String trinoUser,
        @Nullable String source,
        String catalog,
        String connector,
        Origin origin,
        Protocol protocol,
        @Nullable String executionUser,
        @Nullable String remoteCatalog,
        @Nullable String remoteSchema,
        @Nullable String endpoint,
        Operation operation,
        String statementType,
        StatementKind statementKind,
        @Nullable String remoteStatement,
        @Nullable Integer batchSize,
        @Nullable Long affectedRows,
        @Nullable Boolean autoCommit,
        @Nullable Integer httpStatus,
        Result result,
        long durationMillis,
        String nodeId,
        @Nullable String errorType,
        @Nullable String errorMessage)
{
    public enum Origin
    {
        QUERY, BACKGROUND
    }

    public enum Protocol
    {
        JDBC, DORIS_FE_HTTP, DORIS_FLIGHT_SQL
    }

    public enum Operation
    {
        EXECUTE_QUERY,
        EXECUTE_UPDATE,
        EXECUTE_LARGE_UPDATE,
        EXECUTE,
        EXECUTE_BATCH,
        EXECUTE_LARGE_BATCH,
        COMMIT,
        ROLLBACK,
        METADATA,
        PLAN_SPLITS,
        OPEN_READ,
    }

    public enum StatementKind
    {
        SQL, PREPARED_TEMPLATE, CALLABLE_TEMPLATE, BATCH, METADATA_METHOD, NONE
    }

    public enum Result
    {
        SUCCESS, FAILURE
    }
}
