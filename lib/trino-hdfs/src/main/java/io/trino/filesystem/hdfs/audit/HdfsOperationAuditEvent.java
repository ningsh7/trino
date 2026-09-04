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

import io.trino.hdfs.authentication.HdfsExecutionIdentity;

import java.time.Instant;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditOrigin.BACKGROUND;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditOrigin.QUERY;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditResult.FAILURE;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditResult.SUCCESS;
import static java.util.Objects.requireNonNull;

public record HdfsOperationAuditEvent(
        int schemaVersion,
        String operationId,
        Instant timestamp,
        Optional<String> queryId,
        Optional<String> traceToken,
        String trinoUser,
        Optional<String> source,
        String catalog,
        String executionUser,
        HdfsExecutionIdentity.Mode identityMode,
        boolean stronglyAuthenticated,
        HdfsOperation operation,
        String sourcePath,
        Optional<String> targetPath,
        Optional<String> batchId,
        Optional<Integer> batchSize,
        String nodeId,
        HdfsOperationAuditResult result,
        long durationMillis,
        Optional<Long> bytes,
        Optional<String> errorType,
        Optional<String> errorMessage,
        HdfsOperationAuditOrigin origin)
{
    public static final int SCHEMA_VERSION = 1;

    public HdfsOperationAuditEvent
    {
        checkArgument(schemaVersion == SCHEMA_VERSION, "schemaVersion must be %s", SCHEMA_VERSION);
        requireNonNull(operationId, "operationId is null");
        checkArgument(!operationId.isBlank(), "operationId is blank");
        requireNonNull(timestamp, "timestamp is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(traceToken, "traceToken is null");
        requireNonNull(trinoUser, "trinoUser is null");
        checkArgument(!trinoUser.isBlank(), "trinoUser is blank");
        requireNonNull(source, "source is null");
        requireNonNull(catalog, "catalog is null");
        checkArgument(!catalog.isBlank(), "catalog is blank");
        requireNonNull(executionUser, "executionUser is null");
        checkArgument(!executionUser.isBlank(), "executionUser is blank");
        requireNonNull(identityMode, "identityMode is null");
        requireNonNull(operation, "operation is null");
        requireNonNull(sourcePath, "sourcePath is null");
        checkArgument(!sourcePath.isBlank(), "sourcePath is blank");
        requireNonNull(targetPath, "targetPath is null");
        requireNonNull(batchId, "batchId is null");
        requireNonNull(batchSize, "batchSize is null");
        checkArgument(batchId.isPresent() == batchSize.isPresent(), "batchId and batchSize must both be present or absent");
        batchId.ifPresent(value -> checkArgument(!value.isBlank(), "batchId is blank"));
        batchSize.ifPresent(value -> checkArgument(value > 0, "batchSize must be positive"));
        requireNonNull(nodeId, "nodeId is null");
        checkArgument(!nodeId.isBlank(), "nodeId is blank");
        requireNonNull(result, "result is null");
        checkArgument(durationMillis >= 0, "durationMillis is negative");
        requireNonNull(bytes, "bytes is null");
        bytes.ifPresent(value -> checkArgument(value >= 0, "bytes is negative"));
        requireNonNull(errorType, "errorType is null");
        requireNonNull(errorMessage, "errorMessage is null");
        requireNonNull(origin, "origin is null");

        checkArgument(origin != QUERY || queryId.isPresent(), "QUERY event requires queryId");
        checkArgument(origin != BACKGROUND || queryId.isEmpty(), "BACKGROUND event cannot have queryId");
        checkArgument(result != SUCCESS || (errorType.isEmpty() && errorMessage.isEmpty()), "SUCCESS event cannot have error details");
        checkArgument(result != FAILURE || errorType.isPresent(), "FAILURE event requires errorType");
    }
}
