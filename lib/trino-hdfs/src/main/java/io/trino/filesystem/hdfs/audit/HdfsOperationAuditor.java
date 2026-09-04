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

import io.trino.filesystem.Location;
import io.trino.hdfs.HdfsContext;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public interface HdfsOperationAuditor
{
    default <T> T audit(HdfsContext context, HdfsOperation operation, Location source, AuditAction<T> action)
            throws IOException
    {
        return audit(context, operation, source, Optional.empty(), Optional.empty(), action);
    }

    default <T> T audit(HdfsContext context, HdfsOperation operation, Location source, Location target, AuditAction<T> action)
            throws IOException
    {
        return audit(context, operation, source, Optional.of(target), Optional.empty(), action);
    }

    default <T> T audit(
            HdfsContext context,
            HdfsOperation operation,
            Location source,
            Optional<Location> target,
            Optional<BatchContext> batchContext,
            AuditAction<T> action)
            throws IOException
    {
        OperationAudit operationAudit = begin(context, operation, source, target, batchContext);
        try {
            T result = action.run();
            operationAudit.succeeded();
            return result;
        }
        catch (IOException | RuntimeException e) {
            operationAudit.failed(e);
            throw e;
        }
    }

    default OperationAudit begin(HdfsContext context, HdfsOperation operation, Location source)
    {
        return begin(context, operation, source, Optional.empty(), Optional.empty());
    }

    OperationAudit begin(
            HdfsContext context,
            HdfsOperation operation,
            Location source,
            Optional<Location> target,
            Optional<BatchContext> batchContext);

    static HdfsOperationAuditor noop()
    {
        return NoopHolder.INSTANCE;
    }

    record BatchContext(String batchId, int batchSize)
    {
        public BatchContext
        {
            requireNonNull(batchId, "batchId is null");
            checkArgument(!batchId.isBlank(), "batchId is blank");
            checkArgument(batchSize > 0, "batchSize must be positive");
        }
    }

    @FunctionalInterface
    interface AuditAction<T>
    {
        T run()
                throws IOException;
    }

    interface OperationAudit
    {
        default void succeeded()
        {
            succeeded(OptionalLong.empty());
        }

        default void succeeded(long bytes)
        {
            checkArgument(bytes >= 0, "bytes is negative");
            succeeded(OptionalLong.of(bytes));
        }

        void succeeded(OptionalLong bytes);

        default void failed(Throwable failure)
        {
            failed(failure, OptionalLong.empty());
        }

        default void failed(Throwable failure, long bytes)
        {
            checkArgument(bytes >= 0, "bytes is negative");
            failed(failure, OptionalLong.of(bytes));
        }

        void failed(Throwable failure, OptionalLong bytes);
    }

    final class NoopHolder
    {
        private static final HdfsOperationAuditor INSTANCE = new HdfsOperationAuditor()
        {
            @Override
            public OperationAudit begin(
                    HdfsContext context,
                    HdfsOperation operation,
                    Location source,
                    Optional<Location> target,
                    Optional<BatchContext> batchContext)
            {
                return NOOP_OPERATION_AUDIT;
            }
        };
        private static final OperationAudit NOOP_OPERATION_AUDIT = new OperationAudit()
        {
            @Override
            public void succeeded(OptionalLong bytes) {}

            @Override
            public void failed(Throwable failure, OptionalLong bytes) {}
        };

        private NoopHolder() {}
    }
}
