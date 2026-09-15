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

import io.airlift.log.Logger;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.Operation;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.StatementKind;
import io.trino.spi.connector.ConnectorSession;
import jakarta.annotation.Nullable;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.trino.plugin.base.audit.RemoteOperationAuditEvent.Origin.BACKGROUND;
import static io.trino.plugin.base.audit.RemoteOperationAuditEvent.Origin.QUERY;
import static io.trino.plugin.base.audit.RemoteOperationAuditEvent.Result.FAILURE;
import static io.trino.plugin.base.audit.RemoteOperationAuditEvent.Result.SUCCESS;
import static io.trino.plugin.base.audit.RemoteOperationAuditScope.NOOP;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RemoteOperationAuditor
{
    private static final Logger FALLBACK = Logger.get("io.trino.plugin.base.audit.fallback");

    private final RemoteOperationAuditConfig config;
    private final RemoteOperationAuditSink sink;
    private final RemoteOperationAuditStats stats;
    private final String catalog;
    private final String connector;
    private final String nodeId;
    private final AtomicLong lastWarning = new AtomicLong();

    public RemoteOperationAuditor(RemoteOperationAuditConfig config, RemoteOperationAuditSink sink, RemoteOperationAuditStats stats, String catalog, String connector, String nodeId)
    {
        this.config = requireNonNull(config, "config is null");
        this.sink = requireNonNull(sink, "sink is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.connector = requireNonNull(connector, "connector is null");
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
    }

    public static RemoteOperationAuditor noop()
    {
        return new RemoteOperationAuditor(new RemoteOperationAuditConfig(), _ -> {}, new RemoteOperationAuditStats(), "", "", "");
    }

    public boolean isEnabled()
    {
        return config.isEnabled();
    }

    public boolean isMetadataEnabled()
    {
        return config.isEnabled() && config.isMetadataEnabled();
    }

    @Nullable
    public RemoteOperationAuditContext context(@Nullable ConnectorSession session)
    {
        if (!isEnabled()) {
            return null;
        }
        try {
            return RemoteOperationAuditContext.from(session);
        }
        catch (RuntimeException | LinkageError e) {
            contextFailed(e);
            return null;
        }
    }

    public void contextFailed(Throwable failure)
    {
        stats.contextFailures.incrementAndGet();
        reportFailure(failure);
    }

    public RemoteOperationAuditScope begin(
            @Nullable RemoteOperationAuditContext context,
            Supplier<RemoteOperationAuditTarget> target,
            Operation operation,
            StatementKind statementKind,
            @Nullable String statement,
            @Nullable Integer batchSize)
    {
        if (!config.isEnabled() || context == null) {
            return NOOP;
        }
        try {
            String statementType = statementKind == StatementKind.METADATA_METHOD ? "UNKNOWN" : RemoteStatementSanitizer.statementType(statement);
            // executeQuery can also execute INSERT ... RETURNING or a stored procedure.
            // Retain unknown and non-SELECT SQL when read auditing is disabled.
            boolean read = operation == Operation.OPEN_READ
                    || operation == Operation.EXECUTE_QUERY && statementKind != StatementKind.CALLABLE_TEMPLATE && statementType.equals("SELECT");
            if (read && !config.isReadEnabled() || operation == Operation.METADATA && !config.isMetadataEnabled()) {
                stats.filteredEvents.incrementAndGet();
                return NOOP;
            }
            RemoteOperationAuditTarget destination = target.get();
            String operationId = UUID.randomUUID().toString();
            Instant timestamp = Instant.now();
            String remoteStatement = RemoteStatementSanitizer.statement(statement, config.getStatementMode(), config.getStatementMaxLength());
            if (statement != null && !statement.equals(remoteStatement)) {
                stats.sanitizedStatements.incrementAndGet();
            }
            AtomicBoolean completed = new AtomicBoolean();
            long start = System.nanoTime();
            return (affectedRows, httpStatus, failure) -> {
                if (!completed.compareAndSet(false, true)) {
                    return;
                }
                try {
                    if (failure == null) {
                        stats.operationSuccesses.incrementAndGet();
                    }
                    else {
                        stats.operationFailures.incrementAndGet();
                    }
                    // Driver messages may echo bound values, credentials or response bodies.
                    // Record only the failure type; the original exception is still returned to the caller.
                    String errorType = failure == null ? null : failure.getClass().getName();
                    RemoteOperationAuditEvent event = new RemoteOperationAuditEvent(
                            1,
                            operationId,
                            destination.connectionId(),
                            timestamp,
                            clean(context.queryId()),
                            clean(context.traceToken()),
                            clean(context.trinoUser()),
                            clean(context.source()),
                            clean(catalog),
                            clean(connector),
                            context.queryId() == null ? BACKGROUND : QUERY,
                            destination.protocol(),
                            clean(destination.executionUser()),
                            clean(destination.remoteCatalog()),
                            clean(destination.remoteSchema()),
                            RemoteStatementSanitizer.endpoint(destination.endpoint()),
                            operation,
                            statementType,
                            statementKind,
                            remoteStatement,
                            batchSize,
                            affectedRows,
                            destination.autoCommit(),
                            httpStatus,
                            failure == null ? SUCCESS : FAILURE,
                            Math.max(0, NANOSECONDS.toMillis(System.nanoTime() - start)),
                            clean(nodeId),
                            errorType,
                            errorType == null ? null : RemoteStatementSanitizer.truncate(errorType, config.getErrorMessageMaxLength()));
                    try {
                        sink.audit(event);
                        stats.sinkSuccesses.incrementAndGet();
                    }
                    catch (RuntimeException | LinkageError e) {
                        stats.sinkFailures.incrementAndGet();
                        if (e instanceof JsonLogRemoteOperationAuditSink.SerializationException) {
                            stats.serializationFailures.incrementAndGet();
                        }
                        reportFailure(e);
                    }
                }
                catch (RuntimeException | LinkageError e) {
                    contextFailed(e);
                }
            };
        }
        catch (RuntimeException | LinkageError e) {
            contextFailed(e);
            return NOOP;
        }
    }

    @Nullable
    private static String clean(@Nullable String value)
    {
        return value == null ? null : RemoteStatementSanitizer.clean(value);
    }

    private void reportFailure(Throwable failure)
    {
        stats.droppedEvents.incrementAndGet();
        stats.lastFailureTime.set(System.currentTimeMillis());
        try {
            long now = System.nanoTime();
            long previous = lastWarning.get();
            if ((previous == 0 || now - previous >= SECONDS.toNanos(30)) && lastWarning.compareAndSet(previous, now)) {
                // Never pass the Throwable or its message to the fallback logger.
                FALLBACK.warn("Remote operation audit failure: %s", failure.getClass().getName());
            }
        }
        catch (RuntimeException | LinkageError ignored) {
            // Reporting an audit failure must not change the remote operation.
        }
    }
}
