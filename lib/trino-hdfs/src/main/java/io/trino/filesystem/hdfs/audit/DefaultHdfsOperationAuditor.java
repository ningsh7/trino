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

import com.google.common.base.Ticker;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.hdfs.audit.HdfsOperationAuditor.OperationAudit;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.HdfsExecutionIdentity;
import io.trino.spi.Node;
import io.trino.spi.catalog.CatalogName;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.trino.filesystem.hdfs.audit.HdfsOperation.LIST_DIRECTORIES;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.LIST_FILES;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.OPEN_READ;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditEvent.SCHEMA_VERSION;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditOrigin.BACKGROUND;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditOrigin.QUERY;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditResult.FAILURE;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditResult.SUCCESS;
import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditSinkException.Type.SERIALIZATION;
import static java.time.Duration.ofMinutes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DefaultHdfsOperationAuditor
        implements HdfsOperationAuditor
{
    private static final Logger FALLBACK_LOG = Logger.get("io.trino.filesystem.hdfs.audit.fallback");
    private static final long FALLBACK_LOG_INTERVAL_NANOS = ofMinutes(1).toNanos();
    private static final OperationAudit NOOP_AUDIT = new OperationAudit()
    {
        @Override
        public void succeeded(OptionalLong bytes) {}

        @Override
        public void failed(Throwable failure, OptionalLong bytes) {}
    };

    private final HdfsEnvironment environment;
    private final HdfsOperationAuditConfig config;
    private final HdfsAuditPathSanitizer pathSanitizer;
    private final HdfsOperationAuditSink sink;
    private final HdfsOperationAuditStats stats;
    private final String catalog;
    private final String nodeId;
    private final Clock clock;
    private final Ticker ticker;
    private final Supplier<UUID> operationIdSupplier;
    private final AtomicLong lastFallbackLogNanos = new AtomicLong(Long.MIN_VALUE);

    @Inject
    public DefaultHdfsOperationAuditor(
            HdfsEnvironment environment,
            HdfsOperationAuditConfig config,
            HdfsAuditPathSanitizer pathSanitizer,
            HdfsOperationAuditSink sink,
            HdfsOperationAuditStats stats,
            CatalogName catalogName,
            Node node)
    {
        this(environment,
                config,
                pathSanitizer,
                sink,
                stats,
                catalogName.toString(),
                node.getNodeIdentifier(),
                Clock.systemUTC(),
                Ticker.systemTicker(),
                UUID::randomUUID);
    }

    DefaultHdfsOperationAuditor(
            HdfsEnvironment environment,
            HdfsOperationAuditConfig config,
            HdfsAuditPathSanitizer pathSanitizer,
            HdfsOperationAuditSink sink,
            HdfsOperationAuditStats stats,
            String catalog,
            String nodeId,
            Clock clock,
            Ticker ticker,
            Supplier<UUID> operationIdSupplier)
    {
        this.environment = requireNonNull(environment, "environment is null");
        this.config = requireNonNull(config, "config is null");
        this.pathSanitizer = requireNonNull(pathSanitizer, "pathSanitizer is null");
        this.sink = requireNonNull(sink, "sink is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.clock = requireNonNull(clock, "clock is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.operationIdSupplier = requireNonNull(operationIdSupplier, "operationIdSupplier is null");
    }

    @Override
    public OperationAudit begin(
            HdfsContext context,
            HdfsOperation operation,
            Location source,
            Optional<Location> target,
            Optional<BatchContext> batchContext)
    {
        requireNonNull(context, "context is null");
        requireNonNull(operation, "operation is null");
        requireNonNull(source, "source is null");
        requireNonNull(target, "target is null");
        requireNonNull(batchContext, "batchContext is null");

        if (!config.isEnabled()) {
            return NOOP_AUDIT;
        }
        if (!isOperationEnabled(operation)) {
            recordFilteredEvent(operation);
            return NOOP_AUDIT;
        }

        OptionalLong startNanos = readStartNanos();
        return new OperationAudit()
        {
            private final AtomicBoolean completed = new AtomicBoolean();

            @Override
            public void succeeded(OptionalLong bytes)
            {
                complete(SUCCESS, requireNonNull(bytes, "bytes is null"), Optional.empty());
            }

            @Override
            public void failed(Throwable failure, OptionalLong bytes)
            {
                complete(FAILURE, requireNonNull(bytes, "bytes is null"), Optional.of(requireNonNull(failure, "failure is null")));
            }

            private void complete(HdfsOperationAuditResult result, OptionalLong bytes, Optional<Throwable> failure)
            {
                if (completed.compareAndSet(false, true)) {
                    record(context, operation, source, target, batchContext, result, startNanos, bytes, failure);
                }
            }
        };
    }

    private void recordFilteredEvent(HdfsOperation operation)
    {
        try {
            stats.recordFilteredEvent();
        }
        catch (RuntimeException e) {
            logFallback("unavailable", operation, e);
        }
    }

    private OptionalLong readStartNanos()
    {
        try {
            return OptionalLong.of(ticker.read());
        }
        catch (RuntimeException _) {
            return OptionalLong.empty();
        }
    }

    private void record(
            HdfsContext context,
            HdfsOperation operation,
            Location source,
            Optional<Location> target,
            Optional<BatchContext> batchContext,
            HdfsOperationAuditResult result,
            OptionalLong startNanos,
            OptionalLong bytes,
            Optional<Throwable> failure)
    {
        try {
            long durationMillis = elapsedMillis(startNanos);
            if (result == SUCCESS) {
                stats.recordOperationSuccess(operation);
            }
            else {
                stats.recordOperationFailure(operation);
            }
            emit(context, operation, source, target, batchContext, result, durationMillis, toOptionalLong(bytes), failure);
        }
        catch (RuntimeException e) {
            // No audit component is allowed to change the result of the source operation.
            logFallback("unavailable", operation, e);
        }
    }

    private boolean isOperationEnabled(HdfsOperation operation)
    {
        if (operation == OPEN_READ) {
            return config.isReadEnabled();
        }
        if (operation == LIST_FILES || operation == LIST_DIRECTORIES) {
            return config.isListEnabled();
        }
        return true;
    }

    private void emit(
            HdfsContext context,
            HdfsOperation operation,
            Location source,
            Optional<Location> target,
            Optional<BatchContext> batchContext,
            HdfsOperationAuditResult result,
            long durationMillis,
            Optional<Long> bytes,
            Optional<Throwable> failure)
    {
        String operationId = "unavailable";
        Instant timestamp = Instant.EPOCH;
        HdfsOperationAuditEvent event;
        try {
            timestamp = clock.instant();
            operationId = operationIdSupplier.get().toString();
            HdfsExecutionIdentity executionIdentity = environment.getExecutionIdentity(context.getIdentity());
            HdfsOperationAuditOrigin origin = context.getQueryId().isPresent() ? QUERY : BACKGROUND;
            Optional<String> errorType = failure.map(value -> value.getClass().getName());
            Optional<String> errorMessage = failure.flatMap(value -> pathSanitizer.sanitizeErrorMessage(value, source, target));
            event = new HdfsOperationAuditEvent(
                    SCHEMA_VERSION,
                    operationId,
                    timestamp,
                    context.getQueryId(),
                    context.getTraceToken(),
                    context.getIdentity().getUser(),
                    context.getSource(),
                    catalog,
                    executionIdentity.user(),
                    executionIdentity.mode(),
                    executionIdentity.stronglyAuthenticated(),
                    operation,
                    pathSanitizer.sanitize(source),
                    target.map(pathSanitizer::sanitize),
                    batchContext.map(BatchContext::batchId),
                    batchContext.map(BatchContext::batchSize),
                    nodeId,
                    result,
                    durationMillis,
                    bytes,
                    errorType,
                    errorMessage,
                    origin);
        }
        catch (IOException | RuntimeException e) {
            stats.recordDroppedEvent(timestamp);
            logFallback(operationId, operation, e);
            return;
        }

        try {
            sink.audit(event);
            stats.recordSinkSuccess();
        }
        catch (HdfsOperationAuditSinkException e) {
            if (e.getType() == SERIALIZATION) {
                stats.recordSerializationFailure(timestamp);
            }
            else {
                stats.recordSinkFailure(timestamp);
            }
            stats.recordDroppedEvent(timestamp);
            logFallback(operationId, operation, e);
        }
        catch (RuntimeException e) {
            stats.recordSinkFailure(timestamp);
            stats.recordDroppedEvent(timestamp);
            logFallback(operationId, operation, e);
        }
    }

    private long elapsedMillis(OptionalLong startNanos)
    {
        if (startNanos.isEmpty()) {
            return 0;
        }
        return NANOSECONDS.toMillis(Math.max(0, ticker.read() - startNanos.orElseThrow()));
    }

    private static Optional<Long> toOptionalLong(OptionalLong value)
    {
        if (value.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(value.orElseThrow());
    }

    private void logFallback(String operationId, HdfsOperation operation, Exception failure)
    {
        try {
            long now = ticker.read();
            long previous = lastFallbackLogNanos.get();
            if (previous != Long.MIN_VALUE && now - previous < FALLBACK_LOG_INTERVAL_NANOS) {
                return;
            }
            if (!lastFallbackLogNanos.compareAndSet(previous, now)) {
                return;
            }
            FALLBACK_LOG.warn(
                    "HDFS audit output failed operationId=%s operation=%s catalog=%s errorType=%s",
                    operationId,
                    operation,
                    catalog,
                    failure.getClass().getName());
        }
        catch (RuntimeException _) {
            // Audit fallback logging must not affect the source operation.
        }
    }
}
