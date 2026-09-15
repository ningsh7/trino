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
package io.trino.plugin.base.audit.jdbc;

import io.trino.plugin.base.audit.RemoteOperationAuditContext;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.Operation;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent.StatementKind;
import io.trino.plugin.base.audit.RemoteOperationAuditScope;
import io.trino.plugin.base.audit.RemoteOperationAuditTarget;
import io.trino.plugin.base.audit.RemoteOperationAuditor;
import jakarta.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static io.trino.plugin.base.audit.RemoteOperationAuditEvent.Protocol.JDBC;

public final class AuditingJdbc
{
    private AuditingJdbc() {}

    public static Connection wrap(Connection connection, @Nullable RemoteOperationAuditContext context, RemoteOperationAuditor auditor)
    {
        if (!auditor.isEnabled() || context == null) {
            return connection;
        }
        try {
            return new AuditedConnection(connection, context, auditor).wrap();
        }
        catch (RuntimeException | LinkageError e) {
            auditor.contextFailed(e);
            return connection;
        }
    }

    private static final class AuditedConnection
    {
        private final Connection delegate;
        private final RemoteOperationAuditContext context;
        private final RemoteOperationAuditor auditor;
        private final String connectionId = UUID.randomUUID().toString();
        private boolean executionUserResolved;
        @Nullable
        private String executionUser;
        private Connection proxy;

        private AuditedConnection(Connection delegate, RemoteOperationAuditContext context, RemoteOperationAuditor auditor)
        {
            this.delegate = delegate;
            this.context = context;
            this.auditor = auditor;
        }

        private Connection wrap()
        {
            proxy = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (_, method, args) -> {
                if (method.getDeclaringClass() == Object.class) {
                    return objectMethod(proxy, method, args);
                }
                if (method.getName().equals("commit") || method.getName().equals("rollback")) {
                    RemoteOperationAuditScope scope = auditor.begin(
                            context,
                            this::target,
                            method.getName().equals("commit") ? Operation.COMMIT : Operation.ROLLBACK,
                            StatementKind.NONE,
                            null,
                            null);
                    return execute(delegate, method, args, scope);
                }
                Object result = invoke(delegate, method, args);
                try {
                    if (result instanceof Statement statement && switch (method.getName()) {
                        case "createStatement", "prepareStatement", "prepareCall" -> true;
                        default -> false;
                    }) {
                        String sql = args != null && args.length > 0 && args[0] instanceof String value ? value : null;
                        StatementKind kind = switch (method.getName()) {
                            case "prepareCall" -> StatementKind.CALLABLE_TEMPLATE;
                            case "prepareStatement" -> StatementKind.PREPARED_TEMPLATE;
                            default -> StatementKind.SQL;
                        };
                        return new AuditedStatement(statement, sql, kind).wrap();
                    }
                    if (result instanceof DatabaseMetaData metadata && method.getName().equals("getMetaData") && auditor.isMetadataEnabled()) {
                        return wrapMetadata(metadata);
                    }
                }
                catch (RuntimeException | LinkageError e) {
                    auditor.contextFailed(e);
                }
                return result;
            });
            return proxy;
        }

        private RemoteOperationAuditTarget target()
        {
            if (!executionUserResolved) {
                executionUserResolved = true;
                executionUser = read(() -> delegate.getMetaData().getUserName());
            }
            return new RemoteOperationAuditTarget(
                    JDBC,
                    connectionId,
                    executionUser,
                    read(delegate::getCatalog),
                    read(delegate::getSchema),
                    null,
                    read(delegate::getAutoCommit));
        }

        @Nullable
        private <T> T read(SqlSupplier<T> supplier)
        {
            try {
                return supplier.get();
            }
            catch (SQLException | RuntimeException | LinkageError e) {
                // Optional driver metadata is not required for execution.
                return null;
            }
        }

        private DatabaseMetaData wrapMetadata(DatabaseMetaData metadata)
        {
            return (DatabaseMetaData) Proxy.newProxyInstance(DatabaseMetaData.class.getClassLoader(), new Class<?>[] {DatabaseMetaData.class}, (metadataProxy, method, args) -> {
                if (method.getDeclaringClass() == Object.class) {
                    return objectMethod(metadataProxy, method, args);
                }
                if (method.getName().equals("getConnection")) {
                    return proxy;
                }
                if (isWrapperMethod(method)) {
                    return invoke(metadata, method, args);
                }
                RemoteOperationAuditScope scope = auditor.begin(context, this::target, Operation.METADATA, StatementKind.METADATA_METHOD, method.getName(), null);
                return execute(metadata, method, args, scope);
            });
        }

        private final class AuditedStatement
        {
            private final Statement delegate;
            @Nullable
            private final String sql;
            private final StatementKind kind;
            private int batchSize;

            private AuditedStatement(Statement delegate, @Nullable String sql, StatementKind kind)
            {
                this.delegate = delegate;
                this.sql = sql;
                this.kind = kind;
            }

            private Statement wrap()
            {
                Class<?> type = switch (kind) {
                    case CALLABLE_TEMPLATE -> CallableStatement.class;
                    case PREPARED_TEMPLATE -> PreparedStatement.class;
                    default -> Statement.class;
                };
                return (Statement) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (statementProxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return objectMethod(statementProxy, method, args);
                    }
                    if (method.getName().equals("getConnection")) {
                        return proxy;
                    }
                    Operation operation = switch (method.getName()) {
                        case "executeQuery" -> Operation.EXECUTE_QUERY;
                        case "executeUpdate" -> Operation.EXECUTE_UPDATE;
                        case "executeLargeUpdate" -> Operation.EXECUTE_LARGE_UPDATE;
                        case "execute" -> Operation.EXECUTE;
                        case "executeBatch" -> Operation.EXECUTE_BATCH;
                        case "executeLargeBatch" -> Operation.EXECUTE_LARGE_BATCH;
                        default -> null;
                    };
                    if (operation != null) {
                        boolean batch = operation == Operation.EXECUTE_BATCH || operation == Operation.EXECUTE_LARGE_BATCH;
                        String statement = args != null && args.length > 0 && args[0] instanceof String value ? value : sql;
                        // A plain Statement batch may contain unrelated SQL. Retain its size, not an unbounded list of text.
                        RemoteOperationAuditScope scope = auditor.begin(
                                context,
                                AuditedConnection.this::target,
                                operation,
                                batch && kind == StatementKind.SQL ? StatementKind.BATCH : kind,
                                statement,
                                batch ? batchSize : null);
                        try {
                            return execute(delegate, method, args, scope);
                        }
                        finally {
                            if (batch) {
                                batchSize = 0;
                            }
                        }
                    }
                    Object result = invoke(delegate, method, args);
                    if (method.getName().equals("addBatch")) {
                        batchSize++;
                    }
                    else if (method.getName().equals("clearBatch")) {
                        batchSize = 0;
                    }
                    return result;
                });
            }
        }
    }

    private static Object execute(Object delegate, Method method, @Nullable Object[] args, RemoteOperationAuditScope scope)
            throws Throwable
    {
        Object result;
        try {
            result = invoke(delegate, method, args);
        }
        catch (Throwable failure) {
            scope.failed(failure);
            throw failure;
        }
        Long affectedRows = null;
        if (method.getName().equals("executeUpdate") || method.getName().equals("executeLargeUpdate")) {
            long count = ((Number) result).longValue();
            if (count >= 0) {
                affectedRows = count;
            }
        }
        scope.complete(affectedRows, null, null);
        return result;
    }

    private static Object invoke(Object target, Method method, @Nullable Object[] args)
            throws Throwable
    {
        try {
            return method.invoke(target, args);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static boolean isWrapperMethod(Method method)
    {
        return method.getName().equals("unwrap") || method.getName().equals("isWrapperFor");
    }

    private static Object objectMethod(Object proxy, Method method, @Nullable Object[] args)
    {
        return switch (method.getName()) {
            case "equals" -> proxy == args[0];
            case "hashCode" -> System.identityHashCode(proxy);
            case "toString" -> "Audited JDBC " + proxy.getClass().getInterfaces()[0].getSimpleName();
            default -> throw new UnsupportedOperationException(method.getName());
        };
    }

    @FunctionalInterface
    private interface SqlSupplier<T>
    {
        T get()
                throws SQLException;
    }
}
