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
package io.trino.plugin.xcloud;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcAssignmentItem;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.stream.Collectors.joining;

public class XCloudQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public XCloudQueryBuilder(RemoteQueryModifier modifier)
    {
        super(modifier);
    }

    @Override
    public PreparedQuery prepareUpdateQuery(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JdbcNamedRelationHandle baseRelation,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<ParameterizedExpression> additionalPredicate,
            List<JdbcAssignmentItem> assignments)
    {
        ImmutableList.Builder<QueryParameter> accumulator = ImmutableList.builder();

        String sql = "UPDATE " + getRelation(client, baseRelation.getRemoteTableName()) + " SET ";

        assignments.forEach(entry -> {
            if (entry.queryParameter().getValue().isEmpty()) {
                return;
            }
            JdbcColumnHandle columnHandle = entry.column();
            if (isTimestampAssignment(columnHandle)) {
                TimestampType type = (TimestampType) columnHandle.getColumnType();
                Object value = entry.queryParameter().getValue().orElseThrow();
                java.time.LocalDateTime local = type.isShort()
                        ? fromTrinoTimestamp((long) value)
                        : fromLongTrinoTimestamp((LongTimestamp) value, type.getPrecision());
                // Ordinary UPDATE statements have no setTimestamp implementation. INSERT uses its specialized writer.
                accumulator.add(new QueryParameter(VARCHAR, Optional.of(utf8Slice(XCloudTypeMappings.checkedTimestamp(local).toString()))));
                return;
            }
            accumulator.add(
                    new QueryParameter(
                            columnHandle.getJdbcTypeHandle(),
                            columnHandle.getColumnType(),
                            entry.queryParameter().getValue()));
        });

        sql += assignments.stream().map(entry -> {
            JdbcColumnHandle column = entry.column();
            String value;
            if (entry.queryParameter().getValue().isEmpty()) {
                value = "NULL";
            }
            else if (isTimestampAssignment(column)) {
                value = "CAST(? AS TIMESTAMP(" + ((TimestampType) column.getColumnType()).getPrecision() + "))";
            }
            else {
                value = client.toColumnMapping(session, connection, column.getJdbcTypeHandle()).orElseThrow().getWriteFunction().getBindExpression();
            }
            return client.quoted(column.getColumnName()) + " = " + value;
        }).collect(joining(", "));

        ImmutableList.Builder<String> conjuncts = ImmutableList.builder();

        toConjuncts(client, session, connection, tupleDomain, conjuncts, accumulator::add);
        additionalPredicate.ifPresent(predicate -> {
            conjuncts.add(predicate.expression());
            accumulator.addAll(predicate.parameters());
        });
        List<String> clauses = conjuncts.build();
        if (!clauses.isEmpty()) {
            sql += " WHERE " + Joiner.on(" AND ").join(clauses);
        }
        return new PreparedQuery(sql, accumulator.build());
    }

    private static boolean isTimestampAssignment(JdbcColumnHandle column)
    {
        return column.getColumnType() instanceof TimestampType && !column.getJdbcTypeHandle().jdbcTypeName().orElse("").equalsIgnoreCase("DATE");
    }
}
