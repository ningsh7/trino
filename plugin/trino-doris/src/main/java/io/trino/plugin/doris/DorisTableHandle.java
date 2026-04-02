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
package io.trino.plugin.doris;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

public record DorisTableHandle(
        String schemaName,
        String tableName,
        TupleDomain<ColumnHandle> constraint,
        Optional<List<DorisColumnHandle>> projectedColumns,
        Optional<List<DorisColumnHandle>> groupingColumns,
        Optional<List<DorisAggregation>> aggregations,
        Optional<List<DorisSortItem>> sortOrder,
        OptionalLong limit)
        implements ConnectorTableHandle
{
    public DorisTableHandle(String schemaName, String tableName)
    {
        this(schemaName, tableName, TupleDomain.all(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), OptionalLong.empty());
    }

    public DorisTableHandle(
            String schemaName,
            String tableName,
            TupleDomain<ColumnHandle> constraint,
            Optional<List<DorisColumnHandle>> projectedColumns,
            OptionalLong limit)
    {
        this(schemaName, tableName, constraint, projectedColumns, Optional.empty(), Optional.empty(), Optional.empty(), limit);
    }

    public DorisTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        constraint = requireNonNull(constraint, "constraint is null");
        projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null")
                .map(List::copyOf);
        groupingColumns = requireNonNull(groupingColumns, "groupingColumns is null")
                .map(List::copyOf);
        aggregations = requireNonNull(aggregations, "aggregations is null")
                .map(List::copyOf);
        sortOrder = requireNonNull(sortOrder, "sortOrder is null")
                .map(List::copyOf);
        requireNonNull(limit, "limit is null");
    }

    public DorisTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint)
    {
        return new DorisTableHandle(schemaName, tableName, newConstraint, projectedColumns, groupingColumns, aggregations, sortOrder, limit);
    }

    public DorisTableHandle withProjectedColumns(List<DorisColumnHandle> newProjectedColumns)
    {
        return new DorisTableHandle(schemaName, tableName, constraint, Optional.of(List.copyOf(newProjectedColumns)), groupingColumns, aggregations, sortOrder, limit);
    }

    public DorisTableHandle withAggregations(List<DorisAggregation> newAggregations)
    {
        return withAggregations(List.of(), newAggregations);
    }

    public DorisTableHandle withAggregations(List<DorisColumnHandle> newGroupingColumns, List<DorisAggregation> newAggregations)
    {
        return new DorisTableHandle(
                schemaName,
                tableName,
                constraint,
                Optional.empty(),
                Optional.of(List.copyOf(newGroupingColumns)),
                Optional.of(List.copyOf(newAggregations)),
                Optional.empty(),
                OptionalLong.empty());
    }

    public DorisTableHandle withTopN(List<DorisSortItem> newSortOrder, long topNLimit)
    {
        return new DorisTableHandle(
                schemaName,
                tableName,
                constraint,
                projectedColumns,
                groupingColumns,
                aggregations,
                Optional.of(List.copyOf(newSortOrder)),
                OptionalLong.of(topNLimit));
    }

    public DorisTableHandle withLimit(long newLimit)
    {
        return new DorisTableHandle(schemaName, tableName, constraint, projectedColumns, groupingColumns, aggregations, sortOrder, OptionalLong.of(newLimit));
    }

    public List<DorisColumnHandle> aggregationColumns()
    {
        if (aggregations.isEmpty()) {
            return List.of();
        }

        List<DorisAggregation> pushedAggregations = aggregations.orElseThrow();
        return IntStream.range(0, pushedAggregations.size())
                .mapToObj(index -> pushedAggregations.get(index).toColumnHandle(index))
                .toList();
    }

    public List<DorisColumnHandle> outputColumns()
    {
        if (aggregations.isEmpty()) {
            return projectedColumns.orElse(List.of());
        }

        return java.util.stream.Stream.concat(
                        groupingColumns.orElse(List.of()).stream(),
                        aggregationColumns().stream())
                .toList();
    }

    public SchemaTableName toSchemaTableName()
    {
        // Keep schema/table names normalized in one place so later handle evolution stays simple.
        return new SchemaTableName(schemaName, tableName);
    }
}
