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

import com.google.inject.Inject;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class DorisQueryBuilder
{
    private final DorisFilterToSql filterToSql;

    @Inject
    public DorisQueryBuilder(DorisFilterToSql filterToSql)
    {
        this.filterToSql = requireNonNull(filterToSql, "filterToSql is null");
    }

    public DorisQueryBuilder()
    {
        this(new DorisFilterToSql());
    }

    public String buildSelectSql(
            DorisTableHandle tableHandle,
            List<String> columnNames,
            List<Long> tabletIds)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        return buildSelectSql(tableHandle, columnNames, tabletIds, filterToSql.toFilter(tableHandle.constraint()), tableHandle.limit());
    }

    public String buildSelectSql(
            DorisTableHandle tableHandle,
            List<String> columnNames,
            List<Long> tabletIds,
            Optional<String> filter,
            OptionalLong limit)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columnNames, "columnNames is null");
        requireNonNull(tabletIds, "tabletIds is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(limit, "limit is null");

        String projection = buildProjection(tableHandle, columnNames);

        StringBuilder sql = new StringBuilder()
                .append("SELECT ")
                .append(projection)
                .append(" FROM ")
                .append(quoteIdentifier(tableHandle.schemaName()))
                .append(".")
                .append(quoteIdentifier(tableHandle.tableName()));

        if (tableHandle.aggregations().isEmpty() && !tabletIds.isEmpty()) {
            sql.append(" TABLET(");
            for (int i = 0; i < tabletIds.size(); i++) {
                if (i > 0) {
                    sql.append(",");
                }
                sql.append(tabletIds.get(i));
            }
            sql.append(")");
        }

        filter.filter(value -> !value.isBlank())
                .ifPresent(value -> sql.append(" WHERE ").append(value));
        if (tableHandle.aggregations().isPresent()) {
            List<DorisColumnHandle> groupingColumns = tableHandle.groupingColumns().orElse(List.of());
            if (!groupingColumns.isEmpty()) {
                sql.append(" GROUP BY ");
                for (int index = 0; index < groupingColumns.size(); index++) {
                    if (index > 0) {
                        sql.append(", ");
                    }
                    sql.append(quoteIdentifier(groupingColumns.get(index).columnName()));
                }
            }
        }
        else {
            if (tableHandle.sortOrder().isPresent()) {
                sql.append(" ORDER BY ")
                        .append(buildOrderBy(tableHandle.sortOrder().orElseThrow()));
            }
            limit.ifPresent(value -> sql.append(" LIMIT ").append(value));
        }

        return sql.toString();
    }

    public String buildSplitPlanningSql(DorisTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        StringBuilder sql = new StringBuilder()
                .append("SELECT * FROM ")
                .append(quoteIdentifier(tableHandle.schemaName()))
                .append(".")
                .append(quoteIdentifier(tableHandle.tableName()));

        filterToSql.toFilter(tableHandle.constraint())
                .filter(value -> !value.isBlank())
                .ifPresent(value -> sql.append(" WHERE ").append(value));

        return sql.toString();
    }

    private static String buildProjection(DorisTableHandle tableHandle, List<String> columnNames)
    {
        if (tableHandle.aggregations().isPresent()) {
            return buildAggregationProjection(tableHandle, columnNames);
        }

        if (columnNames.isEmpty()) {
            // Zero-column scans still need row counts, but a literal avoids widening the read to SELECT *.
            return "1";
        }

        return columnNames.stream()
                .map(DorisQueryBuilder::quoteIdentifier)
                .reduce((left, right) -> left + ", " + right)
                .orElseThrow();
    }

    private static String buildAggregationProjection(DorisTableHandle tableHandle, List<String> columnNames)
    {
        List<DorisColumnHandle> groupingColumns = tableHandle.groupingColumns().orElse(List.of());
        List<DorisAggregation> aggregations = tableHandle.aggregations().orElseThrow();

        if (columnNames.isEmpty()) {
            if (!groupingColumns.isEmpty()) {
                return "1";
            }
            DorisAggregation aggregation = aggregations.getFirst();
            return aggregation.expression() + " AS " + quoteIdentifier(aggregation.outputColumnName());
        }

        Map<String, String> outputExpressions = new LinkedHashMap<>();
        for (DorisColumnHandle groupingColumn : groupingColumns) {
            outputExpressions.put(
                    groupingColumn.columnName(),
                    quoteIdentifier(groupingColumn.columnName()) + " AS " + quoteIdentifier(groupingColumn.columnName()));
        }
        for (DorisAggregation aggregation : aggregations) {
            outputExpressions.put(
                    aggregation.outputColumnName(),
                    aggregation.expression() + " AS " + quoteIdentifier(aggregation.outputColumnName()));
        }

        return columnNames.stream()
                .map(columnName -> {
                    String expression = outputExpressions.get(columnName);
                    if (expression == null) {
                        throw new IllegalArgumentException("Unknown Doris aggregate output column: " + columnName);
                    }
                    return expression;
                })
                .reduce((left, right) -> left + ", " + right)
                .orElseThrow();
    }

    private static String buildOrderBy(List<DorisSortItem> sortItems)
    {
        return sortItems.stream()
                .flatMap(sortItem -> {
                    String column = quoteIdentifier(sortItem.column().columnName());
                    String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                    String columnSorting = column + " " + ordering;

                    return switch (sortItem.sortOrder()) {
                        case ASC_NULLS_FIRST, DESC_NULLS_LAST -> java.util.stream.Stream.of(columnSorting);
                        case ASC_NULLS_LAST -> java.util.stream.Stream.of("ISNULL(" + column + ") ASC", columnSorting);
                        case DESC_NULLS_FIRST -> java.util.stream.Stream.of("ISNULL(" + column + ") DESC", columnSorting);
                    };
                })
                .reduce((left, right) -> left + ", " + right)
                .orElseThrow();
    }

    static String quoteIdentifier(String value)
    {
        return "`" + value.replace("`", "``") + "`";
    }
}
