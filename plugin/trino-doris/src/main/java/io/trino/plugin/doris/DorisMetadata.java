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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class DorisMetadata
        implements ConnectorMetadata
{
    private final DorisMetadataClient metadataClient;
    private final DorisTypeMapper typeMapper;
    private final DorisFilterToSql filterToSql;

    @Inject
    public DorisMetadata(DorisMetadataClient metadataClient, DorisTypeMapper typeMapper, DorisFilterToSql filterToSql)
    {
        this.metadataClient = requireNonNull(metadataClient, "metadataClient is null");
        this.typeMapper = requireNonNull(typeMapper, "typeMapper is null");
        this.filterToSql = requireNonNull(filterToSql, "filterToSql is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metadataClient.listSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        return metadataClient.getTable(tableName)
                .map(ignored -> new DorisTableHandle(tableName.getSchemaName(), tableName.getTableName()))
                .orElse(null);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        return metadataClient.listTables(optionalSchemaName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DorisTableHandle tableHandle = (DorisTableHandle) table;
        if (tableHandle.aggregations().isPresent()) {
            return new ConnectorTableMetadata(tableHandle.toSchemaTableName(), toColumnMetadataFromHandles(tableHandle.outputColumns()));
        }
        return getRemoteTable(tableHandle)
                .map(this::toTableMetadata)
                .orElse(null);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DorisTableHandle dorisTableHandle = (DorisTableHandle) tableHandle;
        if (dorisTableHandle.aggregations().isPresent()) {
            Map<String, ColumnHandle> aggregationColumns = new LinkedHashMap<>();
            for (DorisColumnHandle columnHandle : dorisTableHandle.outputColumns()) {
                aggregationColumns.put(columnHandle.columnName(), columnHandle);
            }
            return Collections.unmodifiableMap(aggregationColumns);
        }

        DorisRemoteTable remoteTable = getRemoteTable(dorisTableHandle)
                .orElseThrow(() -> new TableNotFoundException(dorisTableHandle.toSchemaTableName()));

        Map<String, ColumnHandle> columnHandles = new LinkedHashMap<>();
        for (DorisColumnHandle columnHandle : toColumnHandles(remoteTable.columns())) {
            columnHandles.put(columnHandle.columnName(), columnHandle);
        }
        return Collections.unmodifiableMap(columnHandles);
    }

    @SuppressWarnings("deprecation")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = new LinkedHashMap<>();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            getRemoteTable(tableName).ifPresent(remoteTable -> tableColumns.put(tableName, toColumnMetadata(remoteTable.columns())));
        }
        return Collections.unmodifiableMap(tableColumns);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((DorisColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        DorisTableHandle handle = (DorisTableHandle) table;
        if (handle.aggregations().isPresent()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> oldDomain = handle.constraint();
        TupleDomain<ColumnHandle> intersected = oldDomain.intersect(constraint.getSummary());

        if (intersected.isNone()) {
            if (oldDomain.isNone()) {
                return Optional.empty();
            }
            DorisTableHandle updatedHandle = handle.withConstraint(TupleDomain.none());
            return Optional.of(new ConstraintApplicationResult<>(updatedHandle, TupleDomain.all(), constraint.getExpression(), false));
        }

        Map<ColumnHandle, Domain> supported = new LinkedHashMap<>();
        Map<ColumnHandle, Domain> unsupported = new LinkedHashMap<>();
        for (Map.Entry<ColumnHandle, Domain> entry : intersected.getDomains().orElseThrow().entrySet()) {
            DorisColumnHandle column = (DorisColumnHandle) entry.getKey();
            Domain domain = entry.getValue();
            if (filterToSql.isPushdownSupported(column, domain)) {
                supported.put(column, domain);
            }
            else {
                unsupported.put(column, domain);
            }
        }

        TupleDomain<ColumnHandle> pushedDomain = supported.isEmpty() ? TupleDomain.all() : TupleDomain.withColumnDomains(supported);
        if (oldDomain.equals(pushedDomain)) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> remainingFilter = unsupported.isEmpty() ? TupleDomain.all() : TupleDomain.withColumnDomains(unsupported);
        DorisTableHandle updatedHandle = handle.withConstraint(pushedDomain);
        return Optional.of(new ConstraintApplicationResult<>(updatedHandle, remainingFilter, constraint.getExpression(), false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        DorisTableHandle handle = (DorisTableHandle) table;
        if (handle.aggregations().isPresent()) {
            return Optional.empty();
        }

        List<DorisColumnHandle> projectedColumns = assignments.values().stream()
                .map(DorisColumnHandle.class::cast)
                .distinct()
                .sorted(comparing(DorisColumnHandle::ordinalPosition))
                .toList();

        if (handle.projectedColumns().isPresent() && handle.projectedColumns().get().equals(projectedColumns)) {
            return Optional.empty();
        }

        DorisTableHandle updatedHandle = handle.withProjectedColumns(projectedColumns);
        List<Assignment> assignmentList = assignments.entrySet().stream()
                .map(entry -> new Assignment(entry.getKey(), entry.getValue(), ((DorisColumnHandle) entry.getValue()).columnType()))
                .toList();

        return Optional.of(new ProjectionApplicationResult<>(updatedHandle, projections, assignmentList, false));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        DorisTableHandle handle = (DorisTableHandle) table;
        if (handle.aggregations().isPresent()) {
            return Optional.empty();
        }

        if (handle.limit().isPresent() && handle.limit().getAsLong() <= limit) {
            return Optional.empty();
        }

        DorisTableHandle updatedHandle = handle.withLimit(limit);
        return Optional.of(new LimitApplicationResult<>(updatedHandle, false, false));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        if (sortItems.isEmpty()) {
            throw new IllegalArgumentException("sortItems are empty");
        }

        DorisTableHandle handle = (DorisTableHandle) table;
        if (handle.aggregations().isPresent() || handle.constraint().isNone() || handle.sortOrder().isPresent() || handle.limit().isPresent()) {
            return Optional.empty();
        }

        List<DorisSortItem> pushedSortOrder = new ArrayList<>(sortItems.size());
        for (SortItem sortItem : sortItems) {
            ColumnHandle columnHandle = assignments.get(sortItem.getName());
            if (!(columnHandle instanceof DorisColumnHandle dorisColumn) || !isTopNPushdownSupported(dorisColumn.columnType())) {
                return Optional.empty();
            }
            pushedSortOrder.add(new DorisSortItem(dorisColumn, sortItem.getSortOrder()));
        }

        DorisTableHandle updatedHandle = handle.withTopN(pushedSortOrder, topNCount);
        return Optional.of(new TopNApplicationResult<>(updatedHandle, true, false));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        if (groupingSets.isEmpty()) {
            throw new IllegalArgumentException("No grouping sets provided");
        }

        if (groupingSets.size() != 1 || aggregates.isEmpty()) {
            return Optional.empty();
        }

        DorisTableHandle handle = (DorisTableHandle) table;
        if (handle.aggregations().isPresent() || handle.limit().isPresent() || handle.constraint().isNone()) {
            return Optional.empty();
        }

        List<ColumnHandle> groupingSet = groupingSets.getFirst();
        List<DorisColumnHandle> groupingColumns = groupingSet.stream()
                .map(DorisColumnHandle.class::cast)
                .toList();

        List<DorisAggregation> pushedAggregations = new ArrayList<>(aggregates.size());
        for (int index = 0; index < aggregates.size(); index++) {
            Optional<DorisAggregation> pushedAggregation = toAggregation(aggregates.get(index), assignments, index);
            if (pushedAggregation.isEmpty()) {
                return Optional.empty();
            }
            pushedAggregations.add(pushedAggregation.orElseThrow());
        }

        DorisTableHandle updatedHandle = handle.withAggregations(groupingColumns, pushedAggregations);
        List<ConnectorExpression> projections = new ArrayList<>(pushedAggregations.size());
        List<Assignment> resultAssignments = new ArrayList<>(pushedAggregations.size());
        for (int index = 0; index < pushedAggregations.size(); index++) {
            DorisColumnHandle outputColumn = pushedAggregations.get(index).toColumnHandle(index);
            projections.add(new Variable(outputColumn.columnName(), outputColumn.columnType()));
            resultAssignments.add(new Assignment(outputColumn.columnName(), outputColumn, outputColumn.columnType()));
        }

        Map<ColumnHandle, ColumnHandle> groupingColumnMapping = new LinkedHashMap<>();
        for (int index = 0; index < groupingSet.size(); index++) {
            groupingColumnMapping.put(groupingSet.get(index), groupingColumns.get(index));
        }

        return Optional.of(new AggregationApplicationResult<>(updatedHandle, projections, resultAssignments, groupingColumnMapping, false));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table)
    {
        DorisTableHandle handle = (DorisTableHandle) table;
        if (handle.aggregations().isPresent()) {
            if (handle.groupingColumns().orElse(List.of()).isEmpty()) {
                return TableStatistics.builder()
                        .setRowCount(Estimate.of(1))
                        .build();
            }
            return TableStatistics.builder()
                    .build();
        }

        if (handle.constraint().isNone()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.zero())
                    .build();
        }

        OptionalLong rowCount = metadataClient.getTableRowCount(handle.toSchemaTableName());
        if (rowCount.isEmpty()) {
            return TableStatistics.empty();
        }

        return TableStatistics.builder()
                .setRowCount(Estimate.of(rowCount.getAsLong()))
                .build();
    }

    private Optional<DorisRemoteTable> getRemoteTable(DorisTableHandle tableHandle)
    {
        return getRemoteTable(tableHandle.toSchemaTableName());
    }

    private Optional<DorisRemoteTable> getRemoteTable(SchemaTableName tableName)
    {
        return metadataClient.getTable(tableName);
    }

    private ConnectorTableMetadata toTableMetadata(DorisRemoteTable remoteTable)
    {
        return new ConnectorTableMetadata(remoteTable.schemaTableName(), toColumnMetadata(remoteTable.columns()));
    }

    private List<ColumnMetadata> toColumnMetadata(List<DorisRemoteColumn> columns)
    {
        List<ColumnMetadata> columnMetadata = new ArrayList<>(columns.size());
        for (DorisRemoteColumn column : columns) {
            columnMetadata.add(new ColumnMetadata(column.columnName(), typeMapper.toTrinoType(column)));
        }
        return List.copyOf(columnMetadata);
    }

    private static List<ColumnMetadata> toColumnMetadataFromHandles(List<DorisColumnHandle> columns)
    {
        return columns.stream()
                .map(DorisColumnHandle::getColumnMetadata)
                .toList();
    }

    private List<DorisColumnHandle> toColumnHandles(List<DorisRemoteColumn> columns)
    {
        List<DorisColumnHandle> columnHandles = new ArrayList<>(columns.size());
        for (DorisRemoteColumn column : columns) {
            // Doris ordinals are 1-based, while Trino handle positions are easier to keep 0-based internally.
            columnHandles.add(new DorisColumnHandle(
                    column.columnName(),
                    typeMapper.toTrinoType(column),
                    Math.max(0, column.ordinalPosition() - 1)));
        }
        return List.copyOf(columnHandles);
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isPresent()) {
            return List.of(prefix.toSchemaTableName());
        }
        return listTables(session, prefix.getSchema());
    }

    private static Optional<DorisAggregation> toAggregation(AggregateFunction aggregate, Map<String, ColumnHandle> assignments, int ordinalPosition)
    {
        if (aggregate.getFilter().isPresent() || !aggregate.getSortItems().isEmpty()) {
            return Optional.empty();
        }

        String outputColumnName = "_trino_agg_" + ordinalPosition;
        return switch (aggregate.getFunctionName().toLowerCase(Locale.ENGLISH)) {
            case "count" -> toCountAggregation(aggregate, assignments, outputColumnName);
            case "min", "max" -> aggregate.isDistinct() ? Optional.empty() : toMinMaxAggregation(aggregate, assignments, outputColumnName);
            case "sum" -> aggregate.isDistinct() ? Optional.empty() : toSumAggregation(aggregate, assignments, outputColumnName);
            case "avg" -> aggregate.isDistinct() ? Optional.empty() : toAvgAggregation(aggregate, assignments, outputColumnName);
            default -> Optional.empty();
        };
    }

    private static Optional<DorisAggregation> toCountAggregation(AggregateFunction aggregate, Map<String, ColumnHandle> assignments, String outputColumnName)
    {
        if (!aggregate.getOutputType().equals(BIGINT)) {
            return Optional.empty();
        }

        if (aggregate.getArguments().isEmpty()) {
            if (aggregate.isDistinct()) {
                return Optional.empty();
            }
            return Optional.of(new DorisAggregation(outputColumnName, "count", "COUNT(*)", BIGINT, Optional.empty()));
        }

        if (aggregate.getArguments().size() != 1) {
            return Optional.empty();
        }

        ConnectorExpression argument = aggregate.getArguments().getFirst();
        if (argument instanceof Constant constant && constant.getValue() != null) {
            if (aggregate.isDistinct()) {
                return Optional.empty();
            }
            return Optional.of(new DorisAggregation(outputColumnName, "count", "COUNT(*)", BIGINT, Optional.empty()));
        }

        Optional<DorisColumnHandle> sourceColumn = toSourceColumn(argument, assignments);
        if (sourceColumn.isEmpty()) {
            return Optional.empty();
        }
        DorisColumnHandle dorisColumn = sourceColumn.orElseThrow();
        if (aggregate.isDistinct()) {
            if (!isCountDistinctPushdownSupported(dorisColumn.columnType())) {
                return Optional.empty();
            }

            return Optional.of(new DorisAggregation(
                    outputColumnName,
                    "count_distinct",
                    "COUNT(DISTINCT " + DorisQueryBuilder.quoteIdentifier(dorisColumn.columnName()) + ")",
                    BIGINT,
                    Optional.of(dorisColumn.columnName())));
        }

        return Optional.of(new DorisAggregation(
                outputColumnName,
                "count",
                "COUNT(" + DorisQueryBuilder.quoteIdentifier(dorisColumn.columnName()) + ")",
                BIGINT,
                Optional.of(dorisColumn.columnName())));
    }

    private static Optional<DorisAggregation> toMinMaxAggregation(AggregateFunction aggregate, Map<String, ColumnHandle> assignments, String outputColumnName)
    {
        Optional<DorisColumnHandle> sourceColumn = toSingleSourceColumn(aggregate, assignments);
        if (sourceColumn.isEmpty()) {
            return Optional.empty();
        }

        DorisColumnHandle dorisColumn = sourceColumn.orElseThrow();
        if (!aggregate.getOutputType().equals(dorisColumn.columnType()) || isCharacterType(dorisColumn.columnType())) {
            return Optional.empty();
        }

        String functionName = aggregate.getFunctionName().toUpperCase(Locale.ENGLISH);
        return Optional.of(new DorisAggregation(
                outputColumnName,
                aggregate.getFunctionName().toLowerCase(Locale.ENGLISH),
                functionName + "(" + DorisQueryBuilder.quoteIdentifier(dorisColumn.columnName()) + ")",
                aggregate.getOutputType(),
                Optional.of(dorisColumn.columnName())));
    }

    private static Optional<DorisAggregation> toSumAggregation(AggregateFunction aggregate, Map<String, ColumnHandle> assignments, String outputColumnName)
    {
        Optional<DorisColumnHandle> sourceColumn = toSingleSourceColumn(aggregate, assignments);
        if (sourceColumn.isEmpty()) {
            return Optional.empty();
        }

        DorisColumnHandle dorisColumn = sourceColumn.orElseThrow();
        if (!isNumericType(dorisColumn.columnType()) || !isNumericType(aggregate.getOutputType())) {
            return Optional.empty();
        }

        return Optional.of(new DorisAggregation(
                outputColumnName,
                "sum",
                "SUM(" + DorisQueryBuilder.quoteIdentifier(dorisColumn.columnName()) + ")",
                aggregate.getOutputType(),
                Optional.of(dorisColumn.columnName())));
    }

    private static Optional<DorisAggregation> toAvgAggregation(AggregateFunction aggregate, Map<String, ColumnHandle> assignments, String outputColumnName)
    {
        Optional<DorisColumnHandle> sourceColumn = toSingleSourceColumn(aggregate, assignments);
        if (sourceColumn.isEmpty()) {
            return Optional.empty();
        }

        DorisColumnHandle dorisColumn = sourceColumn.orElseThrow();
        Type sourceType = dorisColumn.columnType();
        Type outputType = aggregate.getOutputType();
        String quotedColumn = DorisQueryBuilder.quoteIdentifier(dorisColumn.columnName());

        if (isIntegralType(sourceType) && outputType == DOUBLE) {
            return Optional.of(new DorisAggregation(
                    outputColumnName,
                    "avg",
                    "AVG((" + quotedColumn + " * 1.0))",
                    outputType,
                    Optional.of(dorisColumn.columnName())));
        }

        if ((sourceType == RealType.REAL || sourceType == DOUBLE) && (outputType == sourceType || outputType == DOUBLE)) {
            return Optional.of(new DorisAggregation(
                    outputColumnName,
                    "avg",
                    "AVG(" + quotedColumn + ")",
                    outputType,
                    Optional.of(dorisColumn.columnName())));
        }

        if (sourceType instanceof DecimalType && outputType instanceof DecimalType) {
            return Optional.of(new DorisAggregation(
                    outputColumnName,
                    "avg",
                    "AVG(" + quotedColumn + ")",
                    outputType,
                    Optional.of(dorisColumn.columnName())));
        }

        return Optional.empty();
    }

    private static Optional<DorisColumnHandle> toSingleSourceColumn(AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        if (aggregate.getArguments().size() != 1) {
            return Optional.empty();
        }
        return toSourceColumn(aggregate.getArguments().getFirst(), assignments);
    }

    private static Optional<DorisColumnHandle> toSourceColumn(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        if (!(expression instanceof Variable argument)) {
            return Optional.empty();
        }

        ColumnHandle columnHandle = assignments.get(argument.getName());
        if (!(columnHandle instanceof DorisColumnHandle dorisColumn)) {
            return Optional.empty();
        }
        return Optional.of(dorisColumn);
    }

    private static boolean isCharacterType(Type type)
    {
        return type instanceof CharType || type instanceof VarcharType;
    }

    private static boolean isTopNPushdownSupported(Type type)
    {
        return type == BooleanType.BOOLEAN ||
                type == DateType.DATE ||
                type instanceof DecimalType ||
                type instanceof TimestampType ||
                isIntegralType(type);
    }

    private static boolean isCountDistinctPushdownSupported(Type type)
    {
        return type == BooleanType.BOOLEAN ||
                type == DateType.DATE ||
                type instanceof DecimalType ||
                type instanceof TimestampType ||
                isIntegralType(type) ||
                isCharacterType(type);
    }

    private static boolean isIntegralType(Type type)
    {
        return type == TinyintType.TINYINT || type == SmallintType.SMALLINT || type == IntegerType.INTEGER || type == BIGINT;
    }

    private static boolean isNumericType(Type type)
    {
        return isIntegralType(type) || type == RealType.REAL || type == DoubleType.DOUBLE || type instanceof DecimalType;
    }
}
