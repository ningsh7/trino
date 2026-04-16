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

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDorisMetadata
{
    private static final DorisRemoteTable ORDERS = new DorisRemoteTable(
            new SchemaTableName("sales", "orders"),
            List.of(
                    new DorisRemoteColumn("id", "BIGINT", Optional.of(20), Optional.empty(), 1),
                    new DorisRemoteColumn("event_time", "DATETIME_V2", Optional.empty(), Optional.of(6), 2),
                    new DorisRemoteColumn("payload", "JSON", Optional.empty(), Optional.empty(), 3),
                    new DorisRemoteColumn("score", "FLOAT", Optional.empty(), Optional.empty(), 4)));

    private static final DorisRemoteTable EVENTS = new DorisRemoteTable(
            new SchemaTableName("ops", "events"),
            List.of(
                    new DorisRemoteColumn("event_id", "BIGINT UNSIGNED", Optional.of(20), Optional.of(0), 1),
                    new DorisRemoteColumn("details", "STRING", Optional.empty(), Optional.empty(), 2)));

    private final DorisMetadata metadata = new DorisMetadata(
            new TestingDorisMetadataClient(List.of(ORDERS, EVENTS)),
            new DorisTypeMapper(new DorisConfig()),
            new DorisFilterToSql());

    @Test
    void testListSchemaNamesAndTables()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsExactly("sales", "ops");
        assertThat(metadata.listTables(SESSION, Optional.empty())).containsExactly(ORDERS.schemaTableName(), EVENTS.schemaTableName());
        assertThat(metadata.listTables(SESSION, Optional.of("sales"))).containsExactly(ORDERS.schemaTableName());
        assertThat(metadata.listTables(SESSION, Optional.of("missing"))).isEmpty();
    }

    @Test
    void testGetTableHandleAndTableMetadata()
    {
        DorisTableHandle tableHandle = (DorisTableHandle) metadata.getTableHandle(SESSION, ORDERS.schemaTableName(), Optional.empty(), Optional.empty());
        assertThat(tableHandle).isEqualTo(new DorisTableHandle("sales", "orders"));
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("sales", "missing"), Optional.empty(), Optional.empty())).isNull();

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        assertThat(tableMetadata.getTable()).isEqualTo(ORDERS.schemaTableName());
        assertThat(tableMetadata.getColumns()).isEqualTo(List.of(
                new ColumnMetadata("id", BIGINT),
                new ColumnMetadata("event_time", createTimestampType(6)),
                new ColumnMetadata("payload", createUnboundedVarcharType()),
                new ColumnMetadata("score", REAL)));
    }

    @Test
    void testGetTableHandlePreservesRemoteCaseForReads()
    {
        DorisRemoteTable mixedCaseOrders = new DorisRemoteTable(
                new SchemaTableName("sales", "orders"),
                "Sales",
                "Orders",
                ORDERS.columns());
        DorisMetadata mixedCaseMetadata = new DorisMetadata(
                new TestingDorisMetadataClient(List.of(mixedCaseOrders)),
                new DorisTypeMapper(new DorisConfig()),
                new DorisFilterToSql());

        DorisTableHandle tableHandle = (DorisTableHandle) mixedCaseMetadata.getTableHandle(SESSION, new SchemaTableName("sales", "orders"), Optional.empty(), Optional.empty());

        assertThat(tableHandle.schemaName()).isEqualTo("sales");
        assertThat(tableHandle.tableName()).isEqualTo("orders");
        assertThat(tableHandle.remoteSchemaName()).isEqualTo("Sales");
        assertThat(tableHandle.remoteTableName()).isEqualTo("Orders");
    }

    @Test
    void testGetColumnHandlesAndListTableColumns()
    {
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, new DorisTableHandle("ops", "events"));
        assertThat(columnHandles).isEqualTo(Map.of(
                "event_id", new DorisColumnHandle("event_id", createDecimalType(20), 0),
                "details", new DorisColumnHandle("details", createUnboundedVarcharType(), 1)));

        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = metadata.listTableColumns(SESSION, new SchemaTablePrefix("sales"));
        assertThat(tableColumns).isEqualTo(Map.of(
                ORDERS.schemaTableName(),
                List.of(
                        new ColumnMetadata("id", BIGINT),
                        new ColumnMetadata("event_time", createTimestampType(6)),
                        new ColumnMetadata("payload", createUnboundedVarcharType()),
                        new ColumnMetadata("score", REAL))));
    }

    @Test
    void testApplyFilterPushdownSupportsAndLeavesUnsupported()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, tableHandle);
        DorisColumnHandle id = (DorisColumnHandle) columns.get("id");
        DorisColumnHandle score = (DorisColumnHandle) columns.get("score");

        TupleDomain<ColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                id, Domain.singleValue(BIGINT, 10L),
                score, Domain.singleValue(REAL, (long) floatToRawIntBits(1.5f))));

        ConstraintApplicationResult<ConnectorTableHandle> result = metadata.applyFilter(SESSION, tableHandle, new Constraint(filter)).orElseThrow();
        DorisTableHandle updatedHandle = (DorisTableHandle) result.getHandle();

        assertThat(updatedHandle.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(BIGINT, 10L))));
        assertThat(result.getRemainingFilter()).isEqualTo(TupleDomain.withColumnDomains(Map.of(score, Domain.singleValue(REAL, (long) floatToRawIntBits(1.5f)))));
    }

    @Test
    void testApplyProjectionAndLimitPushdown()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, tableHandle);
        DorisColumnHandle id = (DorisColumnHandle) columns.get("id");
        DorisColumnHandle payload = (DorisColumnHandle) columns.get("payload");

        List<ConnectorExpression> projections = List.of(
                new Variable("id", BIGINT),
                new Variable("payload", createUnboundedVarcharType()));

        Map<String, ColumnHandle> assignments = new LinkedHashMap<>();
        assignments.put("id", id);
        assignments.put("payload", payload);

        ProjectionApplicationResult<ConnectorTableHandle> projectionResult = metadata.applyProjection(SESSION, tableHandle, projections, assignments).orElseThrow();
        DorisTableHandle projectedHandle = (DorisTableHandle) projectionResult.getHandle();
        assertThat(projectedHandle.projectedColumns().orElseThrow()).containsExactly(id, payload);

        LimitApplicationResult<ConnectorTableHandle> limitResult = metadata.applyLimit(SESSION, projectedHandle, 50).orElseThrow();
        DorisTableHandle limitedHandle = (DorisTableHandle) limitResult.getHandle();
        assertThat(limitedHandle.limit().orElseThrow()).isEqualTo(50L);
        assertThat(limitResult.isLimitGuaranteed()).isFalse();
    }

    @Test
    void testApplyProjectionAllowsEmptyProjection()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");

        ProjectionApplicationResult<ConnectorTableHandle> projectionResult = metadata.applyProjection(
                SESSION,
                tableHandle,
                List.of(),
                Map.of()).orElseThrow();

        DorisTableHandle projectedHandle = (DorisTableHandle) projectionResult.getHandle();
        assertThat(projectedHandle.projectedColumns()).isPresent();
        assertThat(projectedHandle.projectedColumns().orElseThrow()).isEmpty();
    }

    @Test
    void testApplyTopNPushdown()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders")
                .withProjectedColumns(List.of(new DorisColumnHandle("payload", createUnboundedVarcharType(), 2)));
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, new DorisTableHandle("sales", "orders"));
        DorisColumnHandle id = (DorisColumnHandle) columns.get("id");

        TopNApplicationResult<ConnectorTableHandle> result = metadata.applyTopN(
                SESSION,
                tableHandle,
                10,
                List.of(new SortItem("id", SortOrder.DESC_NULLS_LAST)),
                Map.of("id", id)).orElseThrow();

        DorisTableHandle updatedHandle = (DorisTableHandle) result.getHandle();
        assertThat(updatedHandle.projectedColumns().orElseThrow().stream()
                .map(DorisColumnHandle::columnName)
                .toList())
                .containsExactly("payload");
        assertThat(updatedHandle.limit().orElseThrow()).isEqualTo(10L);
        assertThat(updatedHandle.sortOrder().orElseThrow().stream()
                .map(sortItem -> sortItem.column().columnName())
                .toList())
                .containsExactly("id");
        assertThat(updatedHandle.sortOrder().orElseThrow().stream()
                .map(DorisSortItem::sortOrder)
                .toList())
                .containsExactly(SortOrder.DESC_NULLS_LAST);
        assertThat(result.isTopNGuaranteed()).isTrue();
    }

    @Test
    void testApplyTopNRejectsCharacterSortPushdown()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, tableHandle);
        DorisColumnHandle payload = (DorisColumnHandle) columns.get("payload");

        assertThat(metadata.applyTopN(
                SESSION,
                tableHandle,
                5,
                List.of(new SortItem("payload", SortOrder.ASC_NULLS_LAST)),
                Map.of("payload", payload))).isEmpty();
    }

    @Test
    void testApplyCountStarAggregationPushdown()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");
        AggregateFunction aggregate = new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty());

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                tableHandle,
                List.of(aggregate),
                Map.of(),
                List.of(List.of())).orElseThrow();

        DorisAggregation pushedAggregation = ((DorisTableHandle) result.getHandle()).aggregations().orElseThrow().getFirst();
        assertThat(pushedAggregation.expression()).isEqualTo("COUNT(*)");
        assertThat(pushedAggregation.functionName()).isEqualTo("count");
        assertThat(pushedAggregation.sourceColumnName()).isEmpty();
        assertThat(result.getAssignments().getFirst().getVariable()).isEqualTo("_trino_agg_0");
        assertThat(result.getAssignments().getFirst().getType()).isEqualTo(BIGINT);
    }

    @Test
    void testApplyCountColumnAggregationPushdown()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, tableHandle);
        DorisColumnHandle payload = (DorisColumnHandle) columns.get("payload");
        AggregateFunction aggregate = new AggregateFunction(
                "count",
                BIGINT,
                List.of(new Variable("payload", createUnboundedVarcharType())),
                List.of(),
                false,
                Optional.empty());

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                tableHandle,
                List.of(aggregate),
                Map.of("payload", payload),
                List.of(List.of())).orElseThrow();

        DorisAggregation pushedAggregation = ((DorisTableHandle) result.getHandle()).aggregations().orElseThrow().getFirst();
        assertThat(pushedAggregation.expression()).isEqualTo("COUNT(`payload`)");
        assertThat(pushedAggregation.sourceColumnName()).contains("payload");
    }

    @Test
    void testApplyCountDistinctAggregationPushdown()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, tableHandle);
        DorisColumnHandle id = (DorisColumnHandle) columns.get("id");
        AggregateFunction aggregate = new AggregateFunction(
                "count",
                BIGINT,
                List.of(new Variable("id", BIGINT)),
                List.of(),
                true,
                Optional.empty());

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                tableHandle,
                List.of(aggregate),
                Map.of("id", id),
                List.of(List.of())).orElseThrow();

        DorisAggregation pushedAggregation = ((DorisTableHandle) result.getHandle()).aggregations().orElseThrow().getFirst();
        assertThat(pushedAggregation.expression()).isEqualTo("COUNT(DISTINCT `id`)");
        assertThat(pushedAggregation.functionName()).isEqualTo("count_distinct");
        assertThat(pushedAggregation.sourceColumnName()).contains("id");
    }

    @Test
    void testApplyGroupingOnlyAggregationPushdown()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, tableHandle);
        DorisColumnHandle id = (DorisColumnHandle) columns.get("id");

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                tableHandle,
                List.of(),
                Map.of("id", id),
                List.of(List.of(id))).orElseThrow();

        DorisTableHandle updatedHandle = (DorisTableHandle) result.getHandle();
        assertThat(updatedHandle.groupingColumns().orElseThrow()).containsExactly(id);
        assertThat(updatedHandle.aggregations()).isPresent();
        assertThat(updatedHandle.aggregations().orElseThrow()).isEmpty();
        assertThat(result.getAssignments()).isEmpty();
        assertThat(result.getProjections()).isEmpty();
        assertThat(result.getGroupingColumnMapping()).isEqualTo(Map.of(id, id));
    }

    @Test
    void testCollapseCountDistinctFromGroupingOnlyPushdown()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, tableHandle);
        DorisColumnHandle id = (DorisColumnHandle) columns.get("id");

        DorisTableHandle groupedHandle = (DorisTableHandle) metadata.applyAggregation(
                SESSION,
                tableHandle,
                List.of(),
                Map.of("id", id),
                List.of(List.of(id))).orElseThrow().getHandle();

        AggregateFunction aggregate = new AggregateFunction(
                "count",
                BIGINT,
                List.of(new Variable("id", BIGINT)),
                List.of(),
                false,
                Optional.empty());

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                groupedHandle,
                List.of(aggregate),
                Map.of("id", id),
                List.of(List.of())).orElseThrow();

        DorisTableHandle updatedHandle = (DorisTableHandle) result.getHandle();
        assertThat(updatedHandle.groupingColumns().orElseThrow()).isEmpty();
        assertThat(updatedHandle.aggregations().orElseThrow().stream()
                .map(DorisAggregation::expression)
                .toList())
                .containsExactly("COUNT(DISTINCT `id`)");
    }

    @Test
    void testCollapseGroupedCountDistinctFromGroupingOnlyPushdown()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, tableHandle);
        DorisColumnHandle id = (DorisColumnHandle) columns.get("id");
        DorisColumnHandle payload = (DorisColumnHandle) columns.get("payload");

        DorisTableHandle groupedHandle = (DorisTableHandle) metadata.applyAggregation(
                SESSION,
                tableHandle,
                List.of(),
                Map.of("id", id, "payload", payload),
                List.of(List.of(payload, id))).orElseThrow().getHandle();

        AggregateFunction aggregate = new AggregateFunction(
                "count",
                BIGINT,
                List.of(new Variable("id", BIGINT)),
                List.of(),
                false,
                Optional.empty());

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                groupedHandle,
                List.of(aggregate),
                Map.of("id", id, "payload", payload),
                List.of(List.of(payload))).orElseThrow();

        DorisTableHandle updatedHandle = (DorisTableHandle) result.getHandle();
        assertThat(updatedHandle.groupingColumns().orElseThrow()).containsExactly(payload);
        assertThat(updatedHandle.aggregations().orElseThrow().stream()
                .map(DorisAggregation::expression)
                .toList())
                .containsExactly("COUNT(DISTINCT `id`)");
    }

    @Test
    void testApplyCountDistinctRejectsFloatingPointType()
    {
        AggregateFunction aggregate = new AggregateFunction(
                "count",
                BIGINT,
                List.of(new Variable("ratio", DOUBLE)),
                List.of(),
                true,
                Optional.empty());

        assertThat(metadata.applyAggregation(
                SESSION,
                new DorisTableHandle("sales", "orders"),
                List.of(aggregate),
                Map.of("ratio", new DorisColumnHandle("ratio", DOUBLE, 3)),
                List.of(List.of()))).isEmpty();
    }

    @Test
    void testApplyGroupedSumAndAvgAggregationPushdown()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, tableHandle);
        DorisColumnHandle id = (DorisColumnHandle) columns.get("id");
        DorisColumnHandle payload = (DorisColumnHandle) columns.get("payload");

        AggregateFunction sum = new AggregateFunction(
                "sum",
                BIGINT,
                List.of(new Variable("id", BIGINT)),
                List.of(),
                false,
                Optional.empty());
        AggregateFunction avg = new AggregateFunction(
                "avg",
                DOUBLE,
                List.of(new Variable("id", BIGINT)),
                List.of(),
                false,
                Optional.empty());

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                tableHandle,
                List.of(sum, avg),
                Map.of("id", id, "payload", payload),
                List.of(List.of(payload))).orElseThrow();

        DorisTableHandle updatedHandle = (DorisTableHandle) result.getHandle();
        assertThat(updatedHandle.groupingColumns().orElseThrow()).containsExactly(payload);
        assertThat(updatedHandle.outputColumns().stream()
                .map(DorisColumnHandle::columnName)
                .toList())
                .containsExactly("payload", "_trino_agg_0", "_trino_agg_1");
        assertThat(updatedHandle.aggregations().orElseThrow().stream()
                .map(DorisAggregation::expression)
                .toList())
                .containsExactly("SUM(`id`)", "AVG((`id` * 1.0))");
        assertThat(result.getAssignments().stream()
                .map(Assignment::getVariable)
                .toList())
                .containsExactly("_trino_agg_0", "_trino_agg_1");
        assertThat(result.getGroupingColumnMapping()).isEqualTo(Map.of(payload, payload));
    }

    @Test
    void testApplyAvgAggregationPushdownWithWideningCast()
    {
        AggregateFunction avg = new AggregateFunction(
                "avg",
                DOUBLE,
                List.of(new Call(BIGINT, CAST_FUNCTION_NAME, List.of(new Variable("quantity", INTEGER)))),
                List.of(),
                false,
                Optional.empty());

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                new DorisTableHandle("sales", "orders"),
                List.of(avg),
                Map.of("quantity", new DorisColumnHandle("quantity", INTEGER, 4)),
                List.of(List.of())).orElseThrow();

        DorisAggregation pushedAggregation = ((DorisTableHandle) result.getHandle()).aggregations().orElseThrow().getFirst();
        assertThat(pushedAggregation.expression()).isEqualTo("AVG((`quantity` * 1.0))");
        assertThat(pushedAggregation.functionName()).isEqualTo("avg");
        assertThat(pushedAggregation.sourceColumnName()).contains("quantity");
    }

    @Test
    void testApplyGroupedCountDistinctAndSumAggregationPushdown()
    {
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, tableHandle);
        DorisColumnHandle id = (DorisColumnHandle) columns.get("id");
        DorisColumnHandle payload = (DorisColumnHandle) columns.get("payload");

        AggregateFunction countDistinct = new AggregateFunction(
                "count",
                BIGINT,
                List.of(new Variable("payload", createUnboundedVarcharType())),
                List.of(),
                true,
                Optional.empty());
        AggregateFunction sum = new AggregateFunction(
                "sum",
                BIGINT,
                List.of(new Variable("id", BIGINT)),
                List.of(),
                false,
                Optional.empty());

        AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                SESSION,
                tableHandle,
                List.of(countDistinct, sum),
                Map.of("id", id, "payload", payload),
                List.of(List.of(id))).orElseThrow();

        DorisTableHandle updatedHandle = (DorisTableHandle) result.getHandle();
        assertThat(updatedHandle.groupingColumns().orElseThrow()).containsExactly(id);
        assertThat(updatedHandle.aggregations().orElseThrow().stream()
                .map(DorisAggregation::expression)
                .toList())
                .containsExactly("COUNT(DISTINCT `payload`)", "SUM(`id`)");
    }

    @Test
    void testGetTableStatistics()
    {
        TableStatistics tableStatistics = metadata.getTableStatistics(SESSION, new DorisTableHandle("sales", "orders"));
        assertThat(tableStatistics.getRowCount().getValue()).isEqualTo(1_000.0);

        TableStatistics noneStatistics = metadata.getTableStatistics(
                SESSION,
                new DorisTableHandle("sales", "orders", TupleDomain.none(), Optional.empty(), OptionalLong.empty()));
        assertThat(noneStatistics.getRowCount().getValue()).isEqualTo(0.0);

        TableStatistics aggregatedStatistics = metadata.getTableStatistics(
                SESSION,
                new DorisTableHandle("sales", "orders").withAggregations(List.of(new DorisAggregation("_trino_agg_0", "count", "COUNT(*)", BIGINT, Optional.empty()))));
        assertThat(aggregatedStatistics.getRowCount().getValue()).isEqualTo(1.0);

        TableStatistics groupedAggregatedStatistics = metadata.getTableStatistics(
                SESSION,
                new DorisTableHandle("sales", "orders").withAggregations(
                        List.of(new DorisColumnHandle("payload", createUnboundedVarcharType(), 2)),
                        List.of(new DorisAggregation("_trino_agg_0", "count", "COUNT(*)", BIGINT, Optional.empty()))));
        assertThat(groupedAggregatedStatistics.getRowCount().isUnknown()).isTrue();

        TableStatistics unknownStatistics = metadata.getTableStatistics(SESSION, new DorisTableHandle("sales", "missing"));
        assertThat(unknownStatistics.getRowCount().isUnknown()).isTrue();
    }

    @Test
    void testUnknownTableColumnHandlesFail()
    {
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new DorisTableHandle("sales", "missing")))
                .isInstanceOf(TableNotFoundException.class);
    }

    private static final class TestingDorisMetadataClient
            implements DorisMetadataClient
    {
        private final List<String> schemaNames;
        private final Map<SchemaTableName, DorisRemoteTable> tables;

        private TestingDorisMetadataClient(List<DorisRemoteTable> tables)
        {
            LinkedHashMap<SchemaTableName, DorisRemoteTable> tableMap = new LinkedHashMap<>();
            LinkedHashMap<String, Boolean> schemaMap = new LinkedHashMap<>();
            for (DorisRemoteTable table : tables) {
                tableMap.put(table.schemaTableName(), table);
                schemaMap.put(table.schemaTableName().getSchemaName(), true);
            }
            this.tables = Collections.unmodifiableMap(tableMap);
            this.schemaNames = List.copyOf(schemaMap.keySet());
        }

        @Override
        public List<String> listSchemaNames()
        {
            return schemaNames;
        }

        @Override
        public List<SchemaTableName> listTables(Optional<String> schemaName)
        {
            return tables.keySet().stream()
                    .filter(table -> schemaName.isEmpty() || table.getSchemaName().equals(schemaName.get()))
                    .toList();
        }

        @Override
        public Optional<DorisRemoteTable> getTable(SchemaTableName tableName)
        {
            return Optional.ofNullable(tables.get(tableName));
        }

        @Override
        public OptionalLong getTableRowCount(SchemaTableName tableName)
        {
            if (!tables.containsKey(tableName)) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(1_000L);
        }
    }
}
