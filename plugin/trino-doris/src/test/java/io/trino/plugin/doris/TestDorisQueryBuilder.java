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
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDorisQueryBuilder
{
    @Test
    void testBuildSplitPlanningSql()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();

        assertThat(queryBuilder.buildSplitPlanningSql(new DorisTableHandle("sales", "orders")))
                .isEqualTo("SELECT * FROM `sales`.`orders`");
    }

    @Test
    void testBuildSplitPlanningSqlIgnoresLimitButKeepsFilter()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();
        DorisColumnHandle id = new DorisColumnHandle("id", BIGINT, 0);
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders")
                .withConstraint(TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(BIGINT, 42L))))
                .withLimit(10);

        assertThat(queryBuilder.buildSplitPlanningSql(tableHandle))
                .isEqualTo("SELECT * FROM `sales`.`orders` WHERE `id` = 42");
    }

    @Test
    void testBuildSqlUsesRemoteNames()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders", "Sales", "Orders");

        assertThat(queryBuilder.buildSplitPlanningSql(tableHandle))
                .isEqualTo("SELECT * FROM `Sales`.`Orders`");
        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of("id"), List.of(11L)))
                .isEqualTo("SELECT `id` FROM `Sales`.`Orders` TABLET(11)");
    }

    @Test
    void testBuildSelectSqlUsesLiteralForEmptyProjection()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();

        assertThat(queryBuilder.buildSelectSql(
                new DorisTableHandle("sales", "orders"),
                List.of(),
                List.of(),
                Optional.empty(),
                OptionalLong.of(5)))
                .isEqualTo("SELECT 1 FROM `sales`.`orders` LIMIT 5");
    }

    @Test
    void testBuildSelectSqlWithProjectionTabletsAndFilter()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();

        assertThat(queryBuilder.buildSelectSql(
                new DorisTableHandle("sales", "orders"),
                List.of("id", "event`time"),
                List.of(11L, 12L),
                Optional.of("`id` > 10"),
                OptionalLong.of(50)))
                .isEqualTo("SELECT `id`, `event``time` FROM `sales`.`orders` TABLET(11,12) WHERE `id` > 10 LIMIT 50");
    }

    @Test
    void testBuildSelectSqlFromTableHandlePushdownState()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();
        DorisColumnHandle id = new DorisColumnHandle("id", BIGINT, 0);

        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders")
                .withConstraint(TupleDomain.withColumnDomains(Map.of((ColumnHandle) id, Domain.singleValue(BIGINT, 42L))))
                .withLimit(10);

        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of("id"), List.of(11L)))
                .isEqualTo("SELECT `id` FROM `sales`.`orders` TABLET(11) WHERE `id` = 42 LIMIT 10");
    }

    @Test
    void testBuildSelectSqlForPushedDownCountAggregation()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders")
                .withConstraint(TupleDomain.withColumnDomains(Map.of(new DorisColumnHandle("id", BIGINT, 0), Domain.singleValue(BIGINT, 42L))))
                .withAggregations(List.of(new DorisAggregation("_trino_agg_0", "count", "COUNT(*)", BIGINT, Optional.empty())));

        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of(), List.of(11L)))
                .isEqualTo("SELECT COUNT(*) AS `_trino_agg_0` FROM `sales`.`orders` WHERE `id` = 42");
    }

    @Test
    void testBuildSelectSqlForPushedDownCountDistinctAggregation()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders")
                .withAggregations(List.of(new DorisAggregation("_trino_agg_0", "count_distinct", "COUNT(DISTINCT `id`)", BIGINT, Optional.of("id"))));

        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of(), List.of(11L)))
                .isEqualTo("SELECT COUNT(DISTINCT `id`) AS `_trino_agg_0` FROM `sales`.`orders`");
    }

    @Test
    void testBuildSelectSqlForGroupedAggregations()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders")
                .withConstraint(TupleDomain.withColumnDomains(Map.of(new DorisColumnHandle("id", BIGINT, 0), Domain.singleValue(BIGINT, 42L))))
                .withAggregations(
                        List.of(new DorisColumnHandle("payload", createUnboundedVarcharType(), 2)),
                        List.of(
                                new DorisAggregation("_trino_agg_0", "sum", "SUM(`id`)", BIGINT, Optional.of("id")),
                                new DorisAggregation("_trino_agg_1", "avg", "AVG((`id` * 1.0))", DOUBLE, Optional.of("id"))));

        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of("payload", "_trino_agg_0", "_trino_agg_1"), List.of(11L)))
                .isEqualTo("SELECT `payload` AS `payload`, SUM(`id`) AS `_trino_agg_0`, AVG((`id` * 1.0)) AS `_trino_agg_1` FROM `sales`.`orders` WHERE `id` = 42 GROUP BY `payload`");
    }

    @Test
    void testBuildSelectSqlForGroupedAggregationWithEmptyRequestedColumns()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders")
                .withAggregations(
                        List.of(new DorisColumnHandle("payload", createUnboundedVarcharType(), 2)),
                        List.of(new DorisAggregation("_trino_agg_0", "count", "COUNT(*)", BIGINT, Optional.empty())));

        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of(), List.of(11L)))
                .isEqualTo("SELECT 1 FROM `sales`.`orders` GROUP BY `payload`");
    }

    @Test
    void testBuildSelectSqlForTopNPushdown()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders")
                .withTopN(
                        List.of(
                                new DorisSortItem(new DorisColumnHandle("id", BIGINT, 0), SortOrder.ASC_NULLS_LAST),
                                new DorisSortItem(new DorisColumnHandle("event_time", BIGINT, 1), SortOrder.DESC_NULLS_FIRST)),
                        10);

        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of("payload"), List.of()))
                .isEqualTo("SELECT `payload` FROM `sales`.`orders` ORDER BY ISNULL(`id`) ASC, `id` ASC, ISNULL(`event_time`) DESC, `event_time` DESC LIMIT 10");
    }
}
