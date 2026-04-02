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

import io.trino.spi.connector.SortOrder;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
final class TestDorisConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private final TestingDorisEnvironment environment = new TestingDorisEnvironment();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DorisQueryRunner.builder(environment).build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                    SUPPORTS_ARRAY,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_COMMENT_ON_TABLE,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_CREATE_SCHEMA,
                    SUPPORTS_CREATE_TABLE,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_DELETE,
                    SUPPORTS_DYNAMIC_FILTER_PUSHDOWN,
                    SUPPORTS_INSERT,
                    SUPPORTS_JOIN_PUSHDOWN,
                    SUPPORTS_LIMIT_PUSHDOWN,
                    SUPPORTS_MAP_TYPE,
                    SUPPORTS_MERGE,
                    SUPPORTS_NATIVE_QUERY,
                    SUPPORTS_NOT_NULL_CONSTRAINT,
                    SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                    SUPPORTS_RENAME_COLUMN,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_ROW_TYPE,
                    SUPPORTS_SET_COLUMN_TYPE,
                    SUPPORTS_TOPN_PUSHDOWN,
                    SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR,
                    SUPPORTS_UPDATE,
                    SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                    SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                    SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                    SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                    SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    void testBasicRead()
    {
        assertQuery(
                "SELECT nationkey, name FROM nation WHERE nationkey <= 2 ORDER BY nationkey",
                "VALUES (0, 'ALGERIA'), (1, 'ARGENTINA'), (2, 'BRAZIL')");
    }

    @Test
    void testProjectionFilterAndLimitPushdownWiring()
    {
        environment.clearLastRequest();

        assertQuery("SELECT name FROM nation WHERE nationkey = 1 LIMIT 1", "VALUES ('ARGENTINA')");

        TestingDorisEnvironment.Request request = environment.getLastRequest().orElseThrow();
        assertThat(request.tableHandle().limit().orElseThrow()).isEqualTo(1L);
        assertThat(request.tableHandle().projectedColumns().orElseThrow().stream()
                .map(DorisColumnHandle::columnName)
                .toList())
                .containsExactly("name");
    }

    @Test
    void testZeroColumnScanAvoidsSelectStarFallback()
    {
        environment.clearLastRequest();

        assertQuery("SELECT 1 FROM nation LIMIT 2", "VALUES 1, 1");

        TestingDorisEnvironment.Request request = environment.getLastRequest().orElseThrow();
        assertThat(request.requestedColumns()).isEmpty();
        assertThat(request.tableHandle().projectedColumns().orElseThrow()).isEmpty();
    }

    @Test
    void testCountAggregationPushdownWiring()
    {
        environment.clearLastRequest();

        assertQuery("SELECT count(*) FROM nation", "VALUES 25");

        TestingDorisEnvironment.Request request = environment.getLastRequest().orElseThrow();
        assertThat(request.tableHandle().aggregations().orElseThrow().getFirst().expression()).isEqualTo("COUNT(*)");
        assertThat(request.requestedColumns().stream()
                .map(DorisColumnHandle::columnName)
                .toList())
                .containsExactly("_trino_agg_0");
    }

    @Test
    void testCountDistinctAggregationPushdownWiring()
    {
        environment.clearLastRequest();

        assertQuery("SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation", "VALUES (5, 300)");

        TestingDorisEnvironment.Request request = environment.getLastRequest().orElseThrow();
        assertThat(request.tableHandle().aggregations().orElseThrow().stream()
                .map(DorisAggregation::expression)
                .toList())
                .containsExactly("COUNT(DISTINCT `regionkey`)", "SUM(`nationkey`)");
    }

    @Test
    void testGroupedCountDistinctAndSumPushdownWiring()
    {
        environment.clearLastRequest();

        assertQuery(
                "SELECT regionkey, count(DISTINCT nationkey), sum(nationkey) FROM nation GROUP BY regionkey ORDER BY regionkey",
                "VALUES (0, 5, 50), (1, 5, 47), (2, 5, 68), (3, 5, 77), (4, 5, 58)");

        TestingDorisEnvironment.Request request = environment.getLastRequest().orElseThrow();
        assertThat(request.tableHandle().groupingColumns().orElseThrow().stream()
                .map(DorisColumnHandle::columnName)
                .toList())
                .containsExactly("regionkey");
        assertThat(request.tableHandle().aggregations().orElseThrow().stream()
                .map(DorisAggregation::expression)
                .toList())
                .containsExactly("COUNT(DISTINCT `nationkey`)", "SUM(`nationkey`)");
    }

    @Test
    void testCharacterCountDistinctQueryStaysCorrect()
    {
        assertQuery("SELECT count(DISTINCT orderstatus), sum(shippriority) FROM orders", "VALUES (3, 0)");
    }

    @Test
    void testMultipleCountDistinctAggregationPushdownWiring()
    {
        environment.clearLastRequest();

        assertQuery("SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation", "VALUES (5, 25)");

        TestingDorisEnvironment.Request request = environment.getLastRequest().orElseThrow();
        assertThat(request.tableHandle().aggregations().orElseThrow().stream()
                .map(DorisAggregation::expression)
                .toList())
                .containsExactly("COUNT(DISTINCT `regionkey`)", "COUNT(DISTINCT `nationkey`)");
    }

    @Test
    void testGroupedAggregationPushdownWiring()
    {
        environment.clearLastRequest();

        assertQuery(
                "SELECT regionkey, sum(nationkey), avg(nationkey) FROM nation GROUP BY regionkey ORDER BY regionkey",
                "VALUES (0, 50, 10.0), (1, 47, 9.4), (2, 68, 13.6), (3, 77, 15.4), (4, 58, 11.6)");

        TestingDorisEnvironment.Request request = environment.getLastRequest().orElseThrow();
        assertThat(request.tableHandle().groupingColumns().orElseThrow().stream()
                .map(DorisColumnHandle::columnName)
                .toList())
                .containsExactly("regionkey");
        assertThat(request.tableHandle().aggregations().orElseThrow().stream()
                .map(DorisAggregation::expression)
                .toList())
                .containsExactly("SUM(`nationkey`)", "AVG((`nationkey` * 1.0))");
    }

    @Test
    void testTopNPushdownWiring()
    {
        environment.clearLastRequest();

        assertQuery(
                "SELECT nationkey FROM nation ORDER BY nationkey DESC NULLS LAST LIMIT 3",
                "VALUES 24, 23, 22");

        TestingDorisEnvironment.Request request = environment.getLastRequest().orElseThrow();
        assertThat(request.tableHandle().limit().orElseThrow()).isEqualTo(3L);
        assertThat(request.tableHandle().sortOrder().orElseThrow().stream()
                .map(sortItem -> sortItem.column().columnName())
                .toList())
                .containsExactly("nationkey");
        assertThat(request.tableHandle().sortOrder().orElseThrow().stream()
                .map(DorisSortItem::sortOrder)
                .toList())
                .containsExactly(SortOrder.DESC_NULLS_LAST);
    }

    @Test
    void testCharacterMinMaxAggregationFallsBackToTrino()
    {
        environment.clearLastRequest();

        assertQuery("SELECT max(name) FROM nation", "VALUES 'VIETNAM'");

        TestingDorisEnvironment.Request request = environment.getLastRequest().orElseThrow();
        assertThat(request.tableHandle().aggregations().orElse(List.of())).isEmpty();
        assertThat(request.requestedColumns().stream()
                .map(DorisColumnHandle::columnName)
                .toList())
                .containsExactly("name");
    }

    @Test
    void testShowColumnsShape()
    {
        assertQuery(
                "SHOW COLUMNS FROM orders",
                """
                VALUES
                    ('orderkey', 'bigint', '', ''),
                    ('custkey', 'bigint', '', ''),
                    ('orderstatus', 'varchar(1)', '', ''),
                    ('totalprice', 'double', '', ''),
                    ('orderdate', 'date', '', ''),
                    ('orderpriority', 'varchar(15)', '', ''),
                    ('clerk', 'varchar(15)', '', ''),
                    ('shippriority', 'integer', '', ''),
                    ('comment', 'varchar(79)', '', '')
                """);
    }

    @Test
    void testColumnsInReverseOrder()
    {
        assertQuery("SELECT shippriority, clerk, totalprice FROM orders");
    }

    @Test
    void testUnsupportedPredicateTypeStaysCorrect()
    {
        assertQuery(
                "SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3) AND totalprice > 200000 ORDER BY orderkey",
                "VALUES (3)");
    }
}
