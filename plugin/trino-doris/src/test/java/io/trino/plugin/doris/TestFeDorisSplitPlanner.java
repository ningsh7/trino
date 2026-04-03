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

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestFeDorisSplitPlanner
{
    @Test
    void testSelectBackendsBalancesTabletAssignments()
    {
        DorisQueryPlanResponse queryPlan = new DorisQueryPlanResponse(
                200,
                "opaque-plan",
                Map.of(
                        "100", new DorisQueryPlanTablet(List.of("be-1:9060", "be-2:9060")),
                        "101", new DorisQueryPlanTablet(List.of("be-1:9060")),
                        "102", new DorisQueryPlanTablet(List.of("be-1:9060", "be-2:9060"))));

        assertThat(FeDorisSplitPlanner.selectBackends(queryPlan))
                .isEqualTo(Map.of(
                        "be-1:9060", List.of(100L, 101L),
                        "be-2:9060", List.of(102L)));
    }

    @Test
    void testBuildSplitsProducesDeterministicSplitPayloads()
    {
        DorisQueryPlanResponse queryPlan = new DorisQueryPlanResponse(
                200,
                "opaque-plan",
                Map.of(
                        "7", new DorisQueryPlanTablet(List.of("be-2:9060")),
                        "5", new DorisQueryPlanTablet(List.of("be-1:9060")),
                        "6", new DorisQueryPlanTablet(List.of("be-1:9060", "be-2:9060"))));

        assertThat(FeDorisSplitPlanner.buildSplits(new DorisTableHandle("sales", "orders"), queryPlan))
                .isEqualTo(List.of(
                        new DorisSplit("sales", "orders", "be-1:9060", List.of(5L), Optional.of("opaque-plan")),
                        new DorisSplit("sales", "orders", "be-2:9060", List.of(6L, 7L), Optional.of("opaque-plan"))));
    }

    @Test
    void testParseQueryPlanRejectsTextualFeErrors()
            throws Exception
    {
        FeDorisSplitPlanner planner = new FeDorisSplitPlanner(new DorisConfig(), new DorisQueryBuilder());
        Method parseQueryPlan = FeDorisSplitPlanner.class.getDeclaredMethod("parseQueryPlan", String.class);
        parseQueryPlan.setAccessible(true);

        assertThatThrownBy(() -> invokeParseQueryPlan(parseQueryPlan, planner, """
                {
                  "code": 1,
                  "msg": "planner failed",
                  "data": "errCode = 7, detailMessage = table type is not OLAP"
                }
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("planner failed")
                .hasMessageContaining("table type is not OLAP");
    }

    private static void invokeParseQueryPlan(Method method, FeDorisSplitPlanner planner, String responseBody)
            throws Throwable
    {
        try {
            method.invoke(planner, responseBody);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
