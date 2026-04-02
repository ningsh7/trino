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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

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
}
