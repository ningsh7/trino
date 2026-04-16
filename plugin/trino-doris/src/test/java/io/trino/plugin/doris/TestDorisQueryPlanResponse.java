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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestDorisQueryPlanResponse
{
    @Test
    void testIgnoresUnknownTabletFields()
            throws Exception
    {
        String json = """
                {
                  "status": 200,
                  "opaqued_query_plan": "opaque-plan",
                  "partitions": {
                    "13989": {
                      "routings": ["10.0.0.1:9060"],
                      "version": 12
                    }
                  },
                  "meta": {
                    "request_id": "abc"
                  }
                }
                """;

        DorisQueryPlanResponse response = new ObjectMapper().readValue(json, DorisQueryPlanResponse.class);

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.opaquedQueryPlan()).isEqualTo("opaque-plan");
        assertThat(response.partitions().get("13989"))
                .isEqualTo(new DorisQueryPlanTablet(java.util.List.of("10.0.0.1:9060")));
    }
}
