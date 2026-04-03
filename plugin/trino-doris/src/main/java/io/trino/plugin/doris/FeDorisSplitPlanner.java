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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class FeDorisSplitPlanner
        implements DorisSplitPlanner
{
    private final DorisConfig config;
    private final DorisQueryBuilder queryBuilder;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    @Inject
    public FeDorisSplitPlanner(DorisConfig config, DorisQueryBuilder queryBuilder)
    {
        this.config = requireNonNull(config, "config is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public List<DorisSplit> planSplits(DorisTableHandle tableHandle)
    {
        String sql = queryBuilder.buildSplitPlanningSql(tableHandle);
        DorisQueryPlanResponse queryPlan = fetchQueryPlan(tableHandle, sql);
        return buildSplits(tableHandle, queryPlan);
    }

    static List<DorisSplit> buildSplits(DorisTableHandle tableHandle, DorisQueryPlanResponse queryPlan)
    {
        Map<String, List<Long>> beToTablets = selectBackends(queryPlan);

        List<Map.Entry<String, List<Long>>> entries = new ArrayList<>(beToTablets.entrySet());
        entries.sort(Map.Entry.comparingByKey());

        List<DorisSplit> splits = new ArrayList<>(entries.size());
        for (Map.Entry<String, List<Long>> entry : entries) {
            List<Long> tabletIds = entry.getValue().stream()
                    .sorted()
                    .toList();
            splits.add(new DorisSplit(
                    tableHandle.remoteSchemaName(),
                    tableHandle.remoteTableName(),
                    entry.getKey(),
                    tabletIds,
                    Optional.ofNullable(queryPlan.opaquedQueryPlan())));
        }
        return List.copyOf(splits);
    }

    static Map<String, List<Long>> selectBackends(DorisQueryPlanResponse queryPlan)
    {
        Map<String, List<Long>> beToTablets = new LinkedHashMap<>();
        List<Map.Entry<String, DorisQueryPlanTablet>> tablets = queryPlan.partitions().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .toList();

        for (Map.Entry<String, DorisQueryPlanTablet> partition : tablets) {
            long tabletId = parseTabletId(partition.getKey());
            String targetBackend = chooseBackend(beToTablets, partition.getValue().routings(), tabletId);
            beToTablets.computeIfAbsent(targetBackend, ignored -> new ArrayList<>())
                    .add(tabletId);
        }
        return beToTablets;
    }

    private DorisQueryPlanResponse fetchQueryPlan(DorisTableHandle tableHandle, String sql)
    {
        List<String> failures = new ArrayList<>();
        for (String feEndpoint : getFeEndpoints()) {
            URI uri = URI.create("http://" + feEndpoint + "/api/" + tableHandle.remoteSchemaName() + "/" + tableHandle.remoteTableName() + "/_query_plan");
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .header("Authorization", basicAuthHeader())
                    .header("Content-Type", "application/json; charset=UTF-8")
                    .POST(HttpRequest.BodyPublishers.ofString(queryPlanRequestBody(sql), StandardCharsets.UTF_8))
                    .build();

            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (response.statusCode() != 200) {
                    failures.add("%s -> HTTP %s".formatted(feEndpoint, response.statusCode()));
                    continue;
                }
                return parseQueryPlan(response.body());
            }
            catch (IOException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                failures.add("%s -> %s".formatted(feEndpoint, e.getMessage()));
            }
        }

        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to fetch Doris query plan from FE nodes: " + failures);
    }

    private DorisQueryPlanResponse parseQueryPlan(String responseBody)
            throws IOException
    {
        JsonNode root = objectMapper.readTree(responseBody);
        JsonNode queryPlanNode = root.has("code") && root.has("data") ? root.get("data") : root;
        if (queryPlanNode == null || queryPlanNode.isNull()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Doris FE query plan response does not contain query plan data");
        }
        if (queryPlanNode.isTextual()) {
            String message = queryPlanNode.asText();
            if (root.hasNonNull("msg") && !root.get("msg").asText().isBlank()) {
                message = root.get("msg").asText() + ": " + message;
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Doris FE query plan request failed: " + message);
        }
        DorisQueryPlanResponse queryPlan = objectMapper.treeToValue(queryPlanNode, DorisQueryPlanResponse.class);
        if (queryPlan.status() != 200) {
            String message = root.hasNonNull("msg") ? root.get("msg").asText() : "";
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Doris FE query plan status is not OK: " + queryPlan.status() + (message.isBlank() ? "" : " (" + message + ")"));
        }
        return queryPlan;
    }

    private String queryPlanRequestBody(String sql)
    {
        try {
            return objectMapper.writeValueAsString(Map.of("sql", sql));
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to serialize Doris query plan request", e);
        }
    }

    private List<String> getFeEndpoints()
    {
        return DorisFeEndpoints.getHttpEndpoints(config);
    }

    private String basicAuthHeader()
    {
        String user = config.getUsername().orElse("");
        String password = config.getPassword().orElse("");
        String credentials = user + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    private static String chooseBackend(Map<String, List<Long>> beToTablets, List<String> routings, long tabletId)
    {
        String targetBackend = null;
        int smallestAssignment = Integer.MAX_VALUE;

        for (String routing : routings) {
            if (!beToTablets.containsKey(routing)) {
                return routing;
            }

            int assignmentCount = beToTablets.get(routing).size();
            if (assignmentCount < smallestAssignment) {
                targetBackend = routing;
                smallestAssignment = assignmentCount;
            }
        }

        if (targetBackend == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "No Doris backend is available for tablet " + tabletId);
        }
        return targetBackend;
    }

    private static long parseTabletId(String value)
    {
        try {
            return Long.parseLong(value);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid Doris tablet id: " + value, e);
        }
    }
}
