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

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record DorisSplit(
        String schemaName,
        String tableName,
        String beAddress,
        List<Long> tabletIds,
        Optional<String> opaquedQueryPlan)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(DorisSplit.class);

    public DorisSplit
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(beAddress, "beAddress is null");
        tabletIds = List.copyOf(requireNonNull(tabletIds, "tabletIds is null"));
        opaquedQueryPlan = requireNonNull(opaquedQueryPlan, "opaquedQueryPlan is null");
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        if (beAddress.isBlank()) {
            return List.of();
        }
        return List.of(HostAddress.fromString(beAddress));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(schemaName)
                + estimatedSizeOf(tableName)
                + estimatedSizeOf(beAddress)
                + estimatedSizeOf(tabletIds, ignored -> Long.BYTES)
                + sizeOf(opaquedQueryPlan, io.airlift.slice.SizeOf::estimatedSizeOf);
    }
}
