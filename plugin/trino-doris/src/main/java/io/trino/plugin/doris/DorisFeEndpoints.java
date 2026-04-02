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
import io.trino.spi.TrinoException;

import java.util.Comparator;
import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class DorisFeEndpoints
{
    private DorisFeEndpoints() {}

    public static List<String> getHttpEndpoints(DorisConfig config)
    {
        List<String> endpoints = config.getFenodes()
                .stream()
                .flatMap(value -> List.of(value.split(",")).stream())
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .sorted(Comparator.naturalOrder())
                .toList();
        if (endpoints.isEmpty()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "doris.fenodes must be set for Doris FE access");
        }
        return endpoints;
    }

    public static List<String> getHosts(DorisConfig config)
    {
        return getHttpEndpoints(config).stream()
                .map(HostAddress::fromString)
                .map(HostAddress::getHostText)
                .distinct()
                .toList();
    }
}
