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

import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDorisFilterToSql
{
    @Test
    void testBuildFilterForNumericAndDateDomains()
    {
        DorisFilterToSql filterToSql = new DorisFilterToSql();
        DorisColumnHandle id = new DorisColumnHandle("id", BIGINT, 0);
        DorisColumnHandle createdOn = new DorisColumnHandle("created_on", DATE, 1);

        TupleDomain<ColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                id, Domain.singleValue(BIGINT, 42L),
                createdOn, Domain.singleValue(DATE, LocalDate.of(2024, 3, 20).toEpochDay())));

        assertThat(filterToSql.toFilter(filter).orElseThrow()).isEqualTo("`created_on` = '2024-03-20' AND `id` = 42");
    }

    @Test
    void testBuildFilterForVarcharInDomain()
    {
        DorisFilterToSql filterToSql = new DorisFilterToSql();
        DorisColumnHandle tag = new DorisColumnHandle("tag", createUnboundedVarcharType(), 0);

        TupleDomain<ColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                tag, Domain.multipleValues(createUnboundedVarcharType(), List.of(Slices.utf8Slice("alpha"), Slices.utf8Slice("beta")))));

        assertThat(filterToSql.toFilter(filter).orElseThrow()).isEqualTo("`tag` IN ('alpha', 'beta')");
    }

    @Test
    void testRealTypeIsNotPushedDown()
    {
        DorisFilterToSql filterToSql = new DorisFilterToSql();
        DorisColumnHandle score = new DorisColumnHandle("score", REAL, 0);

        assertThat(filterToSql.isPushdownSupported(score, Domain.singleValue(REAL, (long) floatToRawIntBits(1.5f)))).isFalse();
    }
}
