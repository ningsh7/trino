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

import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class DorisFilterToSql
{
    private static final String ALWAYS_TRUE = "1 = 1";
    private static final String ALWAYS_FALSE = "1 = 0";

    public boolean isPushdownSupported(DorisColumnHandle column, Domain domain)
    {
        requireNonNull(column, "column is null");
        requireNonNull(domain, "domain is null");
        return isTypePushdownSupported(column.columnType());
    }

    public Optional<String> toFilter(TupleDomain<ColumnHandle> tupleDomain)
    {
        requireNonNull(tupleDomain, "tupleDomain is null");

        if (tupleDomain.isAll()) {
            return Optional.empty();
        }
        if (tupleDomain.isNone()) {
            return Optional.of(ALWAYS_FALSE);
        }

        List<Map.Entry<ColumnHandle, Domain>> orderedDomains = tupleDomain.getDomains()
                .orElseThrow()
                .entrySet().stream()
                .sorted(comparing(entry -> ((DorisColumnHandle) entry.getKey()).columnName()))
                .toList();

        List<String> conjuncts = new ArrayList<>(orderedDomains.size());
        for (Map.Entry<ColumnHandle, Domain> entry : orderedDomains) {
            conjuncts.add(toPredicate((DorisColumnHandle) entry.getKey(), entry.getValue()));
        }

        if (conjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(String.join(" AND ", conjuncts));
    }

    private String toPredicate(DorisColumnHandle column, Domain domain)
    {
        Type type = column.columnType();
        if (!isTypePushdownSupported(type)) {
            throw new IllegalArgumentException("Unsupported Doris pushdown type: " + type);
        }

        String quotedColumn = DorisQueryBuilder.quoteIdentifier(column.columnName());
        ValueSet valueSet = domain.getValues();
        if (valueSet.isNone()) {
            return domain.isNullAllowed() ? quotedColumn + " IS NULL" : ALWAYS_FALSE;
        }
        if (valueSet.isAll()) {
            return domain.isNullAllowed() ? ALWAYS_TRUE : quotedColumn + " IS NOT NULL";
        }

        String predicate = toValueSetPredicate(quotedColumn, type, valueSet);
        if (!domain.isNullAllowed()) {
            return predicate;
        }
        return "(" + predicate + " OR " + quotedColumn + " IS NULL)";
    }

    private String toValueSetPredicate(String quotedColumn, Type type, ValueSet valueSet)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : valueSet.getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                singleValues.add(range.getSingleValue());
                continue;
            }

            List<String> rangeConjuncts = new ArrayList<>();
            if (!range.isLowUnbounded()) {
                rangeConjuncts.add(comparisonPredicate(quotedColumn, type, range.isLowInclusive() ? ">=" : ">", range.getLowBoundedValue()));
            }
            if (!range.isHighUnbounded()) {
                rangeConjuncts.add(comparisonPredicate(quotedColumn, type, range.isHighInclusive() ? "<=" : "<", range.getHighBoundedValue()));
            }

            if (rangeConjuncts.size() == 1) {
                disjuncts.add(rangeConjuncts.get(0));
            }
            else if (!rangeConjuncts.isEmpty()) {
                disjuncts.add("(" + String.join(" AND ", rangeConjuncts) + ")");
            }
        }

        if (singleValues.size() == 1) {
            disjuncts.add(comparisonPredicate(quotedColumn, type, "=", singleValues.get(0)));
        }
        else if (singleValues.size() > 1) {
            String inList = singleValues.stream()
                    .map(value -> toSqlLiteral(type, value))
                    .collect(joining(", "));
            disjuncts.add(quotedColumn + " IN (" + inList + ")");
        }

        if (disjuncts.isEmpty()) {
            return ALWAYS_FALSE;
        }
        if (disjuncts.size() == 1) {
            return disjuncts.get(0);
        }
        return "(" + String.join(" OR ", disjuncts) + ")";
    }

    private static String comparisonPredicate(String quotedColumn, Type type, String operator, Object value)
    {
        return quotedColumn + " " + operator + " " + toSqlLiteral(type, value);
    }

    private static String toSqlLiteral(Type type, Object value)
    {
        if (type == BOOLEAN) {
            return ((boolean) value) ? "TRUE" : "FALSE";
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            return Long.toString((long) value);
        }
        if (type == DATE) {
            return quoteStringLiteral(LocalDate.ofEpochDay((long) value).toString());
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return Decimals.toString((long) value, decimalType.getScale());
            }
            return Decimals.toString((Int128) value, decimalType.getScale());
        }
        if (type instanceof TimestampType timestampType && timestampType.isShort()) {
            SqlTimestamp timestamp = SqlTimestamp.newInstance(timestampType.getPrecision(), (long) value, 0);
            return quoteStringLiteral(timestamp.toString());
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return quoteStringLiteral(((Slice) value).toStringUtf8());
        }

        throw new IllegalArgumentException("Unsupported Doris literal type: " + type);
    }

    private static boolean isTypePushdownSupported(Type type)
    {
        if (type == BOOLEAN || type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT || type == DATE) {
            return true;
        }
        if (type instanceof DecimalType) {
            return true;
        }
        if (type instanceof TimestampType timestampType) {
            return timestampType.isShort();
        }
        return type instanceof VarcharType || type instanceof CharType;
    }

    private static String quoteStringLiteral(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }
}
