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

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Locale;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class DorisTypeMapper
{
    private static final int BIGINT_UNSIGNED_PRECISION = 20;
    private static final int MAX_DORIS_DATETIME_PRECISION = 6;

    private final DorisLargeintMapping largeintMapping;

    @Inject
    public DorisTypeMapper(DorisConfig config)
    {
        this.largeintMapping = requireNonNull(config, "config is null").getLargeintMapping();
    }

    public Type toTrinoType(DorisRemoteColumn column)
    {
        String normalizedType = normalizeTypeName(column.dataType());

        return switch (normalizedType) {
            case "BOOLEAN", "BOOL" -> BOOLEAN;
            case "TINYINT" -> isBooleanAlias(column) ? BOOLEAN : TINYINT;
            case "SMALLINT" -> SMALLINT;
            case "INT", "INTEGER" -> INTEGER;
            case "BIGINT" -> BIGINT;
            case "BIGINT_UNSIGNED" -> createDecimalType(BIGINT_UNSIGNED_PRECISION);
            case "LARGEINT" -> switch (largeintMapping) {
                // VARCHAR is the safe default because Doris LARGEINT can exceed Trino DECIMAL(38, 0).
                case VARCHAR -> createUnboundedVarcharType();
                case DECIMAL -> createDecimalType(MAX_PRECISION);
            };
            case "DECIMAL", "DECIMALV2", "DECIMALV3", "DECIMAL_V3", "DECIMAL32", "DECIMAL64", "DECIMAL128I", "DECIMAL128", "DECIMAL256" -> toDecimalType(column);
            case "FLOAT" -> REAL;
            case "DOUBLE" -> DOUBLE;
            case "CHAR" -> toCharType(column);
            case "VARCHAR" -> toVarcharType(column);
            case "DATE", "DATEV2", "DATE_V2" -> DATE;
            // Legacy DATETIME does not carry fractional precision, while DATETIMEV2 does.
            case "DATETIME" -> createTimestampType(0);
            case "DATETIMEV2", "DATETIME_V2" -> createTimestampType(timestampPrecision(column));
            // Complex and extra-wide Doris types stay VARCHAR until the Flight SQL reader can materialize them natively.
            case "STRING", "JSON", "JSONB", "ARRAY", "MAP", "STRUCT", "VARIANT", "IPV4", "IPV6", "BITMAP", "HLL", "QUANTILE_STATE", "AGG_STATE" -> createUnboundedVarcharType();
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported Doris type '%s' for column '%s'".formatted(column.dataType(), column.columnName()));
        };
    }

    private static boolean isBooleanAlias(DorisRemoteColumn column)
    {
        return column.columnSize().orElse(-1) == 0;
    }

    private static Type toDecimalType(DorisRemoteColumn column)
    {
        if (column.columnSize().isEmpty()) {
            return createUnboundedVarcharType();
        }

        int precision = column.columnSize().get();
        int scale = max(column.decimalDigits().orElse(0), 0);

        if (precision <= 0 || precision > MAX_PRECISION || scale > precision) {
            return createUnboundedVarcharType();
        }

        return createDecimalType(precision, scale);
    }

    private static Type toCharType(DorisRemoteColumn column)
    {
        if (column.columnSize().isEmpty() || column.columnSize().get() <= 0) {
            return createUnboundedVarcharType();
        }
        return createCharType(min(column.columnSize().get(), CharType.MAX_LENGTH));
    }

    private static Type toVarcharType(DorisRemoteColumn column)
    {
        if (column.columnSize().isEmpty() || column.columnSize().get() <= 0) {
            return createUnboundedVarcharType();
        }

        int length = column.columnSize().get();
        if (length >= VarcharType.MAX_LENGTH) {
            return createUnboundedVarcharType();
        }
        return createVarcharType(length);
    }

    private static int timestampPrecision(DorisRemoteColumn column)
    {
        return min(max(column.decimalDigits().orElse(0), 0), MAX_DORIS_DATETIME_PRECISION);
    }

    private static String normalizeTypeName(String dataType)
    {
        return dataType.trim()
                .toUpperCase(Locale.ENGLISH)
                .replace(' ', '_');
    }
}
