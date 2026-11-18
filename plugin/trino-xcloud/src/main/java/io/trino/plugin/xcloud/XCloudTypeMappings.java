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
package io.trino.plugin.xcloud;

import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.Optional;

import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.toIntExact;

final class XCloudTypeMappings
{
    private XCloudTypeMappings() {}

    static Optional<ColumnMapping> columnMapping(ConnectorSession session, JdbcTypeHandle handle)
    {
        // XCloud's type codes are not consistently standard (e.g. INT=-1 in getTypeInfo).
        // Prefer the native type name. Never reinterpret an unknown named type merely by its code.
        String name = handle.jdbcTypeName().orElse("").toUpperCase(Locale.ROOT);
        Optional<ColumnMapping> mapping = switch (name) {
            case "INT", "INTEGER" -> Optional.of(integerColumnMapping());
            case "LONG", "BIGINT" -> Optional.of(bigintColumnMapping());
            case "DOUBLE" -> Optional.of(ColumnMapping.doubleMapping(DOUBLE, ResultSet::getDouble, doubleWriteFunction(), DISABLE_PUSHDOWN));
            case "NUMBER", "DECIMAL", "NUMERIC" -> decimalType(handle).map(XCloudTypeMappings::decimalMapping);
            case "CHAR" -> handle.columnSize().filter(size -> size > 0 && size <= CharType.MAX_LENGTH).map(size ->
                    ColumnMapping.sliceMapping(CharType.createCharType(size), charReadFunction(CharType.createCharType(size)), textWriter(size), DISABLE_PUSHDOWN));
            case "VARCHAR", "VARCHAR2", "STRING", "CLOB" -> Optional.of(ColumnMapping.sliceMapping(createUnboundedVarcharType(), (name.equals("CLOB") ? clobReader() : varcharReadFunction(createUnboundedVarcharType())), name.equals("CLOB") ? varcharWriteFunction() : textWriter(handle.columnSize().orElse(6000)), DISABLE_PUSHDOWN));
            case "BLOB" -> Optional.of(ColumnMapping.sliceMapping(VARBINARY, blobReader(), SliceWriteFunction.of(Types.BLOB, (_, _, _) -> { throw unsupported("BLOB writes: XCloud 2.14.7 has no implemented binary setter"); }), DISABLE_PUSHDOWN));
            case "DATE" -> Optional.of(ColumnMapping.longMapping(
                    TimestampType.createTimestampType(0),
                    (result, index) -> toTrinoTimestamp(TimestampType.createTimestampType(0), readTimestamp(result, index, TimestampType.createTimestampType(0))),
                    dateTimestampWriter(),
                    DISABLE_PUSHDOWN));
            case "TIMESTAMP" -> handle.decimalDigits().filter(p -> p >= 0 && p <= 9).map(p -> timestampMapping(TimestampType.createTimestampType(p)));
            default -> Optional.empty();
        };
        if (mapping.isPresent() || getUnsupportedTypeHandling(session) != CONVERT_TO_VARCHAR) {
            return mapping;
        }
        return Optional.of(ColumnMapping.sliceMapping(createUnboundedVarcharType(), varcharReadFunction(createUnboundedVarcharType()), SliceWriteFunction.of(
                Types.VARCHAR,
                (_, _, _) -> { throw unsupported("Writing a type converted to varchar"); }), DISABLE_PUSHDOWN));
    }

    private static io.trino.plugin.jdbc.SliceReadFunction blobReader()
    {
        return (result, index) -> {
            try (java.io.InputStream stream = result.getBlob(index).getBinaryStream()) {
                java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
                byte[] buffer = new byte[8192];
                int length;
                while ((length = stream.read(buffer)) != -1) {
                    if (length == 0) {
                        throw new java.io.IOException("XCloud BLOB reader returned zero bytes");
                    }
                    bytes.write(buffer, 0, length);
                }
                return io.airlift.slice.Slices.wrappedBuffer(bytes.toByteArray());
            }
            catch (java.io.IOException e) {
                throw new java.sql.SQLException("Reading XCloud BLOB failed", e);
            }
        };
    }

    private static io.trino.plugin.jdbc.SliceReadFunction clobReader()
    {
        return (result, index) -> {
            try (java.io.Reader reader = result.getClob(index).getCharacterStream()) {
                StringBuilder value = new StringBuilder();
                char[] buffer = new char[8192];
                int length;
                // XCloud overrides read(char[]); read(char[], offset, length) and transferTo are unavailable.
                while ((length = reader.read(buffer)) != -1) {
                    if (length == 0) {
                        throw new java.io.IOException("XCloud CLOB reader returned zero characters");
                    }
                    value.append(buffer, 0, length);
                }
                return io.airlift.slice.Slices.utf8Slice(value.toString());
            }
            catch (java.io.IOException e) {
                throw new java.sql.SQLException("Reading XCloud CLOB failed", e);
            }
        };
    }

    static Optional<DecimalType> decimalType(JdbcTypeHandle handle)
    {
        int precision = handle.columnSize().orElse(0);
        if (precision < 1 || precision > 38 || handle.decimalDigits().isEmpty()) {
            return Optional.empty();
        }
        int scale = handle.decimalDigits().orElseThrow();
        if (scale < -84 || scale > 127) {
            return Optional.empty();
        }
        int mappedPrecision = Math.max(precision, scale) - Math.min(scale, 0);
        return mappedPrecision <= 38 ? Optional.of(DecimalType.createDecimalType(mappedPrecision, Math.max(scale, 0))) : Optional.empty();
    }

    static ColumnMapping decimalMapping(DecimalType type)
    {
        // Native INSERT cannot bind BigDecimal; decimal text travels without a double conversion.
        if (type.isShort()) {
            return ColumnMapping.longMapping(type, shortDecimalReadFunction(type), decimalShortWriter(type));
        }
        return ColumnMapping.objectMapping(type, longDecimalReadFunction(type), decimalLongWriter(type));
    }

    private static LongWriteFunction decimalShortWriter(DecimalType type)
    {
        return LongWriteFunction.of(Types.VARCHAR, (statement, index, value) -> statement.setString(index, BigDecimal.valueOf(value, type.getScale()).toPlainString()));
    }

    private static ObjectWriteFunction decimalLongWriter(DecimalType type)
    {
        return new ObjectWriteFunction()
        {
            @Override
            public Class<?> getJavaType()
            {
                return Int128.class;
            }

            @Override
            public void set(java.sql.PreparedStatement statement, int index, Object value)
                    throws java.sql.SQLException
            {
                statement.setString(index, new BigDecimal(((Int128) value).toBigInteger(), type.getScale()).toPlainString());
            }

            @Override
            public void setNull(java.sql.PreparedStatement statement, int index)
                    throws java.sql.SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    static SliceWriteFunction textWriter(int bytes)
    {
        return SliceWriteFunction.of(Types.VARCHAR, (statement, index, value) -> {
            if (value.length() > bytes) {
                throw unsupported("XCloud text exceeds " + bytes + " UTF-8 bytes");
            }
            statement.setString(index, value.toStringUtf8());
        });
    }

    static String formatDate(LocalDateTime value)
    {
        checkedTimestamp(value);
        if (value.getNano() != 0) {
            throw unsupported("XCloud DATE cannot represent fractional seconds");
        }
        return value.format(java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss", Locale.ROOT));
    }

    private static LongWriteFunction dateTimestampWriter()
    {
        return LongWriteFunction.of(Types.VARCHAR, (statement, index, value) -> statement.setString(index, formatDate(fromTrinoTimestamp(value))));
    }

    static ColumnMapping timestampMapping(TimestampType type)
    {
        if (type.isShort()) {
            return ColumnMapping.longMapping(
                    type,
                    (result, index) -> toTrinoTimestamp(type, readTimestamp(result, index, type)),
                    LongWriteFunction.of(Types.TIMESTAMP, (statement, index, value) -> statement.setTimestamp(index, checkedTimestamp(fromTrinoTimestamp(value)))),
                    DISABLE_PUSHDOWN);
        }
        return ColumnMapping.objectMapping(
                type,
                ObjectReadFunction.of(LongTimestamp.class, (result, index) -> toLongTrinoTimestamp(type, readTimestamp(result, index, type))),
                new ObjectWriteFunction()
                {
                    @Override
                    public Class<?> getJavaType()
                    {
                        return LongTimestamp.class;
                    }

                    @Override
                    public void set(java.sql.PreparedStatement statement, int index, Object value)
                            throws java.sql.SQLException
                    {
                        statement.setTimestamp(index, checkedTimestamp(fromLongTrinoTimestamp((LongTimestamp) value, type.getPrecision())));
                    }

                    @Override
                    public void setNull(java.sql.PreparedStatement statement, int index)
                            throws java.sql.SQLException
                    {
                        statement.setNull(index, Types.TIMESTAMP);
                    }
                },
                DISABLE_PUSHDOWN);
    }

    private static LocalDateTime readTimestamp(ResultSet result, int index, TimestampType type)
            throws java.sql.SQLException
    {
        LocalDateTime value = result.getTimestamp(index).toLocalDateTime();
        int quantum = 1;
        for (int digit = type.getPrecision(); digit < 9; digit++) {
            quantum *= 10;
        }
        if (value.getNano() % quantum != 0) {
            throw new java.sql.SQLException("XCloud timestamp value exceeds declared precision " + type.getPrecision());
        }
        return value;
    }

    static Timestamp checkedTimestamp(LocalDateTime value)
    {
        if (value.getYear() < 1 || value.getYear() > 9999) {
            throw unsupported("XCloud timestamp year must be between 1 and 9999");
        }
        Timestamp timestamp = Timestamp.valueOf(value);
        if (!timestamp.toLocalDateTime().equals(value)) {
            throw unsupported("XCloud JDBC Timestamp cannot represent this local date/time without normalization");
        }
        return timestamp;
    }

    static WriteMapping writeMapping(Type type)
    {
        if (type == TINYINT || type == SMALLINT || type == INTEGER) {
            return WriteMapping.longMapping("INT", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("LONG", bigintWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("DOUBLE", doubleWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("DOUBLE", LongWriteFunction.of(Types.DOUBLE, (statement, index, value) -> statement.setDouble(index, Float.intBitsToFloat(toIntExact(value)))));
        }
        if (type instanceof DecimalType decimal) {
            String sql = "NUMBER(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
            return decimal.isShort() ? WriteMapping.longMapping(sql, decimalShortWriter(decimal)) : WriteMapping.objectMapping(sql, decimalLongWriter(decimal));
        }
        if (type instanceof VarcharType) {
            // Trino lengths count characters; the remote limit counts bytes. CLOB avoids a false capacity promise.
            return WriteMapping.sliceMapping("CLOB", varcharWriteFunction());
        }
        if (type instanceof CharType character && character.getLength() <= 6000) {
            return WriteMapping.sliceMapping("CHAR(" + character.getLength() + ")", textWriter(character.getLength()));
        }
        if (type == DATE) {
            return WriteMapping.longMapping("DATE", LongWriteFunction.of(
                    Types.VARCHAR,
                    (statement, index, value) -> statement.setString(index, formatDate(LocalDate.ofEpochDay(value).atStartOfDay()))));
        }
        if (type instanceof TimestampType timestamp && timestamp.getPrecision() <= 9) {
            ColumnMapping mapping = timestampMapping(timestamp);
            return timestamp.isShort() ? WriteMapping.longMapping("TIMESTAMP(" + timestamp.getPrecision() + ")", (LongWriteFunction) mapping.getWriteFunction())
                    : WriteMapping.objectMapping("TIMESTAMP(" + timestamp.getPrecision() + ")", (ObjectWriteFunction) mapping.getWriteFunction());
        }
        throw unsupported("Unsupported XCloud write type: " + type);
    }

    private static TrinoException unsupported(String message)
    {
        return new TrinoException(NOT_SUPPORTED, message);
    }
}
