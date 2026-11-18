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
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.StandardColumnMappings.toLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestXCloudTypes
{
    static JdbcTypeHandle handle(String name, int precision, int scale)
    {
        return new JdbcTypeHandle(Types.OTHER, Optional.of(name), Optional.of(precision), Optional.of(scale), Optional.empty(), Optional.empty());
    }

    static ColumnMapping mapping(String name, int precision, int scale)
    {
        return XCloudTypeMappings.columnMapping(testSessionBuilder().build().toConnectorSession(), handle(name, precision, scale)).orElseThrow();
    }

    @Test
    public void testCatalogDecimalDeclarations()
    {
        assertThat(XCloudTypeMappings.decimalType(XCloudClient.catalogType(0, " number ( 5 , -2 ) ", Optional.of(24)))).contains(DecimalType.createDecimalType(7, 0));
        assertThat(XCloudTypeMappings.decimalType(XCloudClient.catalogType(0, "NUMBER(*,9)", Optional.of(24)))).contains(DecimalType.createDecimalType(38, 9));
        assertThat(XCloudTypeMappings.decimalType(XCloudClient.catalogType(0, "NUMBER(15)", Optional.of(24)))).contains(DecimalType.createDecimalType(15, 0));
        // Unconstrained NUMBER has a value-dependent scale. Storage bytes do not supply a decimal precision.
        assertThat(XCloudTypeMappings.decimalType(XCloudClient.catalogType(0, "NUMBER", Optional.of(24)))).isEmpty();
        assertThat(XCloudClient.catalogType(93, "TIMESTAMP(9) WITH TIME ZONE", Optional.of(12)).decimalDigits()).isEmpty();
        assertThat(XCloudClient.catalogType(93, "TIMESTAMP", Optional.of(12)).decimalDigits()).isEmpty();
    }

    @Test
    public void testDecimalBounds()
    {
        assertThat(XCloudTypeMappings.decimalType(handle("NUMBER", 5, -2))).contains(DecimalType.createDecimalType(7, 0));
        assertThat(XCloudTypeMappings.decimalType(handle("NUMBER", 5, 8))).contains(DecimalType.createDecimalType(8, 8));
        for (int[] values : new int[][] {{0, 0}, {38, -1}, {10, 39}, {39, 1}, {1, -85}}) {
            assertThat(XCloudTypeMappings.decimalType(handle("NUMBER", values[0], values[1]))).isEmpty();
        }
    }

    @Test
    public void testExactDecimalAndNull()
            throws Exception
    {
        PreparedStatement statement = mock(PreparedStatement.class);
        ColumnMapping shortMapping = mapping("NUMBER", 15, 4);
        ((LongWriteFunction) shortMapping.getWriteFunction()).set(statement, 1, 123456789012345L);
        verify(statement).setString(1, "12345678901.2345");
        ColumnMapping longMapping = mapping("NUMBER", 38, 9);
        ((ObjectWriteFunction) longMapping.getWriteFunction()).set(statement, 2, Int128.valueOf(new java.math.BigInteger("12345678901234567890123456789012345678")));
        verify(statement).setString(2, "12345678901234567890123456789.012345678");
        longMapping.getWriteFunction().setNull(statement, 2);
        verify(statement).setNull(2, Types.VARCHAR);
        ResultSet result = mock(ResultSet.class);
        when(result.getBigDecimal(1)).thenReturn(new BigDecimal("12.3456"));
        assertThat(((LongReadFunction) shortMapping.getReadFunction()).readLong(result, 1)).isEqualTo(123456);
        when(result.getBigDecimal(1)).thenReturn(new BigDecimal("12.34567"));
        assertThatThrownBy(() -> ((LongReadFunction) shortMapping.getReadFunction()).readLong(result, 1)).isInstanceOf(ArithmeticException.class);
    }

    @Test
    public void testTemporalRoundTrips()
            throws Exception
    {
        // Run with UTC JVM; values include local times in a US DST gap and overlap.
        for (String value : new String[] {"1969-12-31T23:59:59", "2024-02-29T12:34:56", "2024-03-10T02:30:00", "2024-11-03T01:30:00"}) {
            for (int precision : new int[] {0, 3, 6, 9}) {
                LocalDateTime local = LocalDateTime.parse(value).withNano(switch (precision) {
                    case 3 -> 123000000;
                    case 6 -> 123456000;
                    case 9 -> 123456789;
                    default -> 0;
                });
                TimestampType type = createTimestampType(precision);
                ColumnMapping mapping = mapping("TIMESTAMP", 29, precision);
                ResultSet result = mock(ResultSet.class);
                PreparedStatement statement = mock(PreparedStatement.class);
                when(result.getTimestamp(1)).thenReturn(Timestamp.valueOf(local));
                if (type.isShort()) {
                    long expected = toTrinoTimestamp(type, local);
                    assertThat(((LongReadFunction) mapping.getReadFunction()).readLong(result, 1)).isEqualTo(expected);
                    ((LongWriteFunction) mapping.getWriteFunction()).set(statement, 1, expected);
                }
                else {
                    LongTimestamp expected = toLongTrinoTimestamp(type, local);
                    assertThat(((ObjectReadFunction) mapping.getReadFunction()).readObject(result, 1)).isEqualTo(expected);
                    ((ObjectWriteFunction) mapping.getWriteFunction()).set(statement, 1, expected);
                }
                verify(statement).setTimestamp(1, Timestamp.valueOf(local));
                mapping.getWriteFunction().setNull(statement, 1);
                verify(statement).setNull(1, Types.TIMESTAMP);
            }
        }
        assertThat(mapping("DATE", 7, 0).getType()).isEqualTo(createTimestampType(0));
        assertThatThrownBy(() -> XCloudTypeMappings.checkedTimestamp(LocalDateTime.of(0, 1, 1, 0, 0))).hasMessageContaining("year");
    }

    @Test
    public void testTemporalPrecisionMismatch()
            throws Exception
    {
        ResultSet result = mock(ResultSet.class);
        when(result.getTimestamp(1)).thenReturn(Timestamp.valueOf("2024-01-01 00:00:00.000000001"));
        assertThatThrownBy(() -> ((LongReadFunction) mapping("TIMESTAMP", 29, 6).getReadFunction()).readLong(result, 1)).hasMessageContaining("exceeds declared precision");
        assertThatThrownBy(() -> XCloudTypeMappings.checkedTimestamp(LocalDateTime.of(1582, 10, 10, 0, 0))).hasMessageContaining("normalization");
    }

    @Test
    public void testDateKeepsSecondsWithoutFraction()
            throws Exception
    {
        PreparedStatement statement = mock(PreparedStatement.class);
        java.time.LocalDateTime local = java.time.LocalDateTime.parse("2024-02-29T12:34:56");
        ColumnMapping date = mapping("DATE", 7, 0);
        long micros = toTrinoTimestamp(createTimestampType(0), local);
        ((LongWriteFunction) date.getWriteFunction()).set(statement, 1, micros);
        verify(statement).setString(1, "2024-02-29 12:34:56");
        date.getWriteFunction().setNull(statement, 1);
        verify(statement).setNull(1, Types.VARCHAR);
        ((LongWriteFunction) XCloudTypeMappings.writeMapping(io.trino.spi.type.DateType.DATE).getWriteFunction()).set(statement, 2, local.toLocalDate().toEpochDay());
        verify(statement).setString(2, "2024-02-29 00:00:00");
        assertThatThrownBy(() -> ((LongWriteFunction) date.getWriteFunction()).set(statement, 3, micros + 1)).hasMessageContaining("fractional seconds");
        ResultSet result = mock(ResultSet.class);
        when(result.getTimestamp(1)).thenReturn(Timestamp.valueOf(local));
        assertThat(((LongReadFunction) date.getReadFunction()).readLong(result, 1)).isEqualTo(micros);
    }

    @Test
    public void testTextBinaryAndUnsupported()
            throws Exception
    {
        PreparedStatement statement = mock(PreparedStatement.class);
        XCloudTypeMappings.textWriter(6).set(statement, 1, utf8Slice("中文"));
        verify(statement).setString(1, "中文");
        assertThatThrownBy(() -> XCloudTypeMappings.textWriter(5).set(statement, 1, utf8Slice("中文"))).hasMessageContaining("UTF-8 bytes");
        assertThat(XCloudTypeMappings.writeMapping(VarcharType.createVarcharType(6000)).getDataType()).isEqualTo("CLOB");
        assertThatThrownBy(() -> XCloudTypeMappings.writeMapping(VarbinaryType.VARBINARY)).hasMessageContaining("Unsupported XCloud write");
        assertThatThrownBy(() -> XCloudTypeMappings.writeMapping(BooleanType.BOOLEAN)).hasMessageContaining("Unsupported XCloud write");
        ResultSet result = mock(ResultSet.class);
        when(result.getBlob(1)).thenReturn(new javax.sql.rowset.serial.SerialBlob(new byte[] {0, -1, 42}));
        assertThat(((SliceReadFunction) mapping("BLOB", 0, 0).getReadFunction()).readSlice(result, 1).getBytes()).containsExactly(0, -1, 42);
        when(result.getClob(1)).thenReturn(new javax.sql.rowset.serial.SerialClob("中文".toCharArray()));
        assertThat(((SliceReadFunction) mapping("CLOB", 0, 0).getReadFunction()).readSlice(result, 1).toStringUtf8()).isEqualTo("中文");
    }
}
