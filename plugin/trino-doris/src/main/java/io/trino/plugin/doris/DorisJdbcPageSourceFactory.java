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
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.multiplyExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class DorisJdbcPageSourceFactory
{
    static final long JDBC_TOPN_ROW_LIMIT = 1_000;

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .toFormatter();

    private final DorisJdbcConnectionFactory connectionFactory;
    private final DorisQueryBuilder queryBuilder;
    private final AtomicBoolean jdbcFastPathDisabled = new AtomicBoolean();

    @Inject
    public DorisJdbcPageSourceFactory(DorisJdbcConnectionFactory connectionFactory, DorisQueryBuilder queryBuilder)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
    }

    public Optional<ConnectorPageSource> createPageSource(ConnectorSession session, DorisTableHandle tableHandle, DorisSplit split, List<DorisColumnHandle> columns)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        if (!canUseJdbcResultPath(tableHandle, split)) {
            return Optional.empty();
        }
        if (jdbcFastPathDisabled.get()) {
            return Optional.empty();
        }

        List<DorisColumnHandle> queryColumns = tableHandle.aggregations().isPresent()
                ? columns
                : tableHandle.projectedColumns().orElse(columns);
        String sql = queryBuilder.buildSelectSql(
                tableHandle,
                queryColumns.stream()
                        .map(DorisColumnHandle::columnName)
                        .toList(),
                split.tabletIds());

        try {
            return Optional.of(new FixedPageSource(loadPages(session, sql, columns)));
        }
        catch (SQLException e) {
            if (isConnectionFailure(e)) {
                jdbcFastPathDisabled.set(true);
                return Optional.empty();
            }
            throw DorisJdbcConnectionFactory.jdbcOperationFailed("Failed to execute Doris FE JDBC result query", e);
        }
    }

    static boolean canUseJdbcResultPath(DorisTableHandle tableHandle, DorisSplit split)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(split, "split is null");

        if (!split.tabletIds().isEmpty()) {
            return false;
        }

        if (tableHandle.aggregations().isPresent()) {
            return tableHandle.groupingColumns().orElse(List.of()).isEmpty();
        }

        return tableHandle.sortOrder().isPresent()
                && tableHandle.limit().isPresent()
                && tableHandle.limit().getAsLong() <= JDBC_TOPN_ROW_LIMIT;
    }

    private List<Page> loadPages(ConnectorSession session, String sql, List<DorisColumnHandle> columns)
            throws SQLException
    {
        List<Page> pages = new ArrayList<>();
        PageBuilder pageBuilder = new PageBuilder(columns.stream()
                .map(DorisColumnHandle::columnType)
                .toList());

        try (Connection connection = connectionFactory.openConnection(session);
                PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                pageBuilder.declarePosition();
                for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                    DorisColumnHandle column = columns.get(columnIndex);
                    writeColumn(pageBuilder.getBlockBuilder(columnIndex), column.columnType(), resultSet, columnIndex + 1, column.columnName());
                }
                if (pageBuilder.isFull()) {
                    pages.add(pageBuilder.build());
                    pageBuilder.reset();
                }
            }
        }

        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return List.copyOf(pages);
    }

    private void writeColumn(BlockBuilder output, Type type, ResultSet resultSet, int columnIndex, String columnName)
            throws SQLException
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            boolean value = resultSet.getBoolean(columnIndex);
            if (resultSet.wasNull()) {
                output.appendNull();
            }
            else {
                type.writeBoolean(output, value);
            }
            return;
        }
        if (javaType == long.class) {
            writeLongValue(output, type, resultSet, columnIndex, columnName);
            return;
        }
        if (javaType == double.class) {
            double value = resultSet.getDouble(columnIndex);
            if (resultSet.wasNull()) {
                output.appendNull();
            }
            else {
                type.writeDouble(output, value);
            }
            return;
        }
        if (javaType == Slice.class) {
            String value = resultSet.getString(columnIndex);
            if (value == null) {
                output.appendNull();
            }
            else {
                writeSliceValue(output, type, value);
            }
            return;
        }
        if (javaType == Int128.class && type instanceof DecimalType decimalType) {
            BigDecimal value = resultSet.getBigDecimal(columnIndex);
            if (value == null) {
                output.appendNull();
            }
            else {
                decimalType.writeObject(output, Decimals.encodeScaledValue(coerceDecimalValue(columnName, value, decimalType), decimalType.getScale()));
            }
            return;
        }

        throw unsupportedType(type, columnName);
    }

    private void writeLongValue(BlockBuilder output, Type type, ResultSet resultSet, int columnIndex, String columnName)
            throws SQLException
    {
        if (type == TINYINT) {
            byte value = resultSet.getByte(columnIndex);
            if (resultSet.wasNull()) {
                output.appendNull();
            }
            else {
                type.writeLong(output, value);
            }
            return;
        }
        if (type == SMALLINT) {
            short value = resultSet.getShort(columnIndex);
            if (resultSet.wasNull()) {
                output.appendNull();
            }
            else {
                type.writeLong(output, value);
            }
            return;
        }
        if (type == INTEGER) {
            int value = resultSet.getInt(columnIndex);
            if (resultSet.wasNull()) {
                output.appendNull();
            }
            else {
                type.writeLong(output, value);
            }
            return;
        }
        if (type == BIGINT) {
            long value = resultSet.getLong(columnIndex);
            if (resultSet.wasNull()) {
                output.appendNull();
            }
            else {
                type.writeLong(output, value);
            }
            return;
        }
        if (type == REAL) {
            float value = resultSet.getFloat(columnIndex);
            if (resultSet.wasNull()) {
                output.appendNull();
            }
            else {
                type.writeLong(output, floatToRawIntBits(value));
            }
            return;
        }
        if (type == DATE) {
            String value = resultSet.getString(columnIndex);
            if (value == null) {
                output.appendNull();
            }
            else {
                type.writeLong(output, LocalDate.parse(value, DATE_FORMATTER).toEpochDay());
            }
            return;
        }
        if (type instanceof TimestampType timestampType && timestampType.isShort()) {
            String value = resultSet.getString(columnIndex);
            if (value == null) {
                output.appendNull();
            }
            else {
                type.writeLong(output, parseTimestampMicros(value, timestampType.getPrecision()));
            }
            return;
        }
        if (type instanceof DecimalType decimalType && decimalType.isShort()) {
            BigDecimal value = resultSet.getBigDecimal(columnIndex);
            if (value == null) {
                output.appendNull();
            }
            else {
                decimalType.writeLong(output, encodeShortScaledValue(coerceDecimalValue(columnName, value, decimalType), decimalType.getScale()));
            }
            return;
        }

        throw unsupportedType(type, columnName);
    }

    private static void writeSliceValue(BlockBuilder output, Type type, String value)
    {
        Slice slice = utf8Slice(value);
        if (type instanceof CharType charType) {
            type.writeSlice(output, truncateToLengthAndTrimSpaces(slice, charType));
            return;
        }
        type.writeSlice(output, slice);
    }

    private static long parseTimestampMicros(String value, int precision)
    {
        LocalDateTime timestamp = LocalDateTime.parse(value, TIMESTAMP_FORMATTER);
        long epochSecond = timestamp.toEpochSecond(UTC);
        long micros = multiplyExact(epochSecond, MICROSECONDS_PER_SECOND) + (timestamp.getNano() / 1_000L);
        if (precision < 6) {
            micros = round(micros, 6 - precision);
        }
        return micros;
    }

    private static BigDecimal coerceDecimalValue(String columnName, BigDecimal value, DecimalType type)
    {
        BigDecimal scaled = value.setScale(type.getScale(), UNNECESSARY);
        if (overflows(scaled, type.getPrecision())) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Doris decimal value '%s' for column '%s' exceeds Trino type '%s'".formatted(value, columnName, type));
        }
        return scaled;
    }

    private static boolean isConnectionFailure(SQLException exception)
    {
        for (Throwable failure = exception; failure != null; failure = failure.getCause()) {
            if (failure instanceof SQLException sqlException) {
                String sqlState = sqlException.getSQLState();
                if (sqlState != null && sqlState.startsWith("08")) {
                    return true;
                }
            }
            if (failure instanceof ConnectException) {
                return true;
            }
        }
        return false;
    }

    private static TrinoException unsupportedType(Type type, String columnName)
    {
        return new TrinoException(NOT_SUPPORTED, "Doris FE JDBC result type '%s' for column '%s' is not supported".formatted(type, columnName));
    }
}
