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

import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeZoneKey;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDorisJdbcPageSourceFactory
{
    @Test
    void testUsesJdbcForScalarAggregations()
    {
        DorisTableHandle handle = new DorisTableHandle("sales", "orders")
                .withAggregations(List.of(
                        new DorisAggregation("_trino_agg_0", "count", "COUNT(*)", BIGINT, Optional.empty()),
                        new DorisAggregation("_trino_agg_1", "count_distinct", "COUNT(DISTINCT `buyer_key`)", BIGINT, Optional.of("buyer_key"))));

        DorisSplit split = new DorisSplit("sales", "orders", "", List.of(), Optional.empty());

        assertThat(DorisJdbcPageSourceFactory.canUseJdbcResultPath(handle, split)).isTrue();
    }

    @Test
    void testSkipsJdbcForGroupedAggregationsAndLargeTopN()
    {
        DorisColumnHandle groupColumn = new DorisColumnHandle("regionkey", BIGINT, 0);
        DorisTableHandle groupedAggregation = new DorisTableHandle("sales", "orders")
                .withAggregations(List.of(groupColumn), List.of(new DorisAggregation("_trino_agg_0", "count", "COUNT(*)", BIGINT, Optional.empty())));
        DorisTableHandle largeTopN = new DorisTableHandle(
                "sales",
                "orders",
                "sales",
                "orders",
                TupleDomain.all(),
                Optional.of(List.of(new DorisColumnHandle("id", BIGINT, 0))),
                Optional.empty(),
                Optional.empty(),
                Optional.of(List.of(new DorisSortItem(new DorisColumnHandle("id", BIGINT, 0), SortOrder.DESC_NULLS_LAST))),
                java.util.OptionalLong.of(DorisJdbcPageSourceFactory.JDBC_TOPN_ROW_LIMIT + 1));
        DorisSplit split = new DorisSplit("sales", "orders", "", List.of(), Optional.empty());

        assertThat(DorisJdbcPageSourceFactory.canUseJdbcResultPath(groupedAggregation, split)).isFalse();
        assertThat(DorisJdbcPageSourceFactory.canUseJdbcResultPath(largeTopN, split)).isFalse();
    }

    @Test
    void testMaterializesSmallTopNResultThroughJdbc()
    {
        AtomicReference<String> executedSql = new AtomicReference<>();
        DorisJdbcPageSourceFactory factory = createFactory(
                executedSql,
                List.of(
                        row(7L, "north", "2024-07-25 10:02:23.500"),
                        row(5L, "south", "2024-07-24 11:12:13.000")));

        DorisColumnHandle id = new DorisColumnHandle("id", BIGINT, 0);
        DorisColumnHandle name = new DorisColumnHandle("name", createVarcharType(20), 1);
        DorisColumnHandle createdAt = new DorisColumnHandle("created_at", createTimestampType(3), 2);
        DorisTableHandle handle = new DorisTableHandle(
                "sales",
                "orders",
                "sales",
                "orders",
                TupleDomain.all(),
                Optional.of(List.of(id, name, createdAt)),
                Optional.empty(),
                Optional.empty(),
                Optional.of(List.of(new DorisSortItem(id, SortOrder.DESC_NULLS_LAST))),
                java.util.OptionalLong.of(10));

        ConnectorPageSource pageSource = factory.createPageSource(session("jdbc_topn"), handle, new DorisSplit("sales", "orders", "", List.of(), Optional.empty()), List.of(id, name, createdAt))
                .orElseThrow();
        SourcePage page = pageSource.getNextSourcePage();

        assertThat(executedSql.get()).isEqualTo("SELECT `id`, `name`, `created_at` FROM `sales`.`orders` ORDER BY `id` DESC LIMIT 10");
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(7L);
        assertThat(createVarcharType(20).getObjectValue(page.getBlock(1), 1)).isEqualTo("south");
        assertThat(createTimestampType(3).getObjectValue(page.getBlock(2), 0).toString()).isEqualTo("2024-07-25 10:02:23.500");
        assertThat(pageSource.getNextSourcePage()).isNull();
    }

    @Test
    void testMaterializesScalarAggregateResultThroughJdbc()
    {
        AtomicReference<String> executedSql = new AtomicReference<>();
        DorisJdbcPageSourceFactory factory = createFactory(
                executedSql,
                List.of(row(123L, new BigDecimal("42.50"), LocalDate.of(2024, 3, 20).toString())));

        DecimalType amountType = DecimalType.createDecimalType(10, 2);
        DorisColumnHandle countColumn = new DorisColumnHandle("_trino_agg_0", BIGINT, 0);
        DorisColumnHandle amountColumn = new DorisColumnHandle("_trino_agg_1", amountType, 1);
        DorisColumnHandle dateColumn = new DorisColumnHandle("_trino_agg_2", DATE, 2);
        DorisTableHandle handle = new DorisTableHandle("sales", "orders")
                .withAggregations(List.of(
                        new DorisAggregation("_trino_agg_0", "count", "COUNT(*)", BIGINT, Optional.empty()),
                        new DorisAggregation("_trino_agg_1", "sum", "SUM(`amount`)", amountType, Optional.of("amount")),
                        new DorisAggregation("_trino_agg_2", "max", "MAX(`created_on`)", DATE, Optional.of("created_on"))));

        ConnectorPageSource pageSource = factory.createPageSource(
                        session("jdbc_agg"),
                        handle,
                        new DorisSplit("sales", "orders", "", List.of(), Optional.empty()),
                        List.of(countColumn, amountColumn, dateColumn))
                .orElseThrow();
        SourcePage page = pageSource.getNextSourcePage();

        assertThat(executedSql.get()).isEqualTo("SELECT COUNT(*) AS `_trino_agg_0`, SUM(`amount`) AS `_trino_agg_1`, MAX(`created_on`) AS `_trino_agg_2` FROM `sales`.`orders`");
        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(123L);
        assertThat(amountType.getObjectValue(page.getBlock(1), 0).toString()).isEqualTo("42.50");
        assertThat(DATE.getLong(page.getBlock(2), 0)).isEqualTo(LocalDate.of(2024, 3, 20).toEpochDay());
    }

    private static DorisJdbcPageSourceFactory createFactory(AtomicReference<String> executedSql, List<List<Object>> rows)
    {
        DorisJdbcConnectionFactory connectionFactory = new DorisJdbcConnectionFactory(new TestingDriver(executedSql, rows), "jdbc:mysql://doris-fe:9030", new Properties());
        return new DorisJdbcPageSourceFactory(connectionFactory, new DorisQueryBuilder());
    }

    private static List<Object> row(Object... values)
    {
        return List.of(values);
    }

    private static ConnectorSession session(String queryId)
    {
        return new ConnectorSession()
        {
            @Override
            public String getQueryId()
            {
                return queryId;
            }

            @Override
            public Optional<String> getSource()
            {
                return Optional.of("test");
            }

            @Override
            public ConnectorIdentity getIdentity()
            {
                return ConnectorIdentity.ofUser("test");
            }

            @Override
            public TimeZoneKey getTimeZoneKey()
            {
                return TimeZoneKey.UTC_KEY;
            }

            @Override
            public Locale getLocale()
            {
                return Locale.ENGLISH;
            }

            @Override
            public Instant getStart()
            {
                return Instant.EPOCH;
            }

            @Override
            public Optional<String> getTraceToken()
            {
                return Optional.empty();
            }

            @Override
            public <T> T getProperty(String name, Class<T> type)
            {
                throw new UnsupportedOperationException("No test session properties");
            }
        };
    }

    private static final class TestingDriver
            implements Driver
    {
        private final AtomicReference<String> executedSql;
        private final List<List<Object>> rows;

        private TestingDriver(AtomicReference<String> executedSql, List<List<Object>> rows)
        {
            this.executedSql = executedSql;
            this.rows = rows;
        }

        @Override
        public Connection connect(String url, Properties info)
        {
            return (Connection) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {Connection.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "prepareStatement" -> createPreparedStatement((String) args[0], executedSql, rows);
                        case "setReadOnly", "close" -> null;
                        case "isClosed" -> false;
                        case "unwrap" -> proxy;
                        case "isWrapperFor" -> false;
                        case "toString" -> "TestingConnection";
                        default -> throw new UnsupportedOperationException("Unsupported Connection method: " + method.getName());
                    });
        }

        @Override
        public boolean acceptsURL(String url)
        {
            return true;
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
        {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion()
        {
            return 1;
        }

        @Override
        public int getMinorVersion()
        {
            return 0;
        }

        @Override
        public boolean jdbcCompliant()
        {
            return false;
        }

        @Override
        public Logger getParentLogger()
        {
            return Logger.getGlobal();
        }
    }

    private static PreparedStatement createPreparedStatement(String sql, AtomicReference<String> executedSql, List<List<Object>> rows)
    {
        return (PreparedStatement) Proxy.newProxyInstance(
                TestDorisJdbcPageSourceFactory.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "executeQuery" -> {
                        executedSql.set(sql);
                        yield createResultSet(rows);
                    }
                    case "close" -> null;
                    case "unwrap" -> proxy;
                    case "isWrapperFor" -> false;
                    case "toString" -> "TestingPreparedStatement[" + sql + "]";
                    default -> throw new UnsupportedOperationException("Unsupported PreparedStatement method: " + method.getName());
                });
    }

    private static ResultSet createResultSet(List<List<Object>> rows)
    {
        class Cursor
        {
            private int index = -1;
            private boolean wasNull;
        }

        Cursor cursor = new Cursor();

        return (ResultSet) Proxy.newProxyInstance(
                TestDorisJdbcPageSourceFactory.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if (name.equals("next")) {
                        cursor.index++;
                        return cursor.index < rows.size();
                    }
                    if (name.equals("close")) {
                        return null;
                    }
                    if (name.equals("wasNull")) {
                        return cursor.wasNull;
                    }
                    if (name.equals("unwrap")) {
                        return proxy;
                    }
                    if (name.equals("isWrapperFor")) {
                        return false;
                    }
                    if (name.equals("toString")) {
                        return "TestingResultSet";
                    }

                    int columnIndex = ((Integer) args[0]) - 1;
                    Object value = rows.get(cursor.index).get(columnIndex);
                    cursor.wasNull = value == null;

                    return switch (name) {
                        case "getBoolean" -> cursor.wasNull ? false : (boolean) value;
                        case "getByte" -> cursor.wasNull ? (byte) 0 : ((Number) value).byteValue();
                        case "getShort" -> cursor.wasNull ? (short) 0 : ((Number) value).shortValue();
                        case "getInt" -> cursor.wasNull ? 0 : ((Number) value).intValue();
                        case "getLong" -> cursor.wasNull ? 0L : ((Number) value).longValue();
                        case "getFloat" -> cursor.wasNull ? 0F : ((Number) value).floatValue();
                        case "getDouble" -> cursor.wasNull ? 0D : ((Number) value).doubleValue();
                        case "getBigDecimal" -> (BigDecimal) value;
                        case "getString" -> value == null ? null : value.toString();
                        default -> throw new UnsupportedOperationException("Unsupported ResultSet method: " + name);
                    };
                });
    }
}
