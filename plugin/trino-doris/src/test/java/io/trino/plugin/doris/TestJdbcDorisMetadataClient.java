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

import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

final class TestJdbcDorisMetadataClient
{
    @Test
    void testListSchemaNamesAcceptsDorisEngineAndSkipsSystemSchemas()
    {
        AtomicReference<String> preparedSql = new AtomicReference<>();
        JdbcDorisMetadataClient client = new JdbcDorisMetadataClient(connectionFactory(
                preparedSql,
                List.of(Map.of("TABLE_SCHEMA", "test")),
                new ArrayList<>()));

        assertThat(client.listSchemaNames()).containsExactly("test");
        assertThat(preparedSql.get())
                .contains("LOWER(TABLE_SCHEMA) NOT IN ('information_schema', '__internal_schema', 'mysql')")
                .contains("UPPER(COALESCE(ENGINE, '')) IN ('OLAP', 'DORIS')");
    }

    @Test
    void testListTablesKeepsReadableTablesPredicateWhenFilteringSchema()
    {
        AtomicReference<String> preparedSql = new AtomicReference<>();
        List<String> boundParameters = new ArrayList<>();
        JdbcDorisMetadataClient client = new JdbcDorisMetadataClient(connectionFactory(
                preparedSql,
                List.of(Map.of("TABLE_SCHEMA", "Test", "TABLE_NAME", "Nation")),
                boundParameters));

        assertThat(client.listTables(Optional.of("test")))
                .containsExactly(new SchemaTableName("Test", "Nation"));
        assertThat(boundParameters).containsExactly("test");
        assertThat(preparedSql.get())
                .contains("LOWER(TABLE_SCHEMA) NOT IN ('information_schema', '__internal_schema', 'mysql')")
                .contains("UPPER(COALESCE(ENGINE, '')) IN ('OLAP', 'DORIS')");
    }

    private static DorisJdbcConnectionFactory connectionFactory(AtomicReference<String> preparedSql, List<Map<String, Object>> rows, List<String> boundParameters)
    {
        return new DorisJdbcConnectionFactory(new DorisConfig().setJdbcUrl("jdbc:mysql://example.invalid:9030/"))
        {
            @Override
            public Connection openConnection()
            {
                return createConnection(preparedSql, rows, boundParameters);
            }
        };
    }

    private static Connection createConnection(AtomicReference<String> preparedSql, List<Map<String, Object>> rows, List<String> boundParameters)
    {
        return (Connection) Proxy.newProxyInstance(
                TestJdbcDorisMetadataClient.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "prepareStatement" -> {
                        preparedSql.set((String) args[0]);
                        yield createPreparedStatement(rows, boundParameters);
                    }
                    case "close" -> null;
                    case "isClosed" -> false;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "TestConnection";
                    default -> throw new UnsupportedOperationException("Unexpected connection method: " + method.getName());
                });
    }

    private static PreparedStatement createPreparedStatement(List<Map<String, Object>> rows, List<String> boundParameters)
    {
        return (PreparedStatement) Proxy.newProxyInstance(
                TestJdbcDorisMetadataClient.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "setString" -> {
                        boundParameters.add((String) args[1]);
                        yield null;
                    }
                    case "executeQuery" -> createResultSet(rows);
                    case "close" -> null;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "TestPreparedStatement";
                    default -> throw new UnsupportedOperationException("Unexpected prepared statement method: " + method.getName());
                });
    }

    private static ResultSet createResultSet(List<Map<String, Object>> rows)
    {
        AtomicInteger index = new AtomicInteger(-1);
        AtomicBoolean wasNull = new AtomicBoolean();

        return (ResultSet) Proxy.newProxyInstance(
                TestJdbcDorisMetadataClient.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "next" -> index.incrementAndGet() < rows.size();
                    case "getString" -> {
                        Object value = rows.get(index.get()).get(args[0]);
                        wasNull.set(value == null);
                        yield (value == null) ? null : value.toString();
                    }
                    case "wasNull" -> wasNull.get();
                    case "close" -> null;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "TestResultSet";
                    default -> throw new UnsupportedOperationException("Unexpected result set method: " + method.getName());
                });
    }
}
