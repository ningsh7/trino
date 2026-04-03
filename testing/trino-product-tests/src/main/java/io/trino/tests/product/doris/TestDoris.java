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
package io.trino.tests.product.doris;

import io.airlift.units.Duration;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.DORIS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.utils.QueryExecutors.onDoris;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDoris
        extends ProductTest
{
    private static final Duration METADATA_VISIBILITY_TIMEOUT = new Duration(1, MINUTES);

    @Test(groups = {DORIS, PROFILE_SPECIFIC_TESTS})
    public void testReadFromDorisTable()
    {
        onDoris().executeQuery("CREATE DATABASE IF NOT EXISTS test");
        onDoris().executeQuery("DROP TABLE IF EXISTS test.nation");

        try {
            onDoris().executeQuery("""
                    CREATE TABLE test.nation (
                        nationkey INT,
                        name VARCHAR(25),
                        regionkey INT
                    )
                    DUPLICATE KEY(nationkey)
                    DISTRIBUTED BY HASH(nationkey) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                    """);
            onDoris().executeQuery("""
                    INSERT INTO test.nation VALUES
                        (0, 'ALGERIA', 0),
                        (8, 'INDIA', 2),
                        (24, 'UNITED STATES', 1)
                    """);

            assertEventually(METADATA_VISIBILITY_TIMEOUT, () -> {
                assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM doris"))
                        .contains(row("test"));
                assertThat(onTrino().executeQuery("SHOW TABLES FROM doris.test"))
                        .contains(row("nation"));
                assertThat(onTrino().executeQuery("SELECT name FROM doris.test.nation WHERE nationkey = 8"))
                        .containsOnly(row("INDIA"));
                assertThat(onTrino().executeQuery("SELECT regionkey, count(*) FROM doris.test.nation GROUP BY regionkey"))
                        .containsOnly(row(0, 1), row(1, 1), row(2, 1));
            });
        }
        finally {
            onDoris().executeQuery("DROP TABLE IF EXISTS test.nation");
        }
    }

    @Test(groups = {DORIS, PROFILE_SPECIFIC_TESTS})
    public void testDateTimeV2AndLargeintMappings()
    {
        onDoris().executeQuery("CREATE DATABASE IF NOT EXISTS test");
        onDoris().executeQuery("DROP TABLE IF EXISTS test.type_mapping");

        try {
            onDoris().executeQuery("""
                    CREATE TABLE test.type_mapping (
                        user_id BIGINT,
                        created_at DATETIMEV2(3),
                        large_id LARGEINT
                    )
                    DUPLICATE KEY(user_id)
                    DISTRIBUTED BY HASH(user_id) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                    """);
            onDoris().executeQuery("""
                    INSERT INTO test.type_mapping VALUES
                        (1001, '2026-03-31 09:15:01.123', CAST('123456789012345678901234567801' AS LARGEINT))
                    """);

            assertEventually(METADATA_VISIBILITY_TIMEOUT, () -> {
                assertThat(onTrino().executeQuery("SHOW COLUMNS FROM doris.test.type_mapping"))
                        .contains(row("user_id", "bigint", "", ""))
                        .contains(row("created_at", "timestamp(3)", "", ""))
                        .contains(row("large_id", "varchar", "", ""));
                assertThat(onTrino().executeQuery("SELECT CAST(created_at AS VARCHAR), large_id FROM doris.test.type_mapping"))
                        .containsOnly(row("2026-03-31 09:15:01.123", "123456789012345678901234567801"));
            });
        }
        finally {
            onDoris().executeQuery("DROP TABLE IF EXISTS test.type_mapping");
        }
    }
}
