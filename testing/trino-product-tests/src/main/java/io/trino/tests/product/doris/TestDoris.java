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

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.DORIS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onDoris;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDoris
        extends ProductTest
{
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

            assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM doris"))
                    .contains(row("test"));
            assertThat(onTrino().executeQuery("SHOW TABLES FROM doris.test"))
                    .contains(row("nation"));
            assertThat(onTrino().executeQuery("SELECT name FROM doris.test.nation WHERE nationkey = 8"))
                    .containsOnly(row("INDIA"));
            assertThat(onTrino().executeQuery("SELECT regionkey, count(*) FROM doris.test.nation GROUP BY regionkey"))
                    .containsOnly(row(0, 1), row(1, 1), row(2, 1));
        }
        finally {
            onDoris().executeQuery("DROP TABLE IF EXISTS test.nation");
        }
    }
}
