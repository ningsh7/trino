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

import io.trino.plugin.base.audit.RemoteOperationAuditConfig;
import io.trino.plugin.base.audit.RemoteOperationAuditEvent;
import io.trino.plugin.base.audit.RemoteOperationAuditStats;
import io.trino.plugin.base.audit.RemoteOperationAuditor;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

final class TestDorisJdbcConnectionFactory
{
    @Test
    void testAuditsReusedAndBackgroundConnections()
            throws Exception
    {
        List<RemoteOperationAuditEvent> events = new ArrayList<>();
        var auditor = new RemoteOperationAuditor(
                new RemoteOperationAuditConfig().setEnabled(true),
                events::add,
                new RemoteOperationAuditStats(),
                "company_doris",
                "doris",
                "worker");
        TestingDriver driver = new TestingDriver();
        DorisJdbcConnectionFactory factory = new DorisJdbcConnectionFactory(driver, "jdbc:mysql://fe:9030", new Properties(), auditor);
        try {
            for (ConnectorSession session : List.of(session("first"), session("first"), session("second"))) {
                try (Connection connection = factory.openConnection(session);
                        Statement statement = connection.createStatement()) {
                    statement.executeUpdate("SELECT 1");
                }
            }
            try (Connection connection = factory.openConnection();
                    Statement statement = connection.createStatement()) {
                statement.executeUpdate("SELECT 1");
            }
            assertThat(events).hasSize(4);
            assertThat(events).extracting(RemoteOperationAuditEvent::queryId).containsExactly("first", "first", "second", null);
            assertThat(events.getFirst().connectionId()).isEqualTo(events.get(1).connectionId());
            assertThat(events.get(2).connectionId()).isNotEqualTo(events.getFirst().connectionId());
            assertThat(events.getLast().origin()).isEqualTo(RemoteOperationAuditEvent.Origin.BACKGROUND);
            assertThat(events).allSatisfy(event -> assertThat(event.executionUser()).isEqualTo("source_user"));
        }
        finally {
            factory.close();
        }
    }

    @Test
    void testReusesConnectionWithinSameQuery()
            throws Exception
    {
        TestingDriver driver = new TestingDriver();
        DorisJdbcConnectionFactory factory = new DorisJdbcConnectionFactory(driver, "jdbc:mysql://doris-fe:9030", new Properties());
        ConnectorSession session = session("query_reuse");

        try {
            factory.beginQuery(session);

            Connection first = factory.openConnection(session);
            Connection firstDelegate = ((DorisJdbcConnectionFactory.CachedConnection) first).delegate();
            first.close();

            Connection second = factory.openConnection(session);
            Connection secondDelegate = ((DorisJdbcConnectionFactory.CachedConnection) second).delegate();
            second.close();

            assertThat(secondDelegate).isSameAs(firstDelegate);
            assertThat(driver.openedConnections()).isEqualTo(1);

            factory.cleanupQuery(session);
            assertThat(driver.closedConnections()).isEqualTo(1);
        }
        finally {
            factory.close();
        }
    }

    @Test
    void testDoesNotShareCheckedOutConnection()
            throws Exception
    {
        TestingDriver driver = new TestingDriver();
        DorisJdbcConnectionFactory factory = new DorisJdbcConnectionFactory(driver, "jdbc:mysql://doris-fe:9030", new Properties());
        ConnectorSession session = session("query_parallel");

        try {
            factory.beginQuery(session);

            Connection first = factory.openConnection(session);
            Connection second = factory.openConnection(session);

            assertThat(((DorisJdbcConnectionFactory.CachedConnection) first).delegate())
                    .isNotSameAs(((DorisJdbcConnectionFactory.CachedConnection) second).delegate());
            assertThat(driver.openedConnections()).isEqualTo(2);

            first.close();
            second.close();
            factory.cleanupQuery(session);

            assertThat(driver.closedConnections()).isEqualTo(2);
        }
        finally {
            factory.close();
        }
    }

    @Test
    void testDirtyConnectionIsNotReused()
            throws Exception
    {
        TestingDriver driver = new TestingDriver();
        DorisJdbcConnectionFactory factory = new DorisJdbcConnectionFactory(driver, "jdbc:mysql://doris-fe:9030", new Properties());
        ConnectorSession session = session("query_dirty");

        try {
            factory.beginQuery(session);

            Connection first = factory.openConnection(session);
            Connection firstDelegate = ((DorisJdbcConnectionFactory.CachedConnection) first).delegate();
            first.setAutoCommit(false);
            first.close();

            Connection second = factory.openConnection(session);
            Connection secondDelegate = ((DorisJdbcConnectionFactory.CachedConnection) second).delegate();
            second.close();

            assertThat(secondDelegate).isNotSameAs(firstDelegate);
            assertThat(driver.openedConnections()).isEqualTo(2);

            factory.cleanupQuery(session);
            assertThat(driver.closedConnections()).isEqualTo(2);
        }
        finally {
            factory.close();
        }
    }

    @Test
    void testCleanupPreventsLateConnectionReuse()
            throws Exception
    {
        TestingDriver driver = new TestingDriver();
        DorisJdbcConnectionFactory factory = new DorisJdbcConnectionFactory(driver, "jdbc:mysql://doris-fe:9030", new Properties());
        ConnectorSession session = session("query_cleanup");

        try {
            factory.beginQuery(session);

            Connection connection = factory.openConnection(session);
            factory.cleanupQuery(session);
            connection.close();

            assertThat(driver.closedConnections()).isEqualTo(1);

            factory.beginQuery(session("query_next"));
            factory.openConnection(session("query_next")).close();

            assertThat(driver.openedConnections()).isEqualTo(2);
        }
        finally {
            factory.close();
        }
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
        private final AtomicInteger openedConnections = new AtomicInteger();
        private final AtomicInteger closedConnections = new AtomicInteger();

        @Override
        public Connection connect(String url, Properties info)
        {
            openedConnections.incrementAndGet();
            return new TestingConnection(closedConnections::incrementAndGet);
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

        public int openedConnections()
        {
            return openedConnections.get();
        }

        public int closedConnections()
        {
            return closedConnections.get();
        }
    }

    private static final class TestingConnection
            extends ForwardingConnection
    {
        private final Runnable onClose;
        private boolean closed;

        private TestingConnection(Runnable onClose)
        {
            this.onClose = onClose;
        }

        @Override
        protected Connection delegate()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAutoCommit(boolean autoCommit) {}

        @Override
        public void setReadOnly(boolean readOnly) {}

        @Override
        public Statement createStatement()
        {
            return (Statement) Proxy.newProxyInstance(
                    Statement.class.getClassLoader(),
                    new Class<?>[] {Statement.class},
                    (_, method, _) -> method.getName().equals("executeUpdate") ? 1 : null);
        }

        @Override
        public DatabaseMetaData getMetaData()
        {
            return (DatabaseMetaData) Proxy.newProxyInstance(
                    DatabaseMetaData.class.getClassLoader(),
                    new Class<?>[] {DatabaseMetaData.class},
                    (_, method, _) -> method.getName().equals("getUserName") ? "source_user" : null);
        }

        @Override
        public boolean isClosed()
        {
            return closed;
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            onClose.run();
        }
    }
}
