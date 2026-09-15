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
package io.trino.plugin.jdbc.audit;

import com.google.inject.Inject;
import io.trino.plugin.base.audit.RemoteOperationAuditor;
import io.trino.plugin.base.audit.jdbc.AuditingJdbc;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.RetryingConnectionFactory;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public class AuditingConnectionFactory
        implements ConnectionFactory
{
    private final ConnectionFactory delegate;
    private final RemoteOperationAuditor auditor;

    @Inject
    public AuditingConnectionFactory(RetryingConnectionFactory delegate, RemoteOperationAuditor auditor)
    {
        this((ConnectionFactory) delegate, auditor);
    }

    public AuditingConnectionFactory(ConnectionFactory delegate, RemoteOperationAuditor auditor)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.auditor = requireNonNull(auditor, "auditor is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Connection connection = delegate.openConnection(session);
        return AuditingJdbc.wrap(connection, auditor.context(session), auditor);
    }

    @Override
    public void close()
            throws SQLException
    {
        delegate.close();
    }
}
