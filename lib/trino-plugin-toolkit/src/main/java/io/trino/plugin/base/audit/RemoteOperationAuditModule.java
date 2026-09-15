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
package io.trino.plugin.base.audit;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.json.JsonCodec;
import io.trino.spi.Node;
import io.trino.spi.catalog.CatalogName;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class RemoteOperationAuditModule
        implements Module
{
    private final String connector;

    public RemoteOperationAuditModule(String connector)
    {
        this.connector = requireNonNull(connector, "connector is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(RemoteOperationAuditConfig.class);
        newOptionalBinder(binder, RemoteOperationAuditSink.class).setDefault().to(JsonLogRemoteOperationAuditSink.class).in(Scopes.SINGLETON);
        binder.bind(RemoteOperationAuditStats.class).in(Scopes.SINGLETON);
        var catalogName = binder.getProvider(CatalogName.class);
        newExporter(binder).export(RemoteOperationAuditStats.class)
                .as(generator -> generator.generatedNameOf(RemoteOperationAuditStats.class, catalogName.get().toString()));
    }

    @Provides
    @Singleton
    public JsonCodec<RemoteOperationAuditEvent> eventCodec()
    {
        // Keep the audit schema independent of connector-specific Jackson configuration.
        return jsonCodec(RemoteOperationAuditEvent.class);
    }

    @Provides
    @Singleton
    public RemoteOperationAuditor auditor(RemoteOperationAuditConfig config, RemoteOperationAuditSink sink, RemoteOperationAuditStats stats, CatalogName catalog, Node node)
    {
        return new RemoteOperationAuditor(config, sink, stats, catalog.toString(), connector, node.getNodeIdentifier());
    }
}
