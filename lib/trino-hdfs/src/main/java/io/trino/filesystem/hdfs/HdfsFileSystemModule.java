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
package io.trino.filesystem.hdfs;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.json.JsonModule;
import io.trino.filesystem.hdfs.audit.DefaultHdfsOperationAuditor;
import io.trino.filesystem.hdfs.audit.HdfsAuditPathSanitizer;
import io.trino.filesystem.hdfs.audit.HdfsOperationAuditConfig;
import io.trino.filesystem.hdfs.audit.HdfsOperationAuditEvent;
import io.trino.filesystem.hdfs.audit.HdfsOperationAuditSink;
import io.trino.filesystem.hdfs.audit.HdfsOperationAuditStats;
import io.trino.filesystem.hdfs.audit.HdfsOperationAuditor;
import io.trino.filesystem.hdfs.audit.JsonLogHdfsOperationAuditSink;
import io.trino.hdfs.TrinoHdfsFileSystemStats;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HdfsFileSystemModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.install(new JsonModule());
        configBinder(binder).bindConfig(HdfsOperationAuditConfig.class);
        jsonCodecBinder(binder).bindJsonCodec(HdfsOperationAuditEvent.class);

        binder.bind(HdfsAuditPathSanitizer.class).in(SINGLETON);
        binder.bind(HdfsOperationAuditSink.class).to(JsonLogHdfsOperationAuditSink.class).in(SINGLETON);
        binder.bind(HdfsOperationAuditor.class).to(DefaultHdfsOperationAuditor.class).in(SINGLETON);
        binder.bind(HdfsOperationAuditStats.class).in(SINGLETON);
        newExporter(binder).export(HdfsOperationAuditStats.class).withGeneratedName();

        binder.bind(HdfsFileSystemFactory.class).in(SINGLETON);
        binder.bind(TrinoHdfsFileSystemStats.class).in(SINGLETON);
        newExporter(binder).export(TrinoHdfsFileSystemStats.class).withGeneratedName();
    }
}
