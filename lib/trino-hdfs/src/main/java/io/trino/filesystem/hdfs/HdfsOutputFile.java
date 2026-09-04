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

import io.airlift.stats.TimeStat;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.hdfs.audit.HdfsOperationAuditor;
import io.trino.filesystem.hdfs.audit.HdfsOperationAuditor.OperationAudit;
import io.trino.hdfs.CallStats;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.HdfsAuthentication.ExceptionAction;
import io.trino.memory.context.AggregatedMemoryContext;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;

import static io.trino.filesystem.hdfs.HadoopPaths.hadoopPath;
import static io.trino.filesystem.hdfs.HdfsFileSystem.withCause;
import static io.trino.filesystem.hdfs.audit.HdfsOperation.CREATE_FILE;
import static java.util.Objects.requireNonNull;

class HdfsOutputFile
        implements TrinoOutputFile
{
    private final Location location;
    private final HdfsEnvironment environment;
    private final HdfsContext context;
    private final CallStats createFileCallStat;
    private final HdfsOperationAuditor auditor;

    public HdfsOutputFile(
            Location location,
            HdfsEnvironment environment,
            HdfsContext context,
            CallStats createFileCallStat,
            HdfsOperationAuditor auditor)
    {
        this.location = requireNonNull(location, "location is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.context = requireNonNull(context, "context is null");
        this.createFileCallStat = requireNonNull(createFileCallStat, "createFileCallStat is null");
        this.auditor = requireNonNull(auditor, "auditor is null");
        location.verifyValidFileLocation();
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        requireNonNull(memoryContext, "memoryContext is null");
        // Hadoop output streams do not expose allocation details for memory tracking.
        return create(false);
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        try (OutputStream out = create(true)) {
            out.write(data);
        }
    }

    private OutputStream create(boolean overwrite)
            throws IOException
    {
        OperationAudit operationAudit = auditor.begin(context, CREATE_FILE, location);
        try {
            createFileCallStat.newCall();
            Path file = hadoopPath(location);
            FileSystem fileSystem = environment.getFileSystem(context, file);
            try (TimeStat.BlockTimer _ = createFileCallStat.time()) {
                return create(() -> fileSystem.create(file, overwrite), operationAudit);
            }
        }
        catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
            createFileCallStat.recordException(e);
            FileAlreadyExistsException failure = withCause(new FileAlreadyExistsException(toString()), e);
            operationAudit.failed(failure);
            throw failure;
        }
        catch (IOException e) {
            createFileCallStat.recordException(e);
            IOException failure = new IOException("Creation of file %s failed: %s".formatted(location, e.getMessage()), e);
            operationAudit.failed(failure);
            throw failure;
        }
        catch (RuntimeException e) {
            operationAudit.failed(e);
            throw e;
        }
    }

    private OutputStream create(ExceptionAction<FSDataOutputStream> action, OperationAudit operationAudit)
            throws IOException
    {
        FSDataOutputStream out = environment.doAs(context.getIdentity(), action);
        return new HdfsOutputStream(location, out, environment, context, operationAudit);
    }

    @Override
    public Location location()
    {
        return location;
    }

    @Override
    public String toString()
    {
        return location().toString();
    }
}
