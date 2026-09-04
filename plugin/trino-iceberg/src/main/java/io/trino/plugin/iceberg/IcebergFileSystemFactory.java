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
package io.trino.plugin.iceberg;

import io.trino.filesystem.FileSystemContext;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;

public interface IcebergFileSystemFactory
{
    /**
     * Creates a file system using raw FileIO properties, as delivered directly by the Iceberg catalog.
     */
    TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> fileIoProperties);

    default TrinoFileSystem create(FileSystemContext context, Map<String, String> fileIoProperties)
    {
        return create(context.identity(), fileIoProperties);
    }

    default TrinoFileSystem create(ConnectorSession session, Map<String, String> fileIoProperties)
    {
        return create(FileSystemContext.of(session), fileIoProperties);
    }

    /**
     * Creates a file system from typed table credentials, which carry both the global FileIO properties
     * and any prefixed storage credentials vended by the catalog.
     */
    default TrinoFileSystem create(ConnectorIdentity identity, IcebergTableCredentials tableCredentials)
    {
        return create(identity, tableCredentials.fileIoProperties());
    }

    default TrinoFileSystem create(FileSystemContext context, IcebergTableCredentials tableCredentials)
    {
        return create(context, tableCredentials.fileIoProperties());
    }

    default TrinoFileSystem create(ConnectorSession session, IcebergTableCredentials tableCredentials)
    {
        return create(FileSystemContext.of(session), tableCredentials);
    }
}
