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
package io.trino.filesystem.hdfs.audit;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class HdfsOperationAuditStats
{
    private final CounterStat operationSuccesses = new CounterStat();
    private final CounterStat operationFailures = new CounterStat();
    private final CounterStat sinkSuccesses = new CounterStat();
    private final CounterStat sinkFailures = new CounterStat();
    private final CounterStat droppedEvents = new CounterStat();
    private final CounterStat serializationFailures = new CounterStat();
    private final CounterStat filteredEvents = new CounterStat();
    private final Map<HdfsOperation, CounterStat> operations = new EnumMap<>(HdfsOperation.class);
    private final AtomicLong lastOutputErrorTime = new AtomicLong();

    public HdfsOperationAuditStats()
    {
        for (HdfsOperation operation : HdfsOperation.values()) {
            operations.put(operation, new CounterStat());
        }
    }

    @Managed
    @Nested
    public CounterStat getOperationSuccesses()
    {
        return operationSuccesses;
    }

    @Managed
    @Nested
    public CounterStat getOperationFailures()
    {
        return operationFailures;
    }

    @Managed
    @Nested
    public CounterStat getSinkSuccesses()
    {
        return sinkSuccesses;
    }

    @Managed
    @Nested
    public CounterStat getSinkFailures()
    {
        return sinkFailures;
    }

    @Managed
    @Nested
    public CounterStat getDroppedEvents()
    {
        return droppedEvents;
    }

    @Managed
    @Nested
    public CounterStat getSerializationFailures()
    {
        return serializationFailures;
    }

    @Managed
    @Nested
    public CounterStat getFilteredEvents()
    {
        return filteredEvents;
    }

    @Managed
    public long getLastOutputErrorTime()
    {
        return lastOutputErrorTime.get();
    }

    @Managed
    public long getOpenReadOperations()
    {
        return getOperationCount(HdfsOperation.OPEN_READ);
    }

    @Managed
    public long getCreateFileOperations()
    {
        return getOperationCount(HdfsOperation.CREATE_FILE);
    }

    @Managed
    public long getDeleteFileOperations()
    {
        return getOperationCount(HdfsOperation.DELETE_FILE);
    }

    @Managed
    public long getDeleteFilesOperations()
    {
        return getOperationCount(HdfsOperation.DELETE_FILES);
    }

    @Managed
    public long getDeleteDirectoryOperations()
    {
        return getOperationCount(HdfsOperation.DELETE_DIRECTORY);
    }

    @Managed
    public long getRenameFileOperations()
    {
        return getOperationCount(HdfsOperation.RENAME_FILE);
    }

    @Managed
    public long getRenameDirectoryOperations()
    {
        return getOperationCount(HdfsOperation.RENAME_DIRECTORY);
    }

    @Managed
    public long getCreateDirectoryOperations()
    {
        return getOperationCount(HdfsOperation.CREATE_DIRECTORY);
    }

    @Managed
    public long getListFilesOperations()
    {
        return getOperationCount(HdfsOperation.LIST_FILES);
    }

    @Managed
    public long getListDirectoriesOperations()
    {
        return getOperationCount(HdfsOperation.LIST_DIRECTORIES);
    }

    public long getOperationCount(HdfsOperation operation)
    {
        return operations.get(requireNonNull(operation, "operation is null")).getTotalCount();
    }

    void recordOperationSuccess(HdfsOperation operation)
    {
        operations.get(requireNonNull(operation, "operation is null")).update(1);
        operationSuccesses.update(1);
    }

    void recordOperationFailure(HdfsOperation operation)
    {
        operations.get(requireNonNull(operation, "operation is null")).update(1);
        operationFailures.update(1);
    }

    void recordSinkSuccess()
    {
        sinkSuccesses.update(1);
    }

    void recordSinkFailure(Instant failureTime)
    {
        sinkFailures.update(1);
        recordOutputError(failureTime);
    }

    void recordSerializationFailure(Instant failureTime)
    {
        serializationFailures.update(1);
        recordOutputError(failureTime);
    }

    void recordDroppedEvent(Instant failureTime)
    {
        droppedEvents.update(1);
        recordOutputError(failureTime);
    }

    void recordFilteredEvent()
    {
        filteredEvents.update(1);
    }

    private void recordOutputError(Instant failureTime)
    {
        lastOutputErrorTime.set(requireNonNull(failureTime, "failureTime is null").toEpochMilli());
    }
}
