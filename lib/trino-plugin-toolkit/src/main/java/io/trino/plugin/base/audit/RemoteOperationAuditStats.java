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

import org.weakref.jmx.Managed;

import java.util.concurrent.atomic.AtomicLong;

public class RemoteOperationAuditStats
{
    final AtomicLong operationSuccesses = new AtomicLong();
    final AtomicLong operationFailures = new AtomicLong();
    final AtomicLong sinkSuccesses = new AtomicLong();
    final AtomicLong sinkFailures = new AtomicLong();
    final AtomicLong serializationFailures = new AtomicLong();
    final AtomicLong droppedEvents = new AtomicLong();
    final AtomicLong filteredEvents = new AtomicLong();
    final AtomicLong contextFailures = new AtomicLong();
    final AtomicLong sanitizedStatements = new AtomicLong();
    final AtomicLong lastFailureTime = new AtomicLong();

    @Managed
    public long getOperationSuccesses()
    {
        return operationSuccesses.get();
    }

    @Managed
    public long getOperationFailures()
    {
        return operationFailures.get();
    }

    @Managed
    public long getSinkSuccesses()
    {
        return sinkSuccesses.get();
    }

    @Managed
    public long getSinkFailures()
    {
        return sinkFailures.get();
    }

    @Managed
    public long getSerializationFailures()
    {
        return serializationFailures.get();
    }

    @Managed
    public long getDroppedEvents()
    {
        return droppedEvents.get();
    }

    @Managed
    public long getFilteredEvents()
    {
        return filteredEvents.get();
    }

    @Managed
    public long getContextFailures()
    {
        return contextFailures.get();
    }

    @Managed
    public long getSanitizedStatements()
    {
        return sanitizedStatements.get();
    }

    @Managed
    public long getLastFailureTime()
    {
        return lastFailureTime.get();
    }
}
