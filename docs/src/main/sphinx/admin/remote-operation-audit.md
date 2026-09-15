# Remote operation auditing

Remote operation auditing records database operations on the Trino coordinator
or worker that makes the remote call. It is available to connectors using the
shared JDBC implementation, including MySQL and PostgreSQL, and to the native
Doris connector. It is separate from authorization and does not change the
database account used by a catalog.

Each event contains the Trino user, query ID, trace token, source, catalog,
connector, node, remote execution account when available, operation, result,
and duration. Use the query ID to correlate these events with a persisted
[query event](admin-event-listeners), which contains the original Trino SQL.
Remote operation events contain the SQL sent by the connector. A query can
produce many remote operations and retries, each with its own operation ID.

## Configuration

Configure auditing independently in each catalog properties file:

```properties
remote-operation-audit.enabled=true
remote-operation-audit.read-enabled=true
remote-operation-audit.metadata-enabled=false
remote-operation-audit.statement-mode=TEMPLATE
remote-operation-audit.statement-max-length=16384
remote-operation-audit.error-message-max-length=2048
```

| Property | Default | Description |
| --- | --- | --- |
| `remote-operation-audit.enabled` | `false` | Enable remote operation events. |
| `remote-operation-audit.read-enabled` | `true` | Record JDBC SELECT queries and Doris Flight SQL query execution. Callable, non-SELECT, and unclassified statements remain audited. |
| `remote-operation-audit.metadata-enabled` | `false` | Record JDBC metadata method calls, such as `getTables`. |
| `remote-operation-audit.statement-mode` | `TEMPLATE` | Record sanitized SQL text, its SHA-256 `HASH`, or `NONE`. |
| `remote-operation-audit.statement-max-length` | `16384` | Maximum recorded SQL text length. Hashes use the complete sanitized text before truncation. |
| `remote-operation-audit.error-message-max-length` | `2048` | Maximum length of the safe error summary. Raw driver error messages are not recorded. |

The same properties apply to Doris JDBC metadata access, FE HTTP planning,
and Flight SQL execution. They do not enable
[HDFS operation auditing](/object-storage/file-system-hdfs), which has separate
configuration.

## JDBC operation boundaries

All standard statement creation overloads are wrapped. Events cover
`executeQuery`, `executeUpdate`, `executeLargeUpdate`, `execute`,
`executeBatch`, `executeLargeBatch`, `commit`, and `rollback`. This includes
prepared and callable statements, connector procedures, and native SQL
executed through the shared connection factory.

Prepared and callable statements record the SQL template without retaining
bound parameter values. A batch produces one event with its batch size.
Prepared batches retain their template; plain statement batches contain a
count without retaining an unbounded collection of individual SQL strings.

The connection ID associates statements and explicit transaction operations
on the same audited connection. A statement success does not imply a later
transaction commit. Implicit commits, including those caused by DDL or
changing auto-commit mode, are not separate transaction events.

A query execution succeeds when the driver returns its result set. Result
sets are not wrapped: later fetch failures and completion of result consumption
are outside this audit boundary. Vendor-specific operations obtained through
`unwrap()`, driver heartbeats, and connection initialization are also outside
this boundary. Existing driver unwrapping and streaming configuration remain
available.

Metadata auditing records the JDBC method name, not SQL hidden inside the
driver. The execution account is obtained from JDBC metadata and cached for
the connection. Unsupported account, catalog, or schema metadata is recorded
as null. Calls without a query session use `origin=BACKGROUND` with a null
query ID and user.

## Doris operation boundaries

Native Doris emits the same event format for three protocols:

* `JDBC` covers metadata SQL and Flight SQL port discovery.
* `DORIS_FE_HTTP` records each `PLAN_SPLITS` request, its endpoint, SQL,
  HTTP status, and planning result. Each FE retry produces a separate event.
* `DORIS_FLIGHT_SQL` records `OPEN_READ` around the actual ADBC
  `executeQuery()` call. Connecting, preparing the statement, and fetching
  subsequent Arrow batches are outside this boundary.

The HTTP and Flight SQL execution account comes from `doris.username`.
Query context is supplied for each operation, including when a Flight SQL
connection is reused by another query.

## Output and sensitive data

Events are written as single-line JSON to the logger
`io.trino.plugin.base.audit.RemoteOperationAudit`. Set its level to `INFO`
in `etc/log.properties` on every coordinator and worker:

```properties
io.trino.plugin.base.audit.RemoteOperationAudit=INFO
io.trino.plugin.base.audit.fallback=WARN
```

Collect logs from all Trino nodes and persist query events to correlate
original SQL and remote operations. The default sink confirms serialization
and a logger call, not delivery to an external storage system. SQL comments
and database-side audit plugins are not required. Existing query comment
configuration continues to work independently.

Control characters are removed from recorded text. Endpoint URI user-info,
query strings, and fragments are omitted. SQL text containing recognized
credential keywords is replaced with a redaction marker; URI credentials
and parameters in other SQL text are removed before recording or hashing.
This is conservative sanitization, not a parser for every database dialect.
Direct SQL and prepared templates can still contain sensitive literals.
Choose `HASH` or `NONE` when SQL text must not be retained.

Driver errors can echo bound values or credentials. Events therefore record
the exception type and a bounded type summary instead of the raw exception
message. The original exception remains available to the query caller.

## Failure handling

Audit context, sanitization, serialization, and sink failures do not change
the remote operation's result or replace its exception. Failed output is
not retried by this synchronous sink.

The catalog's `RemoteOperationAuditStats` JMX bean reports remote successes
and failures, sink successes and failures, serialization failures, dropped
and filtered events, context failures, sanitized statements, and the last
failure time in epoch milliseconds. A disabled logger is an output failure.
Rate-limited fallback messages contain only the audit failure's type.

Monitor these metrics and the external collector's delivery status. If a
deployment requires rejecting new queries when auditing is unavailable, that
requires a separate admission or authorization check. This sink does not
reject a database write after it has already completed.
