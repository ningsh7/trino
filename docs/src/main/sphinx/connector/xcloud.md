# XCloud connector

The XCloud connector provides access to CirroData tables and ordinary views over
JDBC. Each catalog connects to one database. Within that catalog, use the XCloud
schema and table name to address a relation.

## Configuration

Install the plugin on the coordinator and all workers. Building the plugin
requires the vendor artifacts `com.bonc.xcloud:XCloudJDBC:2.14.7` and
`com.bonc.xcloud:XCloudJDBC_SP_Procedure_Parser:0.1.4` in a Maven repository
accessible to the build.

Set `-Duser.timezone=UTC` in `etc/jvm.config` on every Trino node. The native
driver uses the JVM default time zone to encode and decode local timestamps.
The connector checks this setting when the catalog starts. Use UTF-8 for the
remote database encoding.

Create `etc/catalog/xcloud.properties` with the connection settings for the
database:

```properties
connector.name=xcloud
connection-url=jdbc:xcloud:@example.net:1803/database
connection-user=example_user
connection-password=example_password
```

The driver handles multi-address URLs and routing. Workers must be able to
connect to the endpoints selected by the driver. Keep bulk column listing
disabled. Schema and table names must be representable as unquoted XCloud
identifiers.

## SQL support

The connector supports reading tables and ordinary views, creating and dropping
basic tables, inserting rows with `VALUES` or `SELECT`, and creating tables with
`CREATE TABLE AS`. It also supports truncating tables and renaming a table within
its schema. Queries support projection, integer and decimal predicates, and
limit pushdown. Trino processes joins, grouping, and sorting.

`UPDATE` supports constant assignments to one or more columns, including
`NULL`. Both `UPDATE` and `DELETE` require a filter that the connector can
execute entirely on XCloud, or an unconditional statement. Text and temporal
predicates require Trino evaluation. Expression assignments, subquery
assignments, and `MERGE` are unsupported.

## Type mapping

| XCloud type | Trino type on read | Write behavior |
| --- | --- | --- |
| INT | integer | tinyint, smallint and integer create INT |
| LONG | bigint | bigint creates LONG |
| DOUBLE | double | real and double create DOUBLE |
| NUMBER(p,s), DECIMAL(p,s), NUMERIC(p,s) | decimal when exactly representable | decimal creates NUMBER(p,s) and binds exact decimal text |
| CHAR | char | capacity is measured in UTF-8 bytes |
| VARCHAR, VARCHAR2, STRING, CLOB | varchar | creating a varchar column uses CLOB |
| BLOB | varbinary | reading uses a binary stream. Creating varbinary columns and writing non-null binary values are unsupported |
| DATE | timestamp(0) | XCloud DATE includes time of day. A Trino date writes midnight |
| TIMESTAMP(p) | timestamp(p) | supports fractional seconds through precision 9 |

For negative decimal scales, the connector maps to decimal(p-s,0). When the
scale is greater than the precision, the connector expands the precision to the
scale. Both mappings require a resulting precision of at most 38.
Unconstrained NUMBER has a value-dependent scale. Types without a representable
precision follow the JDBC `unsupported-type-handling` setting. Columns converted
to varchar by this setting are read-only.

## Write behavior

By default, workers write inserts to a staging table. The coordinator copies the
rows to the target and drops the staging table. For CTAS it renames the staging
table to the target. Grant the remote account the privileges needed to create,
populate, rename, and drop these tables.

The driver has no remote transaction control through JDBC commit or rollback.
Multi-statement rollback and task retries for writes are unsupported. Enabling
`insert.non-transactional-insert.enabled` writes directly to the target and can
leave partially visible data after a failure or cancellation.

If the connection fails during the final copy, check the target before
resubmitting the write. If cleanup fails after the copy succeeds, the query can
report an error even though the target already contains the rows. Use the query
ID and the [remote operation audit](../admin/remote-operation-audit.md) to
identify the staging table before removing it.
