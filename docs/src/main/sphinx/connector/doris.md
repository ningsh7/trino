# Doris connector

The Doris connector allows querying tables in an external
[Apache Doris](https://doris.apache.org/) cluster. The connector is purpose-built
for Doris and plans reads through Doris FE before fetching data with Arrow
Flight SQL.

This initial version is read-only. It focuses on Doris-native query planning,
metadata lookup, and conservative pushdown for analytical reads.

## Requirements

To connect to Doris, you need:

- A Doris cluster with FE query planning and Arrow Flight SQL enabled.
- Network access from the Trino coordinator and workers to Doris FE HTTP,
  JDBC, and Flight SQL endpoints.
- Doris credentials with permission to read metadata and query data.

## Configuration

To configure the Doris connector, create a catalog properties file in
`etc/catalog`, for example `example.properties`, with the following contents:

```text
connector.name=doris
doris.fenodes=fe1.example.net:8030,fe2.example.net:8030
doris.jdbc-url=jdbc:mysql://fe1.example.net:9030
doris.username=trino
doris.password=secret
```

The configuration properties are:

:::{list-table} Doris configuration properties
:widths: 30, 20, 50
:header-rows: 1

* - Property name
  - Required
  - Description
* - `doris.fenodes`
  - Yes
  - Comma-separated list of Doris FE `host:port` endpoints used for FE query
    planning requests.
* - `doris.jdbc-url`
  - Yes
  - Doris FE JDBC URL used for metadata access and Flight SQL port discovery.
* - `doris.username`
  - No
  - Doris username used for FE HTTP basic auth, JDBC metadata access, and
    Flight SQL sessions.
* - `doris.password`
  - No
  - Doris password used with `doris.username`.
* - `doris.flight-sql-port`
  - No
  - Explicit Doris Flight SQL port. The default is `0`, which enables
    automatic discovery from `SHOW FRONTENDS`.
* - `doris.largeint-mapping`
  - No
  - Mapping for Doris `LARGEINT`. Valid values are `VARCHAR` and `DECIMAL`.
    The default is `VARCHAR`.
:::

If Flight SQL auto-discovery is not available in your Doris deployment, set
`doris.flight-sql-port` explicitly.

## Type mapping

The connector currently supports Doris-to-Trino type mapping only, because the
connector is read-only.

### Doris to Trino type mapping

:::{list-table} Doris to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

* - Doris type
  - Trino type
  - Notes
* - `BOOLEAN`, `BOOL`
  - `BOOLEAN`
  -
* - `TINYINT`
  - `BOOLEAN` or `TINYINT`
  - Doris boolean aliases exposed as `TINYINT(0)` map to `BOOLEAN`.
* - `SMALLINT`
  - `SMALLINT`
  -
* - `INT`, `INTEGER`
  - `INTEGER`
  -
* - `BIGINT`
  - `BIGINT`
  -
* - `BIGINT UNSIGNED`
  - `DECIMAL(20, 0)`
  -
* - `LARGEINT`
  - `VARCHAR` or `DECIMAL(38, 0)`
  - Controlled by `doris.largeint-mapping`. `VARCHAR` is the default to avoid
    truncating 128-bit values.
* - `DECIMAL`, `DECIMALV2`, `DECIMALV3`, `DECIMAL32`, `DECIMAL64`,
    `DECIMAL128`, `DECIMAL128I`
  - `DECIMAL(p, s)` or `VARCHAR`
  - Maps to `VARCHAR` when Doris precision cannot fit in Trino `DECIMAL`.
* - `DECIMAL256`
  - `VARCHAR`
  -
* - `FLOAT`
  - `REAL`
  -
* - `DOUBLE`
  - `DOUBLE`
  -
* - `CHAR(n)`
  - `CHAR(n)`
  -
* - `VARCHAR(n)`
  - `VARCHAR(n)`
  -
* - `DATE`, `DATEV2`
  - `DATE`
  -
* - `DATETIME`
  - `TIMESTAMP(0)`
  -
* - `DATETIMEV2`
  - `TIMESTAMP(p)`
  - Precision is capped at `6`. The connector prefers Doris type definitions over
    generic JDBC metadata so `DATETIMEV2(3)` remains `TIMESTAMP(3)`.
* - `STRING`, `JSON`, `JSONB`, `ARRAY`, `MAP`, `STRUCT`, `VARIANT`, `IPV4`,
    `IPV6`, `BITMAP`, `HLL`, `QUANTILE_STATE`, `AGG_STATE`
  - `VARCHAR`
  - These types currently fall back to textual reads.
:::

No other Doris types are supported.

## Querying Doris

The Doris connector provides a schema for every Doris database. You can inspect
available objects with standard metadata commands:

```sql
SHOW SCHEMAS FROM example;
SHOW TABLES FROM example.tpch;
DESCRIBE example.tpch.nation;
SHOW COLUMNS FROM example.tpch.nation;
```

You can query Doris tables with Trino `SELECT` statements:

```sql
SELECT nationkey, name
FROM example.tpch.nation
WHERE nationkey < 5
ORDER BY nationkey;
```

If you used a different catalog properties filename, use that catalog name
instead of `example`.

The connector exposes Doris schemas and tables through Trino's lowercase
identifier model. Mixed-case Doris names are resolved back to the remote Doris
name during reads, so `SHOW SCHEMAS` and `SHOW TABLES` remain queryable from
Trino. Objects that differ only by case are still not addressable separately
because Trino normalizes identifiers to lowercase.

## SQL support

The connector provides read access to Doris metadata and table data. In
addition to the [globally available](sql-globally-available) and
[read operation](sql-read-operations) statements, the connector supports
metadata inspection such as `SHOW SCHEMAS`, `SHOW TABLES`, `SHOW COLUMNS`, and
`DESCRIBE`.

Only readable Doris OLAP base tables are exposed. Internal system objects and
non-OLAP tables are filtered from metadata listings.

Write operations such as `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE`, and
`MERGE` are not supported.

## Performance

The connector includes several Doris-specific performance features.

### Table statistics

The connector exposes table row-count estimates from Doris
`INFORMATION_SCHEMA.TABLES` when they are available. These estimates can help
Trino's cost-based optimizer choose better plans.

### Pushdown

The connector supports pushdown for a number of operations:

- projection
- predicate filters on supported types
- {ref}`limit-pushdown`
- {ref}`topn-pushdown`

{ref}`Aggregate pushdown <aggregation-pushdown>` is supported for the following
functions:

- {func}`avg`
- {func}`count`
- {func}`max`
- {func}`min`
- {func}`sum`

`COUNT(DISTINCT ...)` pushdown is supported for character, integer, decimal,
date, timestamp, and boolean columns.

Pushdown stays conservative for correctness. For example:

- Real-valued predicate domains are evaluated in Trino.
- Character `min` and `max` aggregations are evaluated in Trino.
- Unsupported types fall back to Trino execution or textual reads.



## Doris and MySQL connectors

Apache Doris also exposes a MySQL-compatible endpoint, so some deployments can
be queried with the {doc}`mysql` connector. The Doris connector differs in two
important ways:

- It uses Doris FE query planning plus Arrow Flight SQL for reads instead of
  the generic MySQL JDBC path.
- It is currently read-only and tuned for Doris-specific pushdown behavior.

Use the Doris connector when you want Doris-native read planning and Flight SQL
execution. Use the MySQL connector only if you specifically need generic
MySQL-compatible behavior rather than Doris-native integration.
