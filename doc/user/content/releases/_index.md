---
title: "Releases"
description: "Materialize release notes"
disable_list: true
menu:
  main:
    identifier: "releases"
    weight: 80
aliases:
  - /self-managed/v25.1/release-notes/
---

{{< note >}}
Starting with the v26.1.0 release, Materialize releases on a weekly schedule for
both Cloud and Self-Managed. See [Release schedule](/releases/schedule) for details.
{{</ note >}}

## v26.24.1
*Released to Materialize Cloud: 2026-05-14* <br>
*Released to Materialize Self-Managed: 2026-05-15* <br>

This release introduces the MCP server for Agents, and bug fixes.

### MCP Server for Agents

{{< public-preview />}}

Materialize environments now include a built-in Model Context Protocol (MCP)
[server for agents (`/api/mcp/agent`)](/integrations/mcp-server/mcp-agent/),
giving your production AI agents fresh context from Materialize. Once connected to this
endpoint, an agent can discover your data products, understand the underlying ontology, and run queries to fetch fresh data.

Data products are simply [materialized views](/sql/create-materialized-view/)
or [indexes](/sql/create-index/). Agents authenticate as
[roles](/sql/create-role/) in Materialize, so
[RBAC privileges](/manage/access-control/) govern which data products are
visible. Finally, you can set up a dedicated
[cluster](/concepts/clusters/) for your agents, so they're isolated from the
rest of your environment.

The MCP server for agents complements the [MCP server for
developers](/integrations/mcp-server/mcp-developer/) released in v26.20.2. The
developer server gives coding agents observability into Materialize so you
can build faster on top of it; the agent server gives production agents
governed access to fresh data products.

For more information, refer to:
- [Integrations: MCP Server for Agents](/integrations/mcp-server/mcp-agent/)

### Improvements {#v26.24-improvements}

- **`dbt-materialize` connection overrides**: The dbt adapter now supports
  passing custom connection options via the `options` field in `profiles.yml`,
  enabling OIDC authentication and other advanced connection configurations.
- **`COPY FROM` rejects HTTP redirects**: `COPY FROM` now returns a clear error
  if the target URL responds with an HTTP redirect, preventing unexpected data
  sources and potential security issues.
- **[Agent skills](/integrations/coding-agent-skills/):**
  -  **Improved `mcp-developer-analysis` client setup**: The skill now includes a comprehensive playbook for connecting MCP-capable clients (Claude Code, Cursor, VS Code, Zed, Continue, Windsurf, Claude Desktop) to the [MCP server for developers](/integrations/mcp-server/mcp-developer/).

### Bug Fixes {#v26.24-bug-fixes}

- Fixed MySQL sources with RDS IAM authentication failing when the database
  username contains special characters like `&` or `#`.
- Fixed joins incorrectly failing with a type mismatch error when join columns
  differed only in nullability.
- Fixed fast-path `SELECT` queries returning incorrect results when using
  `OFFSET`.
- Fixed `string_to_array` returning incorrect results when `null_string` is
  specified and the delimiter is empty.
- Fixed `INSERT INTO ... SELECT` ignoring the `OFFSET` clause.
- Fixed `seahash` function catalog metadata reporting the wrong return type
  (`uint4` instead of `uint8`).
- Fixed `mz_egress_ips` storing non-canonical CIDR notation (e.g.,
  `10.0.5.7/24` instead of `10.0.5.0/24`).
- Fixed Console crashing on OIDC-protected routes when the identity provider
  initialization fails, instead of falling through to password-based sign-in.
- Fixed catalog migration bug from v26.18.0 by which a
  `Non-positive multiplicity in DistinctBy` error could occur on queries
  containing `SELECT DISTINCT` over role-derived catalog views (e.g.,
  anything reading from `mz_roles`, `mz_role_members`, or views that
  internally project role columns). The error is resolved automatically by
  upgrading to v26.24.0 or newer. Simple RBAC operations and queries on
  `mz_roles` and `mz_role_members` were not affected.


## v26.23.2
*Released to Materialize Cloud: 2026-05-11* <br>

This patch release includes bug fixes.

### Bug Fixes {#v26.23.2-bug-fixes}

- Fixed a regression in v26.23.0 that caused storage replicas to spend a large
  share of their CPU time walking small data fragments during Parquet decode,
  slowing queries that read from object storage.
- Fixed a regression in v26.23.0 that caused storage replicas to retain extra
  memory when reading from object storage.

## v26.23.0
*Released to Materialize Cloud: 2026-05-07* <br>

This release introduces enhanced Kafka PrivateLink routing options, security
improvements, and bug fixes.

### Features {#v26.23-features}

- **Dynamic Kafka brokers with AWS PrivateLink**: Kafka connections can now
  route dynamically discovered brokers through a PrivateLink tunnel, rather than
  requiring every advertised broker to be enumerated in the `BROKERS (...)`
  clause. Two new options are available:
  - `MATCHING 'pattern' USING AWS PRIVATELINK conn (...)` inside `BROKERS (...)`
    associates a PrivateLink connection with any broker whose advertised
    hostname matches `pattern`, including brokers that only appear in Kafka
    metadata after the connection is established.
  - `BOOTSTRAP BROKER 'addr' USING AWS PRIVATELINK conn (...)` pins the initial
    bootstrap address to an explicit PrivateLink tunnel.

  Together, these resolve availability-zone mismatches that previously affected
  MSK and other Kafka clusters that rely on broker discovery, by ensuring every
  broker, including those learned from metadata, is reached through a
  PrivateLink endpoint in the broker's own AZ. Refer to our documentation on
  [AWS PrivateLink connections](/ingest-data/network-security/privatelink/) and
  the [Kafka `CREATE CONNECTION` PrivateLink syntax](/sql/create-connection/#kafka-privatelink-syntax)
  for more information.

### Improvements {#v26.23-improvements}

- **New `repeat_row_non_negative` SQL function**: The new
  `repeat_row_non_negative` table function generates a specified number of rows
  but errors on negative input rather than silently producing incorrect results,
  making it safer to use in general-purpose queries than the existing
  `repeat_row`.
- **Queries fail gracefully on internal errors**: Certain internal errors that
  previously caused `environmentd` to crash now return a query error instead,
  improving cluster stability.
- **dbt deploy retries on concurrent DDL conflicts**: `dbt deploy` now
  automatically retries the `ALTER SWAP` atomic deployment when it encounters a
  DDL interrupt from concurrent catalog operations, preventing spurious
  deployment failures in busy environments.
- **Clearer temporal filter error messages**: Error messages for unsupported
  temporal predicates now include the actual filter expression, making it easier
  to identify and fix the offending query.
- **`COPY TO S3` Parquet type validation at planning time**: `COPY TO S3` with
  `FORMAT PARQUET` now rejects Parquet-incompatible column types (such as
  `interval`) at query planning time with a clear error, rather than failing at
  execution time with an opaque message.
- **`mcp-developer-analysis`**: A new
  [coding agent skill](/integrations/coding-agent-skills/) that pairs with the
  `/api/mcp/developer` endpoint to provide diagnostic workflows, system catalog
  references, and remediation runbooks for AI-powered troubleshooting.
- **System catalog ontology for the MCP developer server**: The system
  catalog now exposes an ontology that describes how `mz_*` tables relate to
  one another and which tables to consult for common diagnostic questions. The
  [MCP server for developers](/integrations/mcp-server/mcp-developer/) uses
  this ontology to plan catalog queries directly instead of probing the schema,
  reducing the number of round trips needed to answer questions about
  hydration, freshness, and resource usage.
- **~10% faster materialized view hydration**: We've reduced the work performed
  during initial materialized view hydration, observing approximately 10%
  faster hydration times across our benchmarks. This shortens the window
  between creating (or restarting) a materialized view and the point at which
  it begins serving up-to-date results.

### Bug Fixes {#v26.23-bug-fixes}

- Fixed `statement_timeout = 0` (which means "disabled" in PostgreSQL semantics)
  causing every `SELECT` and `EXPLAIN FILTER PUSHDOWN` to fail immediately with a
  spurious `StatementTimeout` error.
- Tightened default validation on headers in Self-Managed deployments.
- Enhanced session-based HTTP authentication.
- Fixed `SHOW CREATE TYPE` emitting the bare type name instead of the
  fully-qualified `database.schema.type` name, unlike every other `SHOW CREATE`
  variant.
- Fixed Self-Managed `orchestratord` `--enable-rbac False` silently inverting
  the value and enabling RBAC instead of disabling it.
- Fixed SQL Server source composite primary key columns being recorded in
  non-deterministic order, causing incorrect constraint definitions and
  non-deterministic behavior across `ALTER SOURCE` and re-purification.
- Fixed PostgreSQL source RLS policy validation producing false positives that
  blocked replication for users whose roles inherit BYPASSRLS through role
  membership.
- Fixed SQL Server source growing memory without bound during table snapshots due
  to a `RowArena` that was never cleared between rows.
- Fixed `SELECT` queries with both `LIMIT` and `OFFSET` processing all remaining
  rows instead of stopping after the limit was reached.
- Fixed SQL Server source opening one upstream connection per Timely worker
  instead of one total, multiplying SQL Server connections and
  `sp_cdc_cleanup_change_table` calls by the worker count.
- Fixed SQL Server source with PrivateLink connections only attempting the first
  resolved IP address instead of trying all available addresses.
- Fixed `regexp_replace` returning an invalid regular expression error instead of
  `NULL` when called with a `NULL` replacement column and a literal pattern that
  fails to compile.
- Fixed `pg_index.indnatts` counting columns of the indexed table instead of the
  index itself, and `pg_class.relnatts` always reporting `0` for index rows,
  improving compatibility with tools that introspect the PostgreSQL catalog.
- Fixed toggling `memory_limiter_interval` from `0s` to a non-zero value at
  runtime potentially triggering an immediate replica kill even when memory usage
  was well below the limit.
- Fixed Self-Managed Kubernetes deployments where setting both
  `cluster_topology_spread_soft = on` and `cluster_topology_spread_min_domains`
  caused all replica pod creation to fail with an admission error.
## v26.22.0
*Released to Materialize Cloud: 2026-04-30* <br>
*Released to Materialize Self-Managed: 2026-05-01* <br>

This release includes various improvements, including faster sink performance
with up to 50% lower memory usage, and bug fixes.

### Improvements {#v26.22-improvements}

#### Sink improvements {#v26.22-improvements-sink}

- **Faster sink performance with up to 50% lower memory usage**: Sink operations
  now process data more efficiently by walking arrangements directly via
  cursors, reducing memory overhead and improving throughput. For large sinks,
  we have seen memory usage reduced by up to 50%.
- **Iceberg sink support for interval and range types**: Iceberg sinks now
  support `interval` and `range` data types, expanding compatibility with
  complex data schemas.

#### MCP security improvements {#v26.22-improvements-mcp-security}

- **Enhanced MCP server security**: MCP server origin validation now uses CORS
  allowlists instead of self-comparison checks, preventing DNS rebinding
  attacks.
- **Stricter MCP search path security**: MCP developer endpoint now sets a tight
  `search_path` to prevent bypass attacks.

#### General improvements {#v26.22-improvements-general}

- Catalog synchronization now uses more efficient consolidation algorithms,
  reducing overhead for environments with many objects.

- Improved query optimization by pushing `COALESCE` operations into `CASE WHEN`
  expressions where beneficial.

### Bug Fixes {#v26.22-bug-fixes}

- Fixed Iceberg upsert sinks dropping delete operations when handling more than
  100,000 distinct keys.
- Fixed `EXPLAIN OPTIMIZED PLAN` failure after renaming materialized views,
  indexes, or continual tasks.
- Fixed Parquet map key handling to properly deduplicate keys and use the final
  value when duplicates exist.
- Fixed subquery handling to properly account for negative diffs in accumulation
  logic.
- Fixed PostgreSQL source compatibility by using only `pg_catalog.server_version_num` for version detection.
- Fixed PostgreSQL `format_type` output to properly quote the `"char"` type (OID
  18).
- Fixed an issue in the Console where the cursor would not appear in the SQL
  editor.
- Fixed incorrect results from `mz_dataflow_global_ids` view when multiple
  objects shared the same dataflow.
- Fixed interval conversion overflow in Arrow utilities when converting
  microseconds to nanoseconds.
- Fixed OpenTelemetry rate limiting filter that was incorrectly suppressing all
  events instead of just rate-limited ones.
- Fixed catalog leak when dropping replacement collections without applying
  them.
- Enhanced security by ensuring sensitive authentication data is properly
  cleared from memory after use.
- Enhanced security by ensuring TLS certificate data is properly zeroized when
  dropped.
- Improved SQL name escaping in catalog operations for better reliability.
- Removed unused `memory_request` field from replica allocation configuration.
- Added regression test for Kafka sink handling of negative accumulations.

## v26.20.2
*Released to Materialize Cloud: 2026-04-16* <br>
*Released to Materialize Self-Managed: 2026-04-17* <br>

This release introduces the built-in Developer MCP server, Console
improvements, and bug fixes.

### Developer MCP server

{{< public-preview />}}

Materialize environments now include a built-in Model Context Protocol (MCP)
[Developer endpoint
(`/api/mcp/developer`)](/integrations/mcp-server/mcp-developer/). Connecting an
MCP-compatible coding agent (such as Claude Code, Claude Desktop, or Cursor) to
this endpoint lets you ask natural language questions about your environment.

For example, you could ask *why is my materialized view stale?* or *how much memory is my cluster using?*. You'll receive a diagnosis and recommendations on how to fix isssues.

For more information, refer to:
- [Integrations: MCP Server for
  Developers](/integrations/mcp-server/mcp-developer/)

### Improvements {#v26-20-improvements}
- **Better Console schema navigation**: The schema dropdown in the SQL Shell now
  prioritizes schemas from the current database, making it easier to find
  relevant schemas.

### Bug Fixes {#v26-20-bug-fixes}
- Fixed Console RBAC users tab that was displaying incorrectly for cloud users
  due to null `rolcanlogin` values.
- Fixed builtin dependency ordering issue that could cause system catalog
  inconsistencies.

## v26.19.0
*Released to Materialize Cloud: 2026-04-09* <br>
*Released to Materialize Self-Managed: 2026-04-10* <br>

This release introduces append mode for [Iceberg sinks](/sql/create-sink/iceberg/),
and bug fixes.

### Iceberg sink append mode

When an [Iceberg sink](/sql/create-sink/iceberg/) is created in append
mode, all changes are written as data rows — no Iceberg delete files are
produced. This is especially useful if you're sinking data from a materialized
view with temporal filters, and you don't want data to be deleted from your Iceberg table as it ages out.

```mzsql
CREATE SINK events_log_iceberg
  IN CLUSTER analytics_cluster
  FROM user_events
  INTO ICEBERG CATALOG CONNECTION iceberg_catalog_connection (
    NAMESPACE = 'events',
    TABLE = 'user_events_log'
  )
  USING AWS CONNECTION aws_connection
  MODE APPEND
  WITH (COMMIT INTERVAL = '5m');
```

For more information, refer to:
- [Guide: Apache Iceberg sink](/serve-results/sink/iceberg/)
- [Reference: `CREATE SINK ICEBERG`](/sql/create-sink/iceberg/)

### Bug Fixes {#v26.19-bug-fixes}

- Fixed identifier display in system catalog tables `mz_kafka_source_tables`,
  `mz_mysql_source_tables`, and `mz_postgres_source_tables` to show raw values
  without SQL quoting (e.g., `my-kafka-topic` instead of `"my-kafka-topic"`).

## v26.18.0
*Released to Materialize Cloud: 2026-04-02* <br>
*Released to Materialize Self-Managed: 2026-04-03* <br>

This release includes various improvements and bug fixes.

### Improvements {#v26.18-improvements}

- **Improved Console reconnect behavior**. The Console shell now reconnects
  more reliably, with toast notifications that no longer stack.

- **Expanded `COPY FROM` data type support**. [`COPY FROM` parquet
  files](/sql/copy-from/#parquet-formatting) now supports `map` and `interval`
  data types.

- **Improved query performance on wide tables**. Queries on tables with many
  columns now execute faster.

### Bug Fixes {#v26.18-bug-fixes}

- Fixed SSL certificate loading to properly handle all certificates in PEM
  bundles instead of only the first one.
- Fixed materialized view sinks getting stuck when instantiated with output
  shards whose initial frontier is less than the dataflow as-of.
- Fixed panic when dropping computed tables with active `SUBSCRIBE` operations.
- Fixed `EXPLAIN ANALYZE` not working correctly due to quoting issues in
  `mz_mappable_objects`.

## v26.17.1
*Released to Materialize Self-Managed: 2026-03-27* <br>

This release includes a bug fix.

### Bug Fixes {#v26.17.1-bug-fixes}

- Fixed Iceberg sinks failing to write unsigned integer types (UInt8,
  UInt16, UInt32, UInt64) by mapping them to Iceberg-compatible signed
  types.

## v26.17.0
*Released to Materialize Cloud: 2026-03-26* <br>
*Released to Materialize Self-Managed: 2026-03-27* <br>

This release includes performance improvements and bug fixes.

### Improvements {#v26.17-improvements}

- **10% improved transactional DDL performance**: We've eliminated an O(n^2) operation. DDL transactions (such as creating multiple tables from a source in a single transaction) now execute faster.
- **Reduced catalog server load during blue/green deploys**: The dbt-materialize adapter now uses a single batched query instead of
  per-cluster sequential polling. This is especially useful when creating a large number of objects.

### Bug Fixes {#v26.17-bug-fixes}

- Fixed a correctness bug where LEFT JOIN, RIGHT JOIN, and FULL JOIN with an
  empty relation produced incorrect results (empty instead of NULLs) due to
  join identity elision.
- Fixed Kafka sinks incorrectly writing negative Avro timestamps (pre-epoch
  dates) by treating the timestamp microseconds as unsigned instead of signed.
- Fixed Avro fixed-decimal encoding not left-padding unscaled bytes to the
  schema's fixed size, which could cause `UnexpectedEof` errors or data
  corruption in downstream consumers.
- Fixed a race condition in persist where a batch could be selected before
  obtaining a lease, potentially causing unexpected read-time halts.
- Fixed PROXY protocol v2 header parsing failing when headers arrived across
  multiple TCP segments, which could corrupt subsequent HTTP parsing between
  balancerd and environmentd.
- Fixed the Fivetran destination connector logging `app_password` in plaintext
  in connection logs.
- Fixed queries with expensive functions in subqueries (e.g., `UNION ALL`,
  `EXISTS`, scalar subqueries) being incorrectly routed to `mz_catalog_server`
  instead of the user's cluster.
- Fixed webhook secret cache not invalidating when secrets are changed,
  requiring a restart to pick up new secret values.
- Fixed orchestratord image reference parsing treating registry ports (e.g.,
  `gcr.io:443/...`) and digest separators (`@sha256:...`) as image tags,
  producing invalid references for Self-Managed deployments.
- Fixed optimizer feature flags being auto-enabled during item parsing, which
  rendered plan caching ineffective.
- Fixed `mz_catalog_raw` not being consistently readable under strict
  serializable isolation by keeping the catalog shard's frontier up-to-date
  with the oracle read timestamp.
- Fixed a security vulnerability in the `lz4_flex` dependency
  (RUSTSEC-2026-0041).
- Fixed a bad assertion in oneshot source storage worker reconciliation that
  could cause panics.
- Fixed hydration check errors during 0dt upgrades for replica-targeted
  collections, where non-target replicas would report `CollectionMissing`
  errors.
- Fixed SQL Server source `Transaction::drop` not sending ROLLBACK, leaving
  the SQL Server session in an open transaction after drop.
- Fixed a panic in authentication when receiving a proof of unexpected length.
- Fixed an issue causing console session variables to be lost after a reconnect.

## v26.16.0
*Released to Materialize Cloud: 2026-03-19* <br>
*Released to Materialize Self-Managed: 2026-03-20* <br>

This release adds support for copying Parquet files from object storage, performance improvements, and bug fixes.

### `COPY FROM` Parquet files in object storage

`COPY FROM` now supports bulk importing data from Parquet files stored in Amazon
S3 and any S3-compatible object storage service, such as Google Cloud Storage,
Cloudflare R2, or MinIO. You can import Parquet files using an AWS connection or
a presigned URL.

```mzsql
COPY INTO my_table
FROM 's3://my_bucket/my_data.parquet'
(FORMAT PARQUET, AWS CONNECTION = my_aws_conn);
```

For more information, refer to:
- [Syntax: COPY FROM](/sql/copy-from/)
- [Syntax: CREATE CONNECTION (S3-compatible)](/sql/create-connection/#s3-compatible-object-storage)

### Improvements {#v26.16-improvements}

- **Improved [`AS OF`](/sql/subscribe/#as-of) error messages**: Error messages
  for `AS OF` queries now use user-facing terminology (e.g., "Indexed
  input", "Storage inputs") instead of internal names.
- **Streamed [WebSocket](/integrations/websocket-api/) query results**:
  WebSocket query results are now streamed directly instead of buffered,
  reducing memory usage for large result sets.

### Bug Fixes {#v26.16-bug-fixes}

- Fixed an RBAC security bypass that allowed a non-superuser with
  `CREATEROLE` privilege to strip superuser status from any role via
  `ALTER ROLE ... NOSUPERUSER`.
- Fixed indexes on older versions of altered tables or replaced
  materialized views being lost during environment bootstrap, which
  could cause panics.
- Fixed pgwire encoding errors leaving partial messages in the connection
  buffer, which caused clients to see "lost synchronization" errors
  instead of proper error messages.
- Fixed unbounded queue growth in storage since-downgrade processing that
  could lead to out-of-memory conditions in environments with many
  storage collections.
- Fixed a correctness bug when parsing large Avro fixed-size decimals
  from Kafka sources, where values were returned as raw bytes instead of
  decoded decimal numbers.
- Fixed subqueries being incorrectly allowed in the `SET` clause of
  `UPDATE` statements.
- Fixed `COPY FROM S3` requiring manual column specification for tables
  with `NOT NULL` columns by removing a redundant non-null check during
  planning.
- Fixed a correctness issue with `COPY FROM STDIN` when using headers.
- Fixed column name deduplication bug in `COPY TO` / Parquet writer that
  could produce duplicate column names.
- Fixed `RETAIN HISTORY` value being ignored for webhook tables.
- Fixed `DROP OWNED BY` and `REASSIGN OWNED BY` not including network
  policies, which could block `DROP ROLE` for roles that own network
  policies.
- Fixed false positive wallclock lag reporting (showing ~56 years of lag)
  during replica startup for compute introspection indexes.

## v26.15.0
*Released to Materialize Cloud: 2026-03-12* <br>
*Released to Materialize Self-Managed: 2026-03-13* <br>

This release includes various improvements and bug fixes.

### Improvements {#v26.15-improvements}

- **Improved memory efficiency for joins on `varchar` and `text` columns**:
  Previously, joining on these columns required creating a new arrangement,
  effectively doubling memory usage. Materialize can now reuse existing
  arrangements on these columns. We've seen memory improvements by as much as 25%
  in some cases involving `varchar` indexes.
- Added support for setting `cpu_request` independently of `cpu_limit`
  in cluster replica sizes for Self-Managed deployments.
- Renamed the **Org ID** label to **Environment ID** in the Console Shell
  to disambiguate organization IDs from environment IDs, which was
  causing confusion for Self-Managed deployments.

### Bug Fixes {#v26.15-bug-fixes}

- Fixed unmaterializable functions (e.g., `now()`) being allowed in
  `AS OF` queries, which could return incorrect results.
- Fixed Kafka sink creation failing with an authorization error when the
  progress topic already exists, which affected workflows where topics
  are pre-created by a superuser.
- Fixed a panic when running `COPY FROM STDIN` concurrently with table
  drops.
- Fixed unbounded command queue buildup in internal storage writer tasks
  that could lead to out-of-memory conditions when environments have a
  large number of indexes.
- Fixed the Role Filters display in dark mode in the Console.
- Fixed an incorrect join condition in the Console cluster list that
  could cause incorrect cluster information to be displayed.

## v26.14.1
*Released to Materialize Cloud: 2026-03-05* <br>
*Released to Materialize Self-Managed: 2026-03-06* <br>

This release introduces `COPY FROM` support for CSVs in object storage, source versioning for SQL Server sources, and performance improvements to DDL.

### `COPY FROM` CSVs in object storage

`COPY FROM` now supports bulk importing data directly from Amazon S3 and any
S3-compatible object storage service, such as Google Cloud Storage, Cloudflare
R2, or MinIO. You can import CSV files using an AWS connection or a presigned
URL.

```mzsql
COPY INTO my_table
FROM 's3://my_bucket/my_data.csv'
(FORMAT CSV, AWS CONNECTION = my_aws_conn);
```

For more information, refer to:
- [Syntax: COPY FROM](/sql/copy-from/)
- [Syntax: CREATE CONNECTION (S3-compatible)](/sql/create-connection/#s3-compatible-object-storage)

### SQL Server: Source versioning

{{< private-preview />}}

For SQL Server sources, we've introduced new syntax
for [`CREATE SOURCE`](/sql/create-source/sql-server-v2/) and [`CREATE
TABLE`](/sql/create-table/). This allows you to better handle schema changes
in your source SQL Server tables.

{{< note >}}
- Changing column types is currently unsupported.
{{< /note >}}

For more information, refer to:
- [Guide: Handling upstream schema changes with zero
  downtime](/ingest-data/sql-server/source-versioning/)
- [Syntax: `CREATE SOURCE`](/sql/create-source/sql-server-v2/)
- [Syntax: `CREATE TABLE`](/sql/create-table/)

### Improvements {#v26.14-improvements}

- **Faster DDL at scale**: We've improved DDL (e.g., `CREATE VIEW`, `CREATE INDEX`, `DROP`) latency by 37-55% for environments with many objects by making the internal catalog state a persistent data structure with structural sharing.
- **Faster Iceberg sink commits**: We've improved Iceberg sink commit performance by disabling the duplicate check for RowDelta actions, which was causing significant commit time overhead.
- **Up to 28x faster `COPY FROM STDIN`**: We've improved `COPY FROM STDIN` performance by parallelizing ingestion and using constant memory.

### Bug Fixes {#v26.14-bug-fixes}

- Fixed the jsonb contains operator (`?`) to correctly return NULL when
  the left operand is NULL, matching PostgreSQL behavior.
- Internal optimization that reduces resource usage of the catalog server; this can
  reduce resource consumption on restart when indexes are added.
- Fixed a panic when using `COPY FROM` with invalid range values (e.g.,
  `[7,3)` where lower bound exceeds upper bound), now returning a
  proper error message.
- Fixed incorrect replication lag display in the Console during
  PostgreSQL source snapshots, where `offset_committed` was incorrectly
  reported as zero until the snapshot completed.
- Fixed a panic when dropping materialized views that had active
  subscribes depending on older GlobalIds.
- Fixed dataflows being incorrectly re-planned after an environmentd
  restart due to missing per-cluster optimizer feature overrides.
- Fixed query formatting for SQL Server and MySQL sources.

## v26.13.0
*Released to Materialize Cloud: 2026-02-26* <br>
*Released to Materialize Self-Managed: 2026-02-27* <br>

This release includes the release of our Iceberg Sink, performance improvements to `SUBSCRIBE`, and bugfixes.

### Iceberg Sink
{{< public-preview />}}
Iceberg sinks provide exactly once delivery of updates from Materialize into [Apache
Iceberg](https://iceberg.apache.org/) tables hosted on [Amazon S3
Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html).
As data changes in Materialize, the corresponding Iceberg tables are
automatically kept up to date. You can sink data from a materialized view, a
source, or a table.

```mzsql
CREATE SINK my_iceberg_sink
  IN CLUSTER sink_cluster
  FROM materialized_view_mv1
  INTO ICEBERG CATALOG CONNECTION iceberg_catalog_connection (
    NAMESPACE = 'my_iceberg_namespace',
    TABLE = 'mv1'
  )
  USING AWS CONNECTION aws_connection
  KEY (row_id)
  MODE UPSERT
  WITH (COMMIT INTERVAL = '60s');
```

For more information, refer to:
- [Guide: How to export results from Materialize to Apache Iceberg Tables](/serve-results/sink/iceberg)
- [Blog: Making Iceberg work for Operational Data](https://materialize.com/blog/making-iceberg-work-for-operational-data/)
- [Syntax: CREATE SINK... INTO ICEBERG ](/sql/create-sink/iceberg)

### Improvements {#v26.13-improvements}
- **Improved `SUBSCRIBE` Performance**: We've optimized `SUBSCRIBE` to skip initial snapshots in more cases. This can speed up `SUBSCRIBE` start times.
- **Improved compatibility with external tools**: We've added `strpos` as a
  synonym for the `position` function, improving compatibility with tools such
  as PowerBI.
- **Improved database concurrency**: We've reduced contention when a single
  collection experiences a high volume of updates.

### Bug Fixes {#v26.13-bug-fixes}
- Fixed a panic when constructing multi-dimensional arrays with null values,
  now treating null elements as zero-dimensional arrays consistent with
  PostgreSQL behavior.
- Fixed a bug where dropping a replacement materialized view (instead of
  applying the replacement) could seal the target materialized view for all
  times after an environmentd restart.
- Fixed a bug where `Int2Vector` to `Array` casting did not correctly handle
  element type conversions, potentially causing incorrect results or errors.
- Fixed the Self-Managed bug in the memory-based calculation of replica size
  credits, which was incorrectly multiplying by the number of workers instead of
  using the correct per-process memory limit.
- Fixed an overflow display issue on the roles page in the console.
- Fixed SSO connection configuration pages in the console, which did not load properly due to missing content security policy entries.

## v26.12.0
*Released to Materialize Cloud: 2026-02-19* <br>
*Released to Materialize Self-Managed: 2026-02-20* <br>

This release introduces our Roles and Users page, performance improvements, and bugfixes.

### Role Management
The new Roles and Users page on the Materialize Console allows organization administrators to create roles, grant privileges, and assign roles to users. You can also track the hierarchy of roles using the graph view.

![Create Role experience](/images/releases/v2612_create_role.png)

![Graph View experience](/images/releases/v2612_graph_view.png)

You can navigate to the Roles and Users page directly from the Materialize console. If you're on Materialize Self-Managed, upgrade to v26.12 first. If you're on Materialize Cloud, you can go directly to https://console.materialize.com/roles to reach the page.

### Improvements {#v26.12-improvements}
- **Updated default resource requirements** (<red>*Materialize Self-Managed only*</red>): We've updated the Materialize Self-Managed Helm charts to ensure correct operation on Kind clusters
- **Improved console query history performance**: We've optimized RBAC queries to use OIDs instead of names, resulting in 2-3x faster page execution.

### Bug Fixes {#v26.12-bug-fixes}
- Fixed a panic when using unsupported types (e.g., float) with range
  expressions, now returning a proper error message instead of an internal error.
- Fixed a panic when using empty `int2vector` values, which could cause internal
  errors during query optimization or execution.
- Fixed internal errors that could occur during query optimization due to type
  checking mismatches in `ColumnKnowledge` and related transforms, adding
  fallback handling to prevent crashes.
- Fixed compatibility with older Amazon Aurora PostgreSQL versions when using
  parallel snapshots, by using `SELECT current_setting()` instead of `SHOW` for
  version retrieval.
- Fixed version comparison in the Materialize Kubernetes operator to correctly
  follow semver precedence rules, no longer rejecting upgrades that differ only in build metadata.

## v26.11.0
*Released to Materialize Cloud: 2026-02-19* <br>
*Released to Materialize Self-Managed: 2026-02-13* <br>

This release includes improvements to Avro Schema references, `EXPLAIN` commands, and bug fixes.

### Improvements {#v26.11-improvements}
- **Avro Schema References**: Sources can now use avro schemas which reference
  other schemas when using Confluent Schema Registry.
- **`EXPLAIN` improvements**: `EXPLAIN` now allows you to inspect the query plan
  for `SUBSCRIBE` statements. It also fully qualifies index names if there are
  identically-named indexes across different schemas.
- **More efficient dbt-adapter**: We've added indexes on `mz_hydration_statuses` and `mz_materialization_lag`.
  This should speed up "deployment ready" queries made by our dbt-adapter.

### Bug Fixes {#v26.11-bug-fixes}
- Fixed a bug where `IS DISTINCT FROM` could fail typechecking in certain cases
  involving different data types, causing query errors.
- Improved the error message when `INSERT INTO ... SELECT` transitively
  references a source.

## v26.10.1
*Released to Materialize Cloud: 2026-02-05* <br>
*Released to Materialize Self-Managed: 2026-02-06* <br>

This release introduces Replacement Materialized Views, performance improvements,
and bugfixes.

### Replacement Materialized Views
{{< public-preview />}}
Replacement materialized views allow you to modify the definition of an existing materialized view, while preserving all downstream dependencies. Materialize is able to replace a materialized view in place, by calculating the *diff* between the original and the replacement. Once applied, the *diff* flows downstream to all dependent objects.

For more information, refer to:
- [Guide: Replace Materialized Views](/transform-data/updating-materialized-views/replace-materialized-view)
- [Syntax: CREATE REPLACEMENT MATERIALIZED VIEW](/sql/create-materialized-view)
- [Syntax: ALTER MATERIALIZED VIEW](/sql/alter-materialized-view)

### Improvements {#v26.10-improvements}
- **Improved hydration times for PostgreSQL sources**: PostgreSQL sources now perform parallel snapshots. This should improve initial hydration times, especially for large tables.

### Bug Fixes {#v26.10-bug-fixes}
- Fixed an issue where floating-point values like `-0.0` and `+0.0` could be
  treated as different values in equality comparisons but the same in ordering,
  causing incorrect results in operations like `DISTINCT`.
- Fixed an issue where certain SQL keywords required incorrect quoting in
  expressions.
- Fixed the `ORDER BY` clause in `EXPLAIN ANALYZE MEMORY` to correctly sort by
  memory usage instead of by the text representation.
- Fixed a bug where the optimizer could mishandle nullability inside record
  types.
- Fixed an issue where the `mz_roles` system table could produce invalid
  retractions when certain system variables were changed.
- *Console*: Fixed SQL injection vulnerability in identifier quoting where only
  the first quote character was being escaped.

## v26.9.0
*Released to Materialize Cloud: 2026-01-29* <br>
*Released to Materialize Self-Managed: 2026-01-30* <br>

v26.9 includes significant performance improvements to QPS & query latency.

### Improvements {#v26.9-improvements}
- **Up to 2.5x increased QPS**: <a name="v26.9-qps"></a>We've significantly optimized how `SELECT` statements are processed; they are now processed outside the main thread. In our tests, this change increased QPS by as much as 2.5x.
![Chart of QPS before/after](/images/releases/v2609_qps.png)
- **Significant reduction in query latency**: <a
  name="v26.9-latency-reduction"></a>Moving `SELECT` statements off the main
  thread has significantly reduced latency. p99 has reduced by up to 50% for
some workloads. ![Chart of latency
before/after](/images/releases/v2609_latency.png)
- **Dynamically configure system parameters using a ConfigMap** (<red>*Materialize Self-Managed only*</red>): <a name="v26.9-sm-configmap"></a>You can now use a ConfigMap to dynamically update system parameters at runtime. In many cases, this means you don't need to restart Materialize for new system parameters to take effect. You can also specify system parameters which survive restarts and upgrades. Refer to our [documentation on configuring system parameters](/self-managed-deployments/configuration-system-parameters/#configure-system-parameters-via-configmap).
- Added `ABORT` as a PostgreSQL-compatible alias for the `ROLLBACK` transaction command, to improve compatibility with GraphQL engines like Hasura

### Bug Fixes {#v26.9-bug-fixes}
- Fixed an issue causing new generations to be promoted prematurely when using the `WaitUntilReady` upgrade strategy (<red>*Materialize Self-Managed only*</red>)
- Fixed a race condition in source reclock that could cause panics when the `as_of` timestamp was newer than the cached upper bound.
- Improved error messages when the load balancer cannot connect to the upstream environment server

## v26.8.0
*Released to Materialize Cloud: 2026-01-22* <br>
*Released to Materialize Self-Managed: 2026-01-23* <br>

v26.8 includes a new notice in the Console to help catch common SQL mistakes,
Protobuf compatibility improvements, and performance optimizations for view
creation.

### Improvements {#v26.8-improvements}
- Added a Console notice when users write `= NULL`, `!= NULL`, or `<>
  NULL` in SQL expressions instead of `IS NULL` or `IS NOT NULL`. Comparisons
  using `=`, `!=`, or `<>` with `NULL` always evaluate to `NULL`.
- Protobuf schemas that import well-known types (such as `google.protobuf.Timestamp` or `google.protobuf.Duration`) now work automatically when using a Confluent Schema Registry connection.
- Improved performance of view creation by caching optimized expressions, resulting in approximately 2x faster view creation in some scenarios.

## v26.7.0
*Released to Materialize Self-Managed: 2026-01-16* <br>
*Released to Materialize Cloud: 2026-01-17* <br>

v26.7 improves compatibility with go-jet and includes bug fixes.

### Improvements {#v26.7-improvements}

- **Improved compatibility with go-jet**: We've added the `attndims` column to `pg_attribute`. We've also fixed `pg_type.typelem` to correctly report element types for named list types.
- **Pretty print SQL in the console**: We've made it easier to read the definitions for views and materialized views in the console.

### Bug Fixes {#v26.7-bug-fixes}
- Fixed an issue where type error messages could inadvertently expose constant values from queries.
- The console reconnects more gracefully if the connection to the backend is interrupted

## v26.6.0
*Released to Materialize Cloud: 2026-01-08*<br>
*Released to Materialize Self-Managed: 2026-01-09*<br>

v26.6.0 includes bug fixes for Kafka sinks and Self-Managed deployments.

### Bug Fixes {#v26.6-bug-fixes}
- Fixed an issue where console and balancer deployments could fail to upgrade to the correct version during Self-Managed environment upgrades.
- Fixed an issue where `ALTER SINK ... SET FROM` on Kafka sinks could incorrectly restart in snapshot mode even when the sink had already made progress, causing unnecessary resource consumption and potential out-of-memory errors.

## v26.5.1
*Released to Materialize Self-Managed: 2025-12-23* <br>
*Released to Materialize Cloud: 2026-01-08* <br>

v26.5.1 enhances our SQL Server source, improves performance, and strengthens Materialize Self-Managed reliability.

### Improvements {#v26.5-improvements}
- **VARCHAR(MAX) and NVARCHAR(MAX) support for SQL Server**: The Materialize SQL Server source now supports `varchar(max)` and `nvarchar(max)` data types.
- **Faster authentication for connection poolers**: We've added an index to the `pg_authid` system catalog. This should significantly improve the performance of default authentication queries made by connection poolers like pgbouncer.
- **Faster Kafka sink startup**: We've updated the default Kafka progress topic configuration to reduce the amount of progress data processed when creating new [Kafka sinks](/serve-results/sink/kafka/).
- **dbt strict mode**: We've introduced `strict_mode` to dbt-materialize, our dbt adapter. `strict_mode` enforces production-ready isolation rules and improves cluster health monitoring. It does so by validating source idempotency, schema isolation, cluster isolation and index restrictions.
- **SQL Server Always On HA failover support** (<red>*Materialize Self-Managed only*</red>): Materialize Self-Managed now offers better support for handling failovers, without downtime, in SQL Server Always On sources. [Contact our support team](/support/) to enable this in your environment.
- **Auto-repair accidental changes** (<red>*Materialize Self-Managed only*</red>): Improvements to the controller logic allow Materialize to auto-repair changes such as deleting a StatefulSet. This means that your production setups should be more robust in the face of accidental changes.
- **Track deployment status after upgrades** (<red>*Materialize Self-Managed only*</red>): The Materialize custom resource now displays both active and desired `environmentd` versions. This makes it easier to track deployment status after upgrades.

### Bug fixes {#v26.5-bug-fixes}
- Added additional checks to string functions (`replace`, `translate`, etc.) to help prevent out-of-memory errors from inflationary string operations.
- Fixed an issue which could cause panics during connection drops; this means improved stability when clients disconnect.
- Fixed an issue where disabling console or balancers would fail if they were already running.
- Fixed an issue where balancerd failed to upgrade and remained stuck on its pre-upgrade version.

## v26.4.0

*Released to Materialize Self-Managed: 2025-12-17* <br>
*Released to Materialize Cloud: 2025-12-18*

v26.4.0 introduces several performance improvements and bugfixes.

### Improvements {#v26.4-improvements}
- **Over 2x higher connections per second (CPS)**: We've optimized how Materialize handles inbound connection requests. In our tests, we've observed 2x - 4x improvements to the rate at which new client connections can be established. This is especially beneficial when spinning up new environments, warming up connection pools, or scaling client instances.
- **Up to 3x faster hydration times for large PostgreSQL tables**: We've reduced the overhead incurred by communication between multiple *workers* on a large cluster. We've observed up to 3x throughput improvement when ingesting 1 TB PostgreSQL tables on large clusters.
- **More efficient source ingestion batching**: Sources now batch writes more effectively. This can result in improved freshness and lower resource utilization, especially when a source is doing a large number of writes.
- **CloudSQL HA failover support** (<red>*Materialize Self-Managed only*</red>): Materialize Self-Managed now offers better support for handling failovers in CloudSQL HA sources, without downtime. [Contact our support team](/support/) to enable this in your environment.
- **Manual Promotion** (<red>*Materialize Self-Managed only*</red>): [Rollout strategies](/self-managed-deployments/upgrading/#rollout-strategies) allow you control how Materialize transitions from the current generation to a new generation during an upgrade. We've added a new rollout strategy called `ManuallyPromote` which allows you to choose when to promote the new generation. This means that you can minimize the impact of potential downtime.

### Bug Fixes {#v26.4-bug-fixes}
- Fixed timestamp determination logic to handle empty read holds correctly.
- Fixed lazy creation of temporary schemas to prevent schema-related errors.
- Reduced SCRAM iterations in scalability framework and fixed fallback image configuration.

## v26.3.0

*Released to Materialize Cloud & Materialize Self-Managed: 2025-12-12*<br>

### Improvements {#v26.3-improvements}
- For Self-Managed: added version upgrade window validation, to prevent skipping required intermediate versions during upgrades.
- Improved activity log throttling to apply across all statement executions, not just initial prepared statement execution, providing more consistent logging behavior.

### Bug Fixes {#v26.3-bug-fixes}
- Fixed validation for replica sizes to prevent configurations with zero scale or workers, which previously caused division-by-zero errors and panics.
- Fixed frontend `SELECT` sequencing to gracefully handle collections that are dropped during real-time recent timestamp determination.

## v26.2.0

*Released Cloud: 2025-12-05*<br>
*Released Self-Managed: 2025-12-09*

This release focuses primarily on bug fixes.

### Bug fixes {#v26.2-bug-fixes}
- **Catalog updates**: Fixed a bug where catalog item version updates were incorrectly ignored when the `create_sql` didn't change, which could cause version updates to not be applied properly.

- **Console division by zero**: Fixed a division by zero error in the console, specifically when viewing `mz_console_cluster_utilization_overview`.

- **ALTER SINK improvements**: Fixed `ALTER SINK ... SET FROM` to prevent panics in certain situations.

- **Improved rollout handling**: Fixed an issue where rollouts could leave a pod at their previous configuration.

- **Dependency drop handling**: Fixed panics that could occur when dependencies are dropped during a SELECT or COPY TO. These operations now gracefully return a `ConcurrentDependencyDrop` error.


## v26.1.0
*Released Self-Managed: 2025-11-26*

v26.1.0 introduces `EXPLAIN ANALYZE CLUSTER`, console bugfixes, and improvements for SQL Server support, including the ability to create a SQL Server Source via the Console.

### `EXPLAIN ANALYZE CLUSTER`
The [`EXPLAIN ANALYZE`](/sql/explain-analyze/) statement helps analyze how objects, namely indexes or materialized views, are running. We've introduced a variation of this statement, `EXPLAIN ANALYZE CLUSTER`, which presents a summary of every object running on your current cluster.

You can use this statement to understand the CPU time spent and memory consumed per object on a given cluster. You can also reveal whether an object has skewed operators, where work isn't evenly distributed among workers.

For example, to get a report on memory, you can run `EXPLAIN ANALYZE CLUSTER MEMORY`, and you'll receive an output similar to the table below:
| object                                  | global_id | total_memory | total_records |
| --------------------------------------- | --------- | ------------ | ------------- |
| materialize.public.idx_top_buyers       | u85496    | 2086 bytes   | 25            |
| materialize.public.idx_sales_by_product | u85492    | 1909 kB      | 148607        |
| materialize.public.idx_top_buyers       | u85495    | 1332 kB      | 77133         |

To understand worker skew, you can run `EXPLAIN ANALYZE CLUSTER CPU WITH SKEW`, and you'll receive an output similar the table below:
| object                                  | global_id | worker_id | max_operator_cpu_ratio | worker_elapsed  | avg_elapsed     | total_elapsed   |
| --------------------------------------- | --------- | --------- | ---------------------- | --------------- | --------------- | --------------- |
| materialize.public.idx_sales_by_product | u85492    | 0         | 1.18                   | 00:00:00.094447 | 00:00:00.079829 | 00:00:00.159659 |
| materialize.public.idx_top_buyers       | u85495    | 0         | 1.15                   | 00:00:01.371221 | 00:00:01.363659 | 00:00:02.727319 |
| materialize.public.idx_top_buyers       | u85495    | 1         | 1.03                   | 00:00:01.356098 | 00:00:01.363659 | 00:00:02.727319 |
| materialize.public.idx_top_buyers       | u85496    | 1         | 1.01                   | 00:00:00.021163 | 00:00:00.021048 | 00:00:00.042096 |
| materialize.public.idx_top_buyers       | u85496    | 0         | 0.99                   | 00:00:00.020932 | 00:00:00.021048 | 00:00:00.042096 |
| materialize.public.idx_sales_by_product | u85492    | 1         | 0.82                   | 00:00:00.065211 | 00:00:00.079829 | 00:00:00.159659 |

### Improved SQL Server support

Materialize v26.1.0 includes improved support for SQLServer, including the ability to create a SQLServer Source via the console.

### Upgrade notes for v26.1.0

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.1.md" >}}

## Self-Managed v26.0.0

*Released: 2025-11-18*

### Swap support

Starting in v26.0.0, Self-Managed Materialize enables swap by default. Swap
allows for infrequently accessed data to be moved from memory to disk. Enabling
swap reduces the memory required to operate Materialize and improves cost
efficiency.

To facilitate upgrades from v25.2, Self-Managed Materialize added new labels to
the node selectors for `clusterd` pods:

- To upgrade using Materialize-provided Terraforms, upgrade your Terraform
  version to `v0.6.1`:
  - {{< include-md
file="shared-content/self-managed/aws-terraform-v0.6.1-upgrade-notes.md" >}}.
  - {{< include-md
file="shared-content/self-managed/gcp-terraform-v0.6.1-upgrade-notes.md" >}}.
  - {{< include-md
  file="shared-content/self-managed/azure-terraform-v0.6.1-upgrade-notes.md"
  >}}.

- To upgrade if <red>**not**</red> using a Materialize-provided Terraforms,  you
must prepare your nodes by adding the required labels. For detailed
instructions, see [Prepare for swap and upgrade to
v26.0](/self-managed-deployments/appendix/upgrade-to-swap/).


### SASL/SCRAM-SHA-256 support

Starting in v26.0.0, Self-Managed Materialize supports SASL/SCRAM-SHA-256
authentication for PostgreSQL wire protocol connections. For more information,
see [Authentication](/security/self-managed/authentication/).

When SASL authentication is enabled:

- **PostgreSQL connections** (e.g., `psql`, client libraries, [connection
  poolers](/integrations/connection-pooling/)) use SCRAM-SHA-256 authentication
- **HTTP/Web Console connections** use standard password authentication

This hybrid approach provides maximum security for SQL connections while maintaining
compatibility with web-based tools.

### License Key

Starting in v26.0.0, Self-Managed Materialize requires a license key.

{{< yaml-table data="self_managed/license_key" >}}

For new deployments, you configure your license key in the Kubernetes Secret
resource during the installation process. For details, see the [installation
guides](/self-managed-deployments/installation/). For existing deployments, you can configure your
license key via:

```bash
kubectl -n materialize-environment patch secret materialize-backend -p '{"stringData":{"license_key":"<your license key goes here>"}}' --type=merge
```

### PostgreSQL: Source versioning

{{< private-preview />}}

For PostgreSQL sources, starting in v26.0.0, Materialize introduces new syntax
for [`CREATE SOURCE`](/sql/create-source/postgres-v2/) and [`CREATE
TABLE`](/sql/create-table/) to allow better handle DDL changes to the upstream
PostgreSQL tables.

{{< note >}}
- This feature is currently supported for PostgreSQL sources, with
additional source types coming soon.

- Changing column types is currently unsupported.
{{< /note >}}

{{% include-example file="examples/create_source/example_postgres_source"
 example="syntax" %}}

{{% include-example file="examples/create_table/example_postgres_table"
 example="syntax" %}}

For more information, see:
- [Guide: Handling upstream schema changes with zero
  downtime](/ingest-data/postgres/source-versioning/)
- [`CREATE SOURCE`](/sql/create-source/postgres-v2/)
- [`CREATE TABLE`](/sql/create-table/)

### Deprecation

The `inPlaceRollout` setting has been deprecated and will be ignored. Instead,
use the new setting `rolloutStrategy` to specify either:

- `WaitUntilReady` (*Default*)
- `ImmediatelyPromoteCausingDowntime`

For more information, see [`rolloutStrategy`](/self-managed-deployments/upgrading/#rollout-strategies).

### Terraform helpers

Corresponding to the v26.0.0 release, the following versions of the sample
Terraform modules have been released:

{{< yaml-table data="self_managed/terraform_list" >}}

{{< tabs >}} {{< tab "Materialize on AWS" >}}

{{< yaml-table data="self_managed/aws_terraform_versions" >}}

{{% self-managed/aws-terraform-upgrade-notes %}}

Click on the Terraform version link to go to the release-specific Upgrade Notes.

{{</ tab >}}

{{< tab "Materialize on Azure" >}}

{{< yaml-table data="self_managed/azure_terraform_versions" >}}

{{% self-managed/azure-terraform-upgrade-notes %}}

See also Upgrade Notes for release specific notes.

{{</ tab >}}

{{< tab "Materialize on GCP" >}}

{{< yaml-table data="self_managed/gcp_terraform_versions" >}}

{{% self-managed/gcp-terraform-upgrade-notes %}}

See also Upgrade Notes for release specific notes.

{{</ tab >}}

{{< tab "terraform-helm-materialize" >}}

{{< yaml-table data="self_managed/terraform_helm_compatibility" >}}

{{</ tab >}} {{</ tabs >}}

#### Upgrade notes for v26.0.0

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.0.md" >}}

See also [Version specific upgrade
notes](/self-managed-deployments/upgrading/#version-specific-upgrade-notes).


## See also

- [Release Schedule](/releases/schedule/)
