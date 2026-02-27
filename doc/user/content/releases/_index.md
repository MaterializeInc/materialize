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

### Improvements
- **Improved `SUBSCRIBE` Performance**: We've optimized `SUBSCRIBE` to skip initial snapshots in more cases. This can speed up `SUBSCRIBE` start times.
- **Improved compatibility with external tools**: We've added `strpos` as a synonym for the `position` function, improving compatibility with tools such as PowerBI.

### Bug Fixes
- Fixed a panic when constructing multi-dimensional arrays with null values,
  now treating null elements as zero-dimensional arrays consistent with
  PostgreSQL behavior.
- Fixed a concurrency issue (futurelock) in persist that could cause data flow
  stalls when multiple writers competed for update leases.
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

### Improvements
- **Updated default resource requirements** (<red>*Materialize Self-Managed only*</red>): We've updated the Materialize Self-Managed Helm charts to ensure correct operation on Kind clusters
- **Improved console query history performance**: We've optimized RBAC queries to use OIDs instead of names, resulting in 2-3x faster page execution.

### Bug Fixes
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

### Improvements
- **Avro Schema References**: Sources can now use avro schemas which reference
  other schemas when using Confluent Schema Registry.
- **`EXPLAIN` improvements**: `EXPLAIN` now allows you to inspect the query plan
  for `SUBSCRIBE` statements. It also fully qualifies index names if there are
  identically-named indexes across different schemas.
- **More efficient dbt-adapter**: We've added indexes on `mz_hydration_statuses` and `mz_materialization_lag`.
  This should speed up "deployment ready" queries made by our dbt-adapter.

### Bug Fixes
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

### Improvements
- **Improved hydration times for PostgreSQL sources**: PostgreSQL sources now perform parallel snapshots. This should improve initial hydration times, especially for large tables.

### Bug Fixes
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

### Improvements
- **Up to 2.5x increased QPS**: <a name="v26.9-qps"></a>We've significantly optimized how `SELECT` statements are processed; they are now processed outside the main thread. In our tests, this change increased QPS by as much as 2.5x.
![Chart of QPS before/after](/images/releases/v2609_qps.png)
- **Significant reduction in query latency**: <a
  name="v26.9-latency-reduction"></a>Moving `SELECT` statements off the main
  thread has significantly reduced latency. p99 has reduced by up to 50% for
some workloads. ![Chart of latency
before/after](/images/releases/v2609_latency.png)
- **Dynamically configure system parameters using a ConfigMap** (<red>*Materialize Self-Managed only*</red>): <a name="v26.9-sm-configmap"></a>You can now use a ConfigMap to dynamically update system parameters at runtime. In many cases, this means you don't need to restart Materialize for new system parameters to take effect. You can also specify system parameters which survive restarts and upgrades. Refer to our [documentation on configuring system parameters](/self-managed-deployments/configuration-system-parameters/#configure-system-parameters-via-configmap).
- Added `ABORT` as a PostgreSQL-compatible alias for the `ROLLBACK` transaction command, to improve compatibility with GraphQL engines like Hasura

### Bug Fixes
- Fixed an issue causing new generations to be promoted prematurely when using the `WaitUntilReady` upgrade strategy (<red>*Materialize Self-Managed only*</red>)
- Fixed a race condition in source reclock that could cause panics when the `as_of` timestamp was newer than the cached upper bound.
- Improved error messages when the load balancer cannot connect to the upstream environment server

## v26.8.0
*Released to Materialize Cloud: 2026-01-22* <br>
*Released to Materialize Self-Managed: 2026-01-23* <br>

v26.8 includes a new notice in the Console to help catch common SQL mistakes,
Protobuf compatibility improvements, and performance optimizations for view
creation.

### Improvements
- Added a Console notice when users write `= NULL`, `!= NULL`, or `<>
  NULL` in SQL expressions instead of `IS NULL` or `IS NOT NULL`. Comparisons
  using `=`, `!=`, or `<>` with `NULL` always evaluate to `NULL`.
- Protobuf schemas that import well-known types (such as `google.protobuf.Timestamp` or `google.protobuf.Duration`) now work automatically when using a Confluent Schema Registry connection.
- Improved performance of view creation by caching optimized expressions, resulting in approximately 2x faster view creation in some scenarios.

## v26.7.0
*Released to Materialize Self-Managed: 2026-01-16* <br>
*Released to Materialize Cloud: 2026-01-17* <br>

v26.7 improves compatibility with go-jet and includes bug fixes.

### Improvements

- **Improved compatibility with go-jet**: We've added the `attndims` column to `pg_attribute`. We've also fixed `pg_type.typelem` to correctly report element types for named list types.
- **Pretty print SQL in the console**: We've made it easier to read the definitions for views and materialized views in the console.

### Bug Fixes
- Fixed an issue where type error messages could inadvertently expose constant values from queries.
- The console reconnects more gracefully if the connection to the backend is interrupted

## v26.6.0
*Released to Materialize Cloud: 2026-01-08*<br>
*Released to Materialize Self-Managed: 2026-01-09*<br>

v26.6.0 includes bug fixes for Kafka sinks and Self-Managed deployments.

### Bug Fixes
- Fixed an issue where console and balancer deployments could fail to upgrade to the correct version during Self-Managed environment upgrades.
- Fixed an issue where `ALTER SINK ... SET FROM` on Kafka sinks could incorrectly restart in snapshot mode even when the sink had already made progress, causing unnecessary resource consumption and potential out-of-memory errors.

## v26.5.1
*Released to Materialize Self-Managed: 2025-12-23* <br>
*Released to Materialize Cloud: 2026-01-08* <br>

v26.5.1 enhances our SQL Server source, improves performance, and strengthens Materialize Self-Managed reliability.

### Improvements
- **VARCHAR(MAX) and NVARCHAR(MAX) support for SQL Server**: The Materialize SQL Server source now supports `varchar(max)` and `nvarchar(max)` data types.
- **Faster authentication for connection poolers**: We've added an index to the `pg_authid` system catalog. This should significantly improve the performance of default authentication queries made by connection poolers like pgbouncer.
- **Faster Kafka sink startup**: We've updated the default Kafka progress topic configuration to reduce the amount of progress data processed when creating new [Kafka sinks](/serve-results/sink/kafka/).
- **dbt strict mode**: We've introduced `strict_mode` to dbt-materialize, our dbt adapter. `strict_mode` enforces production-ready isolation rules and improves cluster health monitoring. It does so by validating source idempotency, schema isolation, cluster isolation and index restrictions.
- **SQL Server Always On HA failover support** (<red>*Materialize Self-Managed only*</red>): Materialize Self-Managed now offers better support for handling failovers, without downtime, in SQL Server Always On sources. [Contact our support team](/support/) to enable this in your environment.
- **Auto-repair accidental changes** (<red>*Materialize Self-Managed only*</red>): Improvements to the controller logic allow Materialize to auto-repair changes such as deleting a StatefulSet. This means that your production setups should be more robust in the face of accidental changes.
- **Track deployment status after upgrades** (<red>*Materialize Self-Managed only*</red>): The Materialize custom resource now displays both active and desired `environmentd` versions. This makes it easier to track deployment status after upgrades.

### Bug fixes
- Added additional checks to string functions (`replace`, `translate`, etc.) to help prevent out-of-memory errors from inflationary string operations.
- Fixed an issue which could cause panics during connection drops; this means improved stability when clients disconnect.
- Fixed an issue where disabling console or balancers would fail if they were already running.
- Fixed an issue where balancerd failed to upgrade and remained stuck on its pre-upgrade version.

## v26.4.0

*Released to Materialize Self-Managed: 2025-12-17* <br>
*Released to Materialize Cloud: 2025-12-18*

v26.4.0 introduces several performance improvements and bugfixes.

### Improvements
- **Over 2x higher connections per second (CPS)**: We've optimized how Materialize handles inbound connection requests. In our tests, we've observed 2x - 4x improvements to the rate at which new client connections can be established. This is especially beneficial when spinning up new environments, warming up connection pools, or scaling client instances.
- **Up to 3x faster hydration times for large PostgreSQL tables**: We've reduced the overhead incurred by communication between multiple *workers* on a large cluster. We've observed up to 3x throughput improvement when ingesting 1 TB PostgreSQL tables on large clusters.
- **More efficient source ingestion batching**: Sources now batch writes more effectively. This can result in improved freshness and lower resource utilization, especially when a source is doing a large number of writes.
- **CloudSQL HA failover support** (<red>*Materialize Self-Managed only*</red>): Materialize Self-Managed now offers better support for handling failovers in CloudSQL HA sources, without downtime. [Contact our support team](/support/) to enable this in your environment.
- **Manual Promotion** (<red>*Materialize Self-Managed only*</red>): [Rollout strategies](/self-managed-deployments/upgrading/#rollout-strategies) allow you control how Materialize transitions from the current generation to a new generation during an upgrade. We've added a new rollout strategy called `ManuallyPromote` which allows you to choose when to promote the new generation. This means that you can minimize the impact of potential downtime.

### Bug Fixes
- Fixed timestamp determination logic to handle empty read holds correctly.
- Fixed lazy creation of temporary schemas to prevent schema-related errors.
- Reduced SCRAM iterations in scalability framework and fixed fallback image configuration.

## v26.3.0

*Released to Materialize Cloud & Materialize Self-Managed: 2025-12-12*<br>

### Improvements
- For Self-Managed: added version upgrade window validation, to prevent skipping required intermediate versions during upgrades.
- Improved activity log throttling to apply across all statement executions, not just initial prepared statement execution, providing more consistent logging behavior.

### Bug Fixes
- Fixed validation for replica sizes to prevent configurations with zero scale or workers, which previously caused division-by-zero errors and panics.
- Fixed frontend `SELECT` sequencing to gracefully handle collections that are dropped during real-time recent timestamp determination.

## v26.2.0

*Released Cloud: 2025-12-05*<br>
*Released Self-Managed: 2025-12-09*

This release focuses primarily on bug fixes.

### Bug fixes
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
