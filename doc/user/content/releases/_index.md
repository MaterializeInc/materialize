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

- Starting with the v26.1.0 release, Materialize releases on a weekly schedule
for both Cloud and Self-Managed. See [Release schedule](/releases/schedule) for
details.

- New Terraform modules for Self-Managed are now GA. For details, see [Terraform
releases](#terraform-releases).

{{</ note >}}

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

## Terraform releases

New Terraform modules for Self-Managed are now GA.

{{< yaml-table data="self_managed/unified_terraform_versions" >}}

## See also

- [Release Schedule](/releases/schedule/)
