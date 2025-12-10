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

## v26.2.0
*Released Cloud: 2025-12-05*
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
v26.0](/installation/upgrade-to-swap/).


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
guides](/installation/). For existing deployments, you can configure your
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

For more information, see [`rolloutStrategy`](/installation/#rollout-strategies).

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

See also [General notes for upgrades](/installation/#general-notes-for-upgrades).


## See also

- [Release Schedule](/releases/schedule/)
