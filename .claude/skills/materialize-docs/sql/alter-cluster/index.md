---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-cluster/
complexity: advanced
description: '`ALTER CLUSTER` changes the configuration of a cluster.'
doc_type: reference
keywords:
- Private preview.
- ALTER CLUSTER
- 'Tip:'
- 'Note:'
- 'Important:'
product_area: Indexes
status: experimental
title: ALTER CLUSTER
---

# ALTER CLUSTER

## Purpose
`ALTER CLUSTER` changes the configuration of a cluster.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER CLUSTER` changes the configuration of a cluster.


Use `ALTER CLUSTER` to:

- Change configuration of a cluster, such as the `SIZE` or
`REPLICATON FACTOR`.
- Rename a cluster.
- Change owner of a cluster.

For completeness, the syntax for `SWAP WITH` operation is provided. However, in
general, you will not need to manually perform this operation.

## Syntax

`ALTER CLUSTER` has the following syntax variations:

#### Set a configuration

### Set a configuration

To set a cluster configuration:

<!-- Syntax example: examples/alter_cluster / syntax-set-configuration -->

#### Reset to default

### Reset to default

To reset a cluster configuration back to its default value:

<!-- Syntax example: examples/alter_cluster / syntax-reset-to-default -->

#### Rename

### Rename

To rename a cluster:

<!-- Syntax example: examples/alter_cluster / syntax-rename -->

> **Note:** 
You cannot rename system clusters, such as `mz_system` and `mz_catalog_server`.


#### Change owner

### Change owner

To change the owner of a cluster:

<!-- Syntax example: examples/alter_cluster / syntax-change-owner -->

#### Swap with

### Swap with

> **Important:** 

Information about the `SWAP WITH` operation is provided for completeness.  The
`SWAP WITH` operation is used for blue/green deployments. In general, you will
not need to manually perform this operation.


To swap the name of this cluster with another cluster:

<!-- Syntax example: examples/alter_cluster / syntax-swap-with -->

### Cluster configuration

<!-- Unresolved shortcode: {{% yaml-table data="syntax_options/alter_cluster_... -->

### `WITH` options


| Command options (optional) | Value | Description |
|----------------------------|-------|-----------------|
| `WAIT UNTIL READY(...)`    |  | ***Private preview.** This option has known performance or stability issues and is under active development.*  |
| `WAIT FOR` | [`interval`](/sql/types/interval/) | ***Private preview.** This option has known performance or stability issues and is under active development.* A fixed duration to wait for the new replicas to be ready. This option can lead to downtime. As such, we recommend using the `WAIT UNTIL READY` option instead.|

## Considerations

This section covers considerations.

### Resizing

> **Tip:** 

For help sizing your clusters, navigate to **Materialize Console >**
[**Monitoring**](/console/monitoring/)>**Environment Overview**. This page
displays cluster resource utilization and sizing advice.


#### Available sizes

#### M.1 Clusters

> **Note:** 
The values set forth in the table are solely for illustrative purposes.
Materialize reserves the right to change the capacity at any time. As such, you
acknowledge and agree that those values in this table may change at any time,
and you should not rely on these values for any capacity planning.


<!-- Dynamic table: m1_cluster_sizing - see original docs -->

#### Legacy cc Clusters

> **Tip:** 
In most cases, you **should not** use legacy sizes. [M.1 sizes](#available-sizes)
offer better performance per credit for nearly all workloads. We recommend using
M.1 sizes for all new clusters, and recommend migrating existing
legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
customers during the transition period as we move to deprecate legacy sizes.

The legacy size information is provided for completeness.


Valid legacy cc cluster sizes are:

* `25cc`
* `50cc`
* `100cc`
* `200cc`
* `300cc`
* `400cc`
* `600cc`
* `800cc`
* `1200cc`
* `1600cc`
* `3200cc`
* `6400cc`
* `128C`
* `256C`
* `512C`

For clusters using legacy cc sizes, resource allocations are proportional to the
number in the size name. For example, a cluster of size `600cc` has 2x as much
CPU, memory, and disk as a cluster of size `300cc`, and 1.5x as much CPU,
memory, and disk as a cluster of size `400cc`.

Clusters of larger sizes can process data faster and handle larger data volumes.

See also:

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog:Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).

#### Resource allocation

To determine the specific resource allocation for a given cluster size, query
the [`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table.

> **Warning:** 
The values in the `mz_cluster_replica_sizes` table may change at any
time. You should not rely on them for any kind of capacity planning.


#### Downtime

Resizing operation can incur downtime unless used with WAIT UNTIL READY option.
See [zero-downtime cluster resizing](#zero-downtime-cluster-resizing) for
details.

#### Zero-downtime cluster resizing

> **Private Preview:** This feature is in private preview.

You can use the `WAIT UNTIL READY` option to perform a zero-downtime resizing,
which incurs **no downtime**. Instead of restarting the cluster, this approach
spins up an additional cluster replica under the covers with the desired new
size, waits for the replica to be hydrated, and then replaces the original
replica.

```sql
ALTER CLUSTER c1
SET (SIZE 'M.1-xsmall') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```text

The `ALTER` statement is blocking and will return only when the new replica
becomes ready. This could take as long as the specified timeout. During this
operation, any other reconfiguration command issued against this cluster will
fail. Additionally, any connection interruption or statement cancelation will
cause a rollback — no size change will take effect in that case.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- Unresolved shortcode: > **Note:**  --> --> -->

Using `WAIT UNTIL READY` requires that the session remain open: you need to
make sure the Console tab remains open or that your `psql` connection remains
stable.

Any interruption will cause a cancellation, no cluster changes will take
effect.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- Unresolved shortcode:  --> --> -->


### Replication factor

The `REPLICATION FACTOR` option determines the number of replicas provisioned
for the cluster. Each replica of the cluster provisions a new pool of compute
resources to perform exactly the same computations on exactly the same data.
Each replica incurs cost, calculated as `cluster size * replication factor` per
second. See [Usage & billing](/administration/billing/) for more details.

#### Replication factor and fault tolerance

Provisioning more than one replica provides **fault tolerance**. Clusters with
multiple replicas can tolerate failures of the underlying hardware that cause a
replica to become unreachable. As long as one replica of the cluster remains
available, the cluster can continue to maintain dataflows and serve queries.

> **Note:** 

- Each replica incurs cost, calculated as `cluster size *
  replication factor` per second. See [Usage &
  billing](/administration/billing/) for more details.

- Increasing the replication factor does **not** increase the cluster's work
  capacity. Replicas are exact copies of one another: each replica must do
  exactly the same work (i.e., maintain the same dataflows and process the same
  queries) as all the other replicas of the cluster.

  To increase the capacity of a cluster, you must increase its
  [size](#resizing).


Materialize automatically assigns names to replicas (e.g., `r1`, `r2`). You can
view information about individual replicas in the Materialize console and the system
catalog.

#### Availability guarantees

When provisioning replicas,

- For clusters sized **under `3200cc`**, Materialize guarantees that all
  provisioned replicas in a cluster are spread across the underlying cloud
  provider's availability zones.

- For clusters sized at **`3200cc` and above**, even distribution of replicas
  across availability zones **cannot** be guaranteed.


## Required privileges

To execute the `ALTER CLUSTER` command, you need:

- Ownership of the cluster.

- To rename a cluster, you must also have membership in the `<new_owner_role>`.

- To swap names with another cluster, you must also have ownership of the other
  cluster.


See also:

- [Access control (Materialize Cloud)](/security/cloud/access-control/)
- [Access control (Materialize
  Self-Managed)](/security/self-managed/access-control/)

### Rename restrictions

You cannot rename system clusters, such as `mz_system` and `mz_catalog_server`.


## Examples

This section covers examples.

### Replication factor

The following example uses `ALTER CLUSTER` to update the `REPLICATION
FACTOR` of cluster `c1` to ``2``:

```mzsql
ALTER CLUSTER c1 SET (REPLICATION FACTOR 2);
```text

Increasing the `REPLICATION FACTOR` increases the cluster's [fault
tolerance](#replication-factor-and-fault-tolerance), not its work capacity.


### Resizing

You can alter the cluster size with **no downtime** (i.e., [zero-downtime
cluster resizing](#zero-downtime-cluster-resizing)) by running the `ALTER
CLUSTER` command with the `WAIT UNTIL READY` [option](#with-options):

```mzsql
ALTER CLUSTER c1
SET (SIZE 'M.1-xsmall') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```text

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- Unresolved shortcode: > **Note:**  --> --> -->

Using `WAIT UNTIL READY` requires that the session remain open: you need to
make sure the Console tab remains open or that your `psql` connection remains
stable.

Any interruption will cause a cancellation, no cluster changes will take
effect.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- Unresolved shortcode:  --> --> -->


Alternatively, you can alter the cluster size immediately, without waiting, by
running the `ALTER CLUSTER` command:

```mzsql
ALTER CLUSTER c1 SET (SIZE 'M.1-xsmall');
```text

This will incur downtime when the cluster contains objects that need
re-hydration before they are ready. This includes indexes, materialized views,
and some types of sources.

### Schedule

> **Private Preview:** This feature is in private preview.

For use cases that require using [scheduled clusters](/sql/create-cluster/#scheduling),
you can set or change the originally configured schedule and related options
using the `ALTER CLUSTER` command.
```sql
ALTER CLUSTER c1 SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'));
```text

See the reference documentation for [`CREATE
CLUSTER`](../create-cluster/#scheduling) or [`CREATE MATERIALIZED
VIEW`](../create-materialized-view/#refresh-strategies) for more details on
scheduled clusters.

### Converting unmanaged to managed clusters

> **Note:** 

When getting started with Materialize, we recommend using managed clusters. You
can convert any unmanaged clusters to managed clusters by following the
instructions below.


Alter the `managed` status of a cluster to managed:

```mzsql
ALTER CLUSTER c1 SET (MANAGED);
```

Materialize permits converting an unmanged cluster to a managed cluster if
the following conditions are met:

* The cluster replica names are `r1`, `r2`, ..., `rN`.
* All replicas have the same size.
* If there are no replicas, `SIZE` needs to be specified.
* If specified, the replication factor must match the number of replicas.

Note that the cluster will not have settings for the availability zones, and
compute-specific settings. If needed, these can be set explicitly.

## See also

- [`CREATE CLUSTER`](/sql/create-cluster/)
- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)