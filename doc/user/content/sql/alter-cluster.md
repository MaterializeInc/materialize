---
title: "ALTER CLUSTER"
description: "`ALTER CLUSTER` changes the configuration of a cluster."
menu:
  main:
    parent: 'commands'
---

Use `ALTER CLUSTER` to:

- Change configuration of a cluster, such as the `SIZE` or
`REPLICATON FACTOR`.
- Rename a cluster.
- Change owner of a cluster.

For completeness, the syntax for `SWAP WITH` operation is provided. However, in
general, you will not need to manually perform this operation.

## Syntax

`ALTER CLUSTER` has the following syntax variations:

{{< tabs >}}
{{< tab "Set a configuration" >}}

### Set a configuration

To set a cluster configuration:

{{% include-syntax file="examples/alter_cluster" example="syntax-set-configuration" %}}

{{< /tab >}}
{{< tab "Reset to default" >}}

### Reset to default

To reset a cluster configuration back to its default value:

{{% include-syntax file="examples/alter_cluster" example="syntax-reset-to-default" %}}

{{< /tab >}}
{{< tab "Rename" >}}

### Rename

To rename a cluster:

{{% include-syntax file="examples/alter_cluster" example="syntax-rename" %}}

{{< note >}}
You cannot rename system clusters, such as `mz_system` and `mz_catalog_server`.
{{< /note >}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a cluster:

{{% include-syntax file="examples/alter_cluster" example="syntax-change-owner" %}}

{{< /tab >}}
{{< tab "Swap with" >}}

### Swap with

{{< important >}}

Information about the `SWAP WITH` operation is provided for completeness.  The
`SWAP WITH` operation is used for blue/green deployments. In general, you will
not need to manually perform this operation.

{{< /important >}}

To swap the name of this cluster with another cluster:

{{% include-syntax file="examples/alter_cluster" example="syntax-swap-with" %}}

{{< /tab >}}
{{< /tabs >}}

## Considerations

### Resizing

{{< tip >}}

For help sizing your clusters, navigate to **Materialize Console >**
[**Monitoring**](/console/monitoring/)>**Environment Overview**. This page
displays cluster resource utilization and sizing advice.

{{< /tip >}}

#### Available sizes

{{< tabs >}}
{{< tab "cc Clusters" >}}

Valid cc cluster sizes are:

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

Resource allocations are proportional to the number in the size name. For
example, a cluster of size `600cc` has 2x as much CPU, memory, and disk as a
cluster of size `300cc`, and 1.5x as much CPU, memory, and disk as a cluster of
size `400cc`.

Clusters of larger sizes can process data faster and handle larger data volumes.
{{< /tab >}}
{{< tab "M.1 Clusters" >}}

{{< note >}}
M.1 sizes provide access to additional disk capacity compared to
equivalently-priced cc sizes, which can be beneficial for disk-intensive
workloads. However, cc sizes offer better compute performance per credit for
most workloads. We recommend using cc sizes unless your workload specifically
requires the additional disk capacity that M.1 sizes provide.
{{< /note >}}

{{< include-md file="shared-content/cluster-size-disclaimer.md" >}}

{{< yaml-table data="m1_cluster_sizing" >}}

{{< /tab >}}
{{< /tabs >}}

See also:

- [cc to M.1 size mapping](/sql/m1-cc-mapping/).

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog:Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).

#### Resource allocation

To determine the specific resource allocation for a given cluster size, query
the [`mz_cluster_replica_sizes`](/reference/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table.

{{< warning >}}
The values in the `mz_cluster_replica_sizes` table may change at any
time. You should not rely on them for any kind of capacity planning.
{{< /warning >}}

#### Downtime

{{< if-unreleased "v26.34" >}}
Resizing operation can incur downtime unless used with WAIT UNTIL READY option.
See [zero-downtime cluster resizing](#zero-downtime-cluster-resizing) for
details.
{{< /if-unreleased >}}

{{< if-released "v26.34" >}}
By default, a bare `ALTER CLUSTER <name> SET (SIZE = ...)` resizes the cluster
**gracefully and without downtime**. The command returns immediately and the
resize proceeds in the background. See [Zero-downtime cluster
resizing](#zero-downtime-cluster-resizing) and [Monitoring a
resize](#monitoring-a-resize) for details.

{{< note >}}
Graceful, no-downtime resizing is the default as of **v26.34**. In earlier
versions, a bare `ALTER CLUSTER ... SET (SIZE = ...)` could incur downtime, and
zero-downtime resizing required the `WAIT UNTIL READY` option.
{{< /note >}}
{{< /if-released >}}

#### Zero-downtime cluster resizing

{{< if-unreleased "v26.34" >}}
{{< private-preview />}}

You can use the `WAIT UNTIL READY` option to perform a zero-downtime resizing,
which incurs **no downtime**. Instead of restarting the cluster, this approach
spins up an additional cluster replica under the covers with the desired new
size, waits for the replica to be hydrated, and then replaces the original
replica.

```mzsql
ALTER CLUSTER c1
SET (SIZE '100cc') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```

The `ALTER` statement is blocking and will return only when the new replica
becomes ready. This could take as long as the specified timeout. During this
operation, any other reconfiguration command issued against this cluster will
fail. Additionally, any connection interruption or statement cancelation will
cause a rollback, and no size change will take effect in that case.

{{% include-headless "/headless/alter-cluster-wait-until-ready-note" %}}
{{< /if-unreleased >}}

{{< if-released "v26.34" >}}
By default, resizing a cluster with `ALTER CLUSTER <name> SET (SIZE = ...)`
incurs **no downtime**. Rather than restarting the cluster in place, Materialize
provisions new replicas at the target size alongside the existing ones, waits
for them to [hydrate](/concepts/clusters/#consider-hydration-requirements), then
retires the old replicas. The command returns immediately and the resize
proceeds in the background.

```mzsql
ALTER CLUSTER c1 SET (SIZE = '100cc');
```

If the new replicas do not hydrate within the reconfiguration timeout (24 hours
by default), Materialize rolls back the resize and the cluster keeps its current
size. See [Monitoring a resize](#monitoring-a-resize) to track progress and
[cancel](#monitoring-a-resize) an in-flight resize.

To customize the timeout and what happens when it expires, use the `WITH`
options. The resize still proceeds in the background, these options only
configure it:

{{< private-preview >}}
Customizing the resize timeout with `WAIT UNTIL READY` or `WAIT FOR`
{{< /private-preview >}}

- `WAIT UNTIL READY (TIMEOUT = ..., ON TIMEOUT = ...)` sets the timeout for the
  resize. On timeout, `ON TIMEOUT` selects whether to `COMMIT` (retire the old
  replicas and proceed with the not-yet-hydrated new ones, which can cause
  downtime) or `ROLLBACK` (keep the current size). Default: `ROLLBACK`.

  ```mzsql
  ALTER CLUSTER c1
  SET (SIZE = '100cc') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'ROLLBACK'));
  ```

- `WAIT FOR '<duration>'` sets the timeout and commits when it expires,
  regardless of hydration status, which can cause downtime. Prefer
  `WAIT UNTIL READY`.
{{< /if-released >}}

{{< if-released "v26.34" >}}
#### Speed up hydration by autoscaling to a larger size

{{< include-md file="shared-content/cluster-hydration-burst.md" >}}
{{< /if-released >}}

{{< if-released "v26.34" >}}
#### Monitoring a resize

During a graceful resize, Materialize:

1. Provisions new replicas at the target size, alongside the current replicas.
2. Waits for the new replicas to
   [hydrate](/concepts/clusters/#consider-hydration-requirements).
3. Retires the old replicas.

Throughout, the cluster keeps serving queries, first from the old replicas,
then from both sets as the new replicas come up, so the resize incurs no
downtime.

To **cancel** an in-flight resize, reissue `ALTER CLUSTER` with the cluster's
current size. Materialize drops the pending replicas and keeps the current
configuration.

You can monitor a resize through the following:

- The `activity` column of [`SHOW CLUSTERS`](/sql/show-clusters/), which
  summarizes any in-flight reconfiguration or hydration burst, and is `NULL`
  when the cluster is steady.

- [`mz_internal.mz_cluster_reconfigurations`](/reference/system-catalog/mz_internal/#mz_cluster_reconfigurations),
  which shows the target shape, deadline, timeout action, and lifecycle status
  of the latest reconfiguration.

- [`mz_internal.mz_cluster_auto_scaling_strategies`](/reference/system-catalog/mz_internal/#mz_cluster_auto_scaling_strategies),
  which shows any in-flight hydration burst.

- [`mz_internal.mz_hydration_statuses`](/reference/system-catalog/mz_internal/#mz_hydration_statuses),
  which shows per-object hydration status.

- The audit log
  ([`mz_catalog.mz_audit_events`](/reference/system-catalog/mz_catalog/#mz_audit_events)),
  which records each reconfiguration transition.
{{< /if-released >}}

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

{{< note >}}

- Each replica incurs cost, calculated as `cluster size *
  replication factor` per second. See [Usage &
  billing](/administration/billing/) for more details.

- Increasing the replication factor does **not** increase the cluster's work
  capacity. Replicas are exact copies of one another: each replica must do
  exactly the same work (i.e., maintain the same dataflows and process the same
  queries) as all the other replicas of the cluster.

  To increase the capacity of a cluster, you must increase its
  [size](#resizing).

{{< /note >}}

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

{{% include-headless "/headless/sql-command-privileges/alter-cluster" %}}

See also:

- [Access control (Materialize Cloud)](/security/cloud/access-control/)
- [Access control (Materialize
  Self-Managed)](/security/self-managed/access-control/)

### Rename restrictions

You cannot rename system clusters, such as `mz_system` and `mz_catalog_server`.


## Examples

### Replication factor

The following example uses `ALTER CLUSTER` to update the `REPLICATION
FACTOR` of cluster `c1` to ``2``:

```mzsql
ALTER CLUSTER c1 SET (REPLICATION FACTOR 2);
```

Increasing the `REPLICATION FACTOR` increases the cluster's [fault
tolerance](#replication-factor-and-fault-tolerance), not its work capacity.


### Resizing

{{< if-unreleased "v26.34" >}}
You can alter the cluster size with **no downtime** (i.e., [zero-downtime
cluster resizing](#zero-downtime-cluster-resizing)) by running the `ALTER
CLUSTER` command with the `WAIT UNTIL READY` [option](#syntax):

```mzsql
ALTER CLUSTER c1
SET (SIZE '100cc') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```

{{% include-headless "/headless/alter-cluster-wait-until-ready-note" %}}

Alternatively, you can alter the cluster size immediately, without waiting, by
running the `ALTER CLUSTER` command:

```mzsql
ALTER CLUSTER c1 SET (SIZE '100cc');
```

This will incur downtime when the cluster contains objects that need
re-hydration before they are ready. This includes indexes, materialized views,
and some types of sources.
{{< /if-unreleased >}}

{{< if-released "v26.34" >}}
By default, altering the cluster size is graceful and incurs **no downtime**.
The command returns immediately and the resize proceeds in the background. See
[Zero-downtime cluster resizing](#zero-downtime-cluster-resizing) and
[Monitoring a resize](#monitoring-a-resize).

```mzsql
ALTER CLUSTER c1 SET (SIZE = '100cc');
```

To customize the timeout and what happens when it expires, use the `WAIT UNTIL
READY` [option](#syntax):

```mzsql
ALTER CLUSTER c1
SET (SIZE = '100cc') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'ROLLBACK'));
```
{{< /if-released >}}

{{< if-released "v26.34" >}}
### Configure autoscaling

To [speed up hydration](#speed-up-hydration-by-autoscaling-to-a-larger-size),
configure an autoscaling strategy that bursts to a larger size while the cluster
has un-hydrated objects:

```mzsql
ALTER CLUSTER c1 SET (
    AUTO SCALING STRATEGY = (
        ON HYDRATION (HYDRATION SIZE = '800cc', LINGER DURATION = '15s')
    )
);
```

To remove the strategy:

```mzsql
ALTER CLUSTER c1 RESET (AUTO SCALING STRATEGY);
```

To inspect the configured strategy and any in-flight burst, query
[`mz_internal.mz_cluster_auto_scaling_strategies`](/reference/system-catalog/mz_internal/#mz_cluster_auto_scaling_strategies).
The `strategy` column holds the configured policy, and the `state` column holds
the in-flight burst, or `NULL` when no burst is running:

```mzsql
SELECT
    c.name AS cluster,
    s.strategy->'on_hydration'->>'hydration_size' AS hydration_size,
    s.state->'burst'->>'burst_size' AS bursting_at
FROM mz_internal.mz_cluster_auto_scaling_strategies AS s
JOIN mz_clusters AS c ON c.id = s.cluster_id;
```

```nofmt
 cluster | hydration_size | bursting_at
---------+----------------+-------------
 c1      | 800cc          |
```

Here, `c1` is configured to burst to `800cc` and no burst is currently running
(`bursting_at` is `NULL`). While a burst is in flight, `bursting_at` reports the
burst replica's size.
{{< /if-released >}}

### Converting unmanaged to managed clusters

{{< note >}}

When getting started with Materialize, we recommend using managed clusters. You
can convert any unmanaged clusters to managed clusters by following the
instructions below.

{{< /note >}}

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
- [`SHOW CLUSTERS`](/sql/show-clusters/)
- [`DROP CLUSTER`](/sql/drop-cluster/)
