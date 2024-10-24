---
title: "ALTER CLUSTER"
description: "`ALTER CLUSTER` changes the configuration of a cluster."
menu:
  main:
    parent: 'commands'
---

`ALTER CLUSTER` changes the configuration of a cluster, such as the `SIZE` or
`REPLICATON FACTOR`.

{{< admonition type="Disambiguation" >}}

This page covers the usage of `ALTER CLUSTER` to change the cluster's
*configuration*. To change the name of a cluster, see [`ALTER ... RENAME
TO`](/sql/alter-rename/).

{{</ admonition >}}

## Syntax

`ALTER CLUSTER` has the following syntax forms:

- To set a cluster configuration:

  ```mzsql
  ALTER CLUSTER <cluster_name>
  SET (
    <config1> = <config_value1> [, ... ]  -- equal sign ('=') is optional
  )
  WITH ( <command_options> )               -- Optional.
  ;
  ```

- To reset a cluster configuration back to its default value:

  ```mzsql
  ALTER CLUSTER <cluster_name>
  RESET (
    <config1>[,  <config2>, ...]
  )
  WITH ( <command_options> )              -- Optional.
  ;

  ```

### Cluster Configurations

The following cluster configurations can be modified with `ALTER CLUSTER`:

{{% alter-cluster/alter-cluster-options %}}

### Command Options

Optionally, you can include a `WITH (<command option>)` to the [`ALTER CLUSTER`
statement](#syntax) to run with command options.

The following command options are
available:

| Command options (optional) | Value | Description |
|----------------------------|-------|-----------------|
| `WAIT UNTIL READY(...)`    |  | <em>Private preview for <a href="#graceful-cluster-resizing" >graceful cluster resizing.</a></em><br>{{< alter-cluster/alter-clusters-cmd-options >}} |
| `WAIT FOR` | `duration` | <em>Private preview for <a href="#graceful-cluster-resizing" >graceful cluster resizing.</a></em><br>A fixed duration to wait for the new replicas to be ready. This option can lead to downtime. As such, we recommend using the `WAIT UNTIL READY` option instead.|

## Considerations

### Resizing

#### Resource allocations

The resource allocations are proportional to the number in the size name. For
example, a cluster of size `600cc` has 2x as much CPU, memory, and disk as a
cluster of size `300cc`, and 1.5x as much CPU, memory, and disk as a cluster of
size `400cc`.

To determine the specific resource allocations for a size, query the
[`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
table.

{{< warning >}}
The values in the `mz_cluster_replica_sizes` table may change at any
time. You should not rely on them for any kind of capacity planning.
{{< /warning >}}

#### Downtime

Changing the cluster size requires the cluster to restart, incurring
**downtime**. For clusters that **do not contain sources or sinks**, you can
perform [graceful cluster resizing](#graceful-cluster-resizing) to avoid
incurring downtime during resizing.

#### Graceful cluster resizing

{{< private-preview />}}

Changing the size of a cluster using the `ALTER CLUSTER` command requires the
cluster to restart, which incurs **downtime**. For clusters that  **do not
contain sources or sinks**, you can use the `WAIT UNTIL READY` option to perform
a graceful resizing, which spins up an additional cluster replica under the
covers with the desired new size, waits for the replica to be hydrated, and then
replaces the original replica. This allows you to perform cluster resizing with
**no downtime**.

```sql
ALTER CLUSTER c1
SET (SIZE '100CC') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```

The `ALTER` statement is blocking and will return only when the new replica
becomes ready. This could take as long as the specified timeout. During this
operation, any other reconfiguration command issued against this cluster will
fail. Additionally, any connection interruption or statement cancelation will
cause a rollback — no size change will take effect in that case.

### Replication factor

The `REPLICATION FACTOR` option determines the number of replicas provisioned
for the cluster. Each replica of the cluster provisions a new pool of compute
resources to perform exactly the same computations on exactly the same data.

#### Replication factor and fault tolerance

Provisioning more than one replica improves **fault tolerance**. Clusters with
multiple replicas can tolerate failures of the underlying hardware that cause a
replica to become unreachable. As long as one replica of the cluster remains
available, the cluster can continue to maintain dataflows and serve queries.

{{< note >}}

Increasing the replication factor increases the cluster's **fault tolerance**;
it does **not** increase the cluster's work capacity. Replicas are exact copies
of one another: each replica must do exactly the same work (i.e., maintain the
same dataflows and process the same queries) as all the other replicas of the
cluster.

To increase a cluster's capacity, you should instead increase the cluster's
[size](#resizing).
{{< /note >}}

#### Guarantees

Materialize makes the following guarantees when provisioning replicas:

- Replicas of a given cluster are never provisioned on the same underlying
  hardware.
- Replicas of a given cluster are spread as evenly as possible across the
  underlying cloud provider's availability zones. However, when a cluster of
  size `3200cc` or larger uses multiple replicas, those replicas are not
  guaranteed to be spread evenly across the underlying cloud provider's
  availability zones.

Materialize automatically assigns names to replicas like `r1`, `r2`, etc. You
can view information about individual replicas in the console and the system
catalog, but you cannot directly modify individual replicas.

#### Replication factor `0`

You can pause a cluster's work by specifying a replication factor of `0`. Doing
so removes all replicas of the cluster. Any indexes, materialized views,
sources, and sinks on the cluster **will cease** to make progress, and any
queries directed to the cluster will block. You can later resume the cluster's
work by using [`ALTER CLUSTER`] to set a nonzero replication factor.

#### Clusters with sources and sinks

Clusters containing sources and sinks can only have a replication factor of `0`
or `1`.

### Required privileges

The privileges required to execute this statement are:

- Ownership of the cluster.

See also:

- [Access control](/manage/access-control)
- [Manage privileges](/manage/access-control/manage-privileges/)

## Examples

### Replication factor

The following example uses `ALTER CLUSTER` to update the `REPLICATION
FACTOR` of cluster `c1` to ``2``:

```mzsql
ALTER CLUSTER c1 SET (REPLICATION FACTOR 2);
```

Increasing the `REPLICATION FACTOR` increases the cluster's [fault
tolerance](#replication-factor-and-fault-tolerance), not its work capacity.

{{< note >}}
Clusters containing sources and sinks can only have a replication factor of `0`
or `1`.
{{</ note >}}

### Resizing

- For clusters **without any sources and sinks**, you can alter the cluster size
  with **no downtime** (i.e., [graceful cluster
  resizing](#graceful-cluster-resizing)) by running the `ALTER CLUSTER` command
  `WITH` the `WAIT UNTIL READY` [command option](#command-options):

  ```mzsql
  ALTER CLUSTER c1
  SET (SIZE '100CC') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
  ````

- For clusters **with sources or sinks,** you can alter the cluster size;
  however, this operation incurs **downtime**:

  ```mzsql
  ALTER CLUSTER c1 SET (SIZE '100cc');
  ```

### Schedule

{{< private-preview />}}

You can use `ALTER CLUSTER` to [automatically schedule when the cluster is
provisioned with compute resources)](../create-cluster/#scheduling) to align
with the [refresh schedule of your materialized
view(s)](/sql/create-materialized-view/#refresh-strategies). Scheduled clusters
only consume credits for the duration of the refreshes.

```sql
ALTER CLUSTER c1 SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'));
```

See the reference documentation for [`CREATE
CLUSTER`](../create-cluster/#scheduling) or [`CREATE MATERIALIZED
VIEW`](../create-materialized-view/#refresh-strategies) for more details on
scheduled clusters.

### Converting unmanaged to managed clusters

{{< warning >}}
[Unmanaged clusters](/sql/create-cluster-replica) are a deprecated feature of
Materialize that required manual management of cluster replicas.

We recommend converting any unmanaged clusters to managed clusters
by following the instructions below.
{{< /warning >}}

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

- [`ALTER ... RENAME`](/sql/alter-rename/)
- [`CREATE CLUSTER`](/sql/create-cluster/)
- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)
