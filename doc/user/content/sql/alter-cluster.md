---
title: "ALTER CLUSTER"
description: "`ALTER CLUSTER` changes the configuration of a cluster."
menu:
  main:
    parent: 'commands'
---

`ALTER CLUSTER` changes the configuration of a cluster, such as the `SIZE` or
`REPLICATON FACTOR`.

## Syntax

`ALTER CLUSTER` has the following syntax variations:

{{< tabs >}}
{{< tab "Set a configuration" >}}

To set a cluster configuration:

  ```mzsql
  ALTER CLUSTER <cluster_name>
  SET (
      SIZE = <text>,
      REPLICATION FACTOR = <int>,
      INTROSPECTION INTERVAL = <interval>,
      INTROSPECTION DEBUGGING = <bool>,
      MANAGED = <bool>,
      SCHEDULE = { MANUAL | ON REFRESH (...) }
  )
  [WITH ({ WAIT UNTIL READY({TIMEOUT | ON TIMEOUT {COMMIT|ROLLBACK}}) | WAIT FOR <duration> })]
  ;
  ```

{{< /tab >}}
{{< tab "Reset to default" >}}

To reset a cluster configuration back to its default value:

  ```mzsql
  ALTER CLUSTER <cluster_name>
  RESET (
      REPLICATION FACTOR,
      INTROSPECTION INTERVAL,
      INTROSPECTION DEBUGGING,
      MANAGED,
      SCHEDULE
  )
  ;
  ```

{{< /tab >}}
{{< tab "Rename cluster" >}}

To rename a cluster:

  ```mzsql
  ALTER CLUSTER <cluster_name> RENAME TO <new_cluster_name>;
  ```

{{< /tab >}}
{{< tab "Change owner to" >}}

To change the owner of a cluster:

  ```mzsql
  ALTER CLUSTER <cluster_name> OWNER TO <new_owner_role>;
  ```

To rename a cluster, you must have ownership of the cluster and membership in
the `<new_owner_role>`. See also [Required privileges](#required-privileges).

{{< /tab >}}
{{< tab "Swap names with" >}}

{{< important >}}

Information about the `SWAP WITH` operation is provided for completeness.  The
`SWAP WITH` operation is used for blue/green deployments. In general, you will
not need to manually perform this operation.

{{< /important >}}

To swap the name of this cluster with another cluster:

  ```mzsql
  ALTER CLUSTER <cluster1> SWAP WITH <cluster2>;
  ```

{{< /tab >}}
{{< /tabs >}}

### Cluster configuration

{{% alter-cluster/alter-cluster-options %}}

### `WITH` options


| Command options (optional) | Value | Description |
|----------------------------|-------|-----------------|
| `WAIT UNTIL READY(...)`    |  | ***Private preview.** This option has known performance or stability issues and is under active development.* {{< alter-cluster/alter-clusters-cmd-options >}} |
| `WAIT FOR` | [`interval`](/sql/types/interval/) | ***Private preview.** This option has known performance or stability issues and is under active development.* A fixed duration to wait for the new replicas to be ready. This option can lead to downtime. As such, we recommend using the `WAIT UNTIL READY` option instead.|

## Considerations

### Resizing

{{< tip >}}

For help sizing your clusters, navigate to **Materialize Console >**
[**Monitoring**](/console/monitoring/)>**Environment Overview**. This page
displays cluster resource utilization and sizing advice.

{{< /tip >}}

#### Resource allocation

The allocation of resources increases proportionally to the size of the cluster.
For example, a cluster of size `600cc` has 2x as much CPU, memory, and disk as
a cluster of size `300cc`, and 1.5x as much CPU, memory, and disk as a cluster
of size `400cc`.

To determine the specific resource allocation for a given cluster size, query
the [`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table.

{{< warning >}}
The values in the `mz_cluster_replica_sizes` system catalog table may change at
any time. You should not rely on them for any kind of capacity planning.
{{< /warning >}}

#### Downtime

Depending on the type of objects in a cluster, a resizing operation might incur
**downtime**.

* For clusters that contain sources and/or sinks, resizing requires the cluster
  to **restart**. This operation incurs downtime for the duration it takes for
  all objects in the cluster to hydrate.

* For clusters that **do not contain sources or sinks**, it's possible to avoid
  downtime by performing a [graceful cluster resizing](#graceful-cluster-resizing).

#### Graceful cluster resizing

{{< private-preview />}}

For clusters that do not contain sources or sinks, you can use the `WAIT UNTIL
READY` option to perform a graceful resizing, which incurs **no downtime**.
Instead of restarting the cluster, this approach spins up an additional cluster
replica under the covers with the desired new size, waits for the replica to be
hydrated, and then replaces the original replica.

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
Each replica incurs cost, calculated as `cluster
[size](#resizing) * replication factor` per second. See [Usage &
billing](/administration/billing/) for more details.

#### Replication factor and fault tolerance

Provisioning more than one replica provides **fault tolerance**. Clusters with
multiple replicas can tolerate failures of the underlying hardware that cause a
replica to become unreachable. As long as one replica of the cluster remains
available, the cluster can continue to maintain dataflows and serve queries.

{{< note >}}

- Each replica incurs cost, calculated as `cluster [size](#resizing)
  * replication factor` per second. See [Usage & billing](/administration/billing/)
  for more details.

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

#### Clusters with sources and sinks

Clusters containing sources and sinks can only have a replication factor of `0`
or `1`.

## Required privileges

To execute the `ALTER CLUSTER` command, you need:

- Ownership of the cluster.

In addition,

- To rename a cluster, you must also have membership in the `<new_owner_role>`.

- To swap names with another cluster, you must also have ownership of the other
  cluster.

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

- For clusters **without any sources or sinks**, you can alter the cluster size
  with **no downtime** (i.e., [graceful cluster
  resizing](#graceful-cluster-resizing)) by running the `ALTER CLUSTER` command
  with the `WAIT UNTIL READY` [option](#with-options):

  ```mzsql
  ALTER CLUSTER c1
  SET (SIZE '100CC') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
  ````

- For clusters **with sources or sinks**, it's not yet possible to perform
  graceful cluster resizing. This means that resizing clusters with sources or
  sinks requires a cluster **restart**, which incurs **downtime**. You can
  alter the cluster size by running the `ALTER CLUSTER` command:

  ```mzsql
  ALTER CLUSTER c1 SET (SIZE '100cc');
  ```

### Schedule

{{< private-preview />}}

For use cases that require using [scheduled clusters](/sql/create-cluster/#scheduling),
you can set or change the originally configured schedule and related options
using the `ALTER CLUSTER` command.
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
