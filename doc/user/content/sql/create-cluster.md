---
title: "CREATE CLUSTER"
description: "`CREATE CLUSTER` creates a logical cluster, which contains indexes."
pagerank: 40
menu:
  main:
    parent: commands
---

`CREATE CLUSTER` creates a logical [cluster](/get-started/key-concepts#clusters),
which contains dataflow-powered objects.
By default, each new environment contains cluster called `default` with a replication factor of `1`.

To switch your active cluster, use the `SET` command:

```sql
SET cluster = other_cluster;
```

## Conceptual framework

A cluster is a set of compute resources in Materialize, providing CPU, memory, and temporary storage.
Materialize uses a cluster for performing the following operations: 

- Execute SQL [SELECT](/sql/select/) statements that require compute resources.
- Maintaining [indexes](#indexes) and [materialized views](#materialized-views). 
- Running [sources](#sources), [sinks](#sinks), and [subscribes](/sql/subscribe/). 

{{< note >}}
A given cluster may contain any number of indexes and materialized views *or*
any number of sources and sinks, but not both types of objects. For example,
you may not create a cluster with a source and an index.

We plan to remove this restriction in a future version of Materialize.
{{< /note >}}

When running any of the above operations, you must specify which cluster you want to use.
Not explicitly naming a cluster uses your session's default cluster.


### Syntax

{{< diagram "create-managed-cluster.svg" >}}

#### `CLUSTER` options

{{% cluster-options %}}

## Details

### Cluster Size 

Size specifies the amount of compute resources available per cluster in a cluster.
Materialize supports the following cluster sizes:

| Size | Credits / Hour |
|------|----------------|
| 3xsmall | 0.25 |
| 2xsmall | 0.5 |
| xsmall | 1|
| small | 2 |
| medium | 4 |
| large | 8 |
| xlarge | 16 |
| 2xlarge | 32 |
| 3xlarge | 64 |
| 4xlarge | 128 |
| 5xlarge | 256 |
| 6xlarge | 512 |

The [`mz_internal.mz_cluster_replica_sizes`](/sql/system-catalog/mz_internal/#mz_cluster_replica_sizes) table lists the CPU, memory, and disk allocation for each cluster size.

{{< warning >}}
The values in the `mz_internal.mz_cluster_replica_sizes` may change at any time.
You should not rely on them for any kind of capacity planning.
{{< /warning >}}

### Replication Factor

{{< note >}}
Clusters containing sources and sinks can only have a replication factor of 0 or 1.

We plan to remove this restriction in a future version of Materialize.
{{< /note >}}

A clusters replication factor dictates the number of [cluster replicas](/get-started/key-concepts/#cluster-replicas) spawned for a cluster.
Cluster replicas are the physical counterpart to clusters and inherit the cluster's size and configurations.
Materialize ensures the replica set matches the declared size and replication factor.
The replicas of a cluster are visible in the system catalog but cannot be directly modified by users.

Each replica receives a copy of all data from sources its dataflows use and uses the data to perform identical computations.
This design provides Materialize with active replication, and so long as one replica is still reachable, the cluster continues making progress.

This also means that a cluster's dataflows contend for the same resources on each replica. 
For instance, instead of placing many complex materialized views on the same cluster, you choose another distribution or resize the cluster to provide you with more powerful machines.

### Availability zone assignment

Materialize guarantees the following when scheduling each replica when a cluster has a replication factor greater than 1.

- Different replicas are _never_ scheduled on the same node.
- Different replicas are _always_ spread evenly across availability
  zones. **Known limitation:** replicas with more than 1 process are excluded
  from this constraint. See [`mz_internal.mz_cluster_replica_sizes`](/sql/system-catalog/mz_internal/#mz_cluster_replica_sizes)
  to determine if that is the case for your replicas.

### Disk-attached clusters

{{< private-preview />}}

{{< warning >}}

**Pricing for this feature is likely to change.**
Disk-attached clusters currently consume credits at the same rate as
non-disk-attached clusters. In the future, disk-attached clusters will likely
consume credits at a faster rate.
{{< /warning >}}

The `DISK` option attaches a disk to the cluster.

Attaching a disk allows you to trade off performance for cost. A cluster of a
given size has access to several times more disk than memory, allowing the
processing of larger data sets at that replica size. Operations on a disk,
however, are much slower than operations in memory, and so a workload that
spills to disk will perform more slowly than a workload that does not. Note that
exact storage medium for the attached disk is not specified, and its performance
characteristics are subject to change.

Consider attaching a disk to clusters that contain sources that use the
[upsert envelope](/sql/create-source/#upsert-envelope) or the
[Debezium envelope](/sql/create-source/#debezium-envelope). When you place
these sources on a cluster with an attached disk, they will automatically spill
state to disk. These sources will therefore use less memory but may ingest
data more slowly. See [Sizing a source](/sql/create-source/#sizing-a-source) for details.

## Examples

### Basic

Create a cluster with two medium replicas:

```sql
CREATE CLUSTER c1 SIZE = 'medium', REPLICATION FACTOR = 2;
```

### Introspection disabled

Create a cluster with a single replica and introspection disabled:

```sql
CREATE CLUSTER c  SIZE = 'xsmall', INTROSPECTION INTERVAL = 0;
```

Disabling introspection can yield a small performance improvement, but you lose
the ability to run [troubleshooting queries](/ops/troubleshooting/) against
that cluster replica.

### Empty

Create a cluster with no replicas:

```sql
CREATE CLUSTER c1 SIZE 'xsmall', REPLICATION FACTOR = 0;
```

You can later add replicas to this cluster with [`ALTER CLUSTER`](/sql/alter-cluster/).

## Privileges

The privileges required to execute this statement are:

- `CREATECLUSTER` privileges on the system.

## See also

- [`ALTER CLUSTER`](/sql/alter-cluster/)
- [`DROP CLUSTER`](/sql/drop-cluster/)

[AWS availability zone IDs]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
