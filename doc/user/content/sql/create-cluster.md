---
title: "CREATE CLUSTER"
description: "`CREATE CLUSTER` creates a new cluster."
pagerank: 40
menu:
  main:
    parent: commands
---

`CREATE CLUSTER` creates a new [cluster](/get-started/key-concepts#clusters).

## Conceptual framework

A cluster is a pool of compute resources (CPU, memory, and,
optionally, scratch disk space) for running your workloads.

The following operations require compute resources in Materialize, and so need
to be associated with a cluster:

- Executing [`SELECT`] and [`SUBSCRIBE`] statements.
- Maintaining [indexes](/get-started/key-concepts#indexes) and [materialized views](/get-started/key-concepts#materialized-views).
- Maintaining [sources](/get-started/key-concepts#sources) and [sinks](/get-started/key-concepts#sinks).

## Syntax

{{< diagram "create-managed-cluster.svg" >}}

### Options

{{% cluster-options %}}

## Details

### Initial state

Each Materialize region initially contains a [pre-installed cluster](/sql/show-clusters/#pre-installed-clusters)
named `quickstart` with a size of `xsmall` and a replication factor of `1`. You
can drop or alter this cluster to suit your needs.

### Choosing a cluster

When performing an operation that requires a cluster, you must specify which
cluster you want to use. Not explicitly naming a cluster uses your session's
active cluster.

To show your session's active cluster, use the [`SHOW`](/sql/show) command:

```sql
SHOW cluster;
```

To switch your session's active cluster, use the [`SET`](/sql/set) command:

```sql
SET cluster = other_cluster;
```

### Resource isolation

Clusters provide **resource isolation.** Each cluster provisions a dedicated
pool of CPU, memory, and, optionally, scratch disk space.

All workloads on a given cluster will compete for access to these compute
resources. However, workloads on different clusters are strictly isolated from
one another. A given workload has access only to the CPU, memory, and scratch
disk of the cluster that it is running on.

Clusters are commonly used to isolate different classes of workloads. For
example, you could place your development workloads in a cluster named
`dev` and your production workloads in a cluster named `prod`.

### Size

The `SIZE` option determines the amount of compute resources (CPU and memory)
available to the cluster. Valid sizes are:

* `3xsmall`
* `2xsmall`
* `xsmall`
* `small`
* `medium`
* `large`
* `xlarge`
* `2xlarge`
* `3xlarge`
* `4xlarge`
* `5xlarge`
* `6xlarge`

Clusters of larger sizes can process data faster and handle larger data volumes.
You can use [`ALTER CLUSTER`] to resize the cluster in order to respond to
changes in the resource requirements of your workload.

The resource allocations for a given size are twice the resource allocations of
the previous size. To determine the specific resource allocations for a size,
query the [`mz_internal.mz_cluster_replica_sizes`] table.

{{< warning >}}
The values in the `mz_internal.mz_cluster_replica_sizes` table may change at any
time. You should not rely on them for any kind of capacity planning.
{{< /warning >}}

#### Disk-enabled sizes

{{< private-preview />}}

Disk-enabled sizes allow clusters to spill data to a scratch disk when a the
data does not fit in memory.

Using a disk-enabled size allows you to trade off performance for cost. A
cluster of a given disk-enabled size can process data sets that are larger than
would be possible with access to memory alone. Operations on a disk, however,
are slower than operations in memory, and so a workload that spills to disk will
perform more slowly than a workload that does not. Note that exact storage
medium for the attached disk is not specified, and its performance
characteristics are subject to change.

The following cluster sizes have disk attached:

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

The resource allocations are proportional to the number in the size name. For
example, a cluster of size `600cc` has 2x as much CPU, memory, and disk as a
cluster of size `300cc`, and 1.5x as much CPU, memory, and disk as a cluster of
size `400cc`.

The correspondence between disk-enabled sizes and traditional sizes is shown
in the [credit usage table](#credit-usage).

### Replication factor

The `REPLICATION FACTOR` option determines the number of replicas provisioned
for the cluster. Each replica of the cluster provisions a new pool of compute
resources to perform exactly the same computations on exactly the same data.

Provisioning more than one replica improves **fault tolerance**. Clusters with
multiple replicas can tolerate failures of the underlying hardware that cause a
replica to become unreachable. As long as one replica of the cluster remains
available, the cluster can continue to maintain dataflows and serve queries.

Materialize makes the following guarantees when provisioning replicas:

- Replicas of a given cluster are never provisioned on the same underlying
  hardware.
- Replicas of a given cluster are spread as evenly as possible across the
  underlying cloud provider's availability zones.

Materialize automatically assigns names to replicas like `r1`, `r2`, etc. You
can view information about individual replicas in the console and the system
catalog, but you cannot directly modify individual replicas.

You can pause a cluster's work by specifying a replication factor of `0`. Doing
so removes all replicas of the cluster. Any indexes, materialized views,
sources, and sinks on the cluster will cease to make progress, and any queries
directed to the cluster will block. You can later resume the cluster's work by
using [`ALTER CLUSTER`] to set a nonzero replication factor.

{{< note >}}
A common misconception is that increasing a cluster's replication
factor will increase its capacity for work. This is not the case. Increasing
the replication factor increases the **fault tolerance** of the cluster, not its
capacity for work. Replicas are exact copies of one another: each replica must
do exactly the same work (i.e., maintain the same dataflows and process the same
queries) as all the other replicas of the cluster.

To increase a cluster's capacity, you should instead increase the cluster's
[size](#size).
{{< /note >}}

### Credit usage

Each [replica](#replication-factor) of the cluster consumes credits at a rate
determined by the cluster's size:

Size      | Disk-enabled size  | Credits per replica per hour
----------|--------------------|---------
`3xsmall` | `25cc`             | 0.25
`2xsmall` | `50cc`             | 0.5
`xsmall`  | `100cc`            | 1
`small`   | `200cc`            | 2
&nbsp;    | `300cc`            | 3
`medium`  | `400cc`            | 4
&nbsp;    | `600cc`            | 6
`large`   | `800cc`            | 8
&nbsp;    | `1200cc`           | 12
`xlarge`  | `1600cc`           | 16
`2xlarge` | `3200cc`           | 32
`3xlarge` | `6400cc`           | 64
`4xlarge` | `128C`             | 128
`5xlarge` | `256C`             | 256
`6xlarge` | `512C`             | 512

Credit usage is measured at a one second granularity. For a given replica,
credit usage begins when a `CREATE CLUSTER` or [`ALTER CLUSTER`] statement
provisions the replica and ends when an [`ALTER CLUSTER`] or [`DROP CLUSTER`]
statement deprovisions the replica.

A cluster with a [replication factor](#replication-factor) of zero uses no
credits.

As an example, consider the following sequence of events:

Time                | Event
--------------------|---------------------------------------------------------
2023-08-29 3:45:00  | `CREATE CLUSTER c (SIZE 'medium', REPLICATION FACTOR 2`)
2023-08-29 3:45:45  | `ALTER CLUSTER c SET (REPLICATION FACTOR 1)`
2023-08-29 3:47:15  | `DROP CLUSTER c`

Cluster `c` will have consumed 0.4 credits in total:

  * Replica `c.r1` was provisioned from 3:45:00 to 3:47:15, consuming 0.3
    credits.
  * Replica `c.r2` was provisioned from 3:45:00 to 3:45:45, consuming 0.1
    credits.

### Known limitations

Clusters have several known limitations:

* Clusters containing sources and sinks can only have a replication factor of
  `0` or `1`.

* When a cluster of size `2xlarge` or larger uses multiple replicas, those
  replicas are not guaranteed to be spread evenly across the underlying
  cloud provider's availability zones.

We plan to remove these restrictions in future versions of Materialize.

## Examples

### Basic

Create a cluster with two `medium` replicas:

```sql
CREATE CLUSTER c1 (SIZE = 'medium', REPLICATION FACTOR = 2);
```

### Introspection disabled

Create a cluster with a single replica and introspection disabled:

```sql
CREATE CLUSTER c (SIZE = 'xsmall', INTROSPECTION INTERVAL = 0);
```

Disabling introspection can yield a small performance improvement, but you lose
the ability to run [troubleshooting queries](/ops/troubleshooting/) against
that cluster replica.

### Empty

Create a cluster with no replicas:

```sql
CREATE CLUSTER c1 (SIZE 'xsmall', REPLICATION FACTOR = 0);
```

You can later add replicas to this cluster with [`ALTER CLUSTER`].

## Privileges

The privileges required to execute this statement are:

- `CREATECLUSTER` privileges on the system.

## See also

- [`ALTER CLUSTER`]
- [`DROP CLUSTER`]

[AWS availability zone IDs]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
[`ALTER CLUSTER`]: /sql/alter-cluster/
[`DROP CLUSTER`]: /sql/drop-cluster/
[`SELECT`]: /sql/select
[`SUBSCRIBE`]: /sql/subscribe
[`mz_internal.mz_cluster_replica_sizes`]: /sql/system-catalog/mz_internal/#mz_cluster_replica_sizes
