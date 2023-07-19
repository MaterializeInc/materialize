---
title: "CREATE CLUSTER"
description: "`CREATE CLUSTER` creates a logical cluster, which contains indexes."
pagerank: 40
menu:
  main:
    parent: commands
---

`CREATE CLUSTER` creates a logical [cluster](/get-started/key-concepts#clusters),
which contains dataflow-powered objects. By default, a cluster named `default`
with a single cluster replica will exist in every environment.

To switch your active cluster, use the `SET` command:

```sql
SET cluster = other_cluster;
```

## Conceptual framework

Clusters are logical components that let you express resource isolation for all
dataflow-powered objects: sources, sinks, indexes, and materialized views. When
creating dataflow-powered objects, you must specify which cluster you want to
use.

For indexes and materialized views, not explicitly naming a cluster uses your
session's default cluster.

{{< warning >}}
A given cluster may contain any number of indexes and materialized views *or*
any number of sources and sinks, but not both types of objects. For example,
you may not create a cluster with a source and an index.

We plan to remove this restriction in a future version of Materialize.
{{< /warning >}}

Importantly, clusters are strictly a logical component; they rely on [cluster
replicas](/get-started/key-concepts#cluster-replicas) to run dataflows. Said a
slightly different way, a cluster with no replicas does no computation. For
example, if you create an index on a cluster with no replicas, you cannot select
from that index because there is no physical representation of the index to read
from.

Though clusters only represent the logic of which objects you want to bundle
together, this impacts the performance characteristics once you provision
cluster replicas. Each object in a cluster gets instantiated on every replica,
meaning that on a given physical replica, objects in the cluster are in
contention for the same physical resources. To achieve the performance you need,
this might require setting up more than one cluster.

{{< warning >}}
Clusters containing sources and sinks can have at most one replica.

We plan to remove this restriction in a future version of Materialize.
{{< /warning >}}

## Managed and unmanaged clusters

A managed cluster is one with a declared size and replication factor, where
Materialize is responsible for ensuring the replica set matches the declared
size and replication factor. The replicas of a managed cluster are visible in
the system catalog, but cannot be directly modified by users.

An unmanaged cluster requires you to manage replicas manually, by creating and
dropping replicas to achieve the desired replication factor and replica size.
The replicas of unmanaged clusters appear in the system catalog and can be
modified by users.


{{< warning >}}
Managed clusters with sources and sinks only support a replication factor of zero or one.

We plan to remove this restriction in a future version of Materialize.
{{< /warning >}}

### Managed clusters

### Syntax


{{< diagram "create-managed-cluster.svg" >}}

#### `CLUSTER` options

{{% cluster-options %}}

We infer the value for `MANAGED` to be true unless you specify replicas manually.

## Unmanaged clusters

### Syntax

{{< diagram "create-cluster.svg" >}}

### `replica_definition`

{{< diagram "cluster-replica-def.svg" >}}

Field | Use
------|-----
_name_ | A name for the cluster.
_inline_replica_ | Any [replicas](#replica_definition) you want to immediately provision.
_replica_name_ | A name for a cluster replica.

### Replica options

{{% replica-options %}}

{{< note >}}
If you do not specify an availability zone, Materialize will automatically
assign the availability zone with the least existing replicas for the
associated cluster to increase the cluster's tolerance to availability zone
failure.

To check the availability zone associated with each replica in a cluster, use
the [`mz_cluster_replicas`](/sql/system-catalog/mz_catalog/#mz_cluster_replicas)
system table.
{{< /note >}}

## Details

### Deployment options

When building your Materialize deployment, you can change its performance characteristics by...

Action | Outcome
-------|---------
Adding clusters + decreasing dataflow density | Reduced contention among dataflows, decoupled dataflow availability
Adding replicas to clusters | See [Cluster replica scaling](/sql/create-cluster#deployment-options)

## Examples

### Basic

Create a managed cluster with two medium replicas:

```sql
CREATE CLUSTER c1 SIZE = 'medium', REPLICATION FACTOR = 2;
```

Alternatively, you can create an unmanaged cluster:

```sql
CREATE CLUSTER c1 REPLICAS (
    r1 (SIZE = 'medium'),
    r2 (SIZE = 'medium')
);
```

### Introspection disabled

Create a managed cluster with a single replica with introspection disabled:

```sql
CREATE CLUSTER c  SIZE = 'xsmall', INTROSPECTION INTERVAL = 0;
```

Alternatively, you can create an unmanaged cluster:

```sql
CREATE CLUSTER c REPLICAS (
    r1 (SIZE = 'xsmall', INTROSPECTION INTERVAL = 0)
);
```

Disabling introspection can yield a small performance improvement, but you lose
the ability to run [troubleshooting queries](/ops/troubleshooting/) against
that cluster replica.

### Empty

Create a managed cluster with no replicas:

```sql
CREATE CLUSTER c1 SIZE 'xsmall', REPLICATION FACTOR 0;
```

You can later add replicas to this managed cluster with [`ALTER CLUSTER`](/sql/alter-cluster/.
)

Alternatively, you can create an unmanaged cluster:

```sql
CREATE CLUSTER c1 REPLICAS ();
```

You can later add replicas to this unmanaged cluster with [`CREATE CLUSTER
REPLICA`](../create-cluster-replica).

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- `CREATECLUSTER` privileges on the system.

## See also

- [`CREATE CLUSTER REPLICA`](/sql/create-cluster-replica)
- [`ALTER CLUSTER`](/sql/alter-cluster/)
- [`DROP CLUSTER`](/sql/drop-cluster/)

[AWS availability zone IDs]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
