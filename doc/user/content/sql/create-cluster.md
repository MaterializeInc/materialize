---
title: "CREATE CLUSTER"
description: "`CREATE CLUSTER` creates a logical cluster, which contains indexes."
pagerank: 40
menu:
  main:
    parent: commands
---

`CREATE CLUSTER` creates a logical [cluster](/overview/key-concepts#clusters),
which contains indexes. By default, a cluster named `default` with a single
cluster replica will exist in every environment.

To switch your active cluster, use the `SET` command:

```sql
SET cluster = other_cluster;
```

## Conceptual framework

Clusters are logical components that let you express resource isolation for all
dataflow-powered objects, e.g. indexes. When creating dataflow-powered
objects, you must specify which cluster you want to use. (Not explicitly naming
a cluster uses your session's default cluster.)

Importantly, clusters are strictly a logical component; they rely on [cluster
replicas](/overview/key-concepts#cluster-replicas) to run dataflows. Said a
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

## Syntax

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

## Details

### Deployment options

When building your Materialize deployment, you can change its performance characteristics by...

Action | Outcome
-------|---------
Adding clusters + decreasing dataflow density | Reduced contention among dataflows, decoupled dataflow availability
Adding replicas to clusters | See [Cluster replica scaling](/sql/create-cluster#deployment-options)

## Examples

### Basic

Create a cluster with two medium replicas:

```sql
CREATE CLUSTER c1 REPLICAS (
    r1 (SIZE = 'medium'),
    r2 (SIZE = 'medium')
);
```

### Introspection disabled

Create a cluster with a single replica with introspection disabled:

```sql
CREATE CLUSTER c REPLICAS (
    r1 (SIZE = 'xsmall', INTROSPECTION INTERVAL = 0)
);
```

Disabling introspection can yield a small performance improvement, but you lose
the ability to run [troubleshooting queries](/ops/troubleshooting/) against
that cluster replica.

### Empty

Create a cluster with no replicas:

```sql
CREATE CLUSTER c1 REPLICAS ();
```

You can later add replicas to this cluster with [`CREATE CLUSTER
REPLICA`](../create-cluster-replica).

[AWS availability zone ID]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
