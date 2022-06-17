---
title: "CREATE CLUSTER"
description: "`CREATE CLUSTER` creates a logical cluster, which contains indexes."
menu:
  main:
    parent: commands
---

`CREATE CLUSTER` creates a logical [cluster](/overview/key-concepts#clusters), which
contains indexes.

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

### `replica_option`

{{< diagram "replica-option.svg" >}}

Field | Use
------|-----
_name_ | A name for the cluster.
_inline_replica_ | Any [replicas](#replica_definition) you want to immediately provision.
_replica_name_ | A name for a cluster replica.
_replica_option_ | This replica's specified [options](#replica_option).
_size_ | A "size" for a managed replica. For valid `size` values, see [Cluster replica sizes](#cluster-replica-sizes)
_az_ | If you want the replica to reside in a specific availability zone. You must specify an [AWS availability zone ID] in either `us-east-1` or `eu-west-1`, e.g. `use1-az1`. Note that we expect the zone's ID, rather than its name.

## Details

### Cluster replica sizes

Valid `size` options are:

- `xsmall`
- `small`
- `medium`
- `large`
- `xlarge`
- `2xlarge`
- `3xlarge`
- `4xlarge`
- `5xlarge`
- `6xlarge`

### Deployment options

When building your Materialize deployment, you can change its performance characteristics by...

Action | Outcome
-------|---------
Adding clusters + decreasing dataflow density | Reduced contention among dataflows, decoupled dataflow availability
Adding replicas to clusters | See [Cluster replica scaling](/sql/create-cluster#deployment-options)

## Example

```sql
CREATE CLUSTER c1 REPLICAS (r1 (SIZE = 'medium'), r2 (SIZE = 'medium'));

-- Create an empty cluster
CREATE CLUSTER c1 REPLICAS ();
```

[AWS availability zone ID]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
