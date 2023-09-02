---
title: "CREATE CLUSTER REPLICA"
description: "`CREATE CLUSTER REPLICA` provisions physical resources to perform computations."
pagerank: 50
menu:
  main:
    parent: commands
---

`CREATE CLUSTER REPLICA` provisions physical resources to perform computations.

## Conceptual framework

Where [clusters](/get-started/key-concepts#clusters) represent the logical set of
dataflows you want to maintain, cluster replicas are their physical
counterparts. Cluster replicas are where Materialize actually creates and
maintains dataflows.

Each cluster replica is essentially a clone, constructing the same dataflows.
Each cluster replica receives a copy of all data that comes in from sources its
dataflows use, and uses the data to perform identical computations. This design
provides Materialize with active replication, and so long as one replica is
still reachable, the cluster continues making progress.

This also means that all of a cluster's dataflows contend for the same resources
on each replica. This might mean, for instance, that instead of placing many
complex materialized views on the same cluster, you choose some other
distribution, or you replace all replicas in a cluster with more powerful
machines.

{{< warning >}}
Clusters containing sources and sinks can have at most one replica.

We plan to remove this restriction in a future version of Materialize.
{{< /warning >}}

## Syntax

{{< diagram "create-cluster-replica.svg" >}}

Field | Use
------|-----
_cluster_name_ | The cluster whose resources you want to create an additional computation of.
_replica_name_ | A name for this replica.

### Options

{{% replica-options %}}

{{< note >}}
If you do not specify an availability zone, Materialize will assign the replica
to an arbitrary availability zone. For details on how replicas are assigned
between availability zones, see [Availability zone assignment](/sql/create-cluster/#availability-zone-assignment).
{{< /note >}}

## Details

### Sizes

Valid `size` options are:

- `3xsmall`
- `2xsmall`
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

The [`mz_internal.mz_cluster_replica_sizes`](/sql/system-catalog/mz_internal/#mz_cluster_replica_sizes) table lists the CPU, memory, and disk allocation for each replica size.

{{< warning >}}
The values in the `mz_internal.mz_cluster_replica_sizes` may change at any time.
You should not rely on them for any kind of capacity planning.
{{< /warning >}}

### Disk-attached replicas

{{< private-preview />}}

{{< warning >}}
**Pricing for this feature is likely to change.**

Disk-attached replicas currently consume credits at the same rate as
non-disk-attached replicas. In the future, disk-attached replicas will likely
consume credits at a faster rate.
{{< /warning >}}

The `DISK` option attaches a disk to the replica.

Attaching a disk allows you to trade off performance for cost. A replica of a
given size has access to several times more disk than memory, allowing the
processing of larger data sets at that replica size. Operations on a disk,
however, are much slower than operations in memory, and so a workload that
spills to disk will perform more slowly than a workload that does not. Note that
exact storage medium for the attached disk is not specified, and its performance
characteristics are subject to change.

Consider attaching a disk to replicas that contain sources that use the
[upsert envelope](/sql/create-source/#upsert-envelope) or the
[Debezium envelope](/sql/create-source/#debezium-envelope). When you place
these sources on a replica with an attached disk, they will automatically spill
state to disk. These sources will therefore use less memory but may ingest
data more slowly. See [Sizing a source](/sql/create-source/#sizing-a-source) for details.


### Deployment options

Materialize is an active-replication-based system, which means you expect each
cluster replica to have the same working set.

With this in mind, when building your Materialize deployment, you can change its
performance characteristics by...

Action | Outcome
---------|---------
Increase all replicas' sizes | Ability to maintain more dataflows or more complex dataflows
Add replicas to a cluster | Greater tolerance to replica failure

### Homogeneous vs. heterogeneous hardware provisioning

Because Materialize uses active replication, all replicas will be asked to do
the same work, irrespective of their resources.

For the most stable performance, we recommend provisioning the same class of
hardware for all replicas.

However, it is possible to provision multiple type of hardware in the same
cluster. In these cases, the slower machines will likely be continually burdened
with a backlog of work. If all of the faster machines become unreachable, the
system might experience delays in replying to requests while the slower machines
catch up to the last known time that the faster machines had computed.

## Example

```sql
CREATE CLUSTER REPLICA c1.r1 SIZE = 'medium';
```

## Privileges

The privileges required to execute this statement are:

- Ownership of `cluster_name`.

[AWS availability zone ID]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
