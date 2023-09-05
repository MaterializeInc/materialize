---
title: "CREATE CLUSTER REPLICA"
description: "`CREATE CLUSTER REPLICA` provisions physical resources to perform computations."
pagerank: 50
menu:
  main:
    parent: commands
---

{{< warning >}}
`CREATE CLUSTER REPLICA` is for working with legacy unmanaged clusters. Consider migrating your cluster to [managed](/sql/alter-cluster/#converting-unmanaged-to-managed-clusters) instead.
{{< /warning >}}

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

### Disk-attached replicas

{{< private-preview />}}

{{< warning >}}
**Pricing for this feature is likely to change.**

Disk-attached replicas currently consume credits at the same rate as
non-disk-attached replicas. In the future, disk-attached replicas will likely
consume credits at a faster rate.
{{< /warning >}}

Disk-attached replicas work identically to [disk-attached clusters](/sql/create-cluster/#disk-attached-clusters), except they apply only to a single replica.

### Deployment options

Materialize is an active-replication-based system, which means you expect each
cluster replica to have the same working set.

With this in mind, when building your Materialize deployment, you can change its
performance characteristics by...

Action | Outcome
---------|---------
Increase all replicas' sizes | Ability to maintain more dataflows or more complex dataflows
Add replicas to a cluster | Greater tolerance to replica failure

### Sizes

Cluster replica sizes work identically to [cluster sizes](/sql/create-cluster/#cluster-size), except they apply only to a single replica.

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
