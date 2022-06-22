---
title: "CREATE CLUSTER REPLICA"
description: "`CREATE CLUSTER REPLICA` provisions physical resources to perform computations."
menu:
  main:
    parent: commands
---

`CREATE CLUSTER REPLICA` provisions physical resources to perform computations.

## Conceptual framework

Where [clusters](/overview/key-concepts#clusters) represent the logical set of
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

## Syntax

{{< diagram "create-cluster-replica.svg" >}}

### `replica_option`

{{< diagram "replica-option.svg" >}}

Field | Use
------|-----
_cluster_name_ | The cluster whose resources you want to create an additional computation of.
_replica_name_ | A name for this replica.
_replica_option_ | This replica's specified [options](#replica_option).
_size_ | A "size" for a managed replica. For valid `size` values, see [Sizes](#sizes)
_az_ | If you want the replica to reside in a specific availability zone. You must specify an [AWS availability zone ID] in either `us-east-1` or `eu-west-1`, e.g. `use1-az1`. Note that we expect the zone's ID, rather than its name.

## Details

### Sizes

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

[AWS availability zone ID]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
