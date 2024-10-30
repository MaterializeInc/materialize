---
title: "Clusters"
description: "Learn about clusters in Materialize."
menu:
  main:
    parent: 'concepts'
    weight: 5
    identifier: 'concepts-clusters'
aliases:
  - /get-started/key-concepts/#clusters
---

## Overview

Clusters are pools of compute resources (CPU, memory, and scratch disk space)
for running your workloads.

The following operations require compute resources in Materialize, and so need
to be associated with a cluster:

- Maintaining [sources](/concepts/sources/) and [sinks](/concepts/sinks/).
- Maintaining [indexes](/concepts/indexes/) and [materialized
  views](/concepts/views/#materialized-views).
- Executing [`SELECT`](/sql/select/) and [`SUBSCRIBE`](/sql/subscribe/)
  statements.

## Resource isolation

Clusters provide **resource isolation.** Each cluster provisions dedicated
compute resources and can fail independently from other clusters.

All workloads on a given cluster will compete for access to these compute
resources. However, workloads on different clusters are strictly isolated from
one another. A given workload has access only to the CPU, memory, and scratch
disk of the cluster that it is running on.

It is a **best practice** to use clusters to isolate different classes of
workloads. For example, you could place your development workloads in a cluster
named `dev` and your production workloads in a cluster named `prod`.

## Fault tolerance

The [**replication factor**](https://materialize.com/docs/sql/create-cluster/#replication-factor)
of a cluster determines the number of replicas provisioned for the cluster. Each
replica of the cluster provisions a new pool of compute resources to perform
exactly the same work on exactly the same data.

Provisioning more than one replica for a cluster improves **fault tolerance**.
Clusters with multiple replicas can tolerate failures of the underlying
hardware that cause a replica to become unreachable. As long as one replica of
the cluster remains available, the cluster can continue to maintain dataflows
and serve queries.

{{< note >}}

Increasing the replication factor does **not** increase the cluster's work
capacity. Replicas are exact copies of one another: each replica must do
exactly the same work (i.e., maintain the same dataflows and process the same
queries) as all the other replicas of the cluster.

To increase the capacity of a cluster, you must increase its [size](#size).

{{< /note >}}

Materialize automatically assigns names to replicas like `r1`, `r2`, etc. You
can view information about individual replicas in the console and the system
catalog, but you cannot directly modify individual replicas.

Clusters that contain sources or sinks can only have a replication factor of
`0` or `1`.

### Availability guarantees

When provisioning replicas, Materialize guarantees that all replicas in a
cluster are spread across the underlying cloud provider's availability zones
for clusters **under `3200cc` sizes**. For clusters sized at `3200cc` and
above, even distribution of replicas across availability zones **cannot** be
guaranteed.

## Sizing your clusters

When creating your cluster, choose the [size of your cluster](/sql/create-cluster/#size)
(e.g., `25cc`, `50cc`, `100cc`) based on the resource requirements of your
workload. Larger clusters have more compute resources available and can
therefore process data faster and handle larger data volumes.

As your workload changes, you can [resize a cluster](/sql/alter-cluster/).
Depending on the type of objects in the cluster, this operation might incur
downtime. See [Downtime](#downtime) for more details.

## Related pages

- [`CREATE CLUSTER`](/sql/create-cluster)
- [`ALTER CLUSTER`](/sql/alter-cluster)
- [System clusters](/sql/system-clusters)
- [Usage & billing](/administration/billing/)
