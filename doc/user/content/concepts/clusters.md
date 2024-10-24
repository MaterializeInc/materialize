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

Clusters provide **resource isolation.** Each cluster provisions a dedicated
pool of CPU, memory, and scratch disk space.

All workloads on a given cluster will compete for access to these compute
resources. However, workloads on different clusters are strictly isolated from
one another. A given workload has access only to the CPU, memory, and scratch
disk of the cluster that it is running on.

Clusters are commonly used to isolate different classes of workloads. For
example, you could place your development workloads in a cluster named
`dev` and your production workloads in a cluster named `prod`.

## Fault tolerance (replication factor)

A cluster's replication factor determines the number of replicas provisioned for
the cluster. Each replica of the cluster provisions a new pool of compute
resources to perform exactly the same computations on exactly the same data.

Provisioning more than one replica for a cluster improves **fault tolerance**.
Clusters with multiple replicas can tolerate failures of the underlying hardware
that cause a replica to become unreachable. As long as one replica of the
cluster remains available, the cluster can continue to maintain dataflows and
serve queries.

{{< note >}}

Increasing the replication factor increases the cluster's **fault tolerance**;
it does **not** increase the cluster's work capacity. Replicas are exact copies
of one another: each replica must do exactly the same work (i.e., maintain the
same dataflows and process the same queries) as all the other replicas of the
cluster.

To increase a cluster's capacity, you should instead increase the cluster's
[size](#size).

{{< /note >}}

Materialize automatically assigns names to replicas like `r1`, `r2`, etc. You
can view information about individual replicas in the console and the system
catalog, but you cannot directly modify individual replicas.

Clusters that contain sources and/or sinks can only have a replication factor of
`0` or `1`.

### Guarantees

Materialize makes the following guarantees when provisioning replicas:

- Replicas of a given cluster are never provisioned on the same underlying
  hardware.
- Replicas of a given cluster are spread as evenly as possible across the
  underlying cloud provider's availability zones with the following exception.
  When a cluster of size `3200cc` or larger uses multiple replicas, those
  replicas are not guaranteed to be spread evenly across the underlying cloud
  provider's availability zones.

## Size

When creating your cluster, choose the [size of your
cluster](/sql/create-cluster/#size) (`25cc`, `50cc`, `100cc`, etc.) based on the
resource requirements of your workload. Larger clusters have more compute
resources available and can therefore process data faster and handle larger data
volumes.

As your workload changes, you can [resize a cluster](/sql/alter-cluster/).

## Related pages

- [`CREATE CLUSTER`](/sql/create-cluster)
- [`ALTER CLUSTER`](/sql/alter-cluster)
- [System clusters](/sql/system-clusters)
- [Usage & billing](/administration/billing/)
