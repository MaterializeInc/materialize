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

Workloads on different clusters are strictly isolated from one another. That is,
a given workload has access only to the CPU, memory, and scratch disk of the
cluster that it is running on. All workloads on a given cluster compete for
access to that cluster's compute resources.

## Fault tolerance

The [replication factor](/sql/create-cluster/#replication-factor) of a cluster
determines the number of replicas provisioned for the cluster. Each replica of
the cluster provisions a new pool of compute resources to perform exactly the
same work on exactly the same data.

Provisioning more than one replica for a cluster improves **fault tolerance**.
Clusters with multiple replicas can tolerate failures of the underlying
hardware that cause a replica to become unreachable. As long as one replica of
the cluster remains available, the cluster can continue to maintain dataflows
and serve queries.

{{< note >}}

{{< include-md file="shared-content/cluster-replica-cost-capacity-notes.md" >}}

{{< /note >}}

Materialize automatically assigns names to replicas (e.g., `r1`, `r2`). You can
view information about individual replicas in the Materialize console and the
system catalog.

### Availability guarantees

When provisioning replicas,

{{< include-md file="shared-content/multi-replica-az.md" >}}

<a name="sizing-your-clusters"></a>

## Cluster sizing

When creating a cluster, you must choose its [size](/sql/create-cluster/#size)
(e.g., `25cc`, `50cc`, `100cc`), which determines its resource allocation
(CPU, memory, and scratch disk space) and [cost](/administration/billing/#compute).
The appropriate size for a cluster depends on the resource requirements of your
workload. Larger clusters have more compute
resources available and can therefore process data faster and handle larger data
volumes.

As your workload changes, you can [resize a cluster](/sql/alter-cluster/).
Depending on the type of objects in the cluster, this operation might incur
downtime. See [Resizing downtime](/sql/alter-cluster/#downtime) for more details.

{{< tip >}}

To gauge the performance and utilization of your clusters, use the
[**Environment Overview** page in the Materialize Console](/console/monitoring/).

{{< /tip >}}

## Best practices

The following provides some general guidelines for clusters. See also
[Operational guidelines](/manage/operational-guidelines/).

### Three-tier architecture in production

{{% best-practices/architecture/three-tier %}}

See also [Operational guidelines](/manage/operational-guidelines/).

#### Alternatives

Alternatively, if a three-tier architecture is not feasible or unnecessary due
to low volume or a non-production setup, a two cluster or a single cluster
architecture may suffice.

See [Appendix: Alternative cluster
architectures](/manage/appendix-alternative-cluster-architectures/) for details.

### Use production clusters for production workloads only

Use production cluster(s) for production workloads only. That is, avoid using
production cluster(s) to run development workloads or non-production tasks.

### Consider hydration requirements

During hydration, materialized views require memory proportional to both the
input and output. When estimating required resources, consider both the
hydration cost and the steady-state cost.

## Related pages

- [`CREATE CLUSTER`](/sql/create-cluster)
- [`ALTER CLUSTER`](/sql/alter-cluster)
- [System clusters](/sql/system-clusters)
- [Usage & billing](/administration/billing/)
- [Operational guidelines](/manage/operational-guidelines/)
