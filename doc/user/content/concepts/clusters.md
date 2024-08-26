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

Clusters are pools of compute resources (CPU, memory, and, optionally, scratch
disk space) for running your workloads.

The following operations require compute resources in Materialize, and so need
to be associated with a cluster:

- Maintaining [sources](/concepts/sources/) and [sinks](/concepts/sinks/).
- Maintaining [indexes](/concepts/indexes/) and [materialized
  views](/concepts/views/#materialized-views).
- Executing [`SELECT`](/sql/select/) and [`SUBSCRIBE`](/sql/subscribe/) statements.

## Sizing your clusters

You choose the size of your cluster (`25cc`, `50cc`, `100cc`, etc.) based on
the resource requirements of your workload. Larger clusters have more compute
resources available and can therefore process data faster and handle larger data
volumes.

You can resize a cluster to respond to changes in your workload.

{{< note >}}

Resizing incurs downtime as it requires all objects in the cluster to hydrate.

**Private Preview**: For clusters that do not contain sources and sinks but only
compute objects (e.g., indexes, views, and materialized views), [graceful
reconfiguration](/sql/alter-cluster/#no-downtime-reconfiguration) is available
in [private preview](https://materialize.com/preview-terms/).

{{</ note >}}

## Key properties

Clusters provide two important properties:

  * [**Resource isolation**](/sql/create-cluster#resource-isolation). Workloads
    running in one cluster cannot interfere with workloads running in another
    cluster. You can use multiple clusters to isolate your production workloads
    from your development workloads, for example.

  * [**Fault tolerance**](/sql/create-cluster#replication-factor). You can
    increase the replication factor of a cluster to increase the number of
    copies (i.e., _replicas_) of the cluster that Materialize provisions.
    Clusters with multiple replicas can tolerate failures of the underlying
    hardware that cause a replica to become unreachable. As long as one replica
    of the cluster remains available, the cluster can continue to ingest data,
    compute results, and serve queries.

## Related pages

- [`CREATE CLUSTER`](/sql/create-cluster)
