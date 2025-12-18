---
title: "Usage (Self-Managed)"
description: "Overview of the resource usage for Self-Managed Materialize."
menu:
  main:
    parent: "manage"
    weight: 50

---

## Compute

In Materialize, [clusters](/concepts/clusters/) are pools of compute resources
(CPU, memory, and scratch disk space) for running your workloads, such as
maintaining up-to-date results while also providing strong [consistency
guarantees](/get-started/isolation-level/).

{{< note >}}

In Materialize,various [system clusters](/sql/system-clusters/) are
pre-installed to improve the user experience as well as support system
administration tasks.

{{</ note >}}

You must provision at least one cluster to power your workloads. You can then
use the cluster to create the objects ([indexes](/concepts/indexes/) and
[materialized views](/concepts/views/#materialized-views)) that provide
always-fresh results. In Materialize, both indexes and materialized views are
incrementally maintained when Materialize ingests new data. That is, Materialize
performs work on writes such that no work is performed when reading from these
objects.

The cluster size for a workload will depend on the workload's compute and
storage requirements.

Clusters are always "on", and you can adjust the [replication
factor](/sql/create-cluster/#replication-factor) for
fault tolerance. See [Compute usage factors](#compute-usage-factors) for more
information on increasing a cluster's replication factor.

## Compute usage factors

Factors that contribute to compute usage include:

| Cost factor | Details       |
|-------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Replication factor for a cluster](/sql/create-cluster/#replication-factor). |  Each replica of the cluster provisions a new pool of compute resources to perform exactly the same computations on exactly the same data.|
| [Indexes](/concepts/indexes/) and [materialized views](/concepts/views) | As data changes (insert/update/delete), [indexes](/concepts/indexes/) and [materialized views](/concepts/views) perform incremental updates to provide up-to-date results. |
| [Sources](/concepts/sources/) |• Sources that use upsert logic (i.e., [`ENVELOPE UPSERT`](/sql/create-sink/kafka/#upsert) or [`ENVELOPE DEBEZIUM` Kafka sources](/sql/create-sink/kafka/#debezium)) can lead to high memory and disk utilization.<br>• Other sources consume a negligible amount of resources in steady state. |
| [`SELECT`s](/sql/select/) and [`SUBSCRIBE`s](/sql/subscribe/)  |• [`SELECT`s](/sql/select/) and [`SUBSCRIBE`s](/sql/subscribe/) that do not use indexes and materialized views perform work. <br>• [`SELECT`s](/sql/select/) and [`SUBSCRIBE`s](/sql/subscribe/) that use indexes and materialized views are computationally **free**.|
| [Sinks](/concepts/sinks/) | Only small CPU/memory costs.|

## Storage

In Materialize, storage is roughly proportional to the size of your source
datasets plus the size of any materialized views, with some overhead from
uncompacted data and system metrics.

Most data in Materialize is continually compacted, with the exception of
[append-only sources](/sql/create-source/#append-only-envelope). As such, the
total state stored in Materialize tends to grow at a rate that is more similar
to OLTP databases than cloud data warehouses.
