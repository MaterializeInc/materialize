---
title: "Usage & billing"
description: "Understand the billing model of Materialize, and learn best practices for cost control."
menu:
  main:
    parent: "manage"
    weight: 50
---

{{< note >}}
Access to usage and billing requires **administrator** privileges.
{{</ note >}}

Materialize determines billing based on your storage and compute usage.

## Invoice

From the [Materialize console](https://console.materialize.com/) (`Admin` >
`Usage & Billing`), administrators can access their invoice. The invoice
provides Compute and Storage usage and cost information.

## Storage

In Materialize, storage is roughly proportional to the size of your source
datasets plus the size of materialized views, with some overhead from
uncompacted previous values of data and system metrics.

Most data in Materialize is continually compacted. As such, the total state stored in Materialize tends to grow at a rate that is more similar to OLTP databases than traditional cloud data warehouses.

## Compute

In Materialize, [clusters](/concepts/clusters/) are pools of compute resources
(CPU, memory, and, optionally, scratch disk space) for running your workloads;
such as maintaining up-to-date results while also providing strong [consistency
guarantees](/get-started/isolation-level/). The credit usage for a cluster is
measured at a one second granularity for each cluster replica.

To help users select the correct cluster size for their workload, Materialize
uses cluster size names that are based on the compute credit, specifically,
"centicredits" or `cc` (1/100th of a compute credit). For example, cluster size
of `25cc` is equivalent to 0.25 compute credits/hour and cluster size of
`200cc` is equivalent to 2 compute credits/hour. Larger clusters can process data faster and handle larger data volumes.

{{< tip >}}
You can resize a cluster at any time, even while the cluster is running, to respond to changes in your workload.
{{</ tip >}}

Clusters are always "on". However, you can adjust the number of replicas that
are associated with a cluster. See [Compute cost
factors](#compute-cost-factors) for more information on compute costs for replicas.

### Compute cost factors

{{< note >}}
  Materialize provides up-to-date results while also providing
  strong [consistency guarantees](/get-started/isolation-level/), including
  strict serializability (the default level). Materialize maintains these
  up-to-date results via incrementally maintained
  [indexes](/concepts/indexes/) and incrementally maintained [materialized
  views](/concepts/views) that perform computation upon insert/update/delete. That is:

  - Materialize performs work on "writes"; i.e., Materialize updates the results
    when data is ingested.

  - Materialize performs **no** work on "reads" from these objects; i.e.,
    [`SELECT`s](/sql/select/) from these objects, including ad-hoc queries, are
    free.

  - Materialize is always "on".

{{</ note >}}

Factors that contribute to compute usage include:

| Factor | Notes       |
|-------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Each [replica for a cluster](/sql/create-cluster/#replication-factor). | The credit usage for a cluster is measured at a one second granularity for each cluster replica. For a cluster replica, its credit usage begins when it is provisioned and ends when it is deprovisioned. |
| [Indexes](/concepts/indexes/) and [materialized views](/concepts/views) | As data changes (insert/update/delete), [indexes](/concepts/indexes/) and [materialized views](/concepts/views) perform incremental updates to provide up-to-date results. That is: <br>• Indexes and materialized views perform work on "writes"<br>• Indexes and materialized views perform **no** work on "reads" from these objects; i.e., [`SELECT`s](/sql/select/) and [`SUBSCRIBE`s](/sql/subscribe/) from these objects are **free**. |
| [Sources](/concepts/sources/) | Depends. |
| [`SELECT`s](/sql/select/) and [`SUBSCRIBE`s](/sql/subscribe/)  |• [`SELECT`s](/sql/select/) and [`SUBSCRIBE`s](/sql/subscribe/) that do not use indexes and materialized views perform work. <br>• [`SELECT`s](/sql/select/) and [`SUBSCRIBE`s](/sql/subscribe/) that use indexes and materialized views are **free**.|
| [Sinks](/concepts/sinks/) | Only small CPU/memory costs.|

### Compute cost reductions

Some considerations for lowering costs:

- [Optimize queries](/transform-data/optimization/).

- Avoid creating unnecessary indexes. For example,

  - If you have created various views to act as the building blocks for a view
    from which you will serve the results, indexing the serving view only may
    be sufficient.

  - In Materialize, underlying data structure that is being maintained for an
    index (or a materialized view) can be reused by other views/queries. As
    such, an existing index may be sufficient to support your queries.

- For <redb>development</redb> clusters, you can set your replicas to zero when
  not using your <redb>development</redb> clusters.

## Additional references

- https://materialize.com/pricing/

- [How Materialized can lower the cost of freshness for data teams](https://materialize.com/promotions/cost-of-freshness/?utm_campaign=General&utm_source=documentation)

<style>
redb { color: Red; font-weight: 500; }
</style>
