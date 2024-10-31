---
title: "Usage & billing"
description: "Understand the billing model of Materialize, and learn best practices for cost control."
menu:
  main:
    parent: "manage"
    weight: 50
---

Materialize determines billing based on your compute and storage usage.
Materialize bills per second based on the [cluster(s)](/concepts/clusters/) you
provision for your workloads. Each cluster is a pool of resources (CPU, memory,
and scratch disk space) that must stay up and running to continually provide you
with always-fresh results.

## Compute

In Materialize, [clusters](/concepts/clusters/) are pools of compute resources
(CPU, memory, and scratch disk space) for running your workloads, such as
maintaining up-to-date results while also providing strong [consistency
guarantees](/get-started/isolation-level/). The credit usage for a cluster is
measured at a one second granularity.

{{< note >}}

When you enable a Materialize region, various [system
clusters](/sql/system-clusters/) are pre-installed to improve the user
experience as well as support system administration tasks. Except for the
default `quickstart` cluster, you are <red>not billed</red> for these system clusters.

{{</ note >}}

You must provision at least one cluster to power your workloads. You can then
use the cluster to create the objects ([indexes](/concepts/indexes/) and
[materialized views](/concepts/views/#materialized-views)) that provide
always-fresh results. In Materialize, both indexes and materialized views are
incrementally maintained when Materialize ingests new data. That is, Materialize
performs work on writes such that no work is performed when reading from these
objects.

The cluster size for a workload will depend on the workload's compute and
storage requirements. To help users select the correct cluster size for their
workload, Materialize uses cluster size names that are based on the compute
credit spend, specifically, "centicredits" or `cc` (1/100th of a compute credit). For
example, the `25cc` cluster size is equivalent to 0.25 compute credits/hour; the
`200cc` cluster size is equivalent to 2 compute credits/hour. Larger clusters
can process data faster and handle larger data volumes.

{{< note >}}

You can resize a cluster to respond to changes in your workload. See [Sizing
your clusters](/sql/alter-cluster/#resizing).

{{</ note >}}

Clusters are always "on", and you can adjust the [replication factor](https://materialize.com/docs/sql/create-cluster/#replication-factor)
for fault tolerance. See [Compute cost factors](#compute-cost-factors) for more
information on the cost of increasing a cluster's replication factor.

### Compute cost factors

The credit usage for a cluster is measured at a one second granularity. Factors
that contribute to compute usage include:

| Cost factor | Details       |
|-------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Replication factor for a cluster](/sql/create-cluster/#replication-factor). | Cost is calculated (at one second granularity) as cluster [`SIZE`](/sql/create-cluster/#size) * [`REPLICATION FACTOR`](/sql/create-cluster/#replication-factor). |
| [Indexes](/concepts/indexes/) and [materialized views](/concepts/views) | As data changes (insert/update/delete), [indexes](/concepts/indexes/) and [materialized views](/concepts/views) perform incremental updates to provide up-to-date results. |
| [Sources](/concepts/sources/) |• Sources that use upsert logic (i.e., [`ENVELOPE UPSERT`](/sql/create-sink/kafka/#upsert) or [`ENVELOPE DEBEZIUM` Kafka sources](/sql/create-sink/kafka/#debezium)) can lead to high memory and disk utilization.<br>• Other sources consume a negligible amount of resources in steady state. |
| [`SELECT`s](/sql/select/) and [`SUBSCRIBE`s](/sql/subscribe/)  |• [`SELECT`s](/sql/select/) and [`SUBSCRIBE`s](/sql/subscribe/) that do not use indexes and materialized views perform work. <br>• [`SELECT`s](/sql/select/) and [`SUBSCRIBE`s](/sql/subscribe/) that use indexes and materialized views are **free**.|
| [Sinks](/concepts/sinks/) | Only small CPU/memory costs.|

## Storage

In Materialize, storage is roughly proportional to the size of your source
datasets plus the size of any materialized views, with some overhead from
uncompacted data and system metrics.

Materialize uses cheap, scalable object storage for its storage layer
(Amazon S3), and primarily passes the cost through to the customer. At a rate
of 0.0000411 USD per GB/hr, 1 TB stored for one month (730 hrs) equates to 30
USD.

Most data in Materialize is continually compacted, with the exception of
[append-only sources](/sql/create-source/#append-only-envelope). As such, the
total state stored in Materialize tends to grow at a rate that is more similar
to OLTP databases than cloud data warehouses.

## Invoices

{{< note >}}
Accessing usage and billing information in Materialize
requires **administrator** privileges.
{{</ note >}}

From the [Materialize console](https://console.materialize.com/) (`Admin` >
`Usage & Billing`), administrators can access their invoice. The invoice
provides Compute and Storage usage and cost information.

## Additional references

- https://materialize.com/pricing/

- [How Materialized can lower the cost of freshness for data teams](https://materialize.com/promotions/cost-of-freshness/?utm_campaign=General&utm_source=documentation)

<style>
redb { color: Red; font-weight: 500; }
</style>
