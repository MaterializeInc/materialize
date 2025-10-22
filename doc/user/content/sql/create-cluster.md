---
title: "CREATE CLUSTER"
description: "`CREATE CLUSTER` creates a new cluster."
pagerank: 40
menu:
  main:
    parent: commands
---

`CREATE CLUSTER` creates a new [cluster](/concepts/clusters/).

## Conceptual framework

A cluster is a pool of compute resources (CPU, memory, and scratch disk space)
for running your workloads.

The following operations require compute resources in Materialize, and so need
to be associated with a cluster:

- Executing [`SELECT`] and [`SUBSCRIBE`] statements.
- Maintaining [indexes](/concepts/indexes/) and [materialized views](/concepts/views/#materialized-views).
- Maintaining [sources](/concepts/sources/) and [sinks](/concepts/sinks/).

## Syntax

{{< diagram "create-managed-cluster.svg" >}}

### Options

{{< yaml-table data="syntax_options/create_cluster_options" >}}

## Details

### Initial state

Each Materialize region initially contains a [pre-installed cluster](/sql/show-clusters/#pre-installed-clusters)
named `quickstart` with a size of `25cc` and a replication factor of `1`. You
can drop or alter this cluster to suit your needs.

### Choosing a cluster

When performing an operation that requires a cluster, you must specify which
cluster you want to use. Not explicitly naming a cluster uses your session's
active cluster.

To show your session's active cluster, use the [`SHOW`](/sql/show) command:

```mzsql
SHOW cluster;
```

To switch your session's active cluster, use the [`SET`](/sql/set) command:

```mzsql
SET cluster = other_cluster;
```

### Resource isolation

Clusters provide **resource isolation.** Each cluster provisions a dedicated
pool of CPU, memory, and, optionally, scratch disk space.

All workloads on a given cluster will compete for access to these compute
resources. However, workloads on different clusters are strictly isolated from
one another. A given workload has access only to the CPU, memory, and scratch
disk of the cluster that it is running on.

Clusters are commonly used to isolate different classes of workloads. For
example, you could place your development workloads in a cluster named
`dev` and your production workloads in a cluster named `prod`.

<a name="legacy-sizes"></a>

### Size

The `SIZE` option determines the amount of compute resources available to the
cluster.

{{< tabs >}}
{{< tab "M.1 Clusters" >}}
{{< yaml-table data="m1_cluster_sizing" >}}

{{< /tab >}}
{{< tab "Legacy cc Clusters" >}}

Materialize offers the following legacy cc cluster sizes:

{{< tip >}}
In most cases, you **should not** use legacy sizes. [M.1 sizes](#size)
offer better performance per credit for nearly all workloads. We recommend using
M.1 sizes for all new clusters, and recommend migrating existing
legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
customers during the transition period as we move to deprecate legacy sizes.

The legacy size information is provided for completeness.
{{< /tip >}}

* `25cc`
* `50cc`
* `100cc`
* `200cc`
* `300cc`
* `400cc`
* `600cc`
* `800cc`
* `1200cc`
* `1600cc`
* `3200cc`
* `6400cc`
* `128C`
* `256C`
* `512C`

The resource allocations are proportional to the number in the size name. For
example, a cluster of size `600cc` has 2x as much CPU, memory, and disk as a
cluster of size `300cc`, and 1.5x as much CPU, memory, and disk as a cluster of
size `400cc`. To determine the specific resource allocations for a size,
query the [`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes) table.

{{< warning >}}
The values in the `mz_cluster_replica_sizes` table may change at any
time. You should not rely on them for any kind of capacity planning.
{{< /warning >}}

Clusters of larger sizes can process data faster and handle larger data volumes.
{{< /tab >}}
{{< tab "Legacy t-shirt Clusters" >}}

Materialize also offers some legacy t-shirt cluster sizes for upsert sources.

{{< tip >}}
In most cases, you **should not** use legacy t-shirt sizes. [M.1 sizes](#size)
offer better performance per credit for nearly all workloads. We recommend using
M.1 sizes for all new clusters, and recommend migrating existing
legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
customers during the transition period as we move to deprecate legacy sizes.

The legacy size information is provided for completeness.

{{< /tip >}}

{{< if-past "2024-04-15" >}}
{{< warning >}}
Materialize regions that were enabled after 15 April 2024 do not have access
to legacy sizes.
{{< /warning >}}
{{< /if-past >}}

When legacy sizes are enabled for a region, the following sizes are available:

* `3xsmall`
* `2xsmall`
* `xsmall`
* `small`
* `medium`
* `large`
* `xlarge`
* `2xlarge`
* `3xlarge`
* `4xlarge`
* `5xlarge`
* `6xlarge`

{{< /tab >}}
{{< /tabs >}}

See also:

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog:Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).

#### Cluster resizing

You can change the size of a cluster to respond to changes in your workload
using [`ALTER CLUSTER`](/sql/alter-cluster). Depending on the type of objects
the cluster is hosting, this operation **might incur downtime**.

See the reference documentation for [`ALTER
CLUSTER`](/sql/alter-cluster#zero-downtime-cluster-resizing) for more details
on cluster resizing.



### Replication factor

The `REPLICATION FACTOR` option determines the number of replicas provisioned
for the cluster. Each replica of the cluster provisions a new pool of compute
resources to perform exactly the same computations on exactly the same data.

Provisioning more than one replica improves **fault tolerance**. Clusters with
multiple replicas can tolerate failures of the underlying hardware that cause a
replica to become unreachable. As long as one replica of the cluster remains
available, the cluster can continue to maintain dataflows and serve queries.

Materialize makes the following guarantees when provisioning replicas:

- Replicas of a given cluster are never provisioned on the same underlying
  hardware.
- Replicas of a given cluster are spread as evenly as possible across the
  underlying cloud provider's availability zones.

Materialize automatically assigns names to replicas like `r1`, `r2`, etc. You
can view information about individual replicas in the console and the system
catalog, but you cannot directly modify individual replicas.

You can pause a cluster's work by specifying a replication factor of `0`. Doing
so removes all replicas of the cluster. Any indexes, materialized views,
sources, and sinks on the cluster will cease to make progress, and any queries
directed to the cluster will block. You can later resume the cluster's work by
using [`ALTER CLUSTER`] to set a nonzero replication factor.

{{< note >}}
A common misconception is that increasing a cluster's replication
factor will increase its capacity for work. This is not the case. Increasing
the replication factor increases the **fault tolerance** of the cluster, not its
capacity for work. Replicas are exact copies of one another: each replica must
do exactly the same work (i.e., maintain the same dataflows and process the same
queries) as all the other replicas of the cluster.

To increase a cluster's capacity, you should instead increase the cluster's
[size](#size).
{{< /note >}}

### Credit usage

Each [replica](#replication-factor) of the cluster consumes credits at a rate
determined by the cluster's size:

Size      | Legacy size  | Credits per replica per hour
----------|--------------|-----------------------------
`25cc`    | `3xsmall`    | 0.25
`50cc`    | `2xsmall`    | 0.5
`100cc`   | `xsmall`     | 1
`200cc`   | `small`      | 2
`300cc`   | &nbsp;       | 3
`400cc`   | `medium`     | 4
`600cc`   | &nbsp;       | 6
`800cc`   | `large`      | 8
`1200cc`  | &nbsp;       | 12
`1600cc`  | `xlarge`     | 16
`3200cc`  | `2xlarge`    | 32
`6400cc`  | `3xlarge`    | 64
`128C`    | `4xlarge`    | 128
`256C`    | `5xlarge`    | 256
`512C`    | `6xlarge`    | 512

Credit usage is measured at a one second granularity. For a given replica,
credit usage begins when a `CREATE CLUSTER` or [`ALTER CLUSTER`] statement
provisions the replica and ends when an [`ALTER CLUSTER`] or [`DROP CLUSTER`]
statement deprovisions the replica.

A cluster with a [replication factor](#replication-factor) of zero uses no
credits.

As an example, consider the following sequence of events:

Time                | Event
--------------------|---------------------------------------------------------
2023-08-29 3:45:00  | `CREATE CLUSTER c (SIZE '400cc', REPLICATION FACTOR 2`)
2023-08-29 3:45:45  | `ALTER CLUSTER c SET (REPLICATION FACTOR 1)`
2023-08-29 3:47:15  | `DROP CLUSTER c`

Cluster `c` will have consumed 0.4 credits in total:

  * Replica `c.r1` was provisioned from 3:45:00 to 3:47:15, consuming 0.3
    credits.
  * Replica `c.r2` was provisioned from 3:45:00 to 3:45:45, consuming 0.1
    credits.

### Scheduling

{{< private-preview />}}

To support [scheduled refreshes in materialized views](../create-materialized-view/#refresh-strategies),
you can configure a cluster to automatically turn on and off using the
`SCHEDULE...ON REFRESH` syntax.

```mzsql
CREATE CLUSTER my_scheduled_cluster (
  SIZE = '3200cc',
  SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour')
);
```

Scheduled clusters should **only** contain materialized views configured with a
non-default [refresh strategy](../create-materialized-view/#refresh-strategies)
(and any indexes built on these views). These clusters will automatically turn
on (i.e., be provisioned with compute resources) based on the configured
refresh strategies, and **only** consume credits for the duration of the
refreshes.

It's not possible to manually turn on a cluster with `ON REFRESH` scheduling. If
you need to turn on a cluster outside its schedule, you can temporarily disable
scheduling and provision compute resources using [`ALTER CLUSTER`](../alter-cluster/#schedule):

```mzsql
ALTER CLUSTER my_scheduled_cluster SET (SCHEDULE = MANUAL, REPLICATION FACTOR = 1);
```

To re-enable scheduling:

```mzsql
ALTER CLUSTER my_scheduled_cluster
SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'));
```

#### Hydration time estimate

<p style="font-size:14px"><b>Syntax:</b> <code>HYDRATION TIME ESTIMATE</code> <i>interval</i></p>

By default, scheduled clusters will turn on at the scheduled refresh time. To
avoid [unavailability of the objects scheduled for refresh](/sql/create-materialized-view/#querying-materialized-views-with-refresh-strategies) during the refresh
operation, we recommend turning the cluster on ahead of the scheduled time to
allow hydration to complete. This can be controlled using the `HYDRATION
TIME ESTIMATE` clause.

#### Introspection

To check the scheduling strategy associated with a cluster, you can query the
[`mz_internal.mz_cluster_schedules`](/sql/system-catalog/mz_internal/#mz_cluster_schedules)
system catalog table:

```mzsql
SELECT c.id AS cluster_id,
       c.name AS cluster_name,
       cs.type AS schedule_type,
       cs.refresh_hydration_time_estimate
FROM mz_internal.mz_cluster_schedules cs
JOIN mz_clusters c ON cs.cluster_id = c.id
WHERE c.name = 'my_refresh_cluster';
```

To check if a scheduled cluster is turned on, you can query the
[`mz_catalog.mz_cluster_replicas`](/sql/system-catalog/mz_catalog/#mz_cluster_replicas)
system catalog table:

```mzsql
SELECT cs.cluster_id,
       -- A cluster with scheduling is "on" when it has compute resources
       -- (i.e. a replica) attached.
       CASE WHEN cr.id IS NOT NULL THEN true
       ELSE false END AS is_on
FROM mz_internal.mz_cluster_schedules cs
JOIN mz_clusters c ON cs.cluster_id = c.id AND cs.type = 'on-refresh'
LEFT JOIN mz_cluster_replicas cr ON c.id = cr.cluster_id;
```

You can also use the [audit log](../system-catalog/mz_catalog/#mz_audit_events)
to observe the commands that are automatically run when a scheduled cluster is
turned on and off for materialized view refreshes:

```mzsql
SELECT *
FROM mz_audit_events
WHERE object_type = 'cluster-replica'
ORDER BY occurred_at DESC;
```

Any commands attributed to scheduled refreshes will be marked with
`"reason":"schedule"` under the `details` column.

### Known limitations

Clusters have several known limitations:

* When a cluster of size `3200cc` or larger uses multiple replicas, those
  replicas are not guaranteed to be spread evenly across the underlying cloud
  provider's availability zones.

We plan to remove these restrictions in future versions of Materialize.

## Examples

### Basic

Create a cluster with two `400cc` replicas:

```mzsql
CREATE CLUSTER c1 (SIZE = '400cc', REPLICATION FACTOR = 2);
```

### Introspection disabled

Create a cluster with a single replica and introspection disabled:

```mzsql
CREATE CLUSTER c (SIZE = '100cc', INTROSPECTION INTERVAL = 0);
```

Disabling introspection can yield a small performance improvement, but you lose
the ability to run [troubleshooting queries](/ops/troubleshooting/) against
that cluster replica.

### Empty

Create a cluster with no replicas:

```mzsql
CREATE CLUSTER c1 (SIZE '100cc', REPLICATION FACTOR = 0);
```

You can later add replicas to this cluster with [`ALTER CLUSTER`].

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-cluster.md"
>}}

## See also

- [`ALTER CLUSTER`]
- [`DROP CLUSTER`]

[AWS availability zone IDs]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
[`ALTER CLUSTER`]: /sql/alter-cluster/
[`DROP CLUSTER`]: /sql/drop-cluster/
[`SELECT`]: /sql/select
[`SUBSCRIBE`]: /sql/subscribe
[`mz_cluster_replica_sizes`]: /sql/system-catalog/mz_catalog#mz_cluster_replica_sizes
