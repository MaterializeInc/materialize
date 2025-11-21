<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)

</div>

# CREATE CLUSTER

`CREATE CLUSTER` creates a new [cluster](/docs/concepts/clusters/).

## Conceptual framework

A cluster is a pool of compute resources (CPU, memory, and scratch disk
space) for running your workloads.

The following operations require compute resources in Materialize, and
so need to be associated with a cluster:

- Executing [`SELECT`](/docs/sql/select) and
  [`SUBSCRIBE`](/docs/sql/subscribe) statements.
- Maintaining [indexes](/docs/concepts/indexes/) and [materialized
  views](/docs/concepts/views/#materialized-views).
- Maintaining [sources](/docs/concepts/sources/) and
  [sinks](/docs/concepts/sinks/).

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIzNzMiIGhlaWdodD0iMTQ3Ij4KICAgPHBvbHlnb24gcG9pbnRzPSIxMSAxNyAzIDEzIDMgMjEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSIxOSAxNyAxMSAxMyAxMSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMyIgeT0iMyIgd2lkdGg9Ijc2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjMxIiB5PSIxIiB3aWR0aD0iNzYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjQxIiB5PSIyMSI+Q1JFQVRFPC90ZXh0PgogICA8cmVjdCB4PSIxMjkiIHk9IjMiIHdpZHRoPSI4NCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMjciIHk9IjEiIHdpZHRoPSI4NCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTM3IiB5PSIyMSI+Q0xVU1RFUjwvdGV4dD4KICAgPHJlY3QgeD0iMjMzIiB5PSIzIiB3aWR0aD0iNTYiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjIzMSIgeT0iMSIgd2lkdGg9IjU2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjQxIiB5PSIyMSI+bmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iMzA5IiB5PSIzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzA3IiB5PSIxIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjMxNyIgeT0iMjEiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjQ1IiB5PSIxMTMiIHdpZHRoPSIxMTIiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjQzIiB5PSIxMTEiIHdpZHRoPSIxMTIiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI1MyIgeT0iMTMxIj5jbHVzdGVyX29wdGlvbjwvdGV4dD4KICAgPHJlY3QgeD0iMTc3IiB5PSIxMTMiIHdpZHRoPSIyOCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxNzUiIHk9IjExMSIgd2lkdGg9IjI4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxODUiIHk9IjEzMSI+PTwvdGV4dD4KICAgPHJlY3QgeD0iMjI1IiB5PSIxMTMiIHdpZHRoPSI1NCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjIzIiB5PSIxMTEiIHdpZHRoPSI1NCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjIzMyIgeT0iMTMxIj52YWx1ZTwvdGV4dD4KICAgPHJlY3QgeD0iNDUiIHk9IjY5IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDMiIHk9IjY3IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjUzIiB5PSI4NyI+LDwvdGV4dD4KICAgPHJlY3QgeD0iMzE5IiB5PSIxMTMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzMTciIHk9IjExMSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzMjciIHk9IjEzMSI+KTwvdGV4dD4KICAgPHBhdGggY2xhc3M9ImxpbmUiIGQ9Im0xOSAxNyBoMiBtMCAwIGgxMCBtNzYgMCBoMTAgbTAgMCBoMTAgbTg0IDAgaDEwIG0wIDAgaDEwIG01NiAwIGgxMCBtMCAwIGgxMCBtMjYgMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0zNTQgMTEwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMjIgMCBoMTAgbTExMiAwIGgxMCBtMCAwIGgxMCBtMjggMCBoMTAgbTAgMCBoMTAgbTU0IDAgaDEwIG0tMjc0IDAgbDIwIDAgbS0xIDAgcS05IDAgLTkgLTEwIGwwIC0yNCBxMCAtMTAgMTAgLTEwIG0yNTQgNDQgbDIwIDAgbS0yMCAwIHExMCAwIDEwIC0xMCBsMCAtMjQgcTAgLTEwIC0xMCAtMTAgbS0yNTQgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDIxMCBtMjAgNDQgaDEwIG0yNiAwIGgxMCBtMyAwIGgtMyIgLz4KICAgPHBvbHlnb24gcG9pbnRzPSIzNjMgMTI3IDM3MSAxMjMgMzcxIDEzMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjM2MyAxMjcgMzU1IDEyMyAzNTUgMTMxIj48L3BvbHlnb24+Cjwvc3ZnPg==)

</div>

### Options

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field</th>
<th>Value</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>SIZE</code></td>
<td>text</td>
<td><p>The size of the resource allocations for the cluster.</p>
<ul>
<li><strong>M.1-nano</strong></li>
<li><strong>M.1-micro</strong></li>
<li><strong>M.1-xsmall</strong></li>
<li><strong>M.1-small</strong></li>
<li><strong>M.1-medium</strong></li>
<li><strong>M.1-large</strong></li>
<li><strong>M.1-1.5xlarge</strong></li>
<li><strong>M.1-2xlarge</strong></li>
<li><strong>M.1-3xlarge</strong></li>
<li><strong>M.1-4xlarge</strong></li>
<li><strong>M.1-8xlarge</strong></li>
<li><strong>M.1-16xlarge</strong></li>
<li><strong>M.1-32xlarge</strong></li>
<li><strong>M.1-64xlarge</strong></li>
<li><strong>M.1-128xlarge</strong></li>
</ul>
<p>See <a href="/docs/sql/create-cluster#size">Size</a> for details as
well as legacy sizes available.</p></td>
</tr>
<tr>
<td><code>REPLICATION FACTOR</code></td>
<td>text</td>
<td><p>The number of replicas to provision for the cluster. See <a
href="/docs/sql/create-cluster#replication-factor">Replication
factor</a> for details.</p>
<p>Default: <code>1</code></p></td>
</tr>
<tr>
<td><code>MANAGED</code></td>
<td>bool</td>
<td>Whether to automatically manage the cluster’s replicas based on the
configured size and replication factor. <span
id="unmanaged-clusters"></span> Specify <code>FALSE</code> to create an
<strong>unmanaged</strong> cluster. With unmanaged clusters, you need to
manually manage the cluster’s replicas using the the <a
href="/docs/sql/create-cluster-replica"><code>CREATE CLUSTER REPLICA</code></a>
and <a
href="/docs/sql/drop-cluster-replica"><code>DROP CLUSTER REPLICA</code></a>
commands. When creating an unmanaged cluster, you must specify the
<code>REPLICAS</code> option as well.
<div class="tip">
<strong>💡 Tip:</strong> When getting started with Materialize, we
recommend starting with managed clusters.
</div>
Default: <code>TRUE</code></td>
</tr>
<tr>
<td><code>SCHEDULE</code></td>
<td>[<code>MANUAL</code>,<code>ON REFRESH</code>]</td>
<td><p>The <a href="/docs/sql/create-cluster/#scheduling">scheduling
type</a> for the cluster.</p>
<p>Default: <code>MANUAL</code></p></td>
</tr>
</tbody>
</table>

## Details

### Initial state

Each Materialize region initially contains a [pre-installed
cluster](/docs/sql/show-clusters/#pre-installed-clusters) named
`quickstart` with a size of `25cc` and a replication factor of `1`. You
can drop or alter this cluster to suit your needs.

### Choosing a cluster

When performing an operation that requires a cluster, you must specify
which cluster you want to use. Not explicitly naming a cluster uses your
session’s active cluster.

To show your session’s active cluster, use the [`SHOW`](/docs/sql/show)
command:

<div class="highlight">

``` chroma
SHOW cluster;
```

</div>

To switch your session’s active cluster, use the [`SET`](/docs/sql/set)
command:

<div class="highlight">

``` chroma
SET cluster = other_cluster;
```

</div>

### Resource isolation

Clusters provide **resource isolation.** Each cluster provisions a
dedicated pool of CPU, memory, and, optionally, scratch disk space.

All workloads on a given cluster will compete for access to these
compute resources. However, workloads on different clusters are strictly
isolated from one another. A given workload has access only to the CPU,
memory, and scratch disk of the cluster that it is running on.

Clusters are commonly used to isolate different classes of workloads.
For example, you could place your development workloads in a cluster
named `dev` and your production workloads in a cluster named `prod`.

<span id="legacy-sizes"></span>

### Size

The `SIZE` option determines the amount of compute resources available
to the cluster.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-m1-clusters" class="tab-pane" title="M.1 Clusters">

<div class="note">

**NOTE:** The values set forth in the table are solely for illustrative
purposes. Materialize reserves the right to change the capacity at any
time. As such, you acknowledge and agree that those values in this table
may change at any time, and you should not rely on these values for any
capacity planning.

</div>

| Cluster size | Compute Credits/Hour | Total Capacity | Notes |
|----|----|----|----|
| **M.1-nano** | 0.75 | 26 GiB |  |
| **M.1-micro** | 1.5 | 53 GiB |  |
| **M.1-xsmall** | 3 | 106 GiB |  |
| **M.1-small** | 6 | 212 GiB |  |
| **M.1-medium** | 9 | 318 GiB |  |
| **M.1-large** | 12 | 424 GiB |  |
| **M.1-1.5xlarge** | 18 | 636 GiB |  |
| **M.1-2xlarge** | 24 | 849 GiB |  |
| **M.1-3xlarge** | 36 | 1273 GiB |  |
| **M.1-4xlarge** | 48 | 1645 GiB |  |
| **M.1-8xlarge** | 96 | 3290 GiB |  |
| **M.1-16xlarge** | 192 | 6580 GiB | Available upon request |
| **M.1-32xlarge** | 384 | 13160 GiB | Available upon request |
| **M.1-64xlarge** | 768 | 26320 GiB | Available upon request |
| **M.1-128xlarge** | 1536 | 52640 GiB | Available upon request |

</div>

<div id="tab-legacy-cc-clusters" class="tab-pane"
title="Legacy cc Clusters">

Materialize offers the following legacy cc cluster sizes:

<div class="tip">

**💡 Tip:**

In most cases, you **should not** use legacy sizes. [M.1 sizes](#size)
offer better performance per credit for nearly all workloads. We
recommend using M.1 sizes for all new clusters, and recommend migrating
existing legacy-sized clusters to M.1 sizes. Materialize is committed to
supporting customers during the transition period as we move to
deprecate legacy sizes.

The legacy size information is provided for completeness.

</div>

- `25cc`
- `50cc`
- `100cc`
- `200cc`
- `300cc`
- `400cc`
- `600cc`
- `800cc`
- `1200cc`
- `1600cc`
- `3200cc`
- `6400cc`
- `128C`
- `256C`
- `512C`

The resource allocations are proportional to the number in the size
name. For example, a cluster of size `600cc` has 2x as much CPU, memory,
and disk as a cluster of size `300cc`, and 1.5x as much CPU, memory, and
disk as a cluster of size `400cc`. To determine the specific resource
allocations for a size, query the
[`mz_cluster_replica_sizes`](/docs/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
table.

<div class="warning">

**WARNING!** The values in the `mz_cluster_replica_sizes` table may
change at any time. You should not rely on them for any kind of capacity
planning.

</div>

Clusters of larger sizes can process data faster and handle larger data
volumes.

</div>

<div id="tab-legacy-t-shirt-clusters" class="tab-pane"
title="Legacy t-shirt Clusters">

Materialize also offers some legacy t-shirt cluster sizes for upsert
sources.

<div class="tip">

**💡 Tip:**

In most cases, you **should not** use legacy t-shirt sizes. [M.1
sizes](#size) offer better performance per credit for nearly all
workloads. We recommend using M.1 sizes for all new clusters, and
recommend migrating existing legacy-sized clusters to M.1 sizes.
Materialize is committed to supporting customers during the transition
period as we move to deprecate legacy sizes.

The legacy size information is provided for completeness.

</div>

<div class="warning">

**WARNING!** Materialize regions that were enabled after 15 April 2024
do not have access to legacy sizes.

</div>

When legacy sizes are enabled for a region, the following sizes are
available:

- `3xsmall`
- `2xsmall`
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

</div>

</div>

</div>

See also:

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog:Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).

#### Cluster resizing

You can change the size of a cluster to respond to changes in your
workload using [`ALTER CLUSTER`](/docs/sql/alter-cluster). Depending on
the type of objects the cluster is hosting, this operation **might incur
downtime**.

See the reference documentation for
[`ALTER CLUSTER`](/docs/sql/alter-cluster#zero-downtime-cluster-resizing)
for more details on cluster resizing.

### Replication factor

The `REPLICATION FACTOR` option determines the number of replicas
provisioned for the cluster. Each replica of the cluster provisions a
new pool of compute resources to perform exactly the same computations
on exactly the same data.

Provisioning more than one replica improves **fault tolerance**.
Clusters with multiple replicas can tolerate failures of the underlying
hardware that cause a replica to become unreachable. As long as one
replica of the cluster remains available, the cluster can continue to
maintain dataflows and serve queries.

Materialize makes the following guarantees when provisioning replicas:

- Replicas of a given cluster are never provisioned on the same
  underlying hardware.
- Replicas of a given cluster are spread as evenly as possible across
  the underlying cloud provider’s availability zones.

Materialize automatically assigns names to replicas like `r1`, `r2`,
etc. You can view information about individual replicas in the console
and the system catalog, but you cannot directly modify individual
replicas.

You can pause a cluster’s work by specifying a replication factor of
`0`. Doing so removes all replicas of the cluster. Any indexes,
materialized views, sources, and sinks on the cluster will cease to make
progress, and any queries directed to the cluster will block. You can
later resume the cluster’s work by using
[`ALTER CLUSTER`](/docs/sql/alter-cluster/) to set a nonzero replication
factor.

<div class="note">

**NOTE:**

A common misconception is that increasing a cluster’s replication factor
will increase its capacity for work. This is not the case. Increasing
the replication factor increases the **fault tolerance** of the cluster,
not its capacity for work. Replicas are exact copies of one another:
each replica must do exactly the same work (i.e., maintain the same
dataflows and process the same queries) as all the other replicas of the
cluster.

To increase a cluster’s capacity, you should instead increase the
cluster’s [size](#size).

</div>

### Credit usage

Each [replica](#replication-factor) of the cluster consumes credits at a
rate determined by the cluster’s size:

| Size     | Legacy size | Credits per replica per hour |
|----------|-------------|------------------------------|
| `25cc`   | `3xsmall`   | 0.25                         |
| `50cc`   | `2xsmall`   | 0.5                          |
| `100cc`  | `xsmall`    | 1                            |
| `200cc`  | `small`     | 2                            |
| `300cc`  |             | 3                            |
| `400cc`  | `medium`    | 4                            |
| `600cc`  |             | 6                            |
| `800cc`  | `large`     | 8                            |
| `1200cc` |             | 12                           |
| `1600cc` | `xlarge`    | 16                           |
| `3200cc` | `2xlarge`   | 32                           |
| `6400cc` | `3xlarge`   | 64                           |
| `128C`   | `4xlarge`   | 128                          |
| `256C`   | `5xlarge`   | 256                          |
| `512C`   | `6xlarge`   | 512                          |

Credit usage is measured at a one second granularity. For a given
replica, credit usage begins when a `CREATE CLUSTER` or
[`ALTER CLUSTER`](/docs/sql/alter-cluster/) statement provisions the
replica and ends when an [`ALTER CLUSTER`](/docs/sql/alter-cluster/) or
[`DROP CLUSTER`](/docs/sql/drop-cluster/) statement deprovisions the
replica.

A cluster with a [replication factor](#replication-factor) of zero uses
no credits.

As an example, consider the following sequence of events:

| Time | Event |
|----|----|
| 2023-08-29 3:45:00 | `CREATE CLUSTER c (SIZE '400cc', REPLICATION FACTOR 2`) |
| 2023-08-29 3:45:45 | `ALTER CLUSTER c SET (REPLICATION FACTOR 1)` |
| 2023-08-29 3:47:15 | `DROP CLUSTER c` |

Cluster `c` will have consumed 0.4 credits in total:

- Replica `c.r1` was provisioned from 3:45:00 to 3:47:15, consuming 0.3
  credits.
- Replica `c.r2` was provisioned from 3:45:00 to 3:45:45, consuming 0.1
  credits.

### Scheduling

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

To support [scheduled refreshes in materialized
views](../create-materialized-view/#refresh-strategies), you can
configure a cluster to automatically turn on and off using the
`SCHEDULE...ON REFRESH` syntax.

<div class="highlight">

``` chroma
CREATE CLUSTER my_scheduled_cluster (
  SIZE = 'M.1-large',
  SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour')
);
```

</div>

Scheduled clusters should **only** contain materialized views configured
with a non-default [refresh
strategy](../create-materialized-view/#refresh-strategies) (and any
indexes built on these views). These clusters will automatically turn on
(i.e., be provisioned with compute resources) based on the configured
refresh strategies, and **only** consume credits for the duration of the
refreshes.

It’s not possible to manually turn on a cluster with `ON REFRESH`
scheduling. If you need to turn on a cluster outside its schedule, you
can temporarily disable scheduling and provision compute resources using
[`ALTER CLUSTER`](../alter-cluster/#schedule):

<div class="highlight">

``` chroma
ALTER CLUSTER my_scheduled_cluster SET (SCHEDULE = MANUAL, REPLICATION FACTOR = 1);
```

</div>

To re-enable scheduling:

<div class="highlight">

``` chroma
ALTER CLUSTER my_scheduled_cluster
SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'));
```

</div>

#### Hydration time estimate

**Syntax:** `HYDRATION TIME ESTIMATE` *interval*

By default, scheduled clusters will turn on at the scheduled refresh
time. To avoid [unavailability of the objects scheduled for
refresh](/docs/sql/create-materialized-view/#querying-materialized-views-with-refresh-strategies)
during the refresh operation, we recommend turning the cluster on ahead
of the scheduled time to allow hydration to complete. This can be
controlled using the `HYDRATION TIME ESTIMATE` clause.

#### Scheduling strategy

To check the scheduling strategy associated with a cluster, you can
query the
[`mz_internal.mz_cluster_schedules`](/docs/sql/system-catalog/mz_internal/#mz_cluster_schedules)
system catalog table:

<div class="highlight">

``` chroma
SELECT c.id AS cluster_id,
       c.name AS cluster_name,
       cs.type AS schedule_type,
       cs.refresh_hydration_time_estimate
FROM mz_internal.mz_cluster_schedules cs
JOIN mz_clusters c ON cs.cluster_id = c.id
WHERE c.name = 'my_refresh_cluster';
```

</div>

To check if a scheduled cluster is turned on, you can query the
[`mz_catalog.mz_cluster_replicas`](/docs/sql/system-catalog/mz_catalog/#mz_cluster_replicas)
system catalog table:

<div class="highlight">

``` chroma
SELECT cs.cluster_id,
       -- A cluster with scheduling is "on" when it has compute resources
       -- (i.e. a replica) attached.
       CASE WHEN cr.id IS NOT NULL THEN true
       ELSE false END AS is_on
FROM mz_internal.mz_cluster_schedules cs
JOIN mz_clusters c ON cs.cluster_id = c.id AND cs.type = 'on-refresh'
LEFT JOIN mz_cluster_replicas cr ON c.id = cr.cluster_id;
```

</div>

You can also use the [audit
log](../system-catalog/mz_catalog/#mz_audit_events) to observe the
commands that are automatically run when a scheduled cluster is turned
on and off for materialized view refreshes:

<div class="highlight">

``` chroma
SELECT *
FROM mz_audit_events
WHERE object_type = 'cluster-replica'
ORDER BY occurred_at DESC;
```

</div>

Any commands attributed to scheduled refreshes will be marked with
`"reason":"schedule"` under the `details` column.

### Known limitations

Clusters have several known limitations:

- When a cluster using legacy cc size of `3200cc` or larger uses
  multiple replicas, those replicas are not guaranteed to be spread
  evenly across the underlying cloud provider’s availability zones.

## Examples

### Basic

Create a cluster with two `M.1-large` replicas:

<div class="highlight">

``` chroma
CREATE CLUSTER c1 (SIZE = 'M.1-large', REPLICATION FACTOR = 2);
```

</div>

### Empty

Create a cluster with no replicas:

<div class="highlight">

``` chroma
CREATE CLUSTER c1 (SIZE 'M.1-xsmall', REPLICATION FACTOR = 0);
```

</div>

You can later add replicas to this cluster with
[`ALTER CLUSTER`](/docs/sql/alter-cluster/).

## Privileges

The privileges required to execute this statement are:

- `CREATECLUSTER` privileges on the system.

## See also

- [`ALTER CLUSTER`](/docs/sql/alter-cluster/)
- [`DROP CLUSTER`](/docs/sql/drop-cluster/)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-cluster.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
