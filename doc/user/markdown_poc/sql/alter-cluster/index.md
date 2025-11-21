<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)

</div>

# ALTER CLUSTER

`ALTER CLUSTER` changes the configuration of a cluster, such as the
`SIZE` or `REPLICATON FACTOR`.

## Syntax

`ALTER CLUSTER` has the following syntax variations:

<div class="code-tabs">

<div class="tab-content">

<div id="tab-set-a-configuration" class="tab-pane"
title="Set a configuration">

To set a cluster configuration:

<div class="highlight">

``` chroma
ALTER CLUSTER <cluster_name>
SET (
    SIZE = <text>,
    REPLICATION FACTOR = <int>,
    MANAGED = <bool>,
    SCHEDULE = { MANUAL | ON REFRESH (...) }
)
[WITH ({ WAIT UNTIL READY({TIMEOUT | ON TIMEOUT {COMMIT|ROLLBACK}}) | WAIT FOR <duration> })]
;
```

</div>

</div>

<div id="tab-reset-to-default" class="tab-pane"
title="Reset to default">

To reset a cluster configuration back to its default value:

<div class="highlight">

``` chroma
ALTER CLUSTER <cluster_name>
RESET (
    REPLICATION FACTOR,
    MANAGED,
    SCHEDULE
)
;
```

</div>

</div>

<div id="tab-rename-cluster" class="tab-pane" title="Rename cluster">

To rename a cluster:

<div class="highlight">

``` chroma
ALTER CLUSTER <cluster_name> RENAME TO <new_cluster_name>;
```

</div>

</div>

<div id="tab-change-owner-to" class="tab-pane" title="Change owner to">

To change the owner of a cluster:

<div class="highlight">

``` chroma
ALTER CLUSTER <cluster_name> OWNER TO <new_owner_role>;
```

</div>

To rename a cluster, you must have ownership of the cluster and
membership in the `<new_owner_role>`. See also [Required
privileges](#required-privileges).

</div>

<div id="tab-swap-names-with" class="tab-pane" title="Swap names with">

<div class="important">

**! Important:** Information about the `SWAP WITH` operation is provided
for completeness. The `SWAP WITH` operation is used for blue/green
deployments. In general, you will not need to manually perform this
operation.

</div>

To swap the name of this cluster with another cluster:

<div class="highlight">

``` chroma
ALTER CLUSTER <cluster1> SWAP WITH <cluster2>;
```

</div>

</div>

</div>

</div>

### Cluster configuration

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
<td><p><span id="alter-cluster-size"></span> The size of the resource
allocations for the cluster.</p>
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
well as legacy sizes available.</p>
<div class="warning">
<strong>WARNING!</strong> Changing the size of a cluster may incur
downtime. For more information, see <a href="#resizing">Resizing
considerations</a>.
</div>
<p>Not available for <code>ALTER CLUSTER ... RESET</code> since there is
no default <code>SIZE</code> value.</p></td>
</tr>
<tr>
<td><code>REPLICATION FACTOR</code></td>
<td>int</td>
<td><p>The number of replicas to provision for the cluster. Each replica
of the cluster provisions a new pool of compute resources to perform
exactly the same computations on exactly the same data. For more
information, see <a href="#replication-factor">Replication factor
considerations</a>.</p>
<p>Default: <code>1</code></p></td>
</tr>
<tr>
<td><code>MANAGED</code></td>
<td>bool</td>
<td><p>Whether to automatically manage the cluster’s replicas based on
the configured size and replication factor.</p>
<p>If <code>FALSE</code>, enables the use of the <em>deprecated</em> <a
href="/docs/sql/create-cluster-replica"><code>CREATE CLUSTER REPLICA</code></a>
command. Default: <code>TRUE</code></p></td>
</tr>
<tr>
<td><code>SCHEDULE</code></td>
<td>[<code>MANUAL</code>,<code>ON REFRESH</code>]</td>
<td>The <a href="/docs/sql/create-cluster/#scheduling">scheduling
type</a> for the cluster. Default: <code>MANUAL</code></td>
</tr>
</tbody>
</table>

### `WITH` options

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Command options (optional)</th>
<th>Value</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>WAIT UNTIL READY(...)</code></td>
<td></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>TIMEOUT</code></td>
<td>The maximum duration to wait for the new replicas to be ready.</td>
</tr>
<tr>
<td><code>ON TIMEOUT</code></td>
<td>The action to take on timeout.<br />
&#10;<ul>
<li><code>COMMIT</code> cuts over to the new replica regardless of its
hydration status, which may lead to downtime.</li>
<li><code>ROLLBACK</code> removes the pending replica and returns a
timeout error.</li>
</ul>
Default: <code>COMMIT</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><code>WAIT FOR</code></td>
<td><a href="/docs/sql/types/interval/"><code>interval</code></a></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em> A
fixed duration to wait for the new replicas to be ready. This option can
lead to downtime. As such, we recommend using the
<code>WAIT UNTIL READY</code> option instead.</td>
</tr>
</tbody>
</table>

## Considerations

### Resizing

<div class="tip">

**💡 Tip:** For help sizing your clusters, navigate to **Materialize
Console \>** [**Monitoring**](/docs/console/monitoring/)\>**Environment
Overview**. This page displays cluster resource utilization and sizing
advice.

</div>

#### Available sizes

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

<div class="tip">

**💡 Tip:**

In most cases, you **should not** use legacy sizes. [M.1
sizes](#available-sizes) offer better performance per credit for nearly
all workloads. We recommend using M.1 sizes for all new clusters, and
recommend migrating existing legacy-sized clusters to M.1 sizes.
Materialize is committed to supporting customers during the transition
period as we move to deprecate legacy sizes.

The legacy size information is provided for completeness.

</div>

Valid legacy cc cluster sizes are:

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

For clusters using legacy cc sizes, resource allocations are
proportional to the number in the size name. For example, a cluster of
size `600cc` has 2x as much CPU, memory, and disk as a cluster of size
`300cc`, and 1.5x as much CPU, memory, and disk as a cluster of size
`400cc`.

Clusters of larger sizes can process data faster and handle larger data
volumes.

</div>

</div>

</div>

See also:

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog:Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).

#### Resource allocation

To determine the specific resource allocation for a given cluster size,
query the
[`mz_cluster_replica_sizes`](/docs/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table.

<div class="warning">

**WARNING!** The values in the `mz_cluster_replica_sizes` table may
change at any time. You should not rely on them for any kind of capacity
planning.

</div>

#### Downtime

Resizing operation can incur downtime unless used with WAIT UNTIL READY
option. See [zero-downtime cluster
resizing](#zero-downtime-cluster-resizing) for details.

#### Zero-downtime cluster resizing

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

You can use the `WAIT UNTIL READY` option to perform a zero-downtime
resizing, which incurs **no downtime**. Instead of restarting the
cluster, this approach spins up an additional cluster replica under the
covers with the desired new size, waits for the replica to be hydrated,
and then replaces the original replica.

<div class="highlight">

``` chroma
ALTER CLUSTER c1
SET (SIZE 'M.1-xsmall') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```

</div>

The `ALTER` statement is blocking and will return only when the new
replica becomes ready. This could take as long as the specified timeout.
During this operation, any other reconfiguration command issued against
this cluster will fail. Additionally, any connection interruption or
statement cancelation will cause a rollback — no size change will take
effect in that case.

<div class="note">

**NOTE:**

Using `WAIT UNTIL READY` requires that the session remain open: you need
to make sure the Console tab remains open or that your `psql` connection
remains stable.

Any interruption will cause a cancellation, no cluster changes will take
effect.

</div>

### Replication factor

The `REPLICATION FACTOR` option determines the number of replicas
provisioned for the cluster. Each replica of the cluster provisions a
new pool of compute resources to perform exactly the same computations
on exactly the same data. Each replica incurs cost, calculated as
`cluster size * replication factor` per second. See [Usage &
billing](/docs/administration/billing/) for more details.

#### Replication factor and fault tolerance

Provisioning more than one replica provides **fault tolerance**.
Clusters with multiple replicas can tolerate failures of the underlying
hardware that cause a replica to become unreachable. As long as one
replica of the cluster remains available, the cluster can continue to
maintain dataflows and serve queries.

<div class="note">

**NOTE:**

- Each replica incurs cost, calculated as
  `cluster size * replication factor` per second. See [Usage &
  billing](/docs/administration/billing/) for more details.

- Increasing the replication factor does **not** increase the cluster’s
  work capacity. Replicas are exact copies of one another: each replica
  must do exactly the same work (i.e., maintain the same dataflows and
  process the same queries) as all the other replicas of the cluster.

  To increase the capacity of a cluster, you must increase its
  [size](#resizing).

</div>

Materialize automatically assigns names to replicas (e.g., `r1`, `r2`).
You can view information about individual replicas in the Materialize
console and the system catalog.

#### Availability guarantees

When provisioning replicas,

- For clusters sized **under `3200cc`**, Materialize guarantees that all
  provisioned replicas in a cluster are spread across the underlying
  cloud provider’s availability zones.

- For clusters sized at **`3200cc` and above**, even distribution of
  replicas across availability zones **cannot** be guaranteed.

## Required privileges

To execute the `ALTER CLUSTER` command, you need:

- Ownership of the cluster.

- To rename a cluster, you must also have membership in the
  `<new_owner_role>`.

- To swap names with another cluster, you must also have ownership of
  the other cluster.

See also:

- [Access control (Materialize
  Cloud)](/docs/security/cloud/access-control/)
- [Access control (Materialize
  Self-Managed)](/docs/security/self-managed/access-control/)

## Examples

### Replication factor

The following example uses `ALTER CLUSTER` to update the
`REPLICATION FACTOR` of cluster `c1` to `2`:

<div class="highlight">

``` chroma
ALTER CLUSTER c1 SET (REPLICATION FACTOR 2);
```

</div>

Increasing the `REPLICATION FACTOR` increases the cluster’s [fault
tolerance](#replication-factor-and-fault-tolerance), not its work
capacity.

### Resizing

You can alter the cluster size with **no downtime** (i.e.,
[zero-downtime cluster resizing](#zero-downtime-cluster-resizing)) by
running the `ALTER CLUSTER` command with the `WAIT UNTIL READY`
[option](#with-options):

<div class="highlight">

``` chroma
ALTER CLUSTER c1
SET (SIZE 'M.1-xsmall') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```

</div>

<div class="note">

**NOTE:**

Using `WAIT UNTIL READY` requires that the session remain open: you need
to make sure the Console tab remains open or that your `psql` connection
remains stable.

Any interruption will cause a cancellation, no cluster changes will take
effect.

</div>

Alternatively, you can alter the cluster size immediately, without
waiting, by running the `ALTER CLUSTER` command:

<div class="highlight">

``` chroma
ALTER CLUSTER c1 SET (SIZE 'M.1-xsmall');
```

</div>

This will incur downtime when the cluster contains objects that need
re-hydration before they are ready. This includes indexes, materialized
views, and some types of sources.

### Schedule

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

For use cases that require using [scheduled
clusters](/docs/sql/create-cluster/#scheduling), you can set or change
the originally configured schedule and related options using the
`ALTER CLUSTER` command.

<div class="highlight">

``` chroma
ALTER CLUSTER c1 SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'));
```

</div>

See the reference documentation for
[`CREATE CLUSTER`](../create-cluster/#scheduling) or
[`CREATE MATERIALIZED VIEW`](../create-materialized-view/#refresh-strategies)
for more details on scheduled clusters.

### Converting unmanaged to managed clusters

<div class="note">

**NOTE:** When getting started with Materialize, we recommend using
managed clusters. You can convert any unmanaged clusters to managed
clusters by following the instructions below.

</div>

Alter the `managed` status of a cluster to managed:

<div class="highlight">

``` chroma
ALTER CLUSTER c1 SET (MANAGED);
```

</div>

Materialize permits converting an unmanged cluster to a managed cluster
if the following conditions are met:

- The cluster replica names are `r1`, `r2`, …, `rN`.
- All replicas have the same size.
- If there are no replicas, `SIZE` needs to be specified.
- If specified, the replication factor must match the number of
  replicas.

Note that the cluster will not have settings for the availability zones,
and compute-specific settings. If needed, these can be set explicitly.

## See also

- [`ALTER ... RENAME`](/docs/sql/alter-rename/)
- [`CREATE CLUSTER`](/docs/sql/create-cluster/)
- [`CREATE SINK`](/docs/sql/create-sink/)
- [`SHOW SINKS`](/docs/sql/show-sinks)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/alter-cluster.md"
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
