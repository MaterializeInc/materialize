<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)

</div>

# CREATE CLUSTER REPLICA

`CREATE CLUSTER REPLICA` provisions a new replica for an [**unmanaged**
cluster](/docs/self-managed/v25.2/sql/create-cluster/#unmanaged-clusters).

<div class="tip">

**💡 Tip:** When getting started with Materialize, we recommend starting
with managed clusters.

</div>

## Conceptual framework

A cluster consists of zero or more replicas. Each replica of a cluster
is a pool of compute resources that performs exactly the same
computations on exactly the same data.

Using multiple replicas of a cluster facilitates **fault tolerance**.
Clusters with multiple replicas can tolerate failures of the underlying
hardware or network. As long as one replica remains reachable, the
cluster as a whole remains available.

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MzUiIGhlaWdodD0iMTYzIj4KICAgPHBvbHlnb24gcG9pbnRzPSIxMSAxNyAzIDEzIDMgMjEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSIxOSAxNyAxMSAxMyAxMSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMyIgeT0iMyIgd2lkdGg9Ijc2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjMxIiB5PSIxIiB3aWR0aD0iNzYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjQxIiB5PSIyMSI+Q1JFQVRFPC90ZXh0PgogICA8cmVjdCB4PSIxMjkiIHk9IjMiIHdpZHRoPSI4NCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMjciIHk9IjEiIHdpZHRoPSI4NCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTM3IiB5PSIyMSI+Q0xVU1RFUjwvdGV4dD4KICAgPHJlY3QgeD0iMjMzIiB5PSIzIiB3aWR0aD0iODAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjMxIiB5PSIxIiB3aWR0aD0iODAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjI0MSIgeT0iMjEiPlJFUExJQ0E8L3RleHQ+CiAgIDxyZWN0IHg9IjMzMyIgeT0iMyIgd2lkdGg9IjEwOCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMzMxIiB5PSIxIiB3aWR0aD0iMTA4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMzQxIiB5PSIyMSI+Y2x1c3Rlcl9uYW1lPC90ZXh0PgogICA8cmVjdCB4PSI0NjEiIHk9IjMiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0NTkiIHk9IjEiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDY5IiB5PSIyMSI+LjwvdGV4dD4KICAgPHJlY3QgeD0iMjUiIHk9IjExMyIgd2lkdGg9IjEwNiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjMiIHk9IjExMSIgd2lkdGg9IjEwNiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjMzIiB5PSIxMzEiPnJlcGxpY2FfbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iMTUxIiB5PSIxMTMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxNDkiIHk9IjExMSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxNTkiIHk9IjEzMSI+KDwvdGV4dD4KICAgPHJlY3QgeD0iMjM3IiB5PSIxMTMiIHdpZHRoPSI2MiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjM1IiB5PSIxMTEiIHdpZHRoPSI2MiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI0NSIgeT0iMTMxIj5vcHRpb248L3RleHQ+CiAgIDxyZWN0IHg9IjMxOSIgeT0iMTEzIiB3aWR0aD0iMjgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzE3IiB5PSIxMTEiIHdpZHRoPSIyOCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzI3IiB5PSIxMzEiPj08L3RleHQ+CiAgIDxyZWN0IHg9IjM2NyIgeT0iMTEzIiB3aWR0aD0iNTQiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjM2NSIgeT0iMTExIiB3aWR0aD0iNTQiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzNzUiIHk9IjEzMSI+dmFsdWU8L3RleHQ+CiAgIDxyZWN0IHg9IjIzNyIgeT0iNjkiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyMzUiIHk9IjY3IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjI0NSIgeT0iODciPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjQ4MSIgeT0iMTEzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDc5IiB5PSIxMTEiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDg5IiB5PSIxMzEiPik8L3RleHQ+CiAgIDxwYXRoIGNsYXNzPSJsaW5lIiBkPSJtMTkgMTcgaDIgbTAgMCBoMTAgbTc2IDAgaDEwIG0wIDAgaDEwIG04NCAwIGgxMCBtMCAwIGgxMCBtODAgMCBoMTAgbTAgMCBoMTAgbTEwOCAwIGgxMCBtMCAwIGgxMCBtMjQgMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS01MDQgMTEwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGgxMCBtMTA2IDAgaDEwIG0wIDAgaDEwIG0yNiAwIGgxMCBtNDAgMCBoMTAgbTYyIDAgaDEwIG0wIDAgaDEwIG0yOCAwIGgxMCBtMCAwIGgxMCBtNTQgMCBoMTAgbS0yMjQgMCBsMjAgMCBtLTEgMCBxLTkgMCAtOSAtMTAgbDAgLTI0IHEwIC0xMCAxMCAtMTAgbTIwNCA0NCBsMjAgMCBtLTIwIDAgcTEwIDAgMTAgLTEwIGwwIC0yNCBxMCAtMTAgLTEwIC0xMCBtLTIwNCAwIGgxMCBtMjQgMCBoMTAgbTAgMCBoMTYwIG0tMjQ0IDQ0IGgyMCBtMjQ0IDAgaDIwIG0tMjg0IDAgcTEwIDAgMTAgMTAgbTI2NCAwIHEwIC0xMCAxMCAtMTAgbS0yNzQgMTAgdjE0IG0yNjQgMCB2LTE0IG0tMjY0IDE0IHEwIDEwIDEwIDEwIG0yNDQgMCBxMTAgMCAxMCAtMTAgbS0yNTQgMTAgaDEwIG0wIDAgaDIzNCBtMjAgLTM0IGgxMCBtMjYgMCBoMTAgbTMgMCBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTI1IDEyNyA1MzMgMTIzIDUzMyAxMzEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSI1MjUgMTI3IDUxNyAxMjMgNTE3IDEzMSI+PC9wb2x5Z29uPgo8L3N2Zz4=)

</div>

| Field          | Use                                          |
|----------------|----------------------------------------------|
| *cluster_name* | The cluster you want to attach a replica to. |
| *replica_name* | A name for this replica.                     |

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
<td><code>text</code></td>
<td>The size of the replica. For valid sizes, see <a
href="/docs/self-managed/v25.2/sql/create-cluster-replica#size">Size</a>.</td>
</tr>
<tr>
<td><code>AVAILABILITY ZONE</code></td>
<td><code>text</code></td>
<td>The availability zone of the underlying cloud provider in which to
provision the replica. You must specify an <a
href="https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html">AWS
availability zone ID</a> in either <code>us-east-1</code>,
<code>eu-west-1</code>, or <code>us-west-2</code>, e.g.
<code>use1-az1</code>. Note that you must use the zone’s ID, not its
name.</td>
</tr>
<tr>
<td><code>INTROSPECTION INTERVAL</code></td>
<td><code>interval</code></td>
<td>The interval at which to collect introspection data. See <a
href="/docs/self-managed/v25.2/ops/troubleshooting">Troubleshooting</a>
for details about introspection data. The special value <code>0</code>
entirely disables the gathering of introspection data.<br />
Default: <code>1s</code></td>
</tr>
<tr>
<td><code>INTROSPECTION DEBUGGING</code></td>
<td><code>bool</code></td>
<td>Whether to introspect the gathering of the introspection data.<br />
Default: <code>FALSE</code></td>
</tr>
</tbody>
</table>

## Details

### Size

The `SIZE` option for replicas is identical to the [`SIZE` option for
clusters](/docs/self-managed/v25.2/sql/create-cluster/#size) option,
except that the size applies only to the new replica.

### Credit usage

The replica will consume credits at a rate determined by its size:

| Size     | Legacy size | Credits per hour |
|----------|-------------|------------------|
| `25cc`   | `3xsmall`   | 0.25             |
| `50cc`   | `2xsmall`   | 0.5              |
| `100cc`  | `xsmall`    | 1                |
| `200cc`  | `small`     | 2                |
| `300cc`  |             | 3                |
| `400cc`  | `medium`    | 4                |
| `600cc`  |             | 6                |
| `800cc`  | `large`     | 8                |
| `1200cc` |             | 12               |
| `1600cc` | `xlarge`    | 16               |
| `3200cc` | `2xlarge`   | 32               |
| `6400cc` | `3xlarge`   | 64               |
| `128C`   | `4xlarge`   | 128              |
| `256C`   | `5xlarge`   | 256              |
| `512C`   | `6xlarge`   | 512              |

Credit usage is measured at a one second granularity. Credit usage
begins when a `CREATE CLUSTER REPLICA` provisions the replica and ends
when a
[`DROP CLUSTER REPLICA`](/docs/self-managed/v25.2/sql/drop-cluster-replica)
statement deprovisions the replica.

### Homogeneous vs. heterogeneous hardware provisioning

Because Materialize uses active replication, all replicas will be
instructed to do the same work, irrespective of their resource
allocation.

For the most stable performance, we recommend using the same size and
disk configuration for all replicas.

However, it is possible to use different replica configurations in the
same cluster. In these cases, the replicas with less resources will
likely be continually burdened with a backlog of work. If all of the
faster replicas become unreachable, the system might experience delays
in replying to requests while the slower replicas catch up to the last
known time that the faster machines had computed.

## Example

<div class="highlight">

``` chroma
CREATE CLUSTER REPLICA c1.r1 (SIZE = '400cc');
```

</div>

## Privileges

The privileges required to execute this statement are:

- Ownership of the cluster.

## See also

- [`DROP CLUSTER REPLICA`](/docs/self-managed/v25.2/sql/drop-cluster-replica)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-cluster-replica.md"
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
