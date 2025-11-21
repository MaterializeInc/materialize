<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)

</div>

# CREATE INDEX

`CREATE INDEX` creates an in-memory [index](/docs/concepts/indexes/) on
a source, view, or materialized view.

In Materialize, indexes store query results in memory within a specific
[cluster](/docs/concepts/clusters/), and keep these results
**incrementally updated** as new data arrives. This ensures that indexed
data remains [fresh](/docs/concepts/reaction-time), reflecting the
latest changes with minimal latency.

The primary use case for indexes is to accelerate direct queries issued
via [`SELECT`](/docs/sql/select/) statements. By maintaining fresh,
up-to-date results in memory, indexes can significantly [optimize query
performance](/docs/transform-data/optimization/), reducing both response
time and compute load—especially for resource-intensive operations such
as joins, aggregations, and repeated subqueries.

Because indexes are scoped to a single cluster, they are most useful for
accelerating queries within that cluster. For results that must be
shared across clusters or persisted to durable storage, consider using a
[materialized view](/docs/sql/create-materialized-view), which also
maintains fresh results but is accessible system-wide.

### Usage patterns

#### Indexes on views vs. materialized views

In Materialize, both [indexes](/docs/concepts/indexes) on views and
[materialized views](/docs/concepts/views/#materialized-views)
incrementally update the view results when Materialize ingests new data.
Whereas materialized views persist the view results in durable storage
and can be accessed across clusters, indexes on views compute and store
view results in memory within a **single** cluster.

Some general guidelines for usage patterns include:

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Usage Pattern</th>
<th>General Guideline</th>
</tr>
</thead>
<tbody>
<tr>
<td>View results are accessed from a single cluster only;<br />
such as in a 1-cluster or a 2-cluster architecture.</td>
<td>View with an <a href="/docs/sql/create-index">index</a></td>
</tr>
<tr>
<td>View used as a building block for stacked views; i.e., views not
used to serve results.</td>
<td>View</td>
</tr>
<tr>
<td>View results are accessed across <a
href="/docs/concepts/clusters">clusters</a>;<br />
such as in a 3-cluster architecture.</td>
<td>Materialized view (in the transform cluster)<br />
Index on the materialized view (in the serving cluster)</td>
</tr>
<tr>
<td>Use with a <a href="/docs/serve-results/sink/">sink</a> or a <a
href="/docs/sql/subscribe"><code>SUBSCRIBE</code></a> operation</td>
<td>Materialized view</td>
</tr>
<tr>
<td>Use with <a
href="/docs/transform-data/patterns/temporal-filters/">temporal
filters</a></td>
<td>Materialized view</td>
</tr>
</tbody>
</table>

#### Indexes and query optimizations

You might want to create indexes when…

- You want to use non-primary keys (e.g. foreign keys) as a join
  condition. In this case, you could create an index on the columns in
  the join condition.
- You want to speed up searches filtering by literal values or
  expressions.

Specific instances where indexes can be useful to improve performance
include:

- When used in ad-hoc queries.

- When used by multiple queries within the same cluster.

- When used to enable [delta
  joins](/docs/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins).

For more information, see
[Optimization](/docs/transform-data/optimization).

#### Best practices

Before creating an index, consider the following:

- If you create stacked views (i.e., views that depend on other views)
  to reduce SQL complexity, we recommend that you create an index
  **only** on the view that will serve results, taking into account the
  expected data access patterns.

- Materialize can reuse indexes across queries that concurrently access
  the same data in memory, which reduces redundancy and resource
  utilization per query. In particular, this means that joins do **not**
  need to store data in memory multiple times.

- For queries that have no supporting indexes, Materialize uses the same
  mechanics used by indexes to optimize computations. However, since
  this underlying work is discarded after each query run, take into
  account the expected data access patterns to determine if you need to
  index or not.

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxMTk1IiBoZWlnaHQ9IjM0MSI+CiAgIDxwb2x5Z29uIHBvaW50cz0iOSAxNyAxIDEzIDEgMjEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSIxNyAxNyA5IDEzIDkgMjEiPjwvcG9seWdvbj4KICAgPHJlY3QgeD0iMzEiIHk9IjMiIHdpZHRoPSI3NiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyOSIgeT0iMSIgd2lkdGg9Ijc2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzOSIgeT0iMjEiPkNSRUFURTwvdGV4dD4KICAgPHJlY3QgeD0iNDUiIHk9IjExMyIgd2lkdGg9IjYyIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQzIiB5PSIxMTEiIHdpZHRoPSI2MiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTMiIHk9IjEzMSI+SU5ERVg8L3RleHQ+CiAgIDxyZWN0IHg9IjEyNyIgeT0iMTEzIiB3aWR0aD0iOTgiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjEyNSIgeT0iMTExIiB3aWR0aD0iOTgiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIxMzUiIHk9IjEzMSI+aW5kZXhfbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iMjY1IiB5PSIxNDUiIHdpZHRoPSIzNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyNjMiIHk9IjE0MyIgd2lkdGg9IjM0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyNzMiIHk9IjE2MyI+SU48L3RleHQ+CiAgIDxyZWN0IHg9IjMxOSIgeT0iMTQ1IiB3aWR0aD0iODQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzE3IiB5PSIxNDMiIHdpZHRoPSI4NCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzI3IiB5PSIxNjMiPkNMVVNURVI8L3RleHQ+CiAgIDxyZWN0IHg9IjQyMyIgeT0iMTQ1IiB3aWR0aD0iMTA4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI0MjEiIHk9IjE0MyIgd2lkdGg9IjEwOCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjQzMSIgeT0iMTYzIj5jbHVzdGVyX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjU3MSIgeT0iMTEzIiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNTY5IiB5PSIxMTEiIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTc5IiB5PSIxMzEiPk9OPC90ZXh0PgogICA8cmVjdCB4PSI2MzEiIHk9IjExMyIgd2lkdGg9IjgyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI2MjkiIHk9IjExMSIgd2lkdGg9IjgyIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iNjM5IiB5PSIxMzEiPm9ial9uYW1lPC90ZXh0PgogICA8cmVjdCB4PSI3NTMiIHk9IjE0NSIgd2lkdGg9IjY0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9Ijc1MSIgeT0iMTQzIiB3aWR0aD0iNjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9Ijc2MSIgeT0iMTYzIj5VU0lORzwvdGV4dD4KICAgPHJlY3QgeD0iODM3IiB5PSIxNDUiIHdpZHRoPSI3MCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iODM1IiB5PSIxNDMiIHdpZHRoPSI3MCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9Ijg0NSIgeT0iMTYzIj5tZXRob2Q8L3RleHQ+CiAgIDxyZWN0IHg9Ijk0NyIgeT0iMTEzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iOTQ1IiB5PSIxMTEiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iOTU1IiB5PSIxMzEiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjEwMTMiIHk9IjExMyIgd2lkdGg9Ijc0IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIxMDExIiB5PSIxMTEiIHdpZHRoPSI3NCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjEwMjEiIHk9IjEzMSI+Y29sX2V4cHI8L3RleHQ+CiAgIDxyZWN0IHg9IjEwMTMiIHk9IjY5IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTAxMSIgeT0iNjciIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTAyMSIgeT0iODciPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjExMjciIHk9IjExMyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjExMjUiIHk9IjExMSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMTM1IiB5PSIxMzEiPik8L3RleHQ+CiAgIDxyZWN0IHg9IjQ1IiB5PSIxODkiIHdpZHRoPSIxMzIiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDMiIHk9IjE4NyIgd2lkdGg9IjEzMiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTMiIHk9IjIwNyI+REVGQVVMVCBJTkRFWDwvdGV4dD4KICAgPHJlY3QgeD0iMjE3IiB5PSIyMjEiIHdpZHRoPSIzNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyMTUiIHk9IjIxOSIgd2lkdGg9IjM0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyMjUiIHk9IjIzOSI+SU48L3RleHQ+CiAgIDxyZWN0IHg9IjI3MSIgeT0iMjIxIiB3aWR0aD0iODQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjY5IiB5PSIyMTkiIHdpZHRoPSI4NCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjc5IiB5PSIyMzkiPkNMVVNURVI8L3RleHQ+CiAgIDxyZWN0IHg9IjM3NSIgeT0iMjIxIiB3aWR0aD0iMTA4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzNzMiIHk9IjIxOSIgd2lkdGg9IjEwOCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjM4MyIgeT0iMjM5Ij5jbHVzdGVyX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjUyMyIgeT0iMTg5IiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNTIxIiB5PSIxODciIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTMxIiB5PSIyMDciPk9OPC90ZXh0PgogICA8cmVjdCB4PSI1ODMiIHk9IjE4OSIgd2lkdGg9IjgyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI1ODEiIHk9IjE4NyIgd2lkdGg9IjgyIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iNTkxIiB5PSIyMDciPm9ial9uYW1lPC90ZXh0PgogICA8cmVjdCB4PSI3MDUiIHk9IjIyMSIgd2lkdGg9IjY0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjcwMyIgeT0iMjE5IiB3aWR0aD0iNjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjcxMyIgeT0iMjM5Ij5VU0lORzwvdGV4dD4KICAgPHJlY3QgeD0iNzg5IiB5PSIyMjEiIHdpZHRoPSI3MCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iNzg3IiB5PSIyMTkiIHdpZHRoPSI3MCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9Ijc5NyIgeT0iMjM5Ij5tZXRob2Q8L3RleHQ+CiAgIDxyZWN0IHg9IjEwNDUiIHk9IjMwNyIgd2lkdGg9IjEwMiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMTA0MyIgeT0iMzA1IiB3aWR0aD0iMTAyIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMTA1MyIgeT0iMzI1Ij53aXRoX29wdGlvbnM8L3RleHQ+CiAgIDxwYXRoIGNsYXNzPSJsaW5lIiBkPSJtMTcgMTcgaDIgbTAgMCBoMTAgbTc2IDAgaDEwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tMTI2IDExMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIyIDAgaDEwIG02MiAwIGgxMCBtMCAwIGgxMCBtOTggMCBoMTAgbTIwIDAgaDEwIG0wIDAgaDI3NiBtLTMwNiAwIGgyMCBtMjg2IDAgaDIwIG0tMzI2IDAgcTEwIDAgMTAgMTAgbTMwNiAwIHEwIC0xMCAxMCAtMTAgbS0zMTYgMTAgdjEyIG0zMDYgMCB2LTEyIG0tMzA2IDEyIHEwIDEwIDEwIDEwIG0yODYgMCBxMTAgMCAxMCAtMTAgbS0yOTYgMTAgaDEwIG0zNCAwIGgxMCBtMCAwIGgxMCBtODQgMCBoMTAgbTAgMCBoMTAgbTEwOCAwIGgxMCBtMjAgLTMyIGgxMCBtNDAgMCBoMTAgbTAgMCBoMTAgbTgyIDAgaDEwIG0yMCAwIGgxMCBtMCAwIGgxNjQgbS0xOTQgMCBoMjAgbTE3NCAwIGgyMCBtLTIxNCAwIHExMCAwIDEwIDEwIG0xOTQgMCBxMCAtMTAgMTAgLTEwIG0tMjA0IDEwIHYxMiBtMTk0IDAgdi0xMiBtLTE5NCAxMiBxMCAxMCAxMCAxMCBtMTc0IDAgcTEwIDAgMTAgLTEwIG0tMTg0IDEwIGgxMCBtNjQgMCBoMTAgbTAgMCBoMTAgbTcwIDAgaDEwIG0yMCAtMzIgaDEwIG0yNiAwIGgxMCBtMjAgMCBoMTAgbTc0IDAgaDEwIG0tMTE0IDAgbDIwIDAgbS0xIDAgcS05IDAgLTkgLTEwIGwwIC0yNCBxMCAtMTAgMTAgLTEwIG05NCA0NCBsMjAgMCBtLTIwIDAgcTEwIDAgMTAgLTEwIGwwIC0yNCBxMCAtMTAgLTEwIC0xMCBtLTk0IDAgaDEwIG0yNCAwIGgxMCBtMCAwIGg1MCBtMjAgNDQgaDEwIG0yNiAwIGgxMCBtLTExNDggMCBoMjAgbTExMjggMCBoMjAgbS0xMTY4IDAgcTEwIDAgMTAgMTAgbTExNDggMCBxMCAtMTAgMTAgLTEwIG0tMTE1OCAxMCB2NTYgbTExNDggMCB2LTU2IG0tMTE0OCA1NiBxMCAxMCAxMCAxMCBtMTEyOCAwIHExMCAwIDEwIC0xMCBtLTExMzggMTAgaDEwIG0xMzIgMCBoMTAgbTIwIDAgaDEwIG0wIDAgaDI3NiBtLTMwNiAwIGgyMCBtMjg2IDAgaDIwIG0tMzI2IDAgcTEwIDAgMTAgMTAgbTMwNiAwIHEwIC0xMCAxMCAtMTAgbS0zMTYgMTAgdjEyIG0zMDYgMCB2LTEyIG0tMzA2IDEyIHEwIDEwIDEwIDEwIG0yODYgMCBxMTAgMCAxMCAtMTAgbS0yOTYgMTAgaDEwIG0zNCAwIGgxMCBtMCAwIGgxMCBtODQgMCBoMTAgbTAgMCBoMTAgbTEwOCAwIGgxMCBtMjAgLTMyIGgxMCBtNDAgMCBoMTAgbTAgMCBoMTAgbTgyIDAgaDEwIG0yMCAwIGgxMCBtMCAwIGgxNjQgbS0xOTQgMCBoMjAgbTE3NCAwIGgyMCBtLTIxNCAwIHExMCAwIDEwIDEwIG0xOTQgMCBxMCAtMTAgMTAgLTEwIG0tMjA0IDEwIHYxMiBtMTk0IDAgdi0xMiBtLTE5NCAxMiBxMCAxMCAxMCAxMCBtMTc0IDAgcTEwIDAgMTAgLTEwIG0tMTg0IDEwIGgxMCBtNjQgMCBoMTAgbTAgMCBoMTAgbTcwIDAgaDEwIG0yMCAtMzIgaDI3NCBtMjIgLTc2IGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtLTE5MiAxNjIgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtMCAwIGgxMTIgbS0xNDIgMCBoMjAgbTEyMiAwIGgyMCBtLTE2MiAwIHExMCAwIDEwIDEwIG0xNDIgMCBxMCAtMTAgMTAgLTEwIG0tMTUyIDEwIHYxMiBtMTQyIDAgdi0xMiBtLTE0MiAxMiBxMCAxMCAxMCAxMCBtMTIyIDAgcTEwIDAgMTAgLTEwIG0tMTMyIDEwIGgxMCBtMTAyIDAgaDEwIG0yMyAtMzIgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjExODUgMjg5IDExOTMgMjg1IDExOTMgMjkzIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iMTE4NSAyODkgMTE3NyAyODUgMTE3NyAyOTMiPjwvcG9seWdvbj4KPC9zdmc+)

</div>

### `with_options`

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI0NzMiIGhlaWdodD0iMTM1Ij4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iMyIgd2lkdGg9IjU4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI5IiB5PSIxIiB3aWR0aD0iNTgiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM5IiB5PSIyMSI+V0lUSDwvdGV4dD4KICAgPHJlY3QgeD0iMTA5IiB5PSIzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTA3IiB5PSIxIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjExNyIgeT0iMjEiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjE1NSIgeT0iMyIgd2lkdGg9IjE0MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxNTMiIHk9IjEiIHdpZHRoPSIxNDAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjE2MyIgeT0iMjEiPlJFVEFJTiBISVNUT1JZPC90ZXh0PgogICA8cmVjdCB4PSIzMzUiIHk9IjM1IiB3aWR0aD0iMjgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzMzIiB5PSIzMyIgd2lkdGg9IjI4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzNDMiIHk9IjUzIj49PC90ZXh0PgogICA8cmVjdCB4PSI0MDMiIHk9IjMiIHdpZHRoPSI0OCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0MDEiIHk9IjEiIHdpZHRoPSI0OCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDExIiB5PSIyMSI+Rk9SPC90ZXh0PgogICA8cmVjdCB4PSIyNzEiIHk9IjEwMSIgd2lkdGg9IjEyOCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjY5IiB5PSI5OSIgd2lkdGg9IjEyOCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI3OSIgeT0iMTE5Ij5yZXRlbnRpb25fcGVyaW9kPC90ZXh0PgogICA8cmVjdCB4PSI0MTkiIHk9IjEwMSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQxNyIgeT0iOTkiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDI3IiB5PSIxMTkiPik8L3RleHQ+CiAgIDxwYXRoIGNsYXNzPSJsaW5lIiBkPSJtMTcgMTcgaDIgbTAgMCBoMTAgbTU4IDAgaDEwIG0wIDAgaDEwIG0yNiAwIGgxMCBtMCAwIGgxMCBtMTQwIDAgaDEwIG0yMCAwIGgxMCBtMCAwIGgzOCBtLTY4IDAgaDIwIG00OCAwIGgyMCBtLTg4IDAgcTEwIDAgMTAgMTAgbTY4IDAgcTAgLTEwIDEwIC0xMCBtLTc4IDEwIHYxMiBtNjggMCB2LTEyIG0tNjggMTIgcTAgMTAgMTAgMTAgbTQ4IDAgcTEwIDAgMTAgLTEwIG0tNTggMTAgaDEwIG0yOCAwIGgxMCBtMjAgLTMyIGgxMCBtNDggMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0yMjQgOTggbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgaDEwIG0xMjggMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0zIDAgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjQ2MyAxMTUgNDcxIDExMSA0NzEgMTE5Ij48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iNDYzIDExNSA0NTUgMTExIDQ1NSAxMTkiPjwvcG9seWdvbj4KPC9zdmc+)

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Field</th>
<th>Use</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>DEFAULT</strong></td>
<td>Creates a default index using a set of columns that uniquely
identify each row. If this set of columns can’t be inferred, all columns
are used.</td>
</tr>
<tr>
<td><em>index_name</em></td>
<td>A name for the index.</td>
</tr>
<tr>
<td><em>obj_name</em></td>
<td>The name of the source, view, or materialized view on which you want
to create an index.</td>
</tr>
<tr>
<td><em>cluster_name</em></td>
<td>The <a href="/docs/sql/create-cluster">cluster</a> to maintain this
index. If not specified, defaults to the active cluster.</td>
</tr>
<tr>
<td><em>method</em></td>
<td>The name of the index method to use. The only supported method is <a
href="/docs/overview/arrangements"><code>arrangement</code></a>.</td>
</tr>
<tr>
<td><em>col_expr</em><strong>…</strong></td>
<td>The expressions to use as the key for the index.</td>
</tr>
<tr>
<td><em>retention_period</em></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active
development.</em><br />
Duration for which Materialize retains historical data, which is useful
to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. <strong>Note:</strong> Configuring indexes to retain
history is not recommended. As an alternative, consider creating a
materialized view for your subscription query and configuring the
history retention period on the view instead. See <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>.<br />
Accepts positive <a href="/docs/sql/types/interval/">interval</a> values
(e.g. <code>'1hr'</code>).<br />
Default: <code>1s</code>.</td>
</tr>
</tbody>
</table>

## Details

### Restrictions

- You can only reference the columns available in the `SELECT` list of
  the query that defines the view. For example, if your view was defined
  as `SELECT a, b FROM src`, you can only reference columns `a` and `b`,
  even if `src` contains additional columns.

- You cannot exclude any columns from being in the index’s “value” set.
  For example, if your view is defined as `SELECT a, b FROM ...`, all
  indexes will contain `{a, b}` as their values.

  If you want to create an index that only stores a subset of these
  columns, consider creating another materialized view that uses
  `SELECT some_subset FROM this_view...`.

### Structure

Indexes in Materialize have the following structure for each unique row:

```
((tuple of indexed expressions), (tuple of the row, i.e. stored columns))
```

#### Indexed expressions vs. stored columns

Automatically created indexes will use all columns as key expressions
for the index, unless Materialize is provided or can infer a unique key
for the source or view.

For instance, unique keys can be…

- **Provided** by the schema provided for the source, e.g. through the
  Confluent Schema Registry.
- **Inferred** when the query…
  - Concludes with a `GROUP BY`.
  - Uses sources or views that have a unique key without damaging this
    property. For example, joining a view with unique keys against a
    second, where the join constraint uses foreign keys.

When creating your own indexes, you can choose the indexed expressions.

### Memory footprint

The in-memory sizes of indexes are proportional to the current size of
the source or view they represent. The actual amount of memory required
depends on several details related to the rate of compaction and the
representation of the types of data in the source or view.

Creating an index may also force the first materialization of a view,
which may cause Materialize to install a dataflow to determine and
maintain the results of the view. This dataflow may have a memory
footprint itself, in addition to that of the index.

## Examples

### Optimizing joins with indexes

You can optimize the performance of `JOIN` on two relations by ensuring
their join keys are the key columns in an index.

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW active_customers AS
    SELECT guid, geo_id, last_active_on
    FROM customer_source
    WHERE last_active_on > now() - INTERVAL '30' DAYS;

CREATE INDEX active_customers_geo_idx ON active_customers (geo_id);

CREATE MATERIALIZED VIEW active_customer_per_geo AS
    SELECT geo.name, count(*)
    FROM geo_regions AS geo
    JOIN active_customers ON active_customers.geo_id = geo.id
    GROUP BY geo.name;
```

</div>

In the above example, the index `active_customers_geo_idx`…

- Helps us because it contains a key that the view
  `active_customer_per_geo` can use to look up values for the join
  condition (`active_customers.geo_id`).

  Because this index is exactly what the query requires, the Materialize
  optimizer will choose to use `active_customers_geo_idx` rather than
  build and maintain a private copy of the index just for this query.

- Obeys our restrictions by containing only a subset of columns in the
  result set.

### Speed up filtering with indexes

If you commonly filter by a certain column being equal to a literal
value, you can set up an index over that column to speed up your
queries:

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW active_customers AS
    SELECT guid, geo_id, last_active_on
    FROM customer_source
    GROUP BY geo_id;

CREATE INDEX active_customers_idx ON active_customers (guid);

-- This should now be very fast!
SELECT * FROM active_customers WHERE guid = 'd868a5bf-2430-461d-a665-40418b1125e7';

-- Using indexed expressions:
CREATE INDEX active_customers_exp_idx ON active_customers (upper(guid));
SELECT * FROM active_customers WHERE upper(guid) = 'D868A5BF-2430-461D-A665-40418B1125E7';

-- Filter using an expression in one field and a literal in another field:
CREATE INDEX active_customers_exp_field_idx ON active_customers (upper(guid), geo_id);
SELECT * FROM active_customers WHERE upper(guid) = 'D868A5BF-2430-461D-A665-40418B1125E7' and geo_id = 'ID_8482';
```

</div>

Create an index with an expression to improve query performance over a
frequently used expression, and avoid building downstream views to apply
the function like the one used in the example: `upper()`. Take into
account that aggregations like `count()` cannot be used as indexed
expressions.

For more details on using indexes to optimize queries, see
[Optimization](../../ops/optimization/).

## Privileges

The privileges required to execute this statement are:

- Ownership of the object on which to create the index.
- `CREATE` privileges on the containing schema.
- `CREATE` privileges on the containing cluster.
- `USAGE` privileges on all types used in the index definition.
- `USAGE` privileges on the schemas that all types in the statement are
  contained in.

## Related pages

- [`SHOW INDEXES`](../show-indexes)
- [`DROP INDEX`](../drop-index)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-index.md"
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
