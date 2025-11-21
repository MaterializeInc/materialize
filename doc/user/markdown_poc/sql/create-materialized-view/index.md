<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)

</div>

# CREATE MATERIALIZED VIEW

`CREATE MATERIALIZED VIEW` defines a view that maintains [fresh
results](/docs/concepts/reaction-time) by persisting them in durable
storage and incrementally updating them as new data arrives.

Materialized views are particularly useful when you need **cross-cluster
access** to results or want to sink data to external systems like
[Kafka](/docs/sql/create-sink). When you create a materialized view, you
specify a [cluster](/docs/concepts/clusters/) responsible for
maintaining it, but the results can be **queried from any cluster**.
This allows you to separate the compute resources used for view
maintenance from those used for serving queries.

If you do not need durability or cross-cluster sharing, and you are
primarily interested in fast query performance within a single cluster,
you may prefer to [create a view and index
it](/docs/concepts/views/#views). In Materialize, [indexes on
views](/docs/concepts/indexes/) also maintain results incrementally, but
store them in memory, scoped to the cluster where the index was created.
This approach offers lower latency for direct querying within that
cluster.

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1ODkiIGhlaWdodD0iMzU5Ij4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iMyIgd2lkdGg9Ijc2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI5IiB5PSIxIiB3aWR0aD0iNzYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM5IiB5PSIyMSI+Q1JFQVRFPC90ZXh0PgogICA8cmVjdCB4PSIxNDciIHk9IjM1IiB3aWR0aD0iMTA4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjE0NSIgeT0iMzMiIHdpZHRoPSIxMDgiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjE1NSIgeT0iNTMiPk9SIFJFUExBQ0U8L3RleHQ+CiAgIDxyZWN0IHg9IjI5NSIgeT0iMyIgd2lkdGg9IjE2NiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyOTMiIHk9IjEiIHdpZHRoPSIxNjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjMwMyIgeT0iMjEiPk1BVEVSSUFMSVpFRCBWSUVXPC90ZXh0PgogICA8cmVjdCB4PSI0NSIgeT0iMTc3IiB3aWR0aD0iMTIwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQzIiB5PSIxNzUiIHdpZHRoPSIxMjAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjUzIiB5PSIxOTUiPklGIE5PVCBFWElTVFM8L3RleHQ+CiAgIDxyZWN0IHg9IjIwNSIgeT0iMTQ1IiB3aWR0aD0iOTIiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjIwMyIgeT0iMTQzIiB3aWR0aD0iOTIiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIyMTMiIHk9IjE2MyI+dmlld19uYW1lPC90ZXh0PgogICA8cmVjdCB4PSIzMzciIHk9IjE0NSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjMzNSIgeT0iMTQzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM0NSIgeT0iMTYzIj4oPC90ZXh0PgogICA8cmVjdCB4PSI0MDMiIHk9IjE0NSIgd2lkdGg9Ijc4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI0MDEiIHk9IjE0MyIgd2lkdGg9Ijc4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iNDExIiB5PSIxNjMiPmNvbF9pZGVudDwvdGV4dD4KICAgPHJlY3QgeD0iNDAzIiB5PSIxMDEiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0MDEiIHk9Ijk5IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjQxMSIgeT0iMTE5Ij4sPC90ZXh0PgogICA8cmVjdCB4PSI1MjEiIHk9IjE0NSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjUxOSIgeT0iMTQzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjUyOSIgeT0iMTYzIj4pPC90ZXh0PgogICA8cmVjdCB4PSI2MCIgeT0iMjU5IiB3aWR0aD0iMTA0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjU4IiB5PSIyNTciIHdpZHRoPSIxMDQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjY4IiB5PSIyNzciPklOIENMVVNURVI8L3RleHQ+CiAgIDxyZWN0IHg9IjE4NCIgeT0iMjU5IiB3aWR0aD0iMTA4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIxODIiIHk9IjI1NyIgd2lkdGg9IjEwOCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjE5MiIgeT0iMjc3Ij5jbHVzdGVyX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjM1MiIgeT0iMjU5IiB3aWR0aD0iNTgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzUwIiB5PSIyNTciIHdpZHRoPSI1OCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzYwIiB5PSIyNzciPldJVEg8L3RleHQ+CiAgIDxyZWN0IHg9IjQzMCIgeT0iMjU5IiB3aWR0aD0iMTAyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI0MjgiIHk9IjI1NyIgd2lkdGg9IjEwMiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjQzOCIgeT0iMjc3Ij53aXRoX29wdGlvbnM8L3RleHQ+CiAgIDxyZWN0IHg9IjQwNSIgeT0iMzI1IiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDAzIiB5PSIzMjMiIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDEzIiB5PSIzNDMiPkFTPC90ZXh0PgogICA8cmVjdCB4PSI0NjUiIHk9IjMyNSIgd2lkdGg9Ijk2IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI0NjMiIHk9IjMyMyIgd2lkdGg9Ijk2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iNDczIiB5PSIzNDMiPnNlbGVjdF9zdG10PC90ZXh0PgogICA8cGF0aCBjbGFzcz0ibGluZSIgZD0ibTE3IDE3IGgyIG0wIDAgaDEwIG03NiAwIGgxMCBtMjAgMCBoMTAgbTAgMCBoMTE4IG0tMTQ4IDAgaDIwIG0xMjggMCBoMjAgbS0xNjggMCBxMTAgMCAxMCAxMCBtMTQ4IDAgcTAgLTEwIDEwIC0xMCBtLTE1OCAxMCB2MTIgbTE0OCAwIHYtMTIgbS0xNDggMTIgcTAgMTAgMTAgMTAgbTEyOCAwIHExMCAwIDEwIC0xMCBtLTEzOCAxMCBoMTAgbTEwOCAwIGgxMCBtMjAgLTMyIGgxMCBtMTY2IDAgaDEwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tNDgwIDE0MiBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIyIDAgaDEwIG0wIDAgaDEzMCBtLTE2MCAwIGgyMCBtMTQwIDAgaDIwIG0tMTgwIDAgcTEwIDAgMTAgMTAgbTE2MCAwIHEwIC0xMCAxMCAtMTAgbS0xNzAgMTAgdjEyIG0xNjAgMCB2LTEyIG0tMTYwIDEyIHEwIDEwIDEwIDEwIG0xNDAgMCBxMTAgMCAxMCAtMTAgbS0xNTAgMTAgaDEwIG0xMjAgMCBoMTAgbTIwIC0zMiBoMTAgbTkyIDAgaDEwIG0yMCAwIGgxMCBtMjYgMCBoMTAgbTIwIDAgaDEwIG03OCAwIGgxMCBtLTExOCAwIGwyMCAwIG0tMSAwIHEtOSAwIC05IC0xMCBsMCAtMjQgcTAgLTEwIDEwIC0xMCBtOTggNDQgbDIwIDAgbS0yMCAwIHExMCAwIDEwIC0xMCBsMCAtMjQgcTAgLTEwIC0xMCAtMTAgbS05OCAwIGgxMCBtMjQgMCBoMTAgbTAgMCBoNTQgbTIwIDQ0IGgxMCBtMjYgMCBoMTAgbS0yNTAgMCBoMjAgbTIzMCAwIGgyMCBtLTI3MCAwIHExMCAwIDEwIDEwIG0yNTAgMCBxMCAtMTAgMTAgLTEwIG0tMjYwIDEwIHYxNCBtMjUwIDAgdi0xNCBtLTI1MCAxNCBxMCAxMCAxMCAxMCBtMjMwIDAgcTEwIDAgMTAgLTEwIG0tMjQwIDEwIGgxMCBtMCAwIGgyMjAgbTIyIC0zNCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS01NzEgODIgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtMCAwIGgyNDIgbS0yNzIgMCBoMjAgbTI1MiAwIGgyMCBtLTI5MiAwIHExMCAwIDEwIDEwIG0yNzIgMCBxMCAtMTAgMTAgLTEwIG0tMjgyIDEwIHYxMiBtMjcyIDAgdi0xMiBtLTI3MiAxMiBxMCAxMCAxMCAxMCBtMjUyIDAgcTEwIDAgMTAgLTEwIG0tMjYyIDEwIGgxMCBtMTA0IDAgaDEwIG0wIDAgaDEwIG0xMDggMCBoMTAgbTQwIC0zMiBoMTAgbTAgMCBoMTkwIG0tMjIwIDAgaDIwIG0yMDAgMCBoMjAgbS0yNDAgMCBxMTAgMCAxMCAxMCBtMjIwIDAgcTAgLTEwIDEwIC0xMCBtLTIzMCAxMCB2MTIgbTIyMCAwIHYtMTIgbS0yMjAgMTIgcTAgMTAgMTAgMTAgbTIwMCAwIHExMCAwIDEwIC0xMCBtLTIxMCAxMCBoMTAgbTU4IDAgaDEwIG0wIDAgaDEwIG0xMDIgMCBoMTAgbTIyIC0zMiBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0xOTEgOTggbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgaDEwIG00MCAwIGgxMCBtMCAwIGgxMCBtOTYgMCBoMTAgbTMgMCBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTc5IDMzOSA1ODcgMzM1IDU4NyAzNDMiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSI1NzkgMzM5IDU3MSAzMzUgNTcxIDM0MyI+PC9wb2x5Z29uPgo8L3N2Zz4=)

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
<td><strong>OR REPLACE</strong></td>
<td>If a materialized view exists with the same name, replace it with
the view defined in this statement. You cannot replace views that other
views or sinks depend on, nor can you replace a non-view object with a
view.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>If specified, <em>do not</em> generate an error if a materialized
view of the same name already exists.<br />
<br />
If <em>not</em> specified, throw an error if a view of the same name
already exists. <em>(Default)</em></td>
</tr>
<tr>
<td><em>view_name</em></td>
<td>A name for the materialized view.</td>
</tr>
<tr>
<td><strong>(</strong> <em>col_ident</em>… <strong>)</strong></td>
<td>Rename the <code>SELECT</code> statement’s columns to the list of
identifiers, both of which must be the same length. Note that this is
required for statements that return multiple columns with the same
identifier.</td>
</tr>
<tr>
<td><em>cluster_name</em></td>
<td>The cluster to maintain this materialized view. If not specified,
defaults to the active cluster.</td>
</tr>
<tr>
<td><em>select_stmt</em></td>
<td>The <a href="../select"><code>SELECT</code> statement</a> whose
results you want to maintain incrementally updated.</td>
</tr>
</tbody>
</table>

#### `with_options`

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MDMiIGhlaWdodD0iMTI5Ij4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDYxIDEgNTcgMSA2NSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDYxIDkgNTcgOSA2NSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSI1MSIgeT0iNDciIHdpZHRoPSI1OCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iNDUiIHdpZHRoPSI1OCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjY1Ij5XSVRIPC90ZXh0PgogICA8cmVjdCB4PSIxMjkiIHk9IjQ3IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTI3IiB5PSI0NSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMzciIHk9IjY1Ij4oPC90ZXh0PgogICA8cmVjdCB4PSIxOTUiIHk9IjQ3IiB3aWR0aD0iNDgiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjE5MyIgeT0iNDUiIHdpZHRoPSI0OCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjIwMyIgeT0iNjUiPmZpZWxkPC90ZXh0PgogICA8cmVjdCB4PSIyODMiIHk9Ijc5IiB3aWR0aD0iMjgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjgxIiB5PSI3NyIgd2lkdGg9IjI4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyOTEiIHk9Ijk3Ij49PC90ZXh0PgogICA8cmVjdCB4PSIzNTEiIHk9IjQ3IiB3aWR0aD0iMzgiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjM0OSIgeT0iNDUiIHdpZHRoPSIzOCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjM1OSIgeT0iNjUiPnZhbDwvdGV4dD4KICAgPHJlY3QgeD0iMTk1IiB5PSIzIiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTkzIiB5PSIxIiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIwMyIgeT0iMjEiPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjQyOSIgeT0iNDciIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0MjciIHk9IjQ1IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjQzNyIgeT0iNjUiPik8L3RleHQ+CiAgIDxwYXRoIGNsYXNzPSJsaW5lIiBkPSJtMTcgNjEgaDIgbTIwIDAgaDEwIG01OCAwIGgxMCBtMCAwIGgxMCBtMjYgMCBoMTAgbTIwIDAgaDEwIG00OCAwIGgxMCBtMjAgMCBoMTAgbTAgMCBoMzggbS02OCAwIGgyMCBtNDggMCBoMjAgbS04OCAwIHExMCAwIDEwIDEwIG02OCAwIHEwIC0xMCAxMCAtMTAgbS03OCAxMCB2MTIgbTY4IDAgdi0xMiBtLTY4IDEyIHEwIDEwIDEwIDEwIG00OCAwIHExMCAwIDEwIC0xMCBtLTU4IDEwIGgxMCBtMjggMCBoMTAgbTIwIC0zMiBoMTAgbTM4IDAgaDEwIG0tMjM0IDAgbDIwIDAgbS0xIDAgcS05IDAgLTkgLTEwIGwwIC0yNCBxMCAtMTAgMTAgLTEwIG0yMTQgNDQgbDIwIDAgbS0yMCAwIHExMCAwIDEwIC0xMCBsMCAtMjQgcTAgLTEwIC0xMCAtMTAgbS0yMTQgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDE3MCBtMjAgNDQgaDEwIG0yNiAwIGgxMCBtLTQ0NCAwIGgyMCBtNDI0IDAgaDIwIG0tNDY0IDAgcTEwIDAgMTAgMTAgbTQ0NCAwIHEwIC0xMCAxMCAtMTAgbS00NTQgMTAgdjQ2IG00NDQgMCB2LTQ2IG0tNDQ0IDQ2IHEwIDEwIDEwIDEwIG00MjQgMCBxMTAgMCAxMCAtMTAgbS00MzQgMTAgaDEwIG0wIDAgaDQxNCBtMjMgLTY2IGgtMyIgLz4KICAgPHBvbHlnb24gcG9pbnRzPSI0OTMgNjEgNTAxIDU3IDUwMSA2NSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjQ5MyA2MSA0ODUgNTcgNDg1IDY1Ij48L3BvbHlnb24+Cjwvc3ZnPg==)

</div>

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
<td><strong>ASSERT NOT NULL</strong> <em>col_ident</em></td>
<td><code>text</code></td>
<td>The column identifier for which to create a <a
href="#non-null-assertions">non-null assertion</a>. To specify multiple
columns, use the option multiple times.</td>
</tr>
<tr>
<td><strong>PARTITION BY</strong> <em>columns</em></td>
<td><code>(ident [, ident]*)</code></td>
<td>The key by which Materialize should internally partition this
durable collection. See the <a
href="/docs/transform-data/patterns/partition-by/">partitioning
guide</a> for restrictions on valid values and other details.</td>
</tr>
<tr>
<td><strong>RETAIN HISTORY FOR</strong> <em>retention_period</em></td>
<td><code>interval</code></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
Duration for which Materialize retains historical data, which is useful
to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. Accepts positive <a
href="/docs/sql/types/interval/">interval</a> values (e.g.
<code>'1hr'</code>). Default: <code>1s</code>.</td>
</tr>
<tr>
<td><strong>REFRESH</strong> <em>refresh_strategy</em></td>
<td></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
The refresh strategy for the materialized view. See <a
href="#refresh-strategies">Refresh strategies</a> for syntax
options.<br />
Default: <code>ON COMMIT</code>.</td>
</tr>
</tbody>
</table>

## Details

### Usage patterns

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

### Indexes

Although you can query a materialized view directly, these queries will
be issued against Materialize’s storage layer. This is expected to be
fast, but still slower than reading from memory. To improve the speed of
queries on materialized views, we recommend creating
[indexes](../create-index) based on common query patterns.

It’s important to keep in mind that indexes are **local** to a cluster,
and maintained in memory. As an example, if you create a materialized
view and build an index on it in the `quickstart` cluster, querying the
view from a different cluster will *not* use the index; you should
create the appropriate indexes in each cluster you are referencing the
materialized view in.

### Non-null assertions

Because materialized views may be created on arbitrary queries, it may
not in all cases be possible for Materialize to automatically infer
non-nullability of some columns that can in fact never be null. In such
a case, `ASSERT NOT NULL` clauses may be used as described in the syntax
section above. Specifying `ASSERT NOT NULL` for a column forces that
column’s type in the materialized view to include `NOT NULL`. If this
clause is used erroneously, and a `NULL` value is in fact produced in a
column for which `ASSERT NOT NULL` was specified, querying the
materialized view will produce an error until the offending row is
deleted.

### Refresh strategies

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

Materialized views in Materialize are incrementally maintained by
default, meaning their results are automatically updated as soon as new
data arrives. This guarantees that queries returns the most up-to-date
information available with minimal delay and that results are always as
[fresh](/docs/concepts/reaction-time) as the input data itself.

In most cases, this default behavior is ideal. However, in some very
specific scenarios like reporting over slow changing historical data, it
may be acceptable to relax freshness in order to reduce compute usage.
For these cases, Materialize supports refresh strategies, which allow
you to configure a materialized view to recompute itself on a fixed
schedule rather than maintaining them incrementally.

<div class="note">

**NOTE:** The use of refresh strategies is discouraged unless you have a
clear and measurable need to reduce maintenance costs on stale or
archival data. For most use cases, the default incremental maintenance
model provides a better experience.

</div>

#### Refresh on commit

**Syntax:** `REFRESH ON COMMIT`

Materialized views in Materialize are incrementally updated by default.
This means that as soon as new data arrives in the system, any dependent
materialized views are automatically and continuously updated. This
behavior, known as **refresh on commit**, ensures that the view’s
contents are always as fresh as the underlying data.

**`REFRESH ON COMMIT` is:**

- **Generally available**
- The **default behavior** for all materialized views
- **Implicit** and does not need to be manually specified
- **Strongly recommended** for the vast majority of use cases

With `REFRESH ON COMMIT`, Materialize provides low-latency, up-to-date
results without requiring user-defined schedules or manual refreshes.
This model is ideal for most workloads, including streaming analytics,
live dashboards, customer-facing queries, and applications that rely on
timely, accurate results.

Only in rare cases—such as batch-oriented processing or reporting over
slowly changing historical datasets—might it make sense to trade off
freshness for potential cost savings. In such cases, consider defining
an explicit [refresh strategy](#refresh-strategies) to control when
recomputation occurs.

#### Refresh at

**Syntax:** `REFRESH AT` { `CREATION` \| *timestamp* }

This strategy allows configuring a materialized view to **refresh at a
specific time**. The refresh time can be specified as a timestamp, or
using the `AT CREATION` clause, which triggers a first refresh when the
materialized view is created.

**Example**

To create a materialized view that is refreshed at creation, and then at
the specified times:

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW mv_refresh_at
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh at a user-specified (future) time
  REFRESH AT '2024-06-06 12:00:00',
  -- Refresh at another user-specified (future) time
  REFRESH AT '2024-06-08 22:00:00'
)
AS SELECT ... FROM ...;
```

</div>

You can specify multiple `REFRESH AT` strategies in the same `CREATE`
statement, and combine them with the [`REFRESH EVERY`
strategy](#refresh-every).

#### Refresh every

**Syntax:** `REFRESH EVERY` *interval* \[ `ALIGNED TO` *timestamp* \]

This strategy allows configuring a materialized view to **refresh at
regular intervals**. The `ALIGNED TO` clause additionally allows
specifying the *phase* of the scheduled refreshes: for daily refreshes,
it specifies the time of the day when the refresh will happen; for
weekly refreshes, it specifies the day of the week and the time of the
day when the refresh will happen. If `ALIGNED TO` is not specified, it
defaults to the time when the materialized view is created.

**Example**

To create a materialized view that is refreshed at creation, and then
once a day at 10PM UTC:

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW mv_refresh_every
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh every day at 10PM UTC
  REFRESH EVERY '1 day' ALIGNED TO '2024-06-06 22:00:00'
) AS
SELECT ...;
```

</div>

You can specify multiple `REFRESH EVERY` strategies in the same `CREATE`
statement, and combine them with the [`REFRESH AT`
strategy](#refresh-at). When this strategy, we recommend **always**
using the [`REFRESH AT CREATION`](#refresh-at) clause, so the
materialized view is available for querying ahead of the first
user-specified refresh time.

#### Querying materialized views with refresh strategies

Materialized views configured with [`REFRESH EVERY`
strategies](#refresh-every) have a period of unavailability around the
scheduled refresh times — during this period, the view **will not return
any results**. To avoid unavailability during the refresh operation, you
must host these views in [**scheduled
clusters**](/docs/sql/create-cluster/#scheduling), which can be
configured to automatically [turn on ahead of the scheduled refresh
time](/docs/sql/create-cluster/#hydration-time-estimate).

**Example**

To create a scheduled cluster that turns on 1 hour ahead of any
scheduled refresh times:

<div class="highlight">

``` chroma
CREATE CLUSTER my_scheduled_cluster (
  SIZE = '3200cc',
  SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour')
);
```

</div>

You can then create a materialized view in this cluster, configured to
refresh at creation, then once a day at 12PM UTC:

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW mv_refresh_every
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh every day at 12PM UTC
  REFRESH EVERY '1 day' ALIGNED TO '2024-06-18 00:00:00'
) AS
SELECT ...;
```

</div>

Because the materialized view is hosted on a scheduled cluster that is
configured to **turn on ahead of any scheduled refreshes**, you can
expect `my_scheduled_cluster` to be provisioned at 11PM UTC — or, 1 hour
ahead of the scheduled refresh time for `mv_refresh_every`. This means
that the cluster can backfill the view with pre-existing data — a
process known as
[*hydration*](/docs/transform-data/troubleshooting/#hydrating-upstream-objects)
— ahead of the refresh operation, which **reduces the total
unavailability window of the view** to just the duration of the refresh.

If the cluster is **not** configured to turn on ahead of scheduled
refreshes (i.e., using the `HYDRATION TIME ESTIMATE` option), the total
unavailability window of the view will be a combination of the hydration
time for all objects in the cluster (typically long) and the duration of
the refresh for the materialized view (typically short).

Depending on the actual time it takes to hydrate the view or set of
views in the cluster, you can later adjust the hydration time estimate
value for the cluster using
[`ALTER CLUSTER`](../alter-cluster/#schedule):

<div class="highlight">

``` chroma
ALTER CLUSTER my_scheduled_cluster
SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '30 minutes'));
```

</div>

#### Introspection

To check details about the (non-default) refresh strategies associated
with any materialized view in the system, you can query the
[`mz_internal.mz_materialized_view_refresh_strategies`](../system-catalog/mz_internal/#mz_materialized_view_refresh_strategies)
and
[`mz_internal.mz_materialized_view_refreshes`](../system-catalog/mz_internal/#mz_materialized_view_refreshes)
system catalog tables:

<div class="highlight">

``` chroma
SELECT mv.id AS materialized_view_id,
       mv.name AS materialized_view_name,
       rs.type AS refresh_strategy,
       rs.interval AS refresh_interval,
       rs.aligned_to AS refresh_interval_phase,
       rs.at AS refresh_time,
       r.last_completed_refresh,
       r.next_refresh
FROM mz_internal.mz_materialized_view_refresh_strategies rs
JOIN mz_internal.mz_materialized_view_refreshes r ON r.materialized_view_id = rs.materialized_view_id
JOIN mz_materialized_views mv ON rs.materialized_view_id = mv.id;
```

</div>

## Examples

### Creating a materialized view

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW winning_bids AS
SELECT auction_id,
       bid_id,
       item,
       amount
FROM highest_bid_per_auction
WHERE end_time < mz_now();
```

</div>

### Using non-null assertions

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW users_and_orders WITH (
  -- The semantics of a FULL OUTER JOIN guarantee that user_id is not null,
  -- because one of `users.id` or `orders.user_id` must be not null, but
  -- Materialize cannot yet automatically infer that fact.
  ASSERT NOT NULL user_id
)
AS
SELECT
  coalesce(users.id, orders.user_id) AS user_id,
  ...
FROM users FULL OUTER JOIN orders ON users.id = orders.user_id
```

</div>

### Using refresh strategies

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW mv
IN CLUSTER my_refresh_cluster
WITH (
  -- Refresh every Tuesday at 12PM UTC
  REFRESH EVERY '7 days' ALIGNED TO '2024-06-04 12:00:00',
  -- Refresh every Thursday at 12PM UTC
  REFRESH EVERY '7 days' ALIGNED TO '2024-06-06 12:00:00',
  -- Refresh on creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION
)
AS SELECT ... FROM ...;
```

</div>

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `CREATE` privileges on the containing cluster.
- `USAGE` privileges on all types used in the materialized view
  definition.
- `USAGE` privileges on the schemas for the types used in the statement.
- Ownership of the existing view if replacing an existing view with the
  same name (i.e., `OR REPLACE` is specified in
  `CREATE MATERIALIZED VIEW` command).

## Additional information

- Materialized views are not monotonic; that is, materialized views
  cannot be recognized as append-only.

## Related pages

- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`DROP MATERIALIZED VIEW`](../drop-materialized-view)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-materialized-view.md"
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
