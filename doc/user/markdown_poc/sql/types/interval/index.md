<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [SQL data
types](/docs/sql/types/)

</div>

# interval type

`interval` data expresses a duration of time.

`interval` data keeps months, days, and microseconds completely separate
and will not try to convert between any of those fields when comparing
`interval`s. This may lead to some unexpected behavior. For example
`1 month` is considered greater than `100 days`. See
[‘justify_days’](../../functions/justify-days),
[‘justify_hours’](../../functions/justify-hours), and
[‘justify_interval’](../../functions/justify-interval) to explicitly
convert between these fields.

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Detail</th>
<th>Info</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>Quick Syntax</strong></td>
<td><code>INTERVAL '1' MINUTE</code><br />
<code>INTERVAL '1-2 3 4:5:6.7'</code><br />
<code>INTERVAL '1 year 2.3 days 4.5 seconds'</code></td>
</tr>
<tr>
<td><strong>Size</strong></td>
<td>20 bytes</td>
</tr>
<tr>
<td><strong>Catalog name</strong></td>
<td><code>pg_catalog.interval</code></td>
</tr>
<tr>
<td><strong>OID</strong></td>
<td>1186</td>
</tr>
<tr>
<td><strong>Min value</strong></td>
<td>-178956970 years -8 months -2147483648 days
-2562047788:00:54.775808</td>
</tr>
<tr>
<td><strong>Max value</strong></td>
<td>178956970 years 7 months 2147483647 days
2562047788:00:54.775807</td>
</tr>
</tbody>
</table>

## Syntax

#### INTERVAL

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI0MzciIGhlaWdodD0iMTcxIj4KICAgPHBvbHlnb24gcG9pbnRzPSIxMSAzMyAzIDI5IDMgMzciPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSIxOSAzMyAxMSAyOSAxMSAzNyI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMyIgeT0iMTkiIHdpZHRoPSI5MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzMSIgeT0iMTciIHdpZHRoPSI5MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDEiIHk9IjM3Ij5JTlRFUlZBTDwvdGV4dD4KICAgPHJlY3QgeD0iMTQzIiB5PSIxOSIgd2lkdGg9IjIyIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjE0MSIgeT0iMTciIHdpZHRoPSIyMiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTUxIiB5PSIzNyI+JiMzOTs8L3RleHQ+CiAgIDxyZWN0IHg9IjIwNSIgeT0iMTkiIHdpZHRoPSI4NCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjAzIiB5PSIxNyIgd2lkdGg9Ijg0IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjEzIiB5PSIzNyI+dGltZV9leHByPC90ZXh0PgogICA8cmVjdCB4PSIzMjkiIHk9IjE5IiB3aWR0aD0iMjIiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzI3IiB5PSIxNyIgd2lkdGg9IjIyIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzMzciIHk9IjM3Ij4mIzM5OzwvdGV4dD4KICAgPHJlY3QgeD0iNjUiIHk9IjEzNyIgd2lkdGg9IjExOCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iNjMiIHk9IjEzNSIgd2lkdGg9IjExOCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjczIiB5PSIxNTUiPmhlYWRfdGltZV91bml0PC90ZXh0PgogICA8cmVjdCB4PSIyMDMiIHk9IjEzNyIgd2lkdGg9IjQwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIwMSIgeT0iMTM1IiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIxMSIgeT0iMTU1Ij5UTzwvdGV4dD4KICAgPHJlY3QgeD0iMjgzIiB5PSIxMDUiIHdpZHRoPSIxMDYiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjI4MSIgeT0iMTAzIiB3aWR0aD0iMTA2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjkxIiB5PSIxMjMiPnRhaWxfdGltZV91bml0PC90ZXh0PgogICA8cGF0aCBjbGFzcz0ibGluZSIgZD0ibTE5IDMzIGgyIG0wIDAgaDEwIG05MCAwIGgxMCBtMCAwIGgxMCBtMjIgMCBoMTAgbTIwIDAgaDEwIG04NCAwIGgxMCBtLTEyNCAwIGwyMCAwIG0tMSAwIHEtOSAwIC05IC0xMCBsMCAtMTIgcTAgLTEwIDEwIC0xMCBtMTA0IDMyIGwyMCAwIG0tMjAgMCBxMTAgMCAxMCAtMTAgbDAgLTEyIHEwIC0xMCAtMTAgLTEwIG0tMTA0IDAgaDEwIG0wIDAgaDk0IG0yMCAzMiBoMTAgbTIyIDAgaDEwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tMzcwIDU0IGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMjIgMCBoMTAgbTAgMCBoMzU0IG0tMzg0IDAgaDIwIG0zNjQgMCBoMjAgbS00MDQgMCBxMTAgMCAxMCAxMCBtMzg0IDAgcTAgLTEwIDEwIC0xMCBtLTM5NCAxMCB2MTIgbTM4NCAwIHYtMTIgbS0zODQgMTIgcTAgMTAgMTAgMTAgbTM2NCAwIHExMCAwIDEwIC0xMCBtLTM1NCAxMCBoMTAgbTAgMCBoMTg4IG0tMjE4IDAgaDIwIG0xOTggMCBoMjAgbS0yMzggMCBxMTAgMCAxMCAxMCBtMjE4IDAgcTAgLTEwIDEwIC0xMCBtLTIyOCAxMCB2MTIgbTIxOCAwIHYtMTIgbS0yMTggMTIgcTAgMTAgMTAgMTAgbTE5OCAwIHExMCAwIDEwIC0xMCBtLTIwOCAxMCBoMTAgbTExOCAwIGgxMCBtMCAwIGgxMCBtNDAgMCBoMTAgbTIwIC0zMiBoMTAgbTEwNiAwIGgxMCBtMjMgLTMyIGgtMyIgLz4KICAgPHBvbHlnb24gcG9pbnRzPSI0MjcgODcgNDM1IDgzIDQzNSA5MSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjQyNyA4NyA0MTkgODMgNDE5IDkxIj48L3BvbHlnb24+Cjwvc3ZnPg==)

</div>

#### `time_expr`

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MTUiIGhlaWdodD0iMTU3Ij4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSI1MSIgeT0iMzUiIHdpZHRoPSIzMCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iMzMiIHdpZHRoPSIzMCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjUzIj4rPC90ZXh0PgogICA8cmVjdCB4PSI1MSIgeT0iNzkiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iNzciIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9Ijk3Ij4tPC90ZXh0PgogICA8cmVjdCB4PSIxNDEiIHk9IjMiIHdpZHRoPSI2NCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMTM5IiB5PSIxIiB3aWR0aD0iNjQiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIxNDkiIHk9IjIxIj55bV9zdHI8L3RleHQ+CiAgIDxyZWN0IHg9IjE0MSIgeT0iNDciIHdpZHRoPSI3MiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMTM5IiB5PSI0NSIgd2lkdGg9IjcyIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMTQ5IiB5PSI2NSI+dGltZV9zdHI8L3RleHQ+CiAgIDxyZWN0IHg9IjE0MSIgeT0iOTEiIHdpZHRoPSIzNiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMTM5IiB5PSI4OSIgd2lkdGg9IjM2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMTQ5IiB5PSIxMDkiPmludDwvdGV4dD4KICAgPHJlY3QgeD0iMjE3IiB5PSIxMjMiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyMTUiIHk9IjEyMSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyMjUiIHk9IjE0MSI+LjwvdGV4dD4KICAgPHJlY3QgeD0iMjYxIiB5PSIxMjMiIHdpZHRoPSI0NiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjU5IiB5PSIxMjEiIHdpZHRoPSI0NiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI2OSIgeT0iMTQxIj5mcmFjPC90ZXh0PgogICA8cmVjdCB4PSIzNjciIHk9IjEyMyIgd2lkdGg9IjgwIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzNjUiIHk9IjEyMSIgd2lkdGg9IjgwIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMzc1IiB5PSIxNDEiPnRpbWVfdW5pdDwvdGV4dD4KICAgPHBhdGggY2xhc3M9ImxpbmUiIGQ9Im0xNyAxNyBoMiBtMjAgMCBoMTAgbTAgMCBoNDAgbS03MCAwIGgyMCBtNTAgMCBoMjAgbS05MCAwIHExMCAwIDEwIDEwIG03MCAwIHEwIC0xMCAxMCAtMTAgbS04MCAxMCB2MTIgbTcwIDAgdi0xMiBtLTcwIDEyIHEwIDEwIDEwIDEwIG01MCAwIHExMCAwIDEwIC0xMCBtLTYwIDEwIGgxMCBtMzAgMCBoMTAgbS02MCAtMTAgdjIwIG03MCAwIHYtMjAgbS03MCAyMCB2MjQgbTcwIDAgdi0yNCBtLTcwIDI0IHEwIDEwIDEwIDEwIG01MCAwIHExMCAwIDEwIC0xMCBtLTYwIDEwIGgxMCBtMjYgMCBoMTAgbTAgMCBoNCBtNDAgLTc2IGgxMCBtNjQgMCBoMTAgbTAgMCBoMjYyIG0tMzY2IDAgaDIwIG0zNDYgMCBoMjAgbS0zODYgMCBxMTAgMCAxMCAxMCBtMzY2IDAgcTAgLTEwIDEwIC0xMCBtLTM3NiAxMCB2MjQgbTM2NiAwIHYtMjQgbS0zNjYgMjQgcTAgMTAgMTAgMTAgbTM0NiAwIHExMCAwIDEwIC0xMCBtLTM1NiAxMCBoMTAgbTcyIDAgaDEwIG0wIDAgaDI1NCBtLTM1NiAtMTAgdjIwIG0zNjYgMCB2LTIwIG0tMzY2IDIwIHYyNCBtMzY2IDAgdi0yNCBtLTM2NiAyNCBxMCAxMCAxMCAxMCBtMzQ2IDAgcTEwIDAgMTAgLTEwIG0tMzU2IDEwIGgxMCBtMzYgMCBoMTAgbTIwIDAgaDEwIG0wIDAgaDEwMCBtLTEzMCAwIGgyMCBtMTEwIDAgaDIwIG0tMTUwIDAgcTEwIDAgMTAgMTAgbTEzMCAwIHEwIC0xMCAxMCAtMTAgbS0xNDAgMTAgdjEyIG0xMzAgMCB2LTEyIG0tMTMwIDEyIHEwIDEwIDEwIDEwIG0xMTAgMCBxMTAgMCAxMCAtMTAgbS0xMjAgMTAgaDEwIG0yNCAwIGgxMCBtMCAwIGgxMCBtNDYgMCBoMTAgbTQwIC0zMiBoMTAgbTAgMCBoOTAgbS0xMjAgMCBoMjAgbTEwMCAwIGgyMCBtLTE0MCAwIHExMCAwIDEwIDEwIG0xMjAgMCBxMCAtMTAgMTAgLTEwIG0tMTMwIDEwIHYxMiBtMTIwIDAgdi0xMiBtLTEyMCAxMiBxMCAxMCAxMCAxMCBtMTAwIDAgcTEwIDAgMTAgLTEwIG0tMTEwIDEwIGgxMCBtODAgMCBoMTAgbTQzIC0xMjAgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjUwNSAxNyA1MTMgMTMgNTEzIDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTA1IDE3IDQ5NyAxMyA0OTcgMjEiPjwvcG9seWdvbj4KPC9zdmc+)

</div>

#### `time_unit`

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyMzUiIGhlaWdodD0iNDc3Ij4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSI1MSIgeT0iMyIgd2lkdGg9IjEwOCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iMSIgd2lkdGg9IjEwOCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjIxIj5NSUxMRU5OSVVNPC90ZXh0PgogICA8cmVjdCB4PSI1MSIgeT0iNDciIHdpZHRoPSI4OCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iNDUiIHdpZHRoPSI4OCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjY1Ij5DRU5UVVJZPC90ZXh0PgogICA8cmVjdCB4PSI1MSIgeT0iOTEiIHdpZHRoPSI3OCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iODkiIHdpZHRoPSI3OCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjEwOSI+REVDQURFPC90ZXh0PgogICA8cmVjdCB4PSI1MSIgeT0iMTM1IiB3aWR0aD0iNTgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDkiIHk9IjEzMyIgd2lkdGg9IjU4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1OSIgeT0iMTUzIj5ZRUFSPC90ZXh0PgogICA8cmVjdCB4PSI1MSIgeT0iMTc5IiB3aWR0aD0iNzQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDkiIHk9IjE3NyIgd2lkdGg9Ijc0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1OSIgeT0iMTk3Ij5NT05USDwvdGV4dD4KICAgPHJlY3QgeD0iNTEiIHk9IjIyMyIgd2lkdGg9IjUwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQ5IiB5PSIyMjEiIHdpZHRoPSI1MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjI0MSI+REFZPC90ZXh0PgogICA8cmVjdCB4PSI1MSIgeT0iMjY3IiB3aWR0aD0iNjAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDkiIHk9IjI2NSIgd2lkdGg9IjYwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1OSIgeT0iMjg1Ij5IT1VSPC90ZXh0PgogICA8cmVjdCB4PSI1MSIgeT0iMzExIiB3aWR0aD0iNzQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDkiIHk9IjMwOSIgd2lkdGg9Ijc0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1OSIgeT0iMzI5Ij5NSU5VVEU8L3RleHQ+CiAgIDxyZWN0IHg9IjUxIiB5PSIzNTUiIHdpZHRoPSI4MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iMzUzIiB3aWR0aD0iODAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjU5IiB5PSIzNzMiPlNFQ09ORDwvdGV4dD4KICAgPHJlY3QgeD0iNTEiIHk9IjM5OSIgd2lkdGg9IjEyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iMzk3IiB3aWR0aD0iMTI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1OSIgeT0iNDE3Ij5NSUxMSVNFQ09ORFM8L3RleHQ+CiAgIDxyZWN0IHg9IjUxIiB5PSI0NDMiIHdpZHRoPSIxMzYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDkiIHk9IjQ0MSIgd2lkdGg9IjEzNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjQ2MSI+TUlDUk9TRUNPTkRTPC90ZXh0PgogICA8cGF0aCBjbGFzcz0ibGluZSIgZD0ibTE3IDE3IGgyIG0yMCAwIGgxMCBtMTA4IDAgaDEwIG0wIDAgaDI4IG0tMTc2IDAgaDIwIG0xNTYgMCBoMjAgbS0xOTYgMCBxMTAgMCAxMCAxMCBtMTc2IDAgcTAgLTEwIDEwIC0xMCBtLTE4NiAxMCB2MjQgbTE3NiAwIHYtMjQgbS0xNzYgMjQgcTAgMTAgMTAgMTAgbTE1NiAwIHExMCAwIDEwIC0xMCBtLTE2NiAxMCBoMTAgbTg4IDAgaDEwIG0wIDAgaDQ4IG0tMTY2IC0xMCB2MjAgbTE3NiAwIHYtMjAgbS0xNzYgMjAgdjI0IG0xNzYgMCB2LTI0IG0tMTc2IDI0IHEwIDEwIDEwIDEwIG0xNTYgMCBxMTAgMCAxMCAtMTAgbS0xNjYgMTAgaDEwIG03OCAwIGgxMCBtMCAwIGg1OCBtLTE2NiAtMTAgdjIwIG0xNzYgMCB2LTIwIG0tMTc2IDIwIHYyNCBtMTc2IDAgdi0yNCBtLTE3NiAyNCBxMCAxMCAxMCAxMCBtMTU2IDAgcTEwIDAgMTAgLTEwIG0tMTY2IDEwIGgxMCBtNTggMCBoMTAgbTAgMCBoNzggbS0xNjYgLTEwIHYyMCBtMTc2IDAgdi0yMCBtLTE3NiAyMCB2MjQgbTE3NiAwIHYtMjQgbS0xNzYgMjQgcTAgMTAgMTAgMTAgbTE1NiAwIHExMCAwIDEwIC0xMCBtLTE2NiAxMCBoMTAgbTc0IDAgaDEwIG0wIDAgaDYyIG0tMTY2IC0xMCB2MjAgbTE3NiAwIHYtMjAgbS0xNzYgMjAgdjI0IG0xNzYgMCB2LTI0IG0tMTc2IDI0IHEwIDEwIDEwIDEwIG0xNTYgMCBxMTAgMCAxMCAtMTAgbS0xNjYgMTAgaDEwIG01MCAwIGgxMCBtMCAwIGg4NiBtLTE2NiAtMTAgdjIwIG0xNzYgMCB2LTIwIG0tMTc2IDIwIHYyNCBtMTc2IDAgdi0yNCBtLTE3NiAyNCBxMCAxMCAxMCAxMCBtMTU2IDAgcTEwIDAgMTAgLTEwIG0tMTY2IDEwIGgxMCBtNjAgMCBoMTAgbTAgMCBoNzYgbS0xNjYgLTEwIHYyMCBtMTc2IDAgdi0yMCBtLTE3NiAyMCB2MjQgbTE3NiAwIHYtMjQgbS0xNzYgMjQgcTAgMTAgMTAgMTAgbTE1NiAwIHExMCAwIDEwIC0xMCBtLTE2NiAxMCBoMTAgbTc0IDAgaDEwIG0wIDAgaDYyIG0tMTY2IC0xMCB2MjAgbTE3NiAwIHYtMjAgbS0xNzYgMjAgdjI0IG0xNzYgMCB2LTI0IG0tMTc2IDI0IHEwIDEwIDEwIDEwIG0xNTYgMCBxMTAgMCAxMCAtMTAgbS0xNjYgMTAgaDEwIG04MCAwIGgxMCBtMCAwIGg1NiBtLTE2NiAtMTAgdjIwIG0xNzYgMCB2LTIwIG0tMTc2IDIwIHYyNCBtMTc2IDAgdi0yNCBtLTE3NiAyNCBxMCAxMCAxMCAxMCBtMTU2IDAgcTEwIDAgMTAgLTEwIG0tMTY2IDEwIGgxMCBtMTI2IDAgaDEwIG0wIDAgaDEwIG0tMTY2IC0xMCB2MjAgbTE3NiAwIHYtMjAgbS0xNzYgMjAgdjI0IG0xNzYgMCB2LTI0IG0tMTc2IDI0IHEwIDEwIDEwIDEwIG0xNTYgMCBxMTAgMCAxMCAtMTAgbS0xNjYgMTAgaDEwIG0xMzYgMCBoMTAgbTIzIC00NDAgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjIyNSAxNyAyMzMgMTMgMjMzIDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iMjI1IDE3IDIxNyAxMyAyMTcgMjEiPjwvcG9seWdvbj4KPC9zdmc+)

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
<td><em>ym_str</em></td>
<td>A string representing years and months in <code>Y-M D</code>
format.</td>
</tr>
<tr>
<td><em>time_str</em></td>
<td>A string representing hours, minutes, seconds, and nanoseconds in
<code>H:M:S.NS</code> format.</td>
</tr>
<tr>
<td><em>head_time_unit</em></td>
<td>Return an interval without <code>time_unit</code>s larger than
<code>head_time_unit</code>. Note that this differs from PostgreSQL’s
implementation, which ignores this clause.</td>
</tr>
<tr>
<td><em>tail_time_unit</em></td>
<td>1. Return an interval without <code>time_unit</code> smaller than
<code>tail_time_unit</code>.<br />
<br />
2. If the final <code>time_expr</code> is only a number, treat the
<code>time_expr</code> as belonging to <code>tail_time_unit</code>. This
is the case of the most common <code>interval</code> format like
<code>INTERVAL '1' MINUTE</code>.</td>
</tr>
</tbody>
</table>

## Details

### `time_expr` Syntax

Materialize strives for full PostgreSQL compatibility with `time_exprs`,
which offers support for two types of `time_expr` syntax:

- SQL Standard, i.e. `'Y-M D H:M:S.NS'`
- PostgreSQL, i.e. repeated `int.frac time_unit`, e.g.:
  - `'1 year 2 months 3.4 days 5 hours 6 minutes 7 seconds 8 milliseconds'`
  - `'1y 2mon 3.4d 5h 6m 7s 8ms'`

Like PostgreSQL, Materialize’s implementation includes the following
stipulations:

- You can freely mix SQL Standard- and PostgreSQL-style `time_expr`s.

- You can write `time_expr`s in any order, e.g `'H:M:S.NS Y-M'`.

- Each `time_unit` can only be written once.

- SQL Standard `time_expr` uses the following groups of `time_unit`s:

  - `Y-M`
  - `D`
  - `H:M:S.NS`

  Using a SQL Standard `time_expr` to write to any of these `time_units`
  writes to all other `time_units` in the same group, even if that
  `time_unit` is not explicitly referenced.

  For example, the `time_expr` `'1:2'` (1 hour, 2 minutes) also writes a
  value of 0 seconds. You cannot then include another `time_expr` which
  writes to the seconds `time_unit`.

- A two-field `time_expr` like `'1:2'` is by default interpreted as
  (hour, minute) while `1:2 MINUTE TO SECOND` interprets as (minute,
  seconds).

- Only PostgreSQL `time_expr`s support non-second fractional
  `time_units`, e.g. `1.2 days`. Materialize only supports 9 places of
  decimal precision.

### Valid casts

#### From `interval`

You can [cast](../../functions/cast) `interval` to:

- [`text`](../text) (by assignment)
- [`time`](../time) (by assignment)

#### To `interval`

You can [cast](../../functions/cast) from the following types to
`interval`:

- [`text`](../text) (explicitly)
- [`time`](../time) (explicitly)

### Valid operations

`interval` data supports the following operations with other types.

| Operation | Computes | Notes |
|----|----|----|
| [`date`](../date) `+` [`interval`](../interval) | [`timestamp`](../timestamp) |  |
| [`date`](../date) `-` [`interval`](../interval) | [`timestamp`](../timestamp) |  |
| [`date`](../date) `-` [`date`](../date) | [`interval`](../interval) |  |
| [`timestamp`](../timestamp) `+` [`interval`](../interval) | [`timestamp`](../timestamp) |  |
| [`timestamp`](../timestamp) `-` [`interval`](../interval) | [`timestamp`](../timestamp) |  |
| [`timestamp`](../timestamp) `-` [`timestamp`](../timestamp) | [`interval`](../interval) |  |
| [`time`](../time) `+` [`interval`](../interval) | `time` |  |
| [`time`](../time) `-` [`interval`](../interval) | `time` |  |
| [`time`](../time) `-` [`time`](../time) | [`interval`](../interval) |  |
| [`interval`](../interval) `*` [`double precision`](../float) | [`interval`](../interval) |  |
| [`interval`](../interval) `/` [`double precision`](../float) | [`interval`](../interval) |  |

## Examples

<div class="highlight">

``` chroma
SELECT INTERVAL '1' MINUTE AS interval_m;
```

</div>

```
 interval_m
------------
 00:01:00
```

### SQL Standard syntax

<div class="highlight">

``` chroma
SELECT INTERVAL '1-2 3 4:5:6.7' AS interval_p;
```

</div>

```
            interval_f
-----------------------------------
 1 year 2 months 3 days 04:05:06.7
```

### PostgreSQL syntax

<div class="highlight">

``` chroma
SELECT INTERVAL '1 year 2.3 days 4.5 seconds' AS interval_p;
```

</div>

```
        interval_p
--------------------------
 1 year 2 days 07:12:04.5
```

### Negative intervals

`interval_n` demonstrates using negative and positive components in an
interval.

<div class="highlight">

``` chroma
SELECT INTERVAL '-1 day 2:3:4.5' AS interval_n;
```

</div>

```
 interval_n
-------------
 -1 days +02:03:04.5
```

### Truncating interval

`interval_r` demonstrates how `head_time_unit` and `tail_time_unit`
truncate the interval.

<div class="highlight">

``` chroma
SELECT INTERVAL '1-2 3 4:5:6.7' DAY TO MINUTE AS interval_r;
```

</div>

```
   interval_r
-----------------
 3 days 04:05:00
```

### Complex example

`interval_w` demonstrates both mixing SQL Standard and PostgreSQL
`time_expr`, as well as using `tail_time_unit` to control the
`time_unit` of the last value of the `interval` string.

<div class="highlight">

``` chroma
SELECT INTERVAL '1 day 2-3 4' MINUTE AS interval_w;
```

</div>

```
           interval_w
---------------------------------
 2 years 3 months 1 day 00:04:00
```

### Interaction with timestamps

<div class="highlight">

``` chroma
SELECT TIMESTAMP '2020-01-01 8:00:00' + INTERVAL '1' DAY AS ts_interaction;
```

</div>

```
   ts_interaction
---------------------
 2020-01-02 08:00:00
```

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/types/interval.md"
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
