<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [SQL data
types](/docs/sql/types/)

</div>

# Timestamp types

`timestamp` and `timestamp with time zone` data expresses a date and
time in UTC.

## `timestamp` info

| Detail             | Info                                                |
|--------------------|-----------------------------------------------------|
| **Quick Syntax**   | `TIMESTAMP WITH TIME ZONE '2007-02-01 15:04:05+06'` |
| **Size**           | 8 bytes                                             |
| **Catalog name**   | `pg_catalog.timestamp`                              |
| **OID**            | 1083                                                |
| **Min value**      | 4713 BC                                             |
| **Max value**      | 294276 AD                                           |
| **Max resolution** | 1 microsecond                                       |

## `timestamp with time zone` info

| Detail             | Info                                   |
|--------------------|----------------------------------------|
| **Quick Syntax**   | `TIMESTAMPTZ '2007-02-01 15:04:05+06'` |
| **Aliases**        | `timestamp with time zone`             |
| **Size**           | 8 bytes                                |
| **Catalog name**   | `pg_catalog.timestamptz`               |
| **OID**            | 1184                                   |
| **Min value**      | 4713 BC                                |
| **Max value**      | 294276 AD                              |
| **Max resolution** | 1 microsecond                          |

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI3NjkiIGhlaWdodD0iNDczIj4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSI1MSIgeT0iMyIgd2lkdGg9IjEwNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iMSIgd2lkdGg9IjEwNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjIxIj5USU1FU1RBTVA8L3RleHQ+CiAgIDxyZWN0IHg9IjE5NyIgeT0iMzUiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxOTUiIHk9IjMzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIwNSIgeT0iNTMiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjI0MyIgeT0iMzUiIHdpZHRoPSI4MCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjQxIiB5PSIzMyIgd2lkdGg9IjgwIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjUxIiB5PSI1MyI+cHJlY2lzaW9uPC90ZXh0PgogICA8cmVjdCB4PSIzNDMiIHk9IjM1IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzQxIiB5PSIzMyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzNTEiIHk9IjUzIj4pPC90ZXh0PgogICA8cmVjdCB4PSI0NDkiIHk9IjM1IiB3aWR0aD0iNTgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDQ3IiB5PSIzMyIgd2lkdGg9IjU4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0NTciIHk9IjUzIj5XSVRIPC90ZXh0PgogICA8cmVjdCB4PSI0NDkiIHk9Ijc5IiB3aWR0aD0iODgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDQ3IiB5PSI3NyIgd2lkdGg9Ijg4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0NTciIHk9Ijk3Ij5XSVRIT1VUPC90ZXh0PgogICA8cmVjdCB4PSI1NzciIHk9IjM1IiB3aWR0aD0iNTIiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjU3NSIgeT0iMzMiIHdpZHRoPSI1MiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjU4NSIgeT0iNTMiPlRJTUU8L3RleHQ+CiAgIDxyZWN0IHg9IjY0OSIgeT0iMzUiIHdpZHRoPSI1OCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iNjQ3IiB5PSIzMyIgd2lkdGg9IjU4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iNjU3IiB5PSI1MyI+Wk9ORTwvdGV4dD4KICAgPHJlY3QgeD0iNTEiIHk9IjEyMyIgd2lkdGg9IjEyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iMTIxIiB3aWR0aD0iMTI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1OSIgeT0iMTQxIj5USU1FU1RBTVBUWjwvdGV4dD4KICAgPHJlY3QgeD0iMjE1IiB5PSIxNTUiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyMTMiIHk9IjE1MyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyMjMiIHk9IjE3MyI+KDwvdGV4dD4KICAgPHJlY3QgeD0iMjYxIiB5PSIxNTUiIHdpZHRoPSI4MCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjU5IiB5PSIxNTMiIHdpZHRoPSI4MCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI2OSIgeT0iMTczIj5wcmVjaXNpb248L3RleHQ+CiAgIDxyZWN0IHg9IjM2MSIgeT0iMTU1IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzU5IiB5PSIxNTMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzY5IiB5PSIxNzMiPik8L3RleHQ+CiAgIDxyZWN0IHg9IjIxOCIgeT0iMjIxIiB3aWR0aD0iMjIiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjE2IiB5PSIyMTkiIHdpZHRoPSIyMiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjI2IiB5PSIyMzkiPiYjMzk7PC90ZXh0PgogICA8cmVjdCB4PSIyNjAiIHk9IjIyMSIgd2lkdGg9Ijc0IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyNTgiIHk9IjIxOSIgd2lkdGg9Ijc0IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjY4IiB5PSIyMzkiPmRhdGVfc3RyPC90ZXh0PgogICA8cmVjdCB4PSIzOTQiIHk9IjI1MyIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjM5MiIgeT0iMjUxIiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjQwMiIgeT0iMjcxIj48L3RleHQ+CiAgIDxyZWN0IHg9IjM5NCIgeT0iMjk3IiB3aWR0aD0iMjgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzkyIiB5PSIyOTUiIHdpZHRoPSIyOCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDAyIiB5PSIzMTUiPlQ8L3RleHQ+CiAgIDxyZWN0IHg9IjQ2MiIgeT0iMjUzIiB3aWR0aD0iNzIiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjQ2MCIgeT0iMjUxIiB3aWR0aD0iNzIiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI0NzAiIHk9IjI3MSI+dGltZV9zdHI8L3RleHQ+CiAgIDxyZWN0IHg9IjUzMSIgeT0iMzk1IiB3aWR0aD0iMzAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNTI5IiB5PSIzOTMiIHdpZHRoPSIzMCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTM5IiB5PSI0MTMiPis8L3RleHQ+CiAgIDxyZWN0IHg9IjUzMSIgeT0iNDM5IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNTI5IiB5PSI0MzciIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTM5IiB5PSI0NTciPi08L3RleHQ+CiAgIDxyZWN0IHg9IjYwMSIgeT0iMzk1IiB3aWR0aD0iNzgiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjU5OSIgeT0iMzkzIiB3aWR0aD0iNzgiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI2MDkiIHk9IjQxMyI+dHpfb2Zmc2V0PC90ZXh0PgogICA8cmVjdCB4PSI3MTkiIHk9IjM2MyIgd2lkdGg9IjIyIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjcxNyIgeT0iMzYxIiB3aWR0aD0iMjIiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjcyNyIgeT0iMzgxIj4mIzM5OzwvdGV4dD4KICAgPHBhdGggY2xhc3M9ImxpbmUiIGQ9Im0xNyAxNyBoMiBtMjAgMCBoMTAgbTEwNiAwIGgxMCBtMjAgMCBoMTAgbTAgMCBoMTgyIG0tMjEyIDAgaDIwIG0xOTIgMCBoMjAgbS0yMzIgMCBxMTAgMCAxMCAxMCBtMjEyIDAgcTAgLTEwIDEwIC0xMCBtLTIyMiAxMCB2MTIgbTIxMiAwIHYtMTIgbS0yMTIgMTIgcTAgMTAgMTAgMTAgbTE5MiAwIHExMCAwIDEwIC0xMCBtLTIwMiAxMCBoMTAgbTI2IDAgaDEwIG0wIDAgaDEwIG04MCAwIGgxMCBtMCAwIGgxMCBtMjYgMCBoMTAgbTQwIC0zMiBoMTAgbTAgMCBoMjg4IG0tMzE4IDAgaDIwIG0yOTggMCBoMjAgbS0zMzggMCBxMTAgMCAxMCAxMCBtMzE4IDAgcTAgLTEwIDEwIC0xMCBtLTMyOCAxMCB2MTIgbTMxOCAwIHYtMTIgbS0zMTggMTIgcTAgMTAgMTAgMTAgbTI5OCAwIHExMCAwIDEwIC0xMCBtLTI4OCAxMCBoMTAgbTU4IDAgaDEwIG0wIDAgaDMwIG0tMTI4IDAgaDIwIG0xMDggMCBoMjAgbS0xNDggMCBxMTAgMCAxMCAxMCBtMTI4IDAgcTAgLTEwIDEwIC0xMCBtLTEzOCAxMCB2MjQgbTEyOCAwIHYtMjQgbS0xMjggMjQgcTAgMTAgMTAgMTAgbTEwOCAwIHExMCAwIDEwIC0xMCBtLTExOCAxMCBoMTAgbTg4IDAgaDEwIG0yMCAtNDQgaDEwIG01MiAwIGgxMCBtMCAwIGgxMCBtNTggMCBoMTAgbS02OTYgLTMyIGgyMCBtNjk2IDAgaDIwIG0tNzM2IDAgcTEwIDAgMTAgMTAgbTcxNiAwIHEwIC0xMCAxMCAtMTAgbS03MjYgMTAgdjEwMCBtNzE2IDAgdi0xMDAgbS03MTYgMTAwIHEwIDEwIDEwIDEwIG02OTYgMCBxMTAgMCAxMCAtMTAgbS03MDYgMTAgaDEwIG0xMjQgMCBoMTAgbTIwIDAgaDEwIG0wIDAgaDE4MiBtLTIxMiAwIGgyMCBtMTkyIDAgaDIwIG0tMjMyIDAgcTEwIDAgMTAgMTAgbTIxMiAwIHEwIC0xMCAxMCAtMTAgbS0yMjIgMTAgdjEyIG0yMTIgMCB2LTEyIG0tMjEyIDEyIHEwIDEwIDEwIDEwIG0xOTIgMCBxMTAgMCAxMCAtMTAgbS0yMDIgMTAgaDEwIG0yNiAwIGgxMCBtMCAwIGgxMCBtODAgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0yMCAtMzIgaDMyMCBtMjIgLTEyMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS01NzMgMjE4IGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGgxMCBtMjIgMCBoMTAgbTAgMCBoMTAgbTc0IDAgaDEwIG0yMCAwIGgxMCBtMCAwIGgxNzAgbS0yMDAgMCBoMjAgbTE4MCAwIGgyMCBtLTIyMCAwIHExMCAwIDEwIDEwIG0yMDAgMCBxMCAtMTAgMTAgLTEwIG0tMjEwIDEwIHYxMiBtMjAwIDAgdi0xMiBtLTIwMCAxMiBxMCAxMCAxMCAxMCBtMTgwIDAgcTEwIDAgMTAgLTEwIG0tMTcwIDEwIGgxMCBtMjQgMCBoMTAgbTAgMCBoNCBtLTY4IDAgaDIwIG00OCAwIGgyMCBtLTg4IDAgcTEwIDAgMTAgMTAgbTY4IDAgcTAgLTEwIDEwIC0xMCBtLTc4IDEwIHYyNCBtNjggMCB2LTI0IG0tNjggMjQgcTAgMTAgMTAgMTAgbTQ4IDAgcTEwIDAgMTAgLTEwIG0tNTggMTAgaDEwIG0yOCAwIGgxMCBtMjAgLTQ0IGgxMCBtNzIgMCBoMTAgbTIyIC0zMiBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0xMDcgMTQyIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMjIgMCBoMTAgbTAgMCBoMTc4IG0tMjA4IDAgaDIwIG0xODggMCBoMjAgbS0yMjggMCBxMTAgMCAxMCAxMCBtMjA4IDAgcTAgLTEwIDEwIC0xMCBtLTIxOCAxMCB2MTIgbTIwOCAwIHYtMTIgbS0yMDggMTIgcTAgMTAgMTAgMTAgbTE4OCAwIHExMCAwIDEwIC0xMCBtLTE3OCAxMCBoMTAgbTMwIDAgaDEwIG0tNzAgMCBoMjAgbTUwIDAgaDIwIG0tOTAgMCBxMTAgMCAxMCAxMCBtNzAgMCBxMCAtMTAgMTAgLTEwIG0tODAgMTAgdjI0IG03MCAwIHYtMjQgbS03MCAyNCBxMCAxMCAxMCAxMCBtNTAgMCBxMTAgMCAxMCAtMTAgbS02MCAxMCBoMTAgbTI2IDAgaDEwIG0wIDAgaDQgbTIwIC00NCBoMTAgbTc4IDAgaDEwIG0yMCAtMzIgaDEwIG0yMiAwIGgxMCBtMyAwIGgtMyIgLz4KICAgPHBvbHlnb24gcG9pbnRzPSI3NTkgMzc3IDc2NyAzNzMgNzY3IDM4MSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9Ijc1OSAzNzcgNzUxIDM3MyA3NTEgMzgxIj48L3BvbHlnb24+Cjwvc3ZnPg==)

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
<td><strong>WITH TIME ZONE</strong></td>
<td>Apply the <em>tz_offset</em> field.</td>
</tr>
<tr>
<td><strong>WITHOUT TIME ZONE</strong></td>
<td>Ignore the <em>tz_offset</em> field.<br />
This is the default if neither <code>WITH TIME ZONE</code> nor
<code>WITHOUT TIME ZONE</code> is specified.</td>
</tr>
<tr>
<td><strong>TIMESTAMPTZ</strong></td>
<td>A shorter alias for <code>TIMESTAMP WITH TIME ZONE</code>.</td>
</tr>
<tr>
<td><em>precision</em></td>
<td>The number of digits of precision to use to represent fractional
seconds. If unspecified, timestamps use six digits of precision—i.e.,
they have a resolution of one microsecond.</td>
</tr>
<tr>
<td><em>date_str</em></td>
<td>A string representing a date in <code>Y-M-D</code>,
<code>Y M-D</code>, <code>Y M D</code> or <code>YMD</code> format.</td>
</tr>
<tr>
<td><em>time_str</em></td>
<td>A string representing a time of day in <code>H:M:S.NS</code>
format.</td>
</tr>
<tr>
<td><em>tz_offset</em></td>
<td>The timezone’s distance, in hours, from UTC.</td>
</tr>
</tbody>
</table>

## Details

- `timestamp` and `timestamp with time zone` store data in
  [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).
- The difference between the two types is that
  `timestamp with time zone` can read or write timestamps with the
  offset specified by the timezone. Importantly,
  `timestamp with time zone` itself doesn’t store any timezone data;
  Materialize simply performs the conversion from the time provided and
  UTC.
- Materialize assumes all clients expect UTC time, and does not
  currently support any other timezones.

### Valid casts

In addition to the casts listed below, `timestamp` and `timestamptz` can
be cast to and from each other implicitly.

#### From `timestamp` or `timestamptz`

You can [cast](../../functions/cast) `timestamp` or `timestamptz` to:

- [`date`](../date) (by assignment)
- [`text`](../text) (by assignment)
- [`time`](../time) (by assignment)

#### To `timestamp` or `timestamptz`

You can [cast](../../functions/cast) the following types to `timestamp`
or `timestamptz`:

- [`date`](../date) (implicitly)
- [`text`](../text) (explicitly)

### Valid operations

`timestamp` and `timestamp with time zone` data (collectively referred
to as `timestamp/tz`) supports the following operations with other
types.

| Operation | Computes |
|----|----|
| [`date`](../date) `+` [`interval`](../interval) | [`timestamp/tz`](../timestamp) |
| [`date`](../date) `-` [`interval`](../interval) | [`timestamp/tz`](../timestamp) |
| [`date`](../date) `+` [`time`](../time) | [`timestamp/tz`](../timestamp) |
| [`timestamp/tz`](../timestamp) `+` [`interval`](../interval) | [`timestamp/tz`](../timestamp) |
| [`timestamp/tz`](../timestamp) `-` [`interval`](../interval) | [`timestamp/tz`](../timestamp) |
| [`timestamp/tz`](../timestamp) `-` [`timestamp/tz`](../timestamp) | [`interval`](../interval) |

## Examples

### Return timestamp

<div class="highlight">

``` chroma
SELECT TIMESTAMP '2007-02-01 15:04:05' AS ts_v;
```

</div>

```
        ts_v
---------------------
 2007-02-01 15:04:05
```

### Return timestamp with time zone

<div class="highlight">

``` chroma
SELECT TIMESTAMPTZ '2007-02-01 15:04:05+06' AS tstz_v;
```

</div>

```
         tstz_v
-------------------------
 2007-02-01 09:04:05 UTC
```

## Related topics

- [`TIMEZONE` and `AT TIME ZONE`
  functions](../../functions/timezone-and-at-time-zone)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/types/timestamp.md"
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
