<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)

</div>

# COPY TO

`COPY TO` outputs results from Materialize to standard output or object
storage. This command is useful to output
[`SUBSCRIBE`](/docs/self-managed/v25.2/sql/subscribe/) results [to
`stdout`](#copy-to-stdout), or perform [bulk exports to Amazon
S3](#copy-to-s3).

## Copy to `stdout`

Copying results to `stdout` is useful to output the stream of updates
from a [`SUBSCRIBE`](/docs/self-managed/v25.2/sql/subscribe/) command in
interactive SQL clients like `psql`.

### Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI0NDkiIGhlaWdodD0iMTk1Ij4KICAgPHBvbHlnb24gcG9pbnRzPSIxMSAxNyAzIDEzIDMgMjEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSIxOSAxNyAxMSAxMyAxMSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMyIgeT0iMyIgd2lkdGg9IjYwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjMxIiB5PSIxIiB3aWR0aD0iNjAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjQxIiB5PSIyMSI+Q09QWTwvdGV4dD4KICAgPHJlY3QgeD0iMTEzIiB5PSIzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTExIiB5PSIxIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjEyMSIgeT0iMjEiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjE1OSIgeT0iMyIgd2lkdGg9IjU4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIxNTciIHk9IjEiIHdpZHRoPSI1OCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjE2NyIgeT0iMjEiPnF1ZXJ5PC90ZXh0PgogICA8cmVjdCB4PSIyMzciIHk9IjMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyMzUiIHk9IjEiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjQ1IiB5PSIyMSI+KTwvdGV4dD4KICAgPHJlY3QgeD0iMjgzIiB5PSIzIiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjgxIiB5PSIxIiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjI5MSIgeT0iMjEiPlRPPC90ZXh0PgogICA8cmVjdCB4PSIzNDMiIHk9IjMiIHdpZHRoPSI3OCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzNDEiIHk9IjEiIHdpZHRoPSI3OCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzUxIiB5PSIyMSI+U1RET1VUPC90ZXh0PgogICA8cmVjdCB4PSI2NSIgeT0iMTQ1IiB3aWR0aD0iNTgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNjMiIHk9IjE0MyIgd2lkdGg9IjU4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI3MyIgeT0iMTYzIj5XSVRIPC90ZXh0PgogICA8cmVjdCB4PSIxNjMiIHk9IjExMyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjE2MSIgeT0iMTExIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjE3MSIgeT0iMTMxIj4oPC90ZXh0PgogICA8cmVjdCB4PSIyMjkiIHk9IjExMyIgd2lkdGg9IjQ4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyMjciIHk9IjExMSIgd2lkdGg9IjQ4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjM3IiB5PSIxMzEiPmZpZWxkPC90ZXh0PgogICA8cmVjdCB4PSIyOTciIHk9IjExMyIgd2lkdGg9IjM4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyOTUiIHk9IjExMSIgd2lkdGg9IjM4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMzA1IiB5PSIxMzEiPnZhbDwvdGV4dD4KICAgPHJlY3QgeD0iMjI5IiB5PSI2OSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIyNyIgeT0iNjciIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjM3IiB5PSI4NyI+LDwvdGV4dD4KICAgPHJlY3QgeD0iMzc1IiB5PSIxMTMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzNzMiIHk9IjExMSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzODMiIHk9IjEzMSI+KTwvdGV4dD4KICAgPHBhdGggY2xhc3M9ImxpbmUiIGQ9Im0xOSAxNyBoMiBtMCAwIGgxMCBtNjAgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0wIDAgaDEwIG01OCAwIGgxMCBtMCAwIGgxMCBtMjYgMCBoMTAgbTAgMCBoMTAgbTQwIDAgaDEwIG0wIDAgaDEwIG03OCAwIGgxMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtLTQ0MCAxMTAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG00MiAwIGgxMCBtMCAwIGg2OCBtLTk4IDAgaDIwIG03OCAwIGgyMCBtLTExOCAwIHExMCAwIDEwIDEwIG05OCAwIHEwIC0xMCAxMCAtMTAgbS0xMDggMTAgdjEyIG05OCAwIHYtMTIgbS05OCAxMiBxMCAxMCAxMCAxMCBtNzggMCBxMTAgMCAxMCAtMTAgbS04OCAxMCBoMTAgbTU4IDAgaDEwIG0yMCAtMzIgaDEwIG0yNiAwIGgxMCBtMjAgMCBoMTAgbTQ4IDAgaDEwIG0wIDAgaDEwIG0zOCAwIGgxMCBtLTE0NiAwIGwyMCAwIG0tMSAwIHEtOSAwIC05IC0xMCBsMCAtMjQgcTAgLTEwIDEwIC0xMCBtMTI2IDQ0IGwyMCAwIG0tMjAgMCBxMTAgMCAxMCAtMTAgbDAgLTI0IHEwIC0xMCAtMTAgLTEwIG0tMTI2IDAgaDEwIG0yNCAwIGgxMCBtMCAwIGg4MiBtMjAgNDQgaDEwIG0yNiAwIGgxMCBtLTM5NiAwIGgyMCBtMzc2IDAgaDIwIG0tNDE2IDAgcTEwIDAgMTAgMTAgbTM5NiAwIHEwIC0xMCAxMCAtMTAgbS00MDYgMTAgdjQ2IG0zOTYgMCB2LTQ2IG0tMzk2IDQ2IHEwIDEwIDEwIDEwIG0zNzYgMCBxMTAgMCAxMCAtMTAgbS0zODYgMTAgaDEwIG0wIDAgaDM2NiBtMjMgLTY2IGgtMyIgLz4KICAgPHBvbHlnb24gcG9pbnRzPSI0MzkgMTI3IDQ0NyAxMjMgNDQ3IDEzMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjQzOSAxMjcgNDMxIDEyMyA0MzEgMTMxIj48L3BvbHlnb24+Cjwvc3ZnPg==)

</div>

| Field | Use |
|----|----|
| *query* | The [`SELECT`](/docs/self-managed/v25.2/sql/select) or [`SUBSCRIBE`](/docs/self-managed/v25.2/sql/subscribe) query to output results for. |
| *field* | The name of the option you want to set. |
| *val* | The value for the option. |

### `WITH` options

| Name | Values | Default value | Description |
|----|----|----|----|
| `FORMAT` | `TEXT`,`BINARY`, `CSV` | `TEXT` | Sets the output formatting method. |

### Examples

#### Subscribing to a view with binary output

<div class="highlight">

``` chroma
COPY (SUBSCRIBE some_view) TO STDOUT WITH (FORMAT binary);
```

</div>

## Copy to Amazon S3 and S3 compatible services

Copying results to Amazon S3 (or S3-compatible services) is useful to
perform tasks like periodic backups for auditing, or downstream
processing in analytical data warehouses like Snowflake, Databricks or
BigQuery. For step-by-step instructions, see the integration guide for
[Amazon S3](/docs/self-managed/v25.2/serve-results/s3/).

The `COPY TO` command is *one-shot*: every time you want to export
results, you must run the command. To automate exporting results on a
regular basis, you can set up scheduling, for example using a simple
`cron`-like service or an orchestration platform like Airflow or
Dagster.

### Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MzkiIGhlaWdodD0iMjczIj4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iMyIgd2lkdGg9IjYwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI5IiB5PSIxIiB3aWR0aD0iNjAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM5IiB5PSIyMSI+Q09QWTwvdGV4dD4KICAgPHJlY3QgeD0iMTMxIiB5PSIzIiB3aWR0aD0iNTgiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjEyOSIgeT0iMSIgd2lkdGg9IjU4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMTM5IiB5PSIyMSI+cXVlcnk8L3RleHQ+CiAgIDxyZWN0IHg9IjEzMSIgeT0iNDciIHdpZHRoPSIxMDQiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjEyOSIgeT0iNDUiIHdpZHRoPSIxMDQiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIxMzkiIHk9IjY1Ij5vYmplY3RfbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iMjc1IiB5PSIzIiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjczIiB5PSIxIiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjI4MyIgeT0iMjEiPlRPPC90ZXh0PgogICA8cmVjdCB4PSIzMzUiIHk9IjMiIHdpZHRoPSI1OCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMzMzIiB5PSIxIiB3aWR0aD0iNTgiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzNDMiIHk9IjIxIj5zM191cmk8L3RleHQ+CiAgIDxyZWN0IHg9IjQxMyIgeT0iMyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQxMSIgeT0iMSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0MjEiIHk9IjIxIj4oPC90ZXh0PgogICA8cmVjdCB4PSI0NTkiIHk9IjMiIHdpZHRoPSI1OCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0NTciIHk9IjEiIHdpZHRoPSI1OCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDY3IiB5PSIyMSI+V0lUSDwvdGV4dD4KICAgPHJlY3QgeD0iNDQiIHk9IjExMyIgd2lkdGg9IjE1NCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0MiIgeT0iMTExIiB3aWR0aD0iMTU0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1MiIgeT0iMTMxIj5BV1MgQ09OTkVDVElPTjwvdGV4dD4KICAgPHJlY3QgeD0iMjE4IiB5PSIxMTMiIHdpZHRoPSIxMzYiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjIxNiIgeT0iMTExIiB3aWR0aD0iMTM2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjI2IiB5PSIxMzEiPmNvbm5lY3Rpb25fbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iMzc0IiB5PSIxMTMiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzNzIiIHk9IjExMSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzODIiIHk9IjEzMSI+LDwvdGV4dD4KICAgPHJlY3QgeD0iNDE4IiB5PSIxMTMiIHdpZHRoPSI4MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0MTYiIHk9IjExMSIgd2lkdGg9IjgwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0MjYiIHk9IjEzMSI+Rk9STUFUPC90ZXh0PgogICA8cmVjdCB4PSIxMjUiIHk9IjE5NSIgd2lkdGg9IjQyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIxMjMiIHk9IjE5MyIgd2lkdGg9IjQyIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMTMzIiB5PSIyMTMiPmNzdjwvdGV4dD4KICAgPHJlY3QgeD0iMTI1IiB5PSIyMzkiIHdpZHRoPSI3MCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMTIzIiB5PSIyMzciIHdpZHRoPSI3MCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjEzMyIgeT0iMjU3Ij5wYXJxdWV0PC90ZXh0PgogICA8cmVjdCB4PSIyNzUiIHk9IjE5NSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI3MyIgeT0iMTkzIiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjI4MyIgeT0iMjEzIj4sPC90ZXh0PgogICA8cmVjdCB4PSIzMTkiIHk9IjE5NSIgd2lkdGg9IjQ4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzMTciIHk9IjE5MyIgd2lkdGg9IjQ4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMzI3IiB5PSIyMTMiPmZpZWxkPC90ZXh0PgogICA8cmVjdCB4PSIzODciIHk9IjE5NSIgd2lkdGg9IjM4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzODUiIHk9IjE5MyIgd2lkdGg9IjM4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMzk1IiB5PSIyMTMiPnZhbDwvdGV4dD4KICAgPHJlY3QgeD0iNDg1IiB5PSIxOTUiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0ODMiIHk9IjE5MyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0OTMiIHk9IjIxMyI+KTwvdGV4dD4KICAgPHBhdGggY2xhc3M9ImxpbmUiIGQ9Im0xNyAxNyBoMiBtMCAwIGgxMCBtNjAgMCBoMTAgbTIwIDAgaDEwIG01OCAwIGgxMCBtMCAwIGg0NiBtLTE0NCAwIGgyMCBtMTI0IDAgaDIwIG0tMTY0IDAgcTEwIDAgMTAgMTAgbTE0NCAwIHEwIC0xMCAxMCAtMTAgbS0xNTQgMTAgdjI0IG0xNDQgMCB2LTI0IG0tMTQ0IDI0IHEwIDEwIDEwIDEwIG0xMjQgMCBxMTAgMCAxMCAtMTAgbS0xMzQgMTAgaDEwIG0xMDQgMCBoMTAgbTIwIC00NCBoMTAgbTQwIDAgaDEwIG0wIDAgaDEwIG01OCAwIGgxMCBtMCAwIGgxMCBtMjYgMCBoMTAgbTAgMCBoMTAgbTU4IDAgaDEwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tNTE3IDExMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBoMTAgbTE1NCAwIGgxMCBtMCAwIGgxMCBtMTM2IDAgaDEwIG0wIDAgaDEwIG0yNCAwIGgxMCBtMCAwIGgxMCBtODAgMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS00MzcgODIgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtNDIgMCBoMTAgbTAgMCBoMjggbS0xMTAgMCBoMjAgbTkwIDAgaDIwIG0tMTMwIDAgcTEwIDAgMTAgMTAgbTExMCAwIHEwIC0xMCAxMCAtMTAgbS0xMjAgMTAgdjI0IG0xMTAgMCB2LTI0IG0tMTEwIDI0IHEwIDEwIDEwIDEwIG05MCAwIHExMCAwIDEwIC0xMCBtLTEwMCAxMCBoMTAgbTcwIDAgaDEwIG02MCAtNDQgaDEwIG0yNCAwIGgxMCBtMCAwIGgxMCBtNDggMCBoMTAgbTAgMCBoMTAgbTM4IDAgaDEwIG0tMTkwIDAgbDIwIDAgbS0xIDAgcS05IDAgLTkgLTEwIGwwIC0xMiBxMCAtMTAgMTAgLTEwIG0xNzAgMzIgbDIwIDAgbS0yMCAwIHExMCAwIDEwIC0xMCBsMCAtMTIgcTAgLTEwIC0xMCAtMTAgbS0xNzAgMCBoMTAgbTAgMCBoMTYwIG0tMjEwIDMyIGgyMCBtMjEwIDAgaDIwIG0tMjUwIDAgcTEwIDAgMTAgMTAgbTIzMCAwIHEwIC0xMCAxMCAtMTAgbS0yNDAgMTAgdjE0IG0yMzAgMCB2LTE0IG0tMjMwIDE0IHEwIDEwIDEwIDEwIG0yMTAgMCBxMTAgMCAxMCAtMTAgbS0yMjAgMTAgaDEwIG0wIDAgaDIwMCBtMjAgLTM0IGgxMCBtMjYgMCBoMTAgbTMgMCBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTI5IDIwOSA1MzcgMjA1IDUzNyAyMTMiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSI1MjkgMjA5IDUyMSAyMDUgNTIxIDIxMyI+PC9wb2x5Z29uPgo8L3N2Zz4=)

</div>

| Field | Use |
|----|----|
| *query* | The [`SELECT`](/docs/self-managed/v25.2/sql/select) query to copy results out for. |
| *object_name* | The name of the object to copy results out for. |
| **AWS CONNECTION** *connection_name* | The name of the AWS connection to use in the `COPY TO` command. For details on creating connections, check the [`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection/#aws) documentation page. |
| *s3_uri* | The unique resource identifier (URI) of the Amazon S3 bucket (and prefix) to store the output results in. |
| **FORMAT** | The file format to write. |
| *field* | The name of the option you want to set. |
| *val* | The value for the option. |

### `WITH` options

| Name | Values | Default value | Description |
|----|----|----|----|
| `MAX FILE SIZE` | `integer` |  | Sets the approximate maximum file size (in bytes) of each file uploaded to the S3 bucket. |

### Supported formats

#### CSV

**Syntax:** `FORMAT = 'csv'`

By default, Materialize writes CSV files using the following writer
settings:

| Setting   | Value   |
|-----------|---------|
| delimiter | `,`     |
| quote     | `"`     |
| escape    | `"`     |
| header    | `false` |

#### Parquet

**Syntax:** `FORMAT = 'parquet'`

Materialize writes Parquet files that aim for maximum compatibility with
downstream systems. By default, the following Parquet writer settings
are used:

| Setting                       | Value            |
|-------------------------------|------------------|
| Writer version                | 1.0              |
| Compression                   | `snappy`         |
| Default column encoding       | Dictionary       |
| Fallback column encoding      | Plain            |
| Dictionary page encoding      | Plain            |
| Dictionary data page encoding | `RLE_DICTIONARY` |

If you run into a snag trying to ingest Parquet files produced by
Materialize into your downstream systems, please [contact our
team](/docs/self-managed/v25.2/support/) or [open a bug
report](https://github.com/MaterializeInc/materialize/discussions/new?category=bug-reports)!

##### Data types

Materialize converts the values in the result set to [Apache
Arrow](https://arrow.apache.org/docs/index.html), and then serializes
this Arrow representation to Parquet. The Arrow schema is embedded in
the Parquet file metadata and allows reconstructing the Arrow
representation using a compatible reader.

Materialize also includes [Parquet `LogicalType`
annotations](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#metadata)
where possible. However, many newer `LogicalType` annotations are not
supported in the 1.0 writer version.

Materialize also embeds its own type information into the Apache Arrow
schema. The field metadata in the schema contains an
`ARROW:extension:name` annotation to indicate the Materialize native
type the field originated from.

| Materialize type | Arrow extension name | [Arrow type](https://github.com/apache/arrow/blob/main/format/Schema.fbs) | [Parquet primitive type](https://parquet.apache.org/docs/file-format/types/) | [Parquet logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) |
|----|----|----|----|----|
| [`bigint`](/docs/self-managed/v25.2/sql/types/integer/#bigint-info) | `materialize.v1.bigint` | `int64` | `INT64` |  |
| [`boolean`](/docs/self-managed/v25.2/sql/types/boolean/) | `materialize.v1.boolean` | `bool` | `BOOLEAN` |  |
| [`bytea`](/docs/self-managed/v25.2/sql/types/bytea/) | `materialize.v1.bytea` | `large_binary` | `BYTE_ARRAY` |  |
| [`date`](/docs/self-managed/v25.2/sql/types/date/) | `materialize.v1.date` | `date32` | `INT32` | `DATE` |
| [`double precision`](/docs/self-managed/v25.2/sql/types/float/#double-precision-info) | `materialize.v1.double` | `float64` | `DOUBLE` |  |
| [`integer`](/docs/self-managed/v25.2/sql/types/integer/#integer-info) | `materialize.v1.integer` | `int32` | `INT32` |  |
| [`jsonb`](/docs/self-managed/v25.2/sql/types/jsonb/) | `materialize.v1.jsonb` | `large_utf8` | `BYTE_ARRAY` |  |
| [`map`](/docs/self-managed/v25.2/sql/types/map/) | `materialize.v1.map` | `map` (`struct` with fields `keys` and `values`) | Nested | `MAP` |
| [`list`](/docs/self-managed/v25.2/sql/types/list/) | `materialize.v1.list` | `list` | Nested |  |
| [`numeric`](/docs/self-managed/v25.2/sql/types/numeric/) | `materialize.v1.numeric` | `decimal128[38, 10 or max-scale]` | `FIXED_LEN_BYTE_ARRAY` | `DECIMAL` |
| [`real`](/docs/self-managed/v25.2/sql/types/float/#real-info) | `materialize.v1.real` | `float32` | `FLOAT` |  |
| [`smallint`](/docs/self-managed/v25.2/sql/types/integer/#smallint-info) | `materialize.v1.smallint` | `int16` | `INT32` | `INT(16, true)` |
| [`text`](/docs/self-managed/v25.2/sql/types/text/) | `materialize.v1.text` | `utf8` or `large_utf8` | `BYTE_ARRAY` | `STRING` |
| [`time`](/docs/self-managed/v25.2/sql/types/time/) | `materialize.v1.time` | `time64[nanosecond]` | `INT64` | `TIME[isAdjustedToUTC = false, unit = NANOS]` |
| [`uint2`](/docs/self-managed/v25.2/sql/types/uint/#uint2-info) | `materialize.v1.uint2` | `uint16` | `INT32` | `INT(16, false)` |
| [`uint4`](/docs/self-managed/v25.2/sql/types/uint/#uint4-info) | `materialize.v1.uint4` | `uint32` | `INT32` | `INT(32, false)` |
| [`uint8`](/docs/self-managed/v25.2/sql/types/uint/#uint8-info) | `materialize.v1.uint8` | `uint64` | `INT64` | `INT(64, false)` |
| [`timestamp`](/docs/self-managed/v25.2/sql/types/timestamp/#timestamp-info) | `materialize.v1.timestamp` | `time64[microsecond]` | `INT64` | `TIMESTAMP[isAdjustedToUTC = false, unit = MICROS]` |
| [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp/#timestamp-with-time-zone-info) | `materialize.v1.timestampz` | `time64[microsecond]` | `INT64` | `TIMESTAMP[isAdjustedToUTC = true, unit = MICROS]` |
| [Arrays](/docs/self-managed/v25.2/sql/types/array/) (`[]`) | `materialize.v1.array` | `struct` with `list` field `items` and `uint8` field `dimensions` | Nested |  |
| [`uuid`](/docs/self-managed/v25.2/sql/types/uuid/) | `materialize.v1.uuid` | `fixed_size_binary(16)` | `FIXED_LEN_BYTE_ARRAY` |  |
| [`oid`](/docs/self-managed/v25.2/sql/types/oid/) | Unsupported |  |  |  |
| [`interval`](/docs/self-managed/v25.2/sql/types/interval/) | Unsupported |  |  |  |
| [`record`](/docs/self-managed/v25.2/sql/types/record/) | Unsupported |  |  |  |

### Examples

<div class="code-tabs">

<div class="tab-content">

<div id="tab-parquet" class="tab-pane" title="Parquet">

<div class="highlight">

``` chroma
COPY some_view TO 's3://mz-to-snow/parquet/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'parquet'
  );
```

</div>

</div>

<div id="tab-csv" class="tab-pane" title="CSV">

<div class="highlight">

``` chroma
COPY some_view TO 's3://mz-to-snow/csv/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'csv'
  );
```

</div>

</div>

</div>

</div>

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations and types in the
  query are contained in.
- `SELECT` privileges on all relations in the query.
  - NOTE: if any item is a view, then the view owner must also have the
    necessary privileges to execute the view definition. Even if the
    view owner is a *superuser*, they still must explicitly be granted
    the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

## Related pages

- [`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection)
- Integration guides:
  - [Amazon S3](/docs/self-managed/v25.2/serve-results/s3/)
  - [Snowflake (via
    S3)](/docs/self-managed/v25.2/serve-results/snowflake/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/copy-to.md"
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
