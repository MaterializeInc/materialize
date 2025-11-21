<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [CREATE
SOURCE](/docs/sql/create-source/)

</div>

# CREATE SOURCE: PostgreSQL (Legacy Syntax)

<div class="annotation">

<div class="annotation-title">

Disambiguation

</div>

<div>

This page reflects the legacy syntax, which requires downtime to handle
upstream DDL changes. For the new syntax which can handle adding or
dropping columns to the upstream tables without downtime, see the [new
reference page](/docs/sql/create-source/postgres-v2).

</div>

</div>

[`CREATE SOURCE`](/docs/sql/create-source/) connects Materialize to an
external system you want to read data from, and provides details about
how to decode and interpret that data.

Materialize supports PostgreSQL (11+) as a data source. To connect to a
PostgreSQL instance, you first need to [create a
connection](#creating-a-connection) that specifies access and
authentication parameters. Once created, a connection is **reusable**
across multiple `CREATE SOURCE` statements.

<div class="warning">

**WARNING!** Before creating a PostgreSQL source, you must set up
logical replication in the upstream database. For step-by-step
instructions, see the integration guide for your PostgreSQL service:
[AlloyDB](/docs/ingest-data/postgres-alloydb/), [Amazon
RDS](/docs/ingest-data/postgres-amazon-rds/), [Amazon
Aurora](/docs/ingest-data/postgres-amazon-aurora/), [Azure
DB](/docs/ingest-data/postgres-azure-db/), [Google Cloud
SQL](/docs/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/docs/ingest-data/postgres-self-hosted/).

</div>

<div class="note">

**NOTE:** Connections using AWS PrivateLink is for Materialize Cloud
only.

</div>

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI3MDciIGhlaWdodD0iMTA5MSI+CiAgIDxwb2x5Z29uIHBvaW50cz0iOSAxNyAxIDEzIDEgMjEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSIxNyAxNyA5IDEzIDkgMjEiPjwvcG9seWdvbj4KICAgPHJlY3QgeD0iMzEiIHk9IjMiIHdpZHRoPSIxNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjkiIHk9IjEiIHdpZHRoPSIxNDAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM5IiB5PSIyMSI+Q1JFQVRFIFNPVVJDRTwvdGV4dD4KICAgPHJlY3QgeD0iMjExIiB5PSIzNSIgd2lkdGg9IjEyMCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyMDkiIHk9IjMzIiB3aWR0aD0iMTIwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyMTkiIHk9IjUzIj5JRiBOT1QgRVhJU1RTPC90ZXh0PgogICA8cmVjdCB4PSIzNzEiIHk9IjMiIHdpZHRoPSI4MiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMzY5IiB5PSIxIiB3aWR0aD0iODIiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzNzkiIHk9IjIxIj5zcmNfbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iMTQxIiB5PSIxMzMiIHdpZHRoPSIxMDQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTM5IiB5PSIxMzEiIHdpZHRoPSIxMDQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjE0OSIgeT0iMTUxIj5JTiBDTFVTVEVSPC90ZXh0PgogICA8cmVjdCB4PSIyNjUiIHk9IjEzMyIgd2lkdGg9IjEwOCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjYzIiB5PSIxMzEiIHdpZHRoPSIxMDgiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIyNzMiIHk9IjE1MSI+Y2x1c3Rlcl9uYW1lPC90ZXh0PgogICA8cmVjdCB4PSI0MTMiIHk9IjEwMSIgd2lkdGg9IjYwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQxMSIgeT0iOTkiIHdpZHRoPSI2MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDIxIiB5PSIxMTkiPkZST008L3RleHQ+CiAgIDxyZWN0IHg9IjQ5MyIgeT0iMTAxIiB3aWR0aD0iOTYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDkxIiB5PSI5OSIgd2lkdGg9Ijk2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1MDEiIHk9IjExOSI+UE9TVEdSRVM8L3RleHQ+CiAgIDxyZWN0IHg9IjEyOCIgeT0iMTk5IiB3aWR0aD0iMTE2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjEyNiIgeT0iMTk3IiB3aWR0aD0iMTE2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMzYiIHk9IjIxNyI+Q09OTkVDVElPTjwvdGV4dD4KICAgPHJlY3QgeD0iMjY0IiB5PSIxOTkiIHdpZHRoPSIxMzYiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjI2MiIgeT0iMTk3IiB3aWR0aD0iMTM2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjcyIiB5PSIyMTciPmNvbm5lY3Rpb25fbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iNDIwIiB5PSIxOTkiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0MTgiIHk9IjE5NyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0MjgiIHk9IjIxNyI+KDwvdGV4dD4KICAgPHJlY3QgeD0iNDY2IiB5PSIxOTkiIHdpZHRoPSIxMTYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDY0IiB5PSIxOTciIHdpZHRoPSIxMTYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjQ3NCIgeT0iMjE3Ij5QVUJMSUNBVElPTjwvdGV4dD4KICAgPHJlY3QgeD0iMjg4IiB5PSIyNjUiIHdpZHRoPSIxMzQiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjI4NiIgeT0iMjYzIiB3aWR0aD0iMTM0IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjk2IiB5PSIyODMiPnB1YmxpY2F0aW9uX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjExNSIgeT0iMzc1IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTEzIiB5PSIzNzMiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTIzIiB5PSIzOTMiPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjE1OSIgeT0iMzc1IiB3aWR0aD0iMTM0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjE1NyIgeT0iMzczIiB3aWR0aD0iMTM0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxNjciIHk9IjM5MyI+VEVYVCBDT0xVTU5TPC90ZXh0PgogICA8cmVjdCB4PSIzMzMiIHk9IjM3NSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjMzMSIgeT0iMzczIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM0MSIgeT0iMzkzIj4oPC90ZXh0PgogICA8cmVjdCB4PSIzOTkiIHk9IjM3NSIgd2lkdGg9IjExMCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMzk3IiB5PSIzNzMiIHdpZHRoPSIxMTAiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI0MDciIHk9IjM5MyI+Y29sdW1uX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjM5OSIgeT0iMzMxIiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzk3IiB5PSIzMjkiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDA3IiB5PSIzNDkiPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjU0OSIgeT0iMzc1IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNTQ3IiB5PSIzNzMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTU3IiB5PSIzOTMiPik8L3RleHQ+CiAgIDxyZWN0IHg9IjEwMSIgeT0iNTE3IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iOTkiIHk9IjUxNSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMDkiIHk9IjUzNSI+LDwvdGV4dD4KICAgPHJlY3QgeD0iMTQ1IiB5PSI1MTciIHdpZHRoPSIxNjIiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTQzIiB5PSI1MTUiIHdpZHRoPSIxNjIiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjE1MyIgeT0iNTM1Ij5FWENMVURFIENPTFVNTlM8L3RleHQ+CiAgIDxyZWN0IHg9IjM0NyIgeT0iNTE3IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzQ1IiB5PSI1MTUiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzU1IiB5PSI1MzUiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjQxMyIgeT0iNTE3IiB3aWR0aD0iMTEwIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI0MTEiIHk9IjUxNSIgd2lkdGg9IjExMCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjQyMSIgeT0iNTM1Ij5jb2x1bW5fbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iNDEzIiB5PSI0NzMiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0MTEiIHk9IjQ3MSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0MjEiIHk9IjQ5MSI+LDwvdGV4dD4KICAgPHJlY3QgeD0iNTYzIiB5PSI1MTciIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI1NjEiIHk9IjUxNSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1NzEiIHk9IjUzNSI+KTwvdGV4dD4KICAgPHJlY3QgeD0iMzQyIiB5PSI2MTUiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzNDAiIHk9IjYxMyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzNTAiIHk9IjYzMyI+KTwvdGV4dD4KICAgPHJlY3QgeD0iNDUiIHk9IjY4MSIgd2lkdGg9IjEzOCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0MyIgeT0iNjc5IiB3aWR0aD0iMTM4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1MyIgeT0iNjk5Ij5GT1IgQUxMIFRBQkxFUzwvdGV4dD4KICAgPHJlY3QgeD0iNjUiIHk9Ijc2OSIgd2lkdGg9IjEwNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI2MyIgeT0iNzY3IiB3aWR0aD0iMTA2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI3MyIgeT0iNzg3Ij5GT1IgVEFCTEVTPC90ZXh0PgogICA8cmVjdCB4PSIxOTEiIHk9Ijc2OSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjE4OSIgeT0iNzY3IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjE5OSIgeT0iNzg3Ij4oPC90ZXh0PgogICA8cmVjdCB4PSIyNTciIHk9Ijc2OSIgd2lkdGg9Ijk2IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyNTUiIHk9Ijc2NyIgd2lkdGg9Ijk2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjY1IiB5PSI3ODciPnRhYmxlX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjM5MyIgeT0iODAxIiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzkxIiB5PSI3OTkiIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDAxIiB5PSI4MTkiPkFTPC90ZXh0PgogICA8cmVjdCB4PSI0NTMiIHk9IjgwMSIgd2lkdGg9IjEwNiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iNDUxIiB5PSI3OTkiIHdpZHRoPSIxMDYiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI0NjEiIHk9IjgxOSI+c3Vic3JjX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjI1NyIgeT0iNzI1IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjU1IiB5PSI3MjMiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjY1IiB5PSI3NDMiPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjY1IiB5PSI4ODkiIHdpZHRoPSIxMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNjMiIHk9Ijg4NyIgd2lkdGg9IjEyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNzMiIHk9IjkwNyI+Rk9SIFNDSEVNQVM8L3RleHQ+CiAgIDxyZWN0IHg9IjIwOSIgeT0iODg5IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjA3IiB5PSI4ODciIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjE3IiB5PSI5MDciPig8L3RleHQ+CiAgIDxyZWN0IHg9IjI3NSIgeT0iODg5IiB3aWR0aD0iMTE0IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyNzMiIHk9Ijg4NyIgd2lkdGg9IjExNCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI4MyIgeT0iOTA3Ij5zY2hlbWFfbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iMjc1IiB5PSI4NDUiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyNzMiIHk9Ijg0MyIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyODMiIHk9Ijg2MyI+LDwvdGV4dD4KICAgPHJlY3QgeD0iNjM5IiB5PSI3NjkiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI2MzciIHk9Ijc2NyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI2NDciIHk9Ijc4NyI+KTwvdGV4dD4KICAgPHJlY3QgeD0iMTIxIiB5PSI5NzEiIHdpZHRoPSI3NiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMTkiIHk9Ijk2OSIgd2lkdGg9Ijc2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMjkiIHk9Ijk4OSI+RVhQT1NFPC90ZXh0PgogICA8cmVjdCB4PSIyMTciIHk9Ijk3MSIgd2lkdGg9Ijk2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIxNSIgeT0iOTY5IiB3aWR0aD0iOTYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIyNSIgeT0iOTg5Ij5QUk9HUkVTUzwvdGV4dD4KICAgPHJlY3QgeD0iMzMzIiB5PSI5NzEiIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzMzEiIHk9Ijk2OSIgd2lkdGg9IjQwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzNDEiIHk9Ijk4OSI+QVM8L3RleHQ+CiAgIDxyZWN0IHg9IjM5MyIgeT0iOTcxIiB3aWR0aD0iMTk2IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzOTEiIHk9Ijk2OSIgd2lkdGg9IjE5NiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjQwMSIgeT0iOTg5Ij5wcm9ncmVzc19zdWJzb3VyY2VfbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iNTU3IiB5PSIxMDU3IiB3aWR0aD0iMTAyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI1NTUiIHk9IjEwNTUiIHdpZHRoPSIxMDIiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI1NjUiIHk9IjEwNzUiPndpdGhfb3B0aW9uczwvdGV4dD4KICAgPHBhdGggY2xhc3M9ImxpbmUiIGQ9Im0xNyAxNyBoMiBtMCAwIGgxMCBtMTQwIDAgaDEwIG0yMCAwIGgxMCBtMCAwIGgxMzAgbS0xNjAgMCBoMjAgbTE0MCAwIGgyMCBtLTE4MCAwIHExMCAwIDEwIDEwIG0xNjAgMCBxMCAtMTAgMTAgLTEwIG0tMTcwIDEwIHYxMiBtMTYwIDAgdi0xMiBtLTE2MCAxMiBxMCAxMCAxMCAxMCBtMTQwIDAgcTEwIDAgMTAgLTEwIG0tMTUwIDEwIGgxMCBtMTIwIDAgaDEwIG0yMCAtMzIgaDEwIG04MiAwIGgxMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtLTM3NiA5OCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIyIDAgaDEwIG0wIDAgaDI0MiBtLTI3MiAwIGgyMCBtMjUyIDAgaDIwIG0tMjkyIDAgcTEwIDAgMTAgMTAgbTI3MiAwIHEwIC0xMCAxMCAtMTAgbS0yODIgMTAgdjEyIG0yNzIgMCB2LTEyIG0tMjcyIDEyIHEwIDEwIDEwIDEwIG0yNTIgMCBxMTAgMCAxMCAtMTAgbS0yNjIgMTAgaDEwIG0xMDQgMCBoMTAgbTAgMCBoMTAgbTEwOCAwIGgxMCBtMjAgLTMyIGgxMCBtNjAgMCBoMTAgbTAgMCBoMTAgbTk2IDAgaDEwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tNTA1IDk4IGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGgxMCBtMTE2IDAgaDEwIG0wIDAgaDEwIG0xMzYgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0wIDAgaDEwIG0xMTYgMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0zMzggNjYgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgaDEwIG0xMzQgMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0zNzEgMTEwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMjIgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDEwIG0xMzQgMCBoMTAgbTIwIDAgaDEwIG0yNiAwIGgxMCBtMjAgMCBoMTAgbTExMCAwIGgxMCBtLTE1MCAwIGwyMCAwIG0tMSAwIHEtOSAwIC05IC0xMCBsMCAtMjQgcTAgLTEwIDEwIC0xMCBtMTMwIDQ0IGwyMCAwIG0tMjAgMCBxMTAgMCAxMCAtMTAgbDAgLTI0IHEwIC0xMCAtMTAgLTEwIG0tMTMwIDAgaDEwIG0yNCAwIGgxMCBtMCAwIGg4NiBtMjAgNDQgaDEwIG0yNiAwIGgxMCBtLTI4MiAwIGgyMCBtMjYyIDAgaDIwIG0tMzAyIDAgcTEwIDAgMTAgMTAgbTI4MiAwIHEwIC0xMCAxMCAtMTAgbS0yOTIgMTAgdjE0IG0yODIgMCB2LTE0IG0tMjgyIDE0IHEwIDEwIDEwIDEwIG0yNjIgMCBxMTAgMCAxMCAtMTAgbS0yNzIgMTAgaDEwIG0wIDAgaDI1MiBtLTUwMCAtMzQgaDIwIG01MDAgMCBoMjAgbS01NDAgMCBxMTAgMCAxMCAxMCBtNTIwIDAgcTAgLTEwIDEwIC0xMCBtLTUzMCAxMCB2MzAgbTUyMCAwIHYtMzAgbS01MjAgMzAgcTAgMTAgMTAgMTAgbTUwMCAwIHExMCAwIDEwIC0xMCBtLTUxMCAxMCBoMTAgbTAgMCBoNDkwIG0yMiAtNTAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tNTc4IDE0MiBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIyIDAgaDEwIG0yNCAwIGgxMCBtMCAwIGgxMCBtMTYyIDAgaDEwIG0yMCAwIGgxMCBtMjYgMCBoMTAgbTIwIDAgaDEwIG0xMTAgMCBoMTAgbS0xNTAgMCBsMjAgMCBtLTEgMCBxLTkgMCAtOSAtMTAgbDAgLTI0IHEwIC0xMCAxMCAtMTAgbTEzMCA0NCBsMjAgMCBtLTIwIDAgcTEwIDAgMTAgLTEwIGwwIC0yNCBxMCAtMTAgLTEwIC0xMCBtLTEzMCAwIGgxMCBtMjQgMCBoMTAgbTAgMCBoODYgbTIwIDQ0IGgxMCBtMjYgMCBoMTAgbS0yODIgMCBoMjAgbTI2MiAwIGgyMCBtLTMwMiAwIHExMCAwIDEwIDEwIG0yODIgMCBxMCAtMTAgMTAgLTEwIG0tMjkyIDEwIHYxNCBtMjgyIDAgdi0xNCBtLTI4MiAxNCBxMCAxMCAxMCAxMCBtMjYyIDAgcTEwIDAgMTAgLTEwIG0tMjcyIDEwIGgxMCBtMCAwIGgyNTIgbS01MjggLTM0IGgyMCBtNTI4IDAgaDIwIG0tNTY4IDAgcTEwIDAgMTAgMTAgbTU0OCAwIHEwIC0xMCAxMCAtMTAgbS01NTggMTAgdjMwIG01NDggMCB2LTMwIG0tNTQ4IDMwIHEwIDEwIDEwIDEwIG01MjggMCBxMTAgMCAxMCAtMTAgbS01MzggMTAgaDEwIG0wIDAgaDUxOCBtMjIgLTUwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtLTMzMSA5OCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBoMTAgbTI2IDAgaDEwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tMzg3IDY2IGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMjIgMCBoMTAgbTEzOCAwIGgxMCBtMCAwIGg0ODIgbS02NjAgMCBoMjAgbTY0MCAwIGgyMCBtLTY4MCAwIHExMCAwIDEwIDEwIG02NjAgMCBxMCAtMTAgMTAgLTEwIG0tNjcwIDEwIHY2OCBtNjYwIDAgdi02OCBtLTY2MCA2OCBxMCAxMCAxMCAxMCBtNjQwIDAgcTEwIDAgMTAgLTEwIG0tNjMwIDEwIGgxMCBtMTA2IDAgaDEwIG0wIDAgaDEwIG0yNiAwIGgxMCBtMjAgMCBoMTAgbTk2IDAgaDEwIG0yMCAwIGgxMCBtMCAwIGgxNzYgbS0yMDYgMCBoMjAgbTE4NiAwIGgyMCBtLTIyNiAwIHExMCAwIDEwIDEwIG0yMDYgMCBxMCAtMTAgMTAgLTEwIG0tMjE2IDEwIHYxMiBtMjA2IDAgdi0xMiBtLTIwNiAxMiBxMCAxMCAxMCAxMCBtMTg2IDAgcTEwIDAgMTAgLTEwIG0tMTk2IDEwIGgxMCBtNDAgMCBoMTAgbTAgMCBoMTAgbTEwNiAwIGgxMCBtLTM0MiAtMzIgbDIwIDAgbS0xIDAgcS05IDAgLTkgLTEwIGwwIC0yNCBxMCAtMTAgMTAgLTEwIG0zNDIgNDQgbDIwIDAgbS0yMCAwIHExMCAwIDEwIC0xMCBsMCAtMjQgcTAgLTEwIC0xMCAtMTAgbS0zNDIgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDI5OCBtLTU1NCA0NCBoMjAgbTU1NCAwIGgyMCBtLTU5NCAwIHExMCAwIDEwIDEwIG01NzQgMCBxMCAtMTAgMTAgLTEwIG0tNTg0IDEwIHYxMDAgbTU3NCAwIHYtMTAwIG0tNTc0IDEwMCBxMCAxMCAxMCAxMCBtNTU0IDAgcTEwIDAgMTAgLTEwIG0tNTY0IDEwIGgxMCBtMTI0IDAgaDEwIG0wIDAgaDEwIG0yNiAwIGgxMCBtMjAgMCBoMTAgbTExNCAwIGgxMCBtLTE1NCAwIGwyMCAwIG0tMSAwIHEtOSAwIC05IC0xMCBsMCAtMjQgcTAgLTEwIDEwIC0xMCBtMTM0IDQ0IGwyMCAwIG0tMjAgMCBxMTAgMCAxMCAtMTAgbDAgLTI0IHEwIC0xMCAtMTAgLTEwIG0tMTM0IDAgaDEwIG0yNCAwIGgxMCBtMCAwIGg5MCBtMjAgNDQgaDE5MCBtMjAgLTEyMCBoMTAgbTI2IDAgaDEwIG0yMiAtODggbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tNjI4IDI1OCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIyIDAgaDEwIG0wIDAgaDQ3OCBtLTUwOCAwIGgyMCBtNDg4IDAgaDIwIG0tNTI4IDAgcTEwIDAgMTAgMTAgbTUwOCAwIHEwIC0xMCAxMCAtMTAgbS01MTggMTAgdjEyIG01MDggMCB2LTEyIG0tNTA4IDEyIHEwIDEwIDEwIDEwIG00ODggMCBxMTAgMCAxMCAtMTAgbS00OTggMTAgaDEwIG03NiAwIGgxMCBtMCAwIGgxMCBtOTYgMCBoMTAgbTAgMCBoMTAgbTQwIDAgaDEwIG0wIDAgaDEwIG0xOTYgMCBoMTAgbTIyIC0zMiBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0xMTYgODYgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtMCAwIGgxMTIgbS0xNDIgMCBoMjAgbTEyMiAwIGgyMCBtLTE2MiAwIHExMCAwIDEwIDEwIG0xNDIgMCBxMCAtMTAgMTAgLTEwIG0tMTUyIDEwIHYxMiBtMTQyIDAgdi0xMiBtLTE0MiAxMiBxMCAxMCAxMCAxMCBtMTIyIDAgcTEwIDAgMTAgLTEwIG0tMTMyIDEwIGgxMCBtMTAyIDAgaDEwIG0yMyAtMzIgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjY5NyAxMDM5IDcwNSAxMDM1IDcwNSAxMDQzIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iNjk3IDEwMzkgNjg5IDEwMzUgNjg5IDEwNDMiPjwvcG9seWdvbj4KPC9zdmc+)

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
<td><em>src_name</em></td>
<td>The name for the source.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Do nothing (except issuing a notice) if a source with the same name
already exists. <em>Default.</em></td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <em>cluster_name</em></td>
<td>The <a href="/docs/sql/create-cluster">cluster</a> to maintain this
source.</td>
</tr>
<tr>
<td><strong>CONNECTION</strong> <em>connection_name</em></td>
<td>The name of the PostgreSQL connection to use in the source. For
details on creating connections, check the <a
href="/docs/sql/create-connection/#postgresql"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><strong>FOR ALL TABLES</strong></td>
<td>Create subsources for all tables in the publication.</td>
</tr>
<tr>
<td><strong>FOR SCHEMAS (</strong> <em>schema_list</em>
<strong>)</strong></td>
<td>Create subsources for specific schemas in the publication.</td>
</tr>
<tr>
<td><strong>FOR TABLES (</strong> <em>table_list</em>
<strong>)</strong></td>
<td>Create subsources for specific tables in the publication.</td>
</tr>
<tr>
<td><strong>EXPOSE PROGRESS AS</strong>
<em>progress_subsource_name</em></td>
<td>The name of the progress collection for the source. If this is not
specified, the progress collection will be named
<code>&lt;src_name&gt;_progress</code>. For more information, see <a
href="#monitoring-source-progress">Monitoring source progress</a>.</td>
</tr>
<tr>
<td><strong>RETAIN HISTORY FOR</strong><br />
<em>retention_period</em></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
Duration for which Materialize retains historical data, which is useful
to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. Accepts positive <a
href="/docs/sql/types/interval/">interval</a> values (e.g.
<code>'1hr'</code>). Default: <code>1s</code>.</td>
</tr>
</tbody>
</table>

### `CONNECTION` options

| Field | Value | Description |
|----|----|----|
| `PUBLICATION` | `text` | **Required.** The PostgreSQL [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) (the replication data set containing the tables to be streamed to Materialize). |
| `TEXT COLUMNS` | A list of names | Decode data as `text` for specific columns that contain PostgreSQL types that are unsupported in Materialize. |
| `EXCLUDE COLUMNS` | A list of fully-qualified names | Exclude specific columns that cannot be decoded or should not be included in the subsources created in Materialize. |

## Features

### Change data capture

This source uses PostgreSQL’s native replication protocol to continually
ingest changes resulting from `INSERT`, `UPDATE` and `DELETE` operations
in the upstream database — a process also known as *change data
capture*.

For this reason, you must configure the upstream PostgreSQL database to
support logical replication before creating a source in Materialize. For
step-by-step instructions, see the integration guide for your PostgreSQL
service: [AlloyDB](/docs/ingest-data/postgres-alloydb/), [Amazon
RDS](/docs/ingest-data/postgres-amazon-rds/), [Amazon
Aurora](/docs/ingest-data/postgres-amazon-aurora/), [Azure
DB](/docs/ingest-data/postgres-azure-db/), [Google Cloud
SQL](/docs/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/docs/ingest-data/postgres-self-hosted/).

#### Creating a source

To avoid creating multiple replication slots in the upstream PostgreSQL
database and minimize the required bandwidth, Materialize ingests the
raw replication stream data for some specific set of tables in your
publication.

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES;
```

</div>

When you define a source, Materialize will automatically:

1.  Create a **replication slot** in the upstream PostgreSQL database
    (see [PostgreSQL replication slots](#postgresql-replication-slots)).

    The name of the replication slot created by Materialize is prefixed
    with `materialize_` for easy identification, and can be looked up in
    `mz_internal.mz_postgres_sources`.

    <div class="highlight">

    ``` chroma
    SELECT id, replication_slot FROM mz_internal.mz_postgres_sources;
    ```

    </div>

    ```
       id   |             replication_slot
    --------+----------------------------------------------
     u8     | materialize_7f8a72d0bf2a4b6e9ebc4e61ba769b71
    ```

2.  Create a **subsource** for each original table in the publication.

    <div class="highlight">

    ``` chroma
    SHOW SOURCES;
    ```

    </div>

    ```
             name         |   type
    ----------------------+-----------
     mz_source            | postgres
     mz_source_progress   | progress
     table_1              | subsource
     table_2              | subsource
    ```

    And perform an initial, snapshot-based sync of the tables in the
    publication before it starts ingesting change events.

3.  Incrementally update any materialized or indexed views that depend
    on the source as change events stream in, as a result of `INSERT`,
    `UPDATE` and `DELETE` operations in the upstream PostgreSQL
    database.

##### PostgreSQL replication slots

Each source ingests the raw replication stream data for all tables in
the specified publication using **a single** replication slot. This
allows you to minimize the performance impact on the upstream database,
as well as reuse the same source across multiple materializations.

<div class="tip">

**💡 Tip:**

- For PostgreSQL 13+, set a reasonable value for
  [`max_slot_wal_keep_size`](https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE)
  to limit the amount of storage used by replication slots.

- If you stop using Materialize, or if either the Materialize instance
  or the PostgreSQL instance crash, delete any replication slots. You
  can query the `mz_internal.mz_postgres_sources` table to look up the
  name of the replication slot created for each source.

- If you delete all objects that depend on a source without also
  dropping the source, the upstream replication slot remains and will
  continue to accumulate data so that the source can resume in the
  future. To avoid unbounded disk space usage, make sure to use
  [`DROP SOURCE`](/docs/sql/drop-source/) or manually delete the
  replication slot.

</div>

##### PostgreSQL schemas

`CREATE SOURCE` will attempt to create each upstream table in the same
schema as the source. This may lead to naming collisions if, for
example, you are replicating `schema1.table_1` and `schema2.table_1`.
Use the `FOR TABLES` clause to provide aliases for each upstream table,
in such cases, or to specify an alternative destination schema in
Materialize.

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES (schema1.table_1 AS s1_table_1, schema2_table_1 AS s2_table_1);
```

</div>

### Monitoring source progress

By default, PostgreSQL sources expose progress metadata as a subsource
that you can use to monitor source **ingestion progress**. The name of
the progress subsource can be specified when creating a source using the
`EXPOSE PROGRESS AS` clause; otherwise, it will be named
`<src_name>_progress`.

The following metadata is available for each source as a progress
subsource:

| Field | Type | Meaning |
|----|----|----|
| `lsn` | [`uint8`](/docs/sql/types/uint/#uint8-info) | The last Log Sequence Number (LSN) consumed from the upstream PostgreSQL replication stream. |

And can be queried using:

<div class="highlight">

``` chroma
SELECT lsn
FROM <src_name>_progress;
```

</div>

The reported LSN should increase as Materialize consumes **new** WAL
records from the upstream PostgreSQL database. For more details on
monitoring source ingestion progress and debugging related issues, see
[Troubleshooting](/docs/ops/troubleshooting/).

## Known limitations

### Schema changes

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes (Legacy syntax)

<div class="note">

**NOTE:** This section refer to the legacy
[`CREATE SOURCE ... FOR ...`](/docs/sql/create-source/postgres/) that
creates subsources as part of the `CREATE SOURCE` operation. To be able
to handle the upstream column additions and drops, see
[`CREATE SOURCE (New Syntax)`](/docs/sql/create-source/postgres-v2/) and
[`CREATE TABLE FROM SOURCE`](/docs/sql/create-table).

</div>

- Adding columns to tables. Materialize will **not ingest** new columns
  added upstream unless you use
  [`DROP SOURCE`](/docs/sql/alter-source/#context) to first drop the
  affected subsource, and then add the table back to the source using
  [`ALTER SOURCE...ADD SUBSOURCE`](/docs/sql/alter-source/).

- Dropping columns that were added after the source was created. These
  columns are never ingested, so you can drop them without issue.

- Adding or removing `NOT NULL` constraints to tables that were nullable
  when the source was created.

#### Incompatible schema changes

All other schema changes to upstream tables will set the corresponding
subsource into an error state, which prevents you from reading from the
source.

To handle incompatible [schema changes](#schema-changes), use
[`DROP SOURCE`](/docs/sql/alter-source/#context) and
[`ALTER SOURCE...ADD SUBSOURCE`](/docs/sql/alter-source/) to first drop
the affected subsource, and then add the table back to the source. When
you add the subsource, it will have the updated schema from the
corresponding upstream table.

### Publication membership

PostgreSQL’s logical replication API does not provide a signal when
users remove tables from publications. Because of this, Materialize
relies on periodic checks to determine if a table has been removed from
a publication, at which time it generates an irrevocable error,
preventing any values from being read from the table.

However, it is possible to remove a table from a publication and then
re-add it before Materialize notices that the table was removed. In this
case, Materialize can no longer provide any consistency guarantees about
the data we present from the table and, unfortunately, is wholly unaware
that this occurred.

To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
*before* re-adding it using the [`DROP SOURCE`](/docs/sql/drop-source/)
command.

### Supported types

Materialize natively supports the following PostgreSQL types (including
the array type for each of the types):

- `bool`
- `bpchar`
- `bytea`
- `char`
- `date`
- `daterange`
- `float4`
- `float8`
- `int2`
- `int2vector`
- `int4`
- `int4range`
- `int8`
- `int8range`
- `interval`
- `json`
- `jsonb`
- `numeric`
- `numrange`
- `oid`
- `text`
- `time`
- `timestamp`
- `timestamptz`
- `tsrange`
- `tstzrange`
- `uuid`
- `varchar`

Replicating tables that contain **unsupported [data
types](/docs/sql/types/)** is possible via the `TEXT COLUMNS` option.
The specified columns will be treated as `text`; i.e., will not have the
expected PostgreSQL type features. For example:

- [`enum`](https://www.postgresql.org/docs/current/datatype-enum.html):
  When decoded as `text`, the implicit ordering of the original
  PostgreSQL `enum` type is not preserved; instead, Materialize will
  sort values as `text`.

- [`money`](https://www.postgresql.org/docs/current/datatype-money.html):
  When decoded as `text`, resulting `text` value cannot be cast back to
  `numeric`, since PostgreSQL adds typical currency formatting to the
  output.

### Truncation

Avoid truncating upstream tables that are being replicated into
Materialize. If a replicated upstream table is truncated, the
corresponding subsource(s)/table(s) in Materialize becomes inaccessible
and will not produce any data until it is recreated.

Instead of truncating, use an unqualified `DELETE` to remove all rows
from the upstream table:

<div class="highlight">

``` chroma
DELETE FROM t;
```

</div>

### Inherited tables

When using [PostgreSQL table
inheritance](https://www.postgresql.org/docs/current/tutorial-inheritance.html),
PostgreSQL serves data from `SELECT`s as if the inheriting tables’ data
is also present in the inherited table. However, both PostgreSQL’s
logical replication and `COPY` only present data written to the tables
themselves, i.e. the inheriting data is *not* treated as part of the
inherited table.

PostgreSQL sources use logical replication and `COPY` to ingest table
data, so inheriting tables’ data will only be ingested as part of the
inheriting table, i.e. in Materialize, the data will not be returned
when serving `SELECT`s from the inherited table.

You can mimic PostgreSQL’s `SELECT` behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using `UNION ALL`). However, if new tables inherit
from the table, data from the inheriting tables will not be available in
the view. You will need to add the inheriting tables via `ADD SUBSOURCE`
and create a new view (materialized or non-) that unions the new table.

## Examples

<div class="important">

**! Important:** Before creating a PostgreSQL source, you must set up
logical replication in the upstream database. For step-by-step
instructions, see the integration guide for your PostgreSQL service:
[AlloyDB](/docs/ingest-data/postgres-alloydb/), [Amazon
RDS](/docs/ingest-data/postgres-amazon-rds/), [Amazon
Aurora](/docs/ingest-data/postgres-amazon-aurora/), [Azure
DB](/docs/ingest-data/postgres-azure-db/), [Google Cloud
SQL](/docs/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/docs/ingest-data/postgres-self-hosted/).

</div>

### Creating a connection

A connection describes how to connect and authenticate to an external
system you want Materialize to read data from.

Once created, a connection is **reusable** across multiple
`CREATE SOURCE` statements. For more details on creating connections,
check the [`CREATE CONNECTION`](/docs/sql/create-connection/#postgresql)
documentation page.

<div class="highlight">

``` chroma
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    SSL MODE 'require',
    DATABASE 'postgres'
);
```

</div>

If your PostgreSQL server is not exposed to the public internet, you can
[tunnel the
connection](/docs/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service (Materialize Cloud) or an SSH bastion
host.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-aws-privatelink" class="tab-pane" title="AWS PrivateLink">

<div class="note">

**NOTE:** Connections using AWS PrivateLink is for Materialize Cloud
only.

</div>

<div class="highlight">

``` chroma
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

</div>

<div class="highlight">

``` chroma
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    AWS PRIVATELINK privatelink_svc,
    DATABASE 'postgres'
);
```

</div>

For step-by-step instructions on creating AWS PrivateLink connections
and configuring an AWS PrivateLink service to accept connections from
Materialize, check [this
guide](/docs/ops/network-security/privatelink/).

</div>

<div id="tab-ssh-tunnel" class="tab-pane" title="SSH tunnel">

<div class="highlight">

``` chroma
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize',
);
```

</div>

<div class="highlight">

``` chroma
CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    SSH TUNNEL ssh_connection,
    DATABASE 'postgres'
);
```

</div>

For step-by-step instructions on creating SSH tunnel connections and
configuring an SSH bastion server to accept connections from
Materialize, check [this guide](/docs/ops/network-security/ssh-tunnel/).

</div>

</div>

</div>

### Creating a source

*Create subsources for all tables included in the PostgreSQL
publication*

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
    FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
    FOR ALL TABLES;
```

</div>

*Create subsources for all tables from specific schemas included in the
PostgreSQL publication*

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR SCHEMAS (public, project);
```

</div>

*Create subsources for specific tables included in the PostgreSQL
publication*

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES (table_1, table_2 AS alias_table_2);
```

</div>

#### Handling unsupported types

If the publication contains tables that use [data
types](/docs/sql/types/) unsupported by Materialize, use the
`TEXT COLUMNS` option to decode data as `text` for the affected columns.
This option expects the upstream names of the replicated table and
column (i.e. as defined in your PostgreSQL database).

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (
    PUBLICATION 'mz_source',
    TEXT COLUMNS (upstream_table_name.column_of_unsupported_type)
  ) FOR ALL TABLES;
```

</div>

### Handling errors and schema changes

<div class="note">

**NOTE:** Work to more smoothly support ddl changes to upstream tables
is currently in progress. The work introduces the ability to re-ingest
the same upstream table under a new schema and switch over without
downtime.

</div>

To handle upstream [schema changes](#schema-changes) or errored
subsources, use the [`DROP SOURCE`](/docs/sql/alter-source/#context)
syntax to drop the affected subsource, and then
[`ALTER SOURCE...ADD SUBSOURCE`](/docs/sql/alter-source/) to add the
subsource back to the source.

<div class="highlight">

``` chroma
-- List all subsources in mz_source
SHOW SUBSOURCES ON mz_source;

-- Get rid of an outdated or errored subsource
DROP SOURCE table_1;

-- Start ingesting the table with the updated schema or fix
ALTER SOURCE mz_source ADD SUBSOURCE table_1;
```

</div>

#### Adding subsources

When adding subsources to a PostgreSQL source, Materialize opens a
temporary replication slot to snapshot the new subsources’ current
states. After completing the snapshot, the table will be kept
up-to-date, like all other tables in the publication.

#### Dropping subsources

Dropping a subsource prevents Materialize from ingesting any data from
it, in addition to dropping any state that Materialize previously had
for the table.

## Related pages

- [`CREATE SECRET`](/docs/sql/create-secret)
- [`CREATE CONNECTION`](/docs/sql/create-connection)
- [`CREATE SOURCE`](../)
- PostgreSQL integration guides:
  - [AlloyDB](/docs/ingest-data/postgres-alloydb/)
  - [Amazon RDS](/docs/ingest-data/postgres-amazon-rds/)
  - [Amazon Aurora](/docs/ingest-data/postgres-amazon-aurora/)
  - [Azure DB](/docs/ingest-data/postgres-azure-db/)
  - [Google Cloud SQL](/docs/ingest-data/postgres-google-cloud-sql/)
  - [Self-hosted](/docs/ingest-data/postgres-self-hosted/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-source/postgres.md"
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
