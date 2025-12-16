<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)  /  [CREATE
SOURCE](/docs/self-managed/v25.2/sql/create-source/)

</div>

# CREATE SOURCE: PostgreSQL

[`CREATE SOURCE`](/docs/self-managed/v25.2/sql/create-source/) connects
Materialize to an external system you want to read data from, and
provides details about how to decode and interpret that data.

Materialize supports PostgreSQL (11+) as a data source. To connect to a
PostgreSQL instance, you first need to [create a
connection](#creating-a-connection) that specifies access and
authentication parameters. Once created, a connection is **reusable**
across multiple `CREATE SOURCE` statements.

<div class="warning">

**WARNING!** Before creating a PostgreSQL source, you must set up
logical replication in the upstream database. For step-by-step
instructions, see the integration guide for your PostgreSQL service:
[AlloyDB](/docs/self-managed/v25.2/ingest-data/postgres-alloydb/),
[Amazon RDS](/docs/self-managed/v25.2/ingest-data/postgres-amazon-rds/),
[Amazon
Aurora](/docs/self-managed/v25.2/ingest-data/postgres-amazon-aurora/),
[Azure DB](/docs/self-managed/v25.2/ingest-data/postgres-azure-db/),
[Google Cloud
SQL](/docs/self-managed/v25.2/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/docs/self-managed/v25.2/ingest-data/postgres-self-hosted/).

</div>

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI3MDciIGhlaWdodD0iOTQ5Ij4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iMyIgd2lkdGg9IjE0MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyOSIgeT0iMSIgd2lkdGg9IjE0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzkiIHk9IjIxIj5DUkVBVEUgU09VUkNFPC90ZXh0PgogICA8cmVjdCB4PSIyMTEiIHk9IjM1IiB3aWR0aD0iMTIwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIwOSIgeT0iMzMiIHdpZHRoPSIxMjAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIxOSIgeT0iNTMiPklGIE5PVCBFWElTVFM8L3RleHQ+CiAgIDxyZWN0IHg9IjM3MSIgeT0iMyIgd2lkdGg9IjgyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzNjkiIHk9IjEiIHdpZHRoPSI4MiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjM3OSIgeT0iMjEiPnNyY19uYW1lPC90ZXh0PgogICA8cmVjdCB4PSIxNDEiIHk9IjEzMyIgd2lkdGg9IjEwNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMzkiIHk9IjEzMSIgd2lkdGg9IjEwNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTQ5IiB5PSIxNTEiPklOIENMVVNURVI8L3RleHQ+CiAgIDxyZWN0IHg9IjI2NSIgeT0iMTMzIiB3aWR0aD0iMTA4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyNjMiIHk9IjEzMSIgd2lkdGg9IjEwOCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI3MyIgeT0iMTUxIj5jbHVzdGVyX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjQxMyIgeT0iMTAxIiB3aWR0aD0iNjAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDExIiB5PSI5OSIgd2lkdGg9IjYwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0MjEiIHk9IjExOSI+RlJPTTwvdGV4dD4KICAgPHJlY3QgeD0iNDkzIiB5PSIxMDEiIHdpZHRoPSI5NiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OTEiIHk9Ijk5IiB3aWR0aD0iOTYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjUwMSIgeT0iMTE5Ij5QT1NUR1JFUzwvdGV4dD4KICAgPHJlY3QgeD0iMTI4IiB5PSIxOTkiIHdpZHRoPSIxMTYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTI2IiB5PSIxOTciIHdpZHRoPSIxMTYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjEzNiIgeT0iMjE3Ij5DT05ORUNUSU9OPC90ZXh0PgogICA8cmVjdCB4PSIyNjQiIHk9IjE5OSIgd2lkdGg9IjEzNiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjYyIiB5PSIxOTciIHdpZHRoPSIxMzYiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIyNzIiIHk9IjIxNyI+Y29ubmVjdGlvbl9uYW1lPC90ZXh0PgogICA8cmVjdCB4PSI0MjAiIHk9IjE5OSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQxOCIgeT0iMTk3IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjQyOCIgeT0iMjE3Ij4oPC90ZXh0PgogICA8cmVjdCB4PSI0NjYiIHk9IjE5OSIgd2lkdGg9IjExNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0NjQiIHk9IjE5NyIgd2lkdGg9IjExNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDc0IiB5PSIyMTciPlBVQkxJQ0FUSU9OPC90ZXh0PgogICA8cmVjdCB4PSIyODgiIHk9IjI2NSIgd2lkdGg9IjEzNCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjg2IiB5PSIyNjMiIHdpZHRoPSIxMzQiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIyOTYiIHk9IjI4MyI+cHVibGljYXRpb25fbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iMTE1IiB5PSIzNzUiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMTMiIHk9IjM3MyIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMjMiIHk9IjM5MyI+LDwvdGV4dD4KICAgPHJlY3QgeD0iMTU5IiB5PSIzNzUiIHdpZHRoPSIxMzQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTU3IiB5PSIzNzMiIHdpZHRoPSIxMzQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjE2NyIgeT0iMzkzIj5URVhUIENPTFVNTlM8L3RleHQ+CiAgIDxyZWN0IHg9IjMzMyIgeT0iMzc1IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzMxIiB5PSIzNzMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzQxIiB5PSIzOTMiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjM5OSIgeT0iMzc1IiB3aWR0aD0iMTEwIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzOTciIHk9IjM3MyIgd2lkdGg9IjExMCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjQwNyIgeT0iMzkzIj5jb2x1bW5fbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iMzk5IiB5PSIzMzEiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzOTciIHk9IjMyOSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0MDciIHk9IjM0OSI+LDwvdGV4dD4KICAgPHJlY3QgeD0iNTQ5IiB5PSIzNzUiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI1NDciIHk9IjM3MyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1NTciIHk9IjM5MyI+KTwvdGV4dD4KICAgPHJlY3QgeD0iMzQyIiB5PSI0NzMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzNDAiIHk9IjQ3MSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzNTAiIHk9IjQ5MSI+KTwvdGV4dD4KICAgPHJlY3QgeD0iNDUiIHk9IjUzOSIgd2lkdGg9IjEzOCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0MyIgeT0iNTM3IiB3aWR0aD0iMTM4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1MyIgeT0iNTU3Ij5GT1IgQUxMIFRBQkxFUzwvdGV4dD4KICAgPHJlY3QgeD0iNjUiIHk9IjYyNyIgd2lkdGg9IjEwNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI2MyIgeT0iNjI1IiB3aWR0aD0iMTA2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI3MyIgeT0iNjQ1Ij5GT1IgVEFCTEVTPC90ZXh0PgogICA8cmVjdCB4PSIxOTEiIHk9IjYyNyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjE4OSIgeT0iNjI1IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjE5OSIgeT0iNjQ1Ij4oPC90ZXh0PgogICA8cmVjdCB4PSIyNTciIHk9IjYyNyIgd2lkdGg9Ijk2IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyNTUiIHk9IjYyNSIgd2lkdGg9Ijk2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjY1IiB5PSI2NDUiPnRhYmxlX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjM5MyIgeT0iNjU5IiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzkxIiB5PSI2NTciIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDAxIiB5PSI2NzciPkFTPC90ZXh0PgogICA8cmVjdCB4PSI0NTMiIHk9IjY1OSIgd2lkdGg9IjEwNiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iNDUxIiB5PSI2NTciIHdpZHRoPSIxMDYiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI0NjEiIHk9IjY3NyI+c3Vic3JjX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjI1NyIgeT0iNTgzIiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjU1IiB5PSI1ODEiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjY1IiB5PSI2MDEiPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjY1IiB5PSI3NDciIHdpZHRoPSIxMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNjMiIHk9Ijc0NSIgd2lkdGg9IjEyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNzMiIHk9Ijc2NSI+Rk9SIFNDSEVNQVM8L3RleHQ+CiAgIDxyZWN0IHg9IjIwOSIgeT0iNzQ3IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjA3IiB5PSI3NDUiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjE3IiB5PSI3NjUiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjI3NSIgeT0iNzQ3IiB3aWR0aD0iMTE0IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyNzMiIHk9Ijc0NSIgd2lkdGg9IjExNCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI4MyIgeT0iNzY1Ij5zY2hlbWFfbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iMjc1IiB5PSI3MDMiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyNzMiIHk9IjcwMSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyODMiIHk9IjcyMSI+LDwvdGV4dD4KICAgPHJlY3QgeD0iNjM5IiB5PSI2MjciIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI2MzciIHk9IjYyNSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI2NDciIHk9IjY0NSI+KTwvdGV4dD4KICAgPHJlY3QgeD0iMTIxIiB5PSI4MjkiIHdpZHRoPSI3NiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMTkiIHk9IjgyNyIgd2lkdGg9Ijc2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMjkiIHk9Ijg0NyI+RVhQT1NFPC90ZXh0PgogICA8cmVjdCB4PSIyMTciIHk9IjgyOSIgd2lkdGg9Ijk2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIxNSIgeT0iODI3IiB3aWR0aD0iOTYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIyNSIgeT0iODQ3Ij5QUk9HUkVTUzwvdGV4dD4KICAgPHJlY3QgeD0iMzMzIiB5PSI4MjkiIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzMzEiIHk9IjgyNyIgd2lkdGg9IjQwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzNDEiIHk9Ijg0NyI+QVM8L3RleHQ+CiAgIDxyZWN0IHg9IjM5MyIgeT0iODI5IiB3aWR0aD0iMTk2IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzOTEiIHk9IjgyNyIgd2lkdGg9IjE5NiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjQwMSIgeT0iODQ3Ij5wcm9ncmVzc19zdWJzb3VyY2VfbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iNTU3IiB5PSI5MTUiIHdpZHRoPSIxMDIiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjU1NSIgeT0iOTEzIiB3aWR0aD0iMTAyIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iNTY1IiB5PSI5MzMiPndpdGhfb3B0aW9uczwvdGV4dD4KICAgPHBhdGggY2xhc3M9ImxpbmUiIGQ9Im0xNyAxNyBoMiBtMCAwIGgxMCBtMTQwIDAgaDEwIG0yMCAwIGgxMCBtMCAwIGgxMzAgbS0xNjAgMCBoMjAgbTE0MCAwIGgyMCBtLTE4MCAwIHExMCAwIDEwIDEwIG0xNjAgMCBxMCAtMTAgMTAgLTEwIG0tMTcwIDEwIHYxMiBtMTYwIDAgdi0xMiBtLTE2MCAxMiBxMCAxMCAxMCAxMCBtMTQwIDAgcTEwIDAgMTAgLTEwIG0tMTUwIDEwIGgxMCBtMTIwIDAgaDEwIG0yMCAtMzIgaDEwIG04MiAwIGgxMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtLTM3NiA5OCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIyIDAgaDEwIG0wIDAgaDI0MiBtLTI3MiAwIGgyMCBtMjUyIDAgaDIwIG0tMjkyIDAgcTEwIDAgMTAgMTAgbTI3MiAwIHEwIC0xMCAxMCAtMTAgbS0yODIgMTAgdjEyIG0yNzIgMCB2LTEyIG0tMjcyIDEyIHEwIDEwIDEwIDEwIG0yNTIgMCBxMTAgMCAxMCAtMTAgbS0yNjIgMTAgaDEwIG0xMDQgMCBoMTAgbTAgMCBoMTAgbTEwOCAwIGgxMCBtMjAgLTMyIGgxMCBtNjAgMCBoMTAgbTAgMCBoMTAgbTk2IDAgaDEwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tNTA1IDk4IGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGgxMCBtMTE2IDAgaDEwIG0wIDAgaDEwIG0xMzYgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0wIDAgaDEwIG0xMTYgMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0zMzggNjYgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgaDEwIG0xMzQgMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0zNzEgMTEwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMjIgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDEwIG0xMzQgMCBoMTAgbTIwIDAgaDEwIG0yNiAwIGgxMCBtMjAgMCBoMTAgbTExMCAwIGgxMCBtLTE1MCAwIGwyMCAwIG0tMSAwIHEtOSAwIC05IC0xMCBsMCAtMjQgcTAgLTEwIDEwIC0xMCBtMTMwIDQ0IGwyMCAwIG0tMjAgMCBxMTAgMCAxMCAtMTAgbDAgLTI0IHEwIC0xMCAtMTAgLTEwIG0tMTMwIDAgaDEwIG0yNCAwIGgxMCBtMCAwIGg4NiBtMjAgNDQgaDEwIG0yNiAwIGgxMCBtLTI4MiAwIGgyMCBtMjYyIDAgaDIwIG0tMzAyIDAgcTEwIDAgMTAgMTAgbTI4MiAwIHEwIC0xMCAxMCAtMTAgbS0yOTIgMTAgdjE0IG0yODIgMCB2LTE0IG0tMjgyIDE0IHEwIDEwIDEwIDEwIG0yNjIgMCBxMTAgMCAxMCAtMTAgbS0yNzIgMTAgaDEwIG0wIDAgaDI1MiBtLTUwMCAtMzQgaDIwIG01MDAgMCBoMjAgbS01NDAgMCBxMTAgMCAxMCAxMCBtNTIwIDAgcTAgLTEwIDEwIC0xMCBtLTUzMCAxMCB2MzAgbTUyMCAwIHYtMzAgbS01MjAgMzAgcTAgMTAgMTAgMTAgbTUwMCAwIHExMCAwIDEwIC0xMCBtLTUxMCAxMCBoMTAgbTAgMCBoNDkwIG0yMiAtNTAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tMzE3IDk4IGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGgxMCBtMjYgMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0zODcgNjYgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtMTM4IDAgaDEwIG0wIDAgaDQ4MiBtLTY2MCAwIGgyMCBtNjQwIDAgaDIwIG0tNjgwIDAgcTEwIDAgMTAgMTAgbTY2MCAwIHEwIC0xMCAxMCAtMTAgbS02NzAgMTAgdjY4IG02NjAgMCB2LTY4IG0tNjYwIDY4IHEwIDEwIDEwIDEwIG02NDAgMCBxMTAgMCAxMCAtMTAgbS02MzAgMTAgaDEwIG0xMDYgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0yMCAwIGgxMCBtOTYgMCBoMTAgbTIwIDAgaDEwIG0wIDAgaDE3NiBtLTIwNiAwIGgyMCBtMTg2IDAgaDIwIG0tMjI2IDAgcTEwIDAgMTAgMTAgbTIwNiAwIHEwIC0xMCAxMCAtMTAgbS0yMTYgMTAgdjEyIG0yMDYgMCB2LTEyIG0tMjA2IDEyIHEwIDEwIDEwIDEwIG0xODYgMCBxMTAgMCAxMCAtMTAgbS0xOTYgMTAgaDEwIG00MCAwIGgxMCBtMCAwIGgxMCBtMTA2IDAgaDEwIG0tMzQyIC0zMiBsMjAgMCBtLTEgMCBxLTkgMCAtOSAtMTAgbDAgLTI0IHEwIC0xMCAxMCAtMTAgbTM0MiA0NCBsMjAgMCBtLTIwIDAgcTEwIDAgMTAgLTEwIGwwIC0yNCBxMCAtMTAgLTEwIC0xMCBtLTM0MiAwIGgxMCBtMjQgMCBoMTAgbTAgMCBoMjk4IG0tNTU0IDQ0IGgyMCBtNTU0IDAgaDIwIG0tNTk0IDAgcTEwIDAgMTAgMTAgbTU3NCAwIHEwIC0xMCAxMCAtMTAgbS01ODQgMTAgdjEwMCBtNTc0IDAgdi0xMDAgbS01NzQgMTAwIHEwIDEwIDEwIDEwIG01NTQgMCBxMTAgMCAxMCAtMTAgbS01NjQgMTAgaDEwIG0xMjQgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0yMCAwIGgxMCBtMTE0IDAgaDEwIG0tMTU0IDAgbDIwIDAgbS0xIDAgcS05IDAgLTkgLTEwIGwwIC0yNCBxMCAtMTAgMTAgLTEwIG0xMzQgNDQgbDIwIDAgbS0yMCAwIHExMCAwIDEwIC0xMCBsMCAtMjQgcTAgLTEwIC0xMCAtMTAgbS0xMzQgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDkwIG0yMCA0NCBoMTkwIG0yMCAtMTIwIGgxMCBtMjYgMCBoMTAgbTIyIC04OCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS02MjggMjU4IGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMjIgMCBoMTAgbTAgMCBoNDc4IG0tNTA4IDAgaDIwIG00ODggMCBoMjAgbS01MjggMCBxMTAgMCAxMCAxMCBtNTA4IDAgcTAgLTEwIDEwIC0xMCBtLTUxOCAxMCB2MTIgbTUwOCAwIHYtMTIgbS01MDggMTIgcTAgMTAgMTAgMTAgbTQ4OCAwIHExMCAwIDEwIC0xMCBtLTQ5OCAxMCBoMTAgbTc2IDAgaDEwIG0wIDAgaDEwIG05NiAwIGgxMCBtMCAwIGgxMCBtNDAgMCBoMTAgbTAgMCBoMTAgbTE5NiAwIGgxMCBtMjIgLTMyIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtLTExNiA4NiBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIyIDAgaDEwIG0wIDAgaDExMiBtLTE0MiAwIGgyMCBtMTIyIDAgaDIwIG0tMTYyIDAgcTEwIDAgMTAgMTAgbTE0MiAwIHEwIC0xMCAxMCAtMTAgbS0xNTIgMTAgdjEyIG0xNDIgMCB2LTEyIG0tMTQyIDEyIHEwIDEwIDEwIDEwIG0xMjIgMCBxMTAgMCAxMCAtMTAgbS0xMzIgMTAgaDEwIG0xMDIgMCBoMTAgbTIzIC0zMiBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iNjk3IDg5NyA3MDUgODkzIDcwNSA5MDEiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSI2OTcgODk3IDY4OSA4OTMgNjg5IDkwMSI+PC9wb2x5Z29uPgo8L3N2Zz4=)

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
<td>The <a
href="/docs/self-managed/v25.2/sql/create-cluster">cluster</a> to
maintain this source.</td>
</tr>
<tr>
<td><strong>CONNECTION</strong> <em>connection_name</em></td>
<td>The name of the PostgreSQL connection to use in the source. For
details on creating connections, check the <a
href="/docs/self-managed/v25.2/sql/create-connection/#postgresql"><code>CREATE CONNECTION</code></a>
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
href="/docs/self-managed/v25.2/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. Accepts positive <a
href="/docs/self-managed/v25.2/sql/types/interval/">interval</a> values
(e.g. <code>'1hr'</code>). Default: <code>1s</code>.</td>
</tr>
</tbody>
</table>

### `CONNECTION` options

| Field | Value | Description |
|----|----|----|
| `PUBLICATION` | `text` | **Required.** The PostgreSQL [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) (the replication data set containing the tables to be streamed to Materialize). |
| `TEXT COLUMNS` | A list of names | Decode data as `text` for specific columns that contain PostgreSQL types that are unsupported in Materialize. |

## Features

### Change data capture

This source uses PostgreSQL’s native replication protocol to continually
ingest changes resulting from `INSERT`, `UPDATE` and `DELETE` operations
in the upstream database — a process also known as *change data
capture*.

For this reason, you must configure the upstream PostgreSQL database to
support logical replication before creating a source in Materialize. For
step-by-step instructions, see the integration guide for your PostgreSQL
service:
[AlloyDB](/docs/self-managed/v25.2/ingest-data/postgres-alloydb/),
[Amazon RDS](/docs/self-managed/v25.2/ingest-data/postgres-amazon-rds/),
[Amazon
Aurora](/docs/self-managed/v25.2/ingest-data/postgres-amazon-aurora/),
[Azure DB](/docs/self-managed/v25.2/ingest-data/postgres-azure-db/),
[Google Cloud
SQL](/docs/self-managed/v25.2/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/docs/self-managed/v25.2/ingest-data/postgres-self-hosted/).

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

It’s important to note that the schema metadata is captured when the
source is initially created, and is validated against the upstream
schema upon restart. If you create new tables upstream after creating a
PostgreSQL source and want to replicate them to Materialize, the source
must be dropped and recreated.

##### PostgreSQL replication slots

Each source ingests the raw replication stream data for all tables in
the specified publication using **a single** replication slot. This
allows you to minimize the performance impact on the upstream database,
as well as reuse the same source across multiple materializations.

<div class="warning">

**WARNING!** Make sure to delete any replication slots if you stop using
Materialize, or if either the Materialize or PostgreSQL instances crash.
To look up the name of the replication slot created for each source, use
`mz_internal.mz_postgres_sources`.

</div>

If you delete all objects that depend on a source without also dropping
the source, the upstream replication slot will linger and continue to
accumulate data so that the source can resume in the future. To avoid
unbounded disk space usage, make sure to use
[`DROP SOURCE`](/docs/self-managed/v25.2/sql/drop-source/) or manually
delete the replication slot.

For PostgreSQL 13+, it is recommended that you set a reasonable value
for
[`max_slot_wal_keep_size`](https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE)
to limit the amount of storage used by replication slots.

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
| `lsn` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint/#uint8-info) | The last Log Sequence Number (LSN) consumed from the upstream PostgreSQL replication stream. |

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
[Troubleshooting](/docs/self-managed/v25.2/ops/troubleshooting/).

## Known limitations

### Schema changes

<div class="note">

**NOTE:** Work to more smoothly support ddl changes to upstream tables
is currently in progress. The work introduces the ability to re-ingest
the same upstream table under a new schema and switch over without
downtime.

</div>

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes

- Adding columns to tables. Materialize will **not ingest** new columns
  added upstream unless you use
  [`DROP SOURCE`](/docs/self-managed/v25.2/sql/alter-source/#context) to
  first drop the affected subsource, and then add the table back to the
  source using
  [`ALTER SOURCE...ADD SUBSOURCE`](/docs/self-managed/v25.2/sql/alter-source/).

- Dropping columns that were added after the source was created. These
  columns are never ingested, so you can drop them without issue.

- Adding or removing `NOT NULL` constraints to tables that were nullable
  when the source was created.

#### Incompatible schema changes

All other schema changes to upstream tables will set the corresponding
subsource into an error state, which prevents you from reading from the
source.

To handle incompatible [schema changes](#schema-changes), use
[`DROP SOURCE`](/docs/self-managed/v25.2/sql/alter-source/#context) and
[`ALTER SOURCE...ADD SUBSOURCE`](/docs/self-managed/v25.2/sql/alter-source/)
to first drop the affected subsource, and then add the table back to the
source. When you add the subsource, it will have the updated schema from
the corresponding upstream table.

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
*before* re-adding it using the
[`DROP SOURCE`](/docs/self-managed/v25.2/sql/drop-source/) command.

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
types](/docs/self-managed/v25.2/sql/types/)** is possible via the
`TEXT COLUMNS` option. The specified columns will be treated as `text`;
i.e., will not have the expected PostgreSQL type features. For example:

- [`enum`](https://www.postgresql.org/docs/current/datatype-enum.html):
  When decoded as `text`, the implicit ordering of the original
  PostgreSQL `enum` type is not preserved; instead, Materialize will
  sort values as `text`.

- [`money`](https://www.postgresql.org/docs/current/datatype-money.html):
  When decoded as `text`, resulting `text` value cannot be cast back to
  `numeric`, since PostgreSQL adds typical currency formatting to the
  output.

### Truncation

Upstream tables replicated into Materialize should not be truncated. If
an upstream table is truncated while replicated, the whole source
becomes inaccessible and will not produce any data until it is
recreated. Instead of truncating, you can use an unqualified `DELETE` to
remove all rows from the table:

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
[AlloyDB](/docs/self-managed/v25.2/ingest-data/postgres-alloydb/),
[Amazon RDS](/docs/self-managed/v25.2/ingest-data/postgres-amazon-rds/),
[Amazon
Aurora](/docs/self-managed/v25.2/ingest-data/postgres-amazon-aurora/),
[Azure DB](/docs/self-managed/v25.2/ingest-data/postgres-azure-db/),
[Google Cloud
SQL](/docs/self-managed/v25.2/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/docs/self-managed/v25.2/ingest-data/postgres-self-hosted/).

</div>

### Creating a connection

A connection describes how to connect and authenticate to an external
system you want Materialize to read data from.

Once created, a connection is **reusable** across multiple
`CREATE SOURCE` statements. For more details on creating connections,
check the
[`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection/#postgresql)
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
connection](/docs/self-managed/v25.2/sql/create-connection/#network-security-connections)
through an SSH bastion host.

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
Materialize, check [this
guide](/docs/self-managed/v25.2/ops/network-security/ssh-tunnel/).

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
types](/docs/self-managed/v25.2/sql/types/) unsupported by Materialize,
use the `TEXT COLUMNS` option to decode data as `text` for the affected
columns. This option expects the upstream names of the replicated table
and column (i.e. as defined in your PostgreSQL database).

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
subsources, use the
[`DROP SOURCE`](/docs/self-managed/v25.2/sql/alter-source/#context)
syntax to drop the affected subsource, and then
[`ALTER SOURCE...ADD SUBSOURCE`](/docs/self-managed/v25.2/sql/alter-source/)
to add the subsource back to the source.

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

- [`CREATE SECRET`](/docs/self-managed/v25.2/sql/create-secret)
- [`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection)
- [`CREATE SOURCE`](../)
- PostgreSQL integration guides:
  - [AlloyDB](/docs/self-managed/v25.2/ingest-data/postgres-alloydb/)
  - [Amazon
    RDS](/docs/self-managed/v25.2/ingest-data/postgres-amazon-rds/)
  - [Amazon
    Aurora](/docs/self-managed/v25.2/ingest-data/postgres-amazon-aurora/)
  - [Azure DB](/docs/self-managed/v25.2/ingest-data/postgres-azure-db/)
  - [Google Cloud
    SQL](/docs/self-managed/v25.2/ingest-data/postgres-google-cloud-sql/)
  - [Self-hosted](/docs/self-managed/v25.2/ingest-data/postgres-self-hosted/)

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
