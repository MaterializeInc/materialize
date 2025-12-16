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

# CREATE SOURCE: SQL Server

[`CREATE SOURCE`](/docs/self-managed/v25.2/sql/create-source/) connects
Materialize to an external system you want to read data from, and
provides details about how to decode and interpret that data.

Materialize supports SQL Server (2016+) as a real-time data source. To
connect to a SQL Server database, you first need to tweak its
configuration to enable [Change Data
Capture](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server)
and [`SNAPSHOT` transaction
isolation](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/snapshot-isolation-in-sql-server)
for the database that you would like to replicate. Then [create a
connection](#creating-a-connection) in Materialize that specifies access
and authentication parameters.

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI2NjciIGhlaWdodD0iODcxIj4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iMyIgd2lkdGg9IjE0MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyOSIgeT0iMSIgd2lkdGg9IjE0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzkiIHk9IjIxIj5DUkVBVEUgU09VUkNFPC90ZXh0PgogICA8cmVjdCB4PSIyMTEiIHk9IjM1IiB3aWR0aD0iMTIwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIwOSIgeT0iMzMiIHdpZHRoPSIxMjAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIxOSIgeT0iNTMiPklGIE5PVCBFWElTVFM8L3RleHQ+CiAgIDxyZWN0IHg9IjM3MSIgeT0iMyIgd2lkdGg9IjgyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzNjkiIHk9IjEiIHdpZHRoPSI4MiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjM3OSIgeT0iMjEiPnNyY19uYW1lPC90ZXh0PgogICA8cmVjdCB4PSIxMTUiIHk9IjEzMyIgd2lkdGg9IjEwNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMTMiIHk9IjEzMSIgd2lkdGg9IjEwNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTIzIiB5PSIxNTEiPklOIENMVVNURVI8L3RleHQ+CiAgIDxyZWN0IHg9IjIzOSIgeT0iMTMzIiB3aWR0aD0iMTA4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyMzciIHk9IjEzMSIgd2lkdGg9IjEwOCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI0NyIgeT0iMTUxIj5jbHVzdGVyX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjM4NyIgeT0iMTAxIiB3aWR0aD0iNjAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzg1IiB5PSI5OSIgd2lkdGg9IjYwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzOTUiIHk9IjExOSI+RlJPTTwvdGV4dD4KICAgPHJlY3QgeD0iNDY3IiB5PSIxMDEiIHdpZHRoPSIxMDgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDY1IiB5PSI5OSIgd2lkdGg9IjEwOCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDc1IiB5PSIxMTkiPlNRTCBTRVJWRVI8L3RleHQ+CiAgIDxyZWN0IHg9IjE5OSIgeT0iMTk5IiB3aWR0aD0iMTE2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjE5NyIgeT0iMTk3IiB3aWR0aD0iMTE2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyMDciIHk9IjIxNyI+Q09OTkVDVElPTjwvdGV4dD4KICAgPHJlY3QgeD0iMzM1IiB5PSIxOTkiIHdpZHRoPSIxMzYiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjMzMyIgeT0iMTk3IiB3aWR0aD0iMTM2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMzQzIiB5PSIyMTciPmNvbm5lY3Rpb25fbmFtZTwvdGV4dD4KICAgPHJlY3QgeD0iOTQiIHk9IjMwOSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjkyIiB5PSIzMDciIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTAyIiB5PSIzMjciPig8L3RleHQ+CiAgIDxyZWN0IHg9IjE0MCIgeT0iMzA5IiB3aWR0aD0iMTM0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjEzOCIgeT0iMzA3IiB3aWR0aD0iMTM0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxNDgiIHk9IjMyNyI+VEVYVCBDT0xVTU5TPC90ZXh0PgogICA8cmVjdCB4PSIzMTQiIHk9IjMwOSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjMxMiIgeT0iMzA3IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjMyMiIgeT0iMzI3Ij4oPC90ZXh0PgogICA8cmVjdCB4PSIzODAiIHk9IjMwOSIgd2lkdGg9IjExMCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMzc4IiB5PSIzMDciIHdpZHRoPSIxMTAiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzODgiIHk9IjMyNyI+Y29sdW1uX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjM4MCIgeT0iMjY1IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzc4IiB5PSIyNjMiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzg4IiB5PSIyODMiPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjUzMCIgeT0iMzA5IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNTI4IiB5PSIzMDciIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTM4IiB5PSIzMjciPik8L3RleHQ+CiAgIDxyZWN0IHg9IjU4IiB5PSI0NTEiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI1NiIgeT0iNDQ5IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjY2IiB5PSI0NjkiPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjEwMiIgeT0iNDUxIiB3aWR0aD0iMTYyIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjEwMCIgeT0iNDQ5IiB3aWR0aD0iMTYyIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMTAiIHk9IjQ2OSI+RVhDTFVERSBDT0xVTU5TPC90ZXh0PgogICA8cmVjdCB4PSIzMDQiIHk9IjQ1MSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjMwMiIgeT0iNDQ5IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjMxMiIgeT0iNDY5Ij4oPC90ZXh0PgogICA8cmVjdCB4PSIzNzAiIHk9IjQ1MSIgd2lkdGg9IjExMCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMzY4IiB5PSI0NDkiIHdpZHRoPSIxMTAiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzNzgiIHk9IjQ2OSI+Y29sdW1uX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjM3MCIgeT0iNDA3IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzY4IiB5PSI0MDUiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzc4IiB5PSI0MjUiPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjUyMCIgeT0iNDUxIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNTE4IiB5PSI0NDkiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTI4IiB5PSI0NjkiPik8L3RleHQ+CiAgIDxyZWN0IHg9IjU4NiIgeT0iNDUxIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNTg0IiB5PSI0NDkiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTk0IiB5PSI0NjkiPik8L3RleHQ+CiAgIDxyZWN0IHg9IjQ1IiB5PSI1NDkiIHdpZHRoPSIxMzgiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDMiIHk9IjU0NyIgd2lkdGg9IjEzOCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTMiIHk9IjU2NyI+Rk9SIEFMTCBUQUJMRVM8L3RleHQ+CiAgIDxyZWN0IHg9IjQ1IiB5PSI2MzciIHdpZHRoPSIxMDYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDMiIHk9IjYzNSIgd2lkdGg9IjEwNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTMiIHk9IjY1NSI+Rk9SIFRBQkxFUzwvdGV4dD4KICAgPHJlY3QgeD0iMTcxIiB5PSI2MzciIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxNjkiIHk9IjYzNSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxNzkiIHk9IjY1NSI+KDwvdGV4dD4KICAgPHJlY3QgeD0iMjM3IiB5PSI2MzciIHdpZHRoPSI5NiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjM1IiB5PSI2MzUiIHdpZHRoPSI5NiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI0NSIgeT0iNjU1Ij50YWJsZV9uYW1lPC90ZXh0PgogICA8cmVjdCB4PSIzNzMiIHk9IjY2OSIgd2lkdGg9IjQwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjM3MSIgeT0iNjY3IiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM4MSIgeT0iNjg3Ij5BUzwvdGV4dD4KICAgPHJlY3QgeD0iNDMzIiB5PSI2NjkiIHdpZHRoPSIxMDYiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjQzMSIgeT0iNjY3IiB3aWR0aD0iMTA2IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iNDQxIiB5PSI2ODciPnN1YnNyY19uYW1lPC90ZXh0PgogICA8cmVjdCB4PSIyMzciIHk9IjU5MyIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIzNSIgeT0iNTkxIiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjI0NSIgeT0iNjExIj4sPC90ZXh0PgogICA8cmVjdCB4PSI1OTkiIHk9IjYzNyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjU5NyIgeT0iNjM1IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjYwNyIgeT0iNjU1Ij4pPC90ZXh0PgogICA8cmVjdCB4PSIxMDEiIHk9Ijc1MSIgd2lkdGg9Ijc2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9Ijk5IiB5PSI3NDkiIHdpZHRoPSI3NiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTA5IiB5PSI3NjkiPkVYUE9TRTwvdGV4dD4KICAgPHJlY3QgeD0iMTk3IiB5PSI3NTEiIHdpZHRoPSI5NiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxOTUiIHk9Ijc0OSIgd2lkdGg9Ijk2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyMDUiIHk9Ijc2OSI+UFJPR1JFU1M8L3RleHQ+CiAgIDxyZWN0IHg9IjMxMyIgeT0iNzUxIiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzExIiB5PSI3NDkiIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzIxIiB5PSI3NjkiPkFTPC90ZXh0PgogICA8cmVjdCB4PSIzNzMiIHk9Ijc1MSIgd2lkdGg9IjE5NiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMzcxIiB5PSI3NDkiIHdpZHRoPSIxOTYiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzODEiIHk9Ijc2OSI+cHJvZ3Jlc3Nfc3Vic291cmNlX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjUxNyIgeT0iODM3IiB3aWR0aD0iMTAyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI1MTUiIHk9IjgzNSIgd2lkdGg9IjEwMiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjUyNSIgeT0iODU1Ij53aXRoX29wdGlvbnM8L3RleHQ+CiAgIDxwYXRoIGNsYXNzPSJsaW5lIiBkPSJtMTcgMTcgaDIgbTAgMCBoMTAgbTE0MCAwIGgxMCBtMjAgMCBoMTAgbTAgMCBoMTMwIG0tMTYwIDAgaDIwIG0xNDAgMCBoMjAgbS0xODAgMCBxMTAgMCAxMCAxMCBtMTYwIDAgcTAgLTEwIDEwIC0xMCBtLTE3MCAxMCB2MTIgbTE2MCAwIHYtMTIgbS0xNjAgMTIgcTAgMTAgMTAgMTAgbTE0MCAwIHExMCAwIDEwIC0xMCBtLTE1MCAxMCBoMTAgbTEyMCAwIGgxMCBtMjAgLTMyIGgxMCBtODIgMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS00MDIgOTggbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtMCAwIGgyNDIgbS0yNzIgMCBoMjAgbTI1MiAwIGgyMCBtLTI5MiAwIHExMCAwIDEwIDEwIG0yNzIgMCBxMCAtMTAgMTAgLTEwIG0tMjgyIDEwIHYxMiBtMjcyIDAgdi0xMiBtLTI3MiAxMiBxMCAxMCAxMCAxMCBtMjUyIDAgcTEwIDAgMTAgLTEwIG0tMjYyIDEwIGgxMCBtMTA0IDAgaDEwIG0wIDAgaDEwIG0xMDggMCBoMTAgbTIwIC0zMiBoMTAgbTYwIDAgaDEwIG0wIDAgaDEwIG0xMDggMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS00MjAgOTggbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgaDEwIG0xMTYgMCBoMTAgbTAgMCBoMTAgbTEzNiAwIGgxMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtLTQ0MSAxMTAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtMjYgMCBoMTAgbTAgMCBoMTAgbTEzNCAwIGgxMCBtMjAgMCBoMTAgbTI2IDAgaDEwIG0yMCAwIGgxMCBtMTEwIDAgaDEwIG0tMTUwIDAgbDIwIDAgbS0xIDAgcS05IDAgLTkgLTEwIGwwIC0yNCBxMCAtMTAgMTAgLTEwIG0xMzAgNDQgbDIwIDAgbS0yMCAwIHExMCAwIDEwIC0xMCBsMCAtMjQgcTAgLTEwIC0xMCAtMTAgbS0xMzAgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDg2IG0yMCA0NCBoMTAgbTI2IDAgaDEwIG0tMjgyIDAgaDIwIG0yNjIgMCBoMjAgbS0zMDIgMCBxMTAgMCAxMCAxMCBtMjgyIDAgcTAgLTEwIDEwIC0xMCBtLTI5MiAxMCB2MTQgbTI4MiAwIHYtMTQgbS0yODIgMTQgcTAgMTAgMTAgMTAgbTI2MiAwIHExMCAwIDEwIC0xMCBtLTI3MiAxMCBoMTAgbTAgMCBoMjUyIG0tNTAyIC0zNCBoMjAgbTUwMiAwIGgyMCBtLTU0MiAwIHExMCAwIDEwIDEwIG01MjIgMCBxMCAtMTAgMTAgLTEwIG0tNTMyIDEwIHYzMCBtNTIyIDAgdi0zMCBtLTUyMiAzMCBxMCAxMCAxMCAxMCBtNTAyIDAgcTEwIDAgMTAgLTEwIG0tNTEyIDEwIGgxMCBtMCAwIGg0OTIgbTIyIC01MCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS02MDIgMTQyIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMjIgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDEwIG0xNjIgMCBoMTAgbTIwIDAgaDEwIG0yNiAwIGgxMCBtMjAgMCBoMTAgbTExMCAwIGgxMCBtLTE1MCAwIGwyMCAwIG0tMSAwIHEtOSAwIC05IC0xMCBsMCAtMjQgcTAgLTEwIDEwIC0xMCBtMTMwIDQ0IGwyMCAwIG0tMjAgMCBxMTAgMCAxMCAtMTAgbDAgLTI0IHEwIC0xMCAtMTAgLTEwIG0tMTMwIDAgaDEwIG0yNCAwIGgxMCBtMCAwIGg4NiBtMjAgNDQgaDEwIG0yNiAwIGgxMCBtLTI4MiAwIGgyMCBtMjYyIDAgaDIwIG0tMzAyIDAgcTEwIDAgMTAgMTAgbTI4MiAwIHEwIC0xMCAxMCAtMTAgbS0yOTIgMTAgdjE0IG0yODIgMCB2LTE0IG0tMjgyIDE0IHEwIDEwIDEwIDEwIG0yNjIgMCBxMTAgMCAxMCAtMTAgbS0yNzIgMTAgaDEwIG0wIDAgaDI1MiBtMjAgLTM0IGgxMCBtMjYgMCBoMTAgbS01OTQgMCBoMjAgbTU3NCAwIGgyMCBtLTYxNCAwIHExMCAwIDEwIDEwIG01OTQgMCBxMCAtMTAgMTAgLTEwIG0tNjA0IDEwIHYzMCBtNTk0IDAgdi0zMCBtLTU5NCAzMCBxMCAxMCAxMCAxMCBtNTc0IDAgcTEwIDAgMTAgLTEwIG0tNTg0IDEwIGgxMCBtMCAwIGg1NjQgbTIyIC01MCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS02NTEgOTggbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtMTM4IDAgaDEwIG0wIDAgaDQ0MiBtLTYyMCAwIGgyMCBtNjAwIDAgaDIwIG0tNjQwIDAgcTEwIDAgMTAgMTAgbTYyMCAwIHEwIC0xMCAxMCAtMTAgbS02MzAgMTAgdjY4IG02MjAgMCB2LTY4IG0tNjIwIDY4IHEwIDEwIDEwIDEwIG02MDAgMCBxMTAgMCAxMCAtMTAgbS02MTAgMTAgaDEwIG0xMDYgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0yMCAwIGgxMCBtOTYgMCBoMTAgbTIwIDAgaDEwIG0wIDAgaDE3NiBtLTIwNiAwIGgyMCBtMTg2IDAgaDIwIG0tMjI2IDAgcTEwIDAgMTAgMTAgbTIwNiAwIHEwIC0xMCAxMCAtMTAgbS0yMTYgMTAgdjEyIG0yMDYgMCB2LTEyIG0tMjA2IDEyIHEwIDEwIDEwIDEwIG0xODYgMCBxMTAgMCAxMCAtMTAgbS0xOTYgMTAgaDEwIG00MCAwIGgxMCBtMCAwIGgxMCBtMTA2IDAgaDEwIG0tMzQyIC0zMiBsMjAgMCBtLTEgMCBxLTkgMCAtOSAtMTAgbDAgLTI0IHEwIC0xMCAxMCAtMTAgbTM0MiA0NCBsMjAgMCBtLTIwIDAgcTEwIDAgMTAgLTEwIGwwIC0yNCBxMCAtMTAgLTEwIC0xMCBtLTM0MiAwIGgxMCBtMjQgMCBoMTAgbTAgMCBoMjk4IG0yMCA0NCBoMTAgbTI2IDAgaDEwIG0yMiAtODggbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tNjA4IDE3MCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIyIDAgaDEwIG0wIDAgaDQ3OCBtLTUwOCAwIGgyMCBtNDg4IDAgaDIwIG0tNTI4IDAgcTEwIDAgMTAgMTAgbTUwOCAwIHEwIC0xMCAxMCAtMTAgbS01MTggMTAgdjEyIG01MDggMCB2LTEyIG0tNTA4IDEyIHEwIDEwIDEwIDEwIG00ODggMCBxMTAgMCAxMCAtMTAgbS00OTggMTAgaDEwIG03NiAwIGgxMCBtMCAwIGgxMCBtOTYgMCBoMTAgbTAgMCBoMTAgbTQwIDAgaDEwIG0wIDAgaDEwIG0xOTYgMCBoMTAgbTIyIC0zMiBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0xMzYgODYgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtMCAwIGgxMTIgbS0xNDIgMCBoMjAgbTEyMiAwIGgyMCBtLTE2MiAwIHExMCAwIDEwIDEwIG0xNDIgMCBxMCAtMTAgMTAgLTEwIG0tMTUyIDEwIHYxMiBtMTQyIDAgdi0xMiBtLTE0MiAxMiBxMCAxMCAxMCAxMCBtMTIyIDAgcTEwIDAgMTAgLTEwIG0tMTMyIDEwIGgxMCBtMTAyIDAgaDEwIG0yMyAtMzIgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjY1NyA4MTkgNjY1IDgxNSA2NjUgODIzIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iNjU3IDgxOSA2NDkgODE1IDY0OSA4MjMiPjwvcG9seWdvbj4KPC9zdmc+)

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
<td>The name of the SQL Server connection to use in the source. For
details on creating connections, check the <a
href="/docs/self-managed/v25.2/sql/create-connection/#sql-server"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><strong>FOR ALL TABLES</strong></td>
<td>Create subsources for all tables with CDC enabled in all schemas
upstream.</td>
</tr>
<tr>
<td><strong>FOR TABLES (</strong> <em>table_list</em>
<strong>)</strong></td>
<td>Create subsources for specific tables upstream. Requires
fully-qualified table names
(<code>&lt;schema&gt;.&lt;table&gt;</code>).</td>
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

## Creating a source

Materialize ingests the CDC stream for all (or a specific set of) tables
in your upstream SQL Server database that have [Change Data Capture
enabled](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server).

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_connection
  FOR ALL TABLES;
```

</div>

When you define a source, Materialize will automatically:

1.  Create a **subsource** for each capture instance upstream, and
    perform an initial, snapshot-based sync of the associated tables
    before it starts ingesting change events.

    <div class="highlight">

    ``` chroma
    SHOW SOURCES;
    ```

    </div>

    ```
             name         |   type     |  cluster  |
    ----------------------+------------+------------
     mz_source            | sql-server |
     mz_source_progress   | progress   |
     table_1              | subsource  |
     table_2              | subsource  |
    ```

2.  Incrementally update any materialized or indexed views that depend
    on the source as change events stream in, as a result of `INSERT`,
    `UPDATE` and `DELETE` operations in the upstream SQL Server
    database.

It’s important to note that the schema metadata is captured when the
source is initially created, and is validated against the upstream
schema upon restart. If you create new tables upstream after creating a
SQL Server source and want to replicate them to Materialize, the source
must be dropped and recreated.

##### SQL Server schemas

`CREATE SOURCE` will attempt to create each upstream table in the same
schema as the source. This may lead to naming collisions if, for
example, you are replicating `schema1.table_1` and `schema2.table_1`.
Use the `FOR TABLES` clause to provide aliases for each upstream table,
in such cases, or to specify an alternative destination schema in
Materialize.

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_connection
  FOR TABLES (schema1.table_1 AS s1_table_1, schema2.table_1 AS s2_table_1);
```

</div>

### Monitoring source progress

By default, SQL Server sources expose progress metadata as a subsource
that you can use to monitor source **ingestion progress**. The name of
the progress subsource can be specified when creating a source using the
`EXPOSE PROGRESS AS` clause; otherwise, it will be named
`<src_name>_progress`.

The following metadata is available for each source as a progress
subsource:

| Field | Type | Details |
|----|----|----|
| `lsn` | [`bytea`](/docs/self-managed/v25.2/sql/types/bytea/) | The upper-bound [Log Sequence Number](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-log-architecture-and-management-guide) replicated thus far into Materialize. |

And can be queried using:

<div class="highlight">

``` chroma
SELECT lsn
FROM <src_name>_progress;
```

</div>

The reported `lsn` should increase as Materialize consumes **new** CDC
events from the upstream SQL Server database. For more details on
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

### Supported types

Materialize natively supports the following SQL Server types:

- `tinyint`
- `smallint`
- `int`
- `bigint`
- `real`
- `double`
- `bit`
- `decimal`
- `numeric`
- `money`
- `smallmoney`
- `char`
- `nchar`
- `varchar`
- `nvarchar`
- `sysname`
- `binary`
- `varbinary`
- `json`
- `date`
- `time`
- `smalldatetime`
- `datetime`
- `datetime2`
- `datetimeoffset`
- `uniqueidentifier`

Replicating tables that contain **unsupported [data
types](/docs/self-managed/v25.2/sql/types/)** is possible via the
[`EXCLUDE COLUMNS`
option](/docs/self-managed/v25.2/sql/create-source/sql-server/#handling-unsupported-types)
for the following types:

- `text`
- `ntext`
- `image`
- `varchar(max)`
- `nvarchar(max)`
- `varbinary(max)`

Columns with the specified types need to be excluded because [SQL Server
does not provide the
“before”](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-2017#large-object-data-types)
value when said column is updated.

### Timestamp Rounding

The `time`, `datetime2`, and `datetimeoffset` types in SQL Server have a
default scale of 7 decimal places, or in other words a accuracy of 100
nanoseconds. But the corresponding types in Materialize only support a
scale of 6 decimal places. If a column in SQL Server has a higher scale
than what Materialize can support, it will be rounded up to the largest
scale possible.

```
-- In SQL Server
CREATE TABLE my_timestamps (a datetime2(7));
INSERT INTO my_timestamps VALUES
  ('2000-12-31 23:59:59.99999'),
  ('2000-12-31 23:59:59.999999'),
  ('2000-12-31 23:59:59.9999999');
– Replicated into Materialize
SELECT * FROM my_timestamps;
'2000-12-31 23:59:59.999990'
'2000-12-31 23:59:59.999999'
'2001-01-01 00:00:00'
```

### Snapshot latency for inactive databases

When a new Source is created, Materialize performs a snapshotting
operation to sync the data. However, for a new SQL Server source, if
none of the replicating tables are receiving write queries, snapshotting
may take up to an additional 5 minutes to complete. The 5 minute
interval is due to a hardcoded interval in the SQL Server Change Data
Capture (CDC) implementation which only notifies CDC consumers every 5
minutes when no changes are made to replicating tables.

See [Monitoring freshness
status](/docs/self-managed/v25.2/ingest-data/monitoring-data-ingestion/#monitoring-hydrationdata-freshness-status)

### Capture Instance Selection

When a new source is created, Materialize selects a capture instance for
each table. SQL Server permits at most two capture instances per table,
which are listed in the
[`sys.cdc_change_tables`](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-change-tables-transact-sql)
system table. For each table, Materialize picks the capture instance
with the most recent `create_date`.

If two capture instances for a table share the same timestamp (unlikely
given the millisecond resolution), Materialize selects the
`capture_instance` with the lexicographically larger name.

## Examples

<div class="important">

**! Important:** Before creating a SQL Server source, you must enable
Change Data Capture and `SNAPSHOT` transaction isolation in the upstream
database.

</div>

### Creating a connection

A connection describes how to connect and authenticate to an external
system you want Materialize to read data from.

Once created, a connection is **reusable** across multiple
`CREATE SOURCE` statements. For more details on creating connections,
check the
[`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection/#sql-server)
documentation page.

<div class="highlight">

``` chroma
CREATE SECRET sqlserver_pass AS '<SQL_SERVER_PASSWORD>';

CREATE CONNECTION sqlserver_connection TO SQL SERVER (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 1433,
    USER 'materialize',
    PASSWORD SECRET sqlserver_pass,
    DATABASE '<DATABASE_NAME>'
);
```

</div>

If your SQL Server instance is not exposed to the public internet, you
can [tunnel the
connection](/docs/self-managed/v25.2/sql/create-connection/#network-security-connections)
through and SSH bastion host.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-ssh-tunnel" class="tab-pane" title="SSH tunnel">

<div class="highlight">

``` chroma
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize',
    DATABASE '<DATABASE_NAME>'
);
```

</div>

<div class="highlight">

``` chroma
CREATE CONNECTION sqlserver_connection TO SQL SERVER (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    SSH TUNNEL ssh_connection,
    DATABASE '<DATABASE_NAME>'
);
```

</div>

For step-by-step instructions on creating SSH tunnel connections and
configuring an SSH bastion server to accept connections from
Materialize, check [this
guide](/docs/self-managed/v25.2/ops/network-security/ssh-tunnel/).

</div>

</div>

</div>

### Creating a source

You **must** enable Change Data Capture, see [Enable Change Data Capture
SQL Server
Instructions](/docs/self-managed/v25.2/ingest-data/sql-server/self-hosted/#a-configure-sql-server).

Once CDC is enabled for all of the relevant tables, you can create a
`SOURCE` in Materialize to begin replicating data!

*Create subsources for all tables in SQL Server*

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
    FROM SQL SERVER CONNECTION sqlserver_connection
    FOR ALL TABLES;
```

</div>

*Create subsources for specific tables in SQL Server*

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sqlserver_connection
  FOR TABLES (mydb.table_1, mydb.table_2 AS alias_table_2);
```

</div>

#### Handling unsupported types

If you’re replicating tables that use [data types
unsupported](#supported-types) by SQL Server’s CDC feature, use the
`EXCLUDE COLUMNS` option to exclude them from replication. This option
expects the upstream fully-qualified names of the replicated table and
column (i.e. as defined in your SQL Server database).

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sqlserver_connection (
    EXCLUDE COLUMNS (mydb.table_1.column_of_unsupported_type)
  )
  FOR ALL TABLES;
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

## Related pages

- [`CREATE SECRET`](/docs/self-managed/v25.2/sql/create-secret)
- [`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection)
- [`CREATE SOURCE`](../)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-source/sql-server.md"
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
