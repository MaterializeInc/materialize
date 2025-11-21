<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [CREATE
SOURCE](/docs/sql/create-source/)

</div>

# CREATE SOURCE: Load generator

[`CREATE SOURCE`](/docs/sql/create-source/) connects Materialize to an
external system you want to read data from, and provides details about
how to decode and interpret that data.

Load generator sources produce synthetic data for use in demos and
performance tests.

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1NTUiIGhlaWdodD0iNjQzIj4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iMyIgd2lkdGg9IjE0MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyOSIgeT0iMSIgd2lkdGg9IjE0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzkiIHk9IjIxIj5DUkVBVEUgU09VUkNFPC90ZXh0PgogICA8cmVjdCB4PSIyMTEiIHk9IjM1IiB3aWR0aD0iMTIwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIwOSIgeT0iMzMiIHdpZHRoPSIxMjAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjIxOSIgeT0iNTMiPklGIE5PVCBFWElTVFM8L3RleHQ+CiAgIDxyZWN0IHg9IjM3MSIgeT0iMyIgd2lkdGg9IjgyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIzNjkiIHk9IjEiIHdpZHRoPSI4MiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjM3OSIgeT0iMjEiPnNyY19uYW1lPC90ZXh0PgogICA8cmVjdCB4PSI1NSIgeT0iMTMzIiB3aWR0aD0iMTA0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjUzIiB5PSIxMzEiIHdpZHRoPSIxMDQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjYzIiB5PSIxNTEiPklOIENMVVNURVI8L3RleHQ+CiAgIDxyZWN0IHg9IjE3OSIgeT0iMTMzIiB3aWR0aD0iMTA4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIxNzciIHk9IjEzMSIgd2lkdGg9IjEwOCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjE4NyIgeT0iMTUxIj5jbHVzdGVyX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjMyNyIgeT0iMTAxIiB3aWR0aD0iMTk2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjMyNSIgeT0iOTkiIHdpZHRoPSIxOTYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjMzNSIgeT0iMTE5Ij5GUk9NIExPQUQgR0VORVJBVE9SPC90ZXh0PgogICA8cmVjdCB4PSI0OCIgeT0iMjQzIiB3aWR0aD0iODYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDYiIHk9IjI0MSIgd2lkdGg9Ijg2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1NiIgeT0iMjYxIj5BVUNUSU9OPC90ZXh0PgogICA8cmVjdCB4PSI0OCIgeT0iMjg3IiB3aWR0aD0iMTA0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQ2IiB5PSIyODUiIHdpZHRoPSIxMDQiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjU2IiB5PSIzMDUiPk1BUktFVElORzwvdGV4dD4KICAgPHJlY3QgeD0iNDgiIHk9IjMzMSIgd2lkdGg9IjYwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQ2IiB5PSIzMjkiIHdpZHRoPSI2MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTYiIHk9IjM0OSI+VFBDSDwvdGV4dD4KICAgPHJlY3QgeD0iNDgiIHk9IjM3NSIgd2lkdGg9IjEwMCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0NiIgeT0iMzczIiB3aWR0aD0iMTAwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1NiIgeT0iMzkzIj5LRVkgVkFMVUU8L3RleHQ+CiAgIDxyZWN0IHg9IjIxMiIgeT0iMjQzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjEwIiB5PSIyNDEiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjIwIiB5PSIyNjEiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjI3OCIgeT0iMjQzIiB3aWR0aD0iMTY2IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyNzYiIHk9IjI0MSIgd2lkdGg9IjE2NiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI4NiIgeT0iMjYxIj5sb2FkX2dlbmVyYXRvcl9vcHRpb248L3RleHQ+CiAgIDxyZWN0IHg9IjI3OCIgeT0iMTk5IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjc2IiB5PSIxOTciIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjg2IiB5PSIyMTciPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjQ4NCIgeT0iMjQzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDgyIiB5PSIyNDEiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDkyIiB5PSIyNjEiPik8L3RleHQ+CiAgIDxyZWN0IHg9IjIxMCIgeT0iNDQxIiB3aWR0aD0iMTM4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIwOCIgeT0iNDM5IiB3aWR0aD0iMTM4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyMTgiIHk9IjQ1OSI+Rk9SIEFMTCBUQUJMRVM8L3RleHQ+CiAgIDxyZWN0IHg9IjQ1IiB5PSI1MjMiIHdpZHRoPSI3NiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0MyIgeT0iNTIxIiB3aWR0aD0iNzYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjUzIiB5PSI1NDEiPkVYUE9TRTwvdGV4dD4KICAgPHJlY3QgeD0iMTQxIiB5PSI1MjMiIHdpZHRoPSI5NiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIxMzkiIHk9IjUyMSIgd2lkdGg9Ijk2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxNDkiIHk9IjU0MSI+UFJPR1JFU1M8L3RleHQ+CiAgIDxyZWN0IHg9IjI1NyIgeT0iNTIzIiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjU1IiB5PSI1MjEiIHdpZHRoPSI0MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjY1IiB5PSI1NDEiPkFTPC90ZXh0PgogICA8cmVjdCB4PSIzMTciIHk9IjUyMyIgd2lkdGg9IjE5NiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMzE1IiB5PSI1MjEiIHdpZHRoPSIxOTYiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzMjUiIHk9IjU0MSI+cHJvZ3Jlc3Nfc3Vic291cmNlX25hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjQwNSIgeT0iNjA5IiB3aWR0aD0iMTAyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI0MDMiIHk9IjYwNyIgd2lkdGg9IjEwMiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjQxMyIgeT0iNjI3Ij53aXRoX29wdGlvbnM8L3RleHQ+CiAgIDxwYXRoIGNsYXNzPSJsaW5lIiBkPSJtMTcgMTcgaDIgbTAgMCBoMTAgbTE0MCAwIGgxMCBtMjAgMCBoMTAgbTAgMCBoMTMwIG0tMTYwIDAgaDIwIG0xNDAgMCBoMjAgbS0xODAgMCBxMTAgMCAxMCAxMCBtMTYwIDAgcTAgLTEwIDEwIC0xMCBtLTE3MCAxMCB2MTIgbTE2MCAwIHYtMTIgbS0xNjAgMTIgcTAgMTAgMTAgMTAgbTE0MCAwIHExMCAwIDEwIC0xMCBtLTE1MCAxMCBoMTAgbTEyMCAwIGgxMCBtMjAgLTMyIGgxMCBtODIgMCBoMTAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS00NjIgOTggbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtMCAwIGgyNDIgbS0yNzIgMCBoMjAgbTI1MiAwIGgyMCBtLTI5MiAwIHExMCAwIDEwIDEwIG0yNzIgMCBxMCAtMTAgMTAgLTEwIG0tMjgyIDEwIHYxMiBtMjcyIDAgdi0xMiBtLTI3MiAxMiBxMCAxMCAxMCAxMCBtMjUyIDAgcTEwIDAgMTAgLTEwIG0tMjYyIDEwIGgxMCBtMTA0IDAgaDEwIG0wIDAgaDEwIG0xMDggMCBoMTAgbTIwIC0zMiBoMTAgbTE5NiAwIGgxMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtLTUzOSAxNDIgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yMiAwIGgxMCBtODYgMCBoMTAgbTAgMCBoMTggbS0xNDQgMCBoMjAgbTEyNCAwIGgyMCBtLTE2NCAwIHExMCAwIDEwIDEwIG0xNDQgMCBxMCAtMTAgMTAgLTEwIG0tMTU0IDEwIHYyNCBtMTQ0IDAgdi0yNCBtLTE0NCAyNCBxMCAxMCAxMCAxMCBtMTI0IDAgcTEwIDAgMTAgLTEwIG0tMTM0IDEwIGgxMCBtMTA0IDAgaDEwIG0tMTM0IC0xMCB2MjAgbTE0NCAwIHYtMjAgbS0xNDQgMjAgdjI0IG0xNDQgMCB2LTI0IG0tMTQ0IDI0IHEwIDEwIDEwIDEwIG0xMjQgMCBxMTAgMCAxMCAtMTAgbS0xMzQgMTAgaDEwIG02MCAwIGgxMCBtMCAwIGg0NCBtLTEzNCAtMTAgdjIwIG0xNDQgMCB2LTIwIG0tMTQ0IDIwIHYyNCBtMTQ0IDAgdi0yNCBtLTE0NCAyNCBxMCAxMCAxMCAxMCBtMTI0IDAgcTEwIDAgMTAgLTEwIG0tMTM0IDEwIGgxMCBtMTAwIDAgaDEwIG0wIDAgaDQgbTQwIC0xMzIgaDEwIG0yNiAwIGgxMCBtMjAgMCBoMTAgbTE2NiAwIGgxMCBtLTIwNiAwIGwyMCAwIG0tMSAwIHEtOSAwIC05IC0xMCBsMCAtMjQgcTAgLTEwIDEwIC0xMCBtMTg2IDQ0IGwyMCAwIG0tMjAgMCBxMTAgMCAxMCAtMTAgbDAgLTI0IHEwIC0xMCAtMTAgLTEwIG0tMTg2IDAgaDEwIG0yNCAwIGgxMCBtMCAwIGgxNDIgbTIwIDQ0IGgxMCBtMjYgMCBoMTAgbS0zMzggMCBoMjAgbTMxOCAwIGgyMCBtLTM1OCAwIHExMCAwIDEwIDEwIG0zMzggMCBxMCAtMTAgMTAgLTEwIG0tMzQ4IDEwIHYxNCBtMzM4IDAgdi0xNCBtLTMzOCAxNCBxMCAxMCAxMCAxMCBtMzE4IDAgcTEwIDAgMTAgLTEwIG0tMzI4IDEwIGgxMCBtMCAwIGgzMDggbTIyIC0zNCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0zNjQgMTk4IGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGgxMCBtMTM4IDAgaDEwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tMzY3IDUwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMjIgMCBoMTAgbTAgMCBoNDc4IG0tNTA4IDAgaDIwIG00ODggMCBoMjAgbS01MjggMCBxMTAgMCAxMCAxMCBtNTA4IDAgcTAgLTEwIDEwIC0xMCBtLTUxOCAxMCB2MTIgbTUwOCAwIHYtMTIgbS01MDggMTIgcTAgMTAgMTAgMTAgbTQ4OCAwIHExMCAwIDEwIC0xMCBtLTQ5OCAxMCBoMTAgbTc2IDAgaDEwIG0wIDAgaDEwIG05NiAwIGgxMCBtMCAwIGgxMCBtNDAgMCBoMTAgbTAgMCBoMTAgbTE5NiAwIGgxMCBtMjIgLTMyIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtLTE5MiA4NiBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIyIDAgaDEwIG0wIDAgaDExMiBtLTE0MiAwIGgyMCBtMTIyIDAgaDIwIG0tMTYyIDAgcTEwIDAgMTAgMTAgbTE0MiAwIHEwIC0xMCAxMCAtMTAgbS0xNTIgMTAgdjEyIG0xNDIgMCB2LTEyIG0tMTQyIDEyIHEwIDEwIDEwIDEwIG0xMjIgMCBxMTAgMCAxMCAtMTAgbS0xMzIgMTAgaDEwIG0xMDIgMCBoMTAgbTIzIC0zMiBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTQ1IDU5MSA1NTMgNTg3IDU1MyA1OTUiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSI1NDUgNTkxIDUzNyA1ODcgNTM3IDU5NSI+PC9wb2x5Z29uPgo8L3N2Zz4=)

</div>

#### `load_generator_option`

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MTciIGhlaWdodD0iNTIxIj4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSI1MSIgeT0iMyIgd2lkdGg9IjEyOCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iMSIgd2lkdGg9IjEyOCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjIxIj5USUNLIElOVEVSVkFMPC90ZXh0PgogICA8cmVjdCB4PSIxOTkiIHk9IjMiIHdpZHRoPSI2OCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMTk3IiB5PSIxIiB3aWR0aD0iNjgiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIyMDciIHk9IjIxIj5pbnRlcnZhbDwvdGV4dD4KICAgPHJlY3QgeD0iNzEiIHk9IjQ3IiB3aWR0aD0iNjIiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNjkiIHk9IjQ1IiB3aWR0aD0iNjIiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9Ijc5IiB5PSI2NSI+QVMgT0Y8L3RleHQ+CiAgIDxyZWN0IHg9IjcxIiB5PSI5MSIgd2lkdGg9IjY0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjY5IiB5PSI4OSIgd2lkdGg9IjY0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI3OSIgeT0iMTA5Ij5VUCBUTzwvdGV4dD4KICAgPHJlY3QgeD0iMTc1IiB5PSI0NyIgd2lkdGg9IjQ0IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIxNzMiIHk9IjQ1IiB3aWR0aD0iNDQiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIxODMiIHk9IjY1Ij50aWNrPC90ZXh0PgogICA8cmVjdCB4PSI1MSIgeT0iMTM1IiB3aWR0aD0iMTMwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQ5IiB5PSIxMzMiIHdpZHRoPSIxMzAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjU5IiB5PSIxNTMiPlNDQUxFIEZBQ1RPUjwvdGV4dD4KICAgPHJlY3QgeD0iMjAxIiB5PSIxMzUiIHdpZHRoPSIxMDAiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjE5OSIgeT0iMTMzIiB3aWR0aD0iMTAwIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMjA5IiB5PSIxNTMiPnNjYWxlX2ZhY3RvcjwvdGV4dD4KICAgPHJlY3QgeD0iNTEiIHk9IjE3OSIgd2lkdGg9IjE1NCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iMTc3IiB3aWR0aD0iMTU0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1OSIgeT0iMTk3Ij5NQVggQ0FSRElOQUxJVFk8L3RleHQ+CiAgIDxyZWN0IHg9IjIyNSIgeT0iMTc5IiB3aWR0aD0iMTIyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyMjMiIHk9IjE3NyIgd2lkdGg9IjEyMiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjIzMyIgeT0iMTk3Ij5tYXhfY2FyZGluYWxpdHk8L3RleHQ+CiAgIDxyZWN0IHg9IjUxIiB5PSIyMjMiIHdpZHRoPSI1OCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iMjIxIiB3aWR0aD0iNTgiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjU5IiB5PSIyNDEiPktFWVM8L3RleHQ+CiAgIDxyZWN0IHg9IjEyOSIgeT0iMjIzIiB3aWR0aD0iNTAiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjEyNyIgeT0iMjIxIiB3aWR0aD0iNTAiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIxMzciIHk9IjI0MSI+a2V5czwvdGV4dD4KICAgPHJlY3QgeD0iNTEiIHk9IjI2NyIgd2lkdGg9IjE2MiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iMjY1IiB3aWR0aD0iMTYyIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1OSIgeT0iMjg1Ij5TTkFQU0hPVCBST1VORFM8L3RleHQ+CiAgIDxyZWN0IHg9IjIzMyIgeT0iMjY3IiB3aWR0aD0iMTMyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyMzEiIHk9IjI2NSIgd2lkdGg9IjEzMiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI0MSIgeT0iMjg1Ij5zbmFwc2hvdF9yb3VuZHM8L3RleHQ+CiAgIDxyZWN0IHg9IjUxIiB5PSIzMTEiIHdpZHRoPSIyMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDkiIHk9IjMwOSIgd2lkdGg9IjIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjMyOSI+VFJBTlNBQ1RJT05BTCBTTkFQU0hPVDwvdGV4dD4KICAgPHJlY3QgeD0iMjk3IiB5PSIzMTEiIHdpZHRoPSIxNzIiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjI5NSIgeT0iMzA5IiB3aWR0aD0iMTcyIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMzA1IiB5PSIzMjkiPnRyYW5zYWN0aW9uYWxfc25hcHNob3Q8L3RleHQ+CiAgIDxyZWN0IHg9IjUxIiB5PSIzNTUiIHdpZHRoPSIxMDIiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDkiIHk9IjM1MyIgd2lkdGg9IjEwMiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjM3MyI+VkFMVUUgU0laRTwvdGV4dD4KICAgPHJlY3QgeD0iMTczIiB5PSIzNTUiIHdpZHRoPSI4OCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMTcxIiB5PSIzNTMiIHdpZHRoPSI4OCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjE4MSIgeT0iMzczIj52YWx1ZV9zaXplPC90ZXh0PgogICA8cmVjdCB4PSI1MSIgeT0iMzk5IiB3aWR0aD0iNTYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDkiIHk9IjM5NyIgd2lkdGg9IjU2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1OSIgeT0iNDE3Ij5TRUVEPC90ZXh0PgogICA8cmVjdCB4PSIxMjciIHk9IjM5OSIgd2lkdGg9IjUyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIxMjUiIHk9IjM5NyIgd2lkdGg9IjUyIiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMTM1IiB5PSI0MTciPnNlZWQ8L3RleHQ+CiAgIDxyZWN0IHg9IjUxIiB5PSI0NDMiIHdpZHRoPSIxMDYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDkiIHk9IjQ0MSIgd2lkdGg9IjEwNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTkiIHk9IjQ2MSI+UEFSVElUSU9OUzwvdGV4dD4KICAgPHJlY3QgeD0iMTc3IiB5PSI0NDMiIHdpZHRoPSI4MiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMTc1IiB5PSI0NDEiIHdpZHRoPSI4MiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjE4NSIgeT0iNDYxIj5wYXJ0aXRpb25zPC90ZXh0PgogICA8cmVjdCB4PSI1MSIgeT0iNDg3IiB3aWR0aD0iMTA2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQ5IiB5PSI0ODUiIHdpZHRoPSIxMDYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjU5IiB5PSI1MDUiPkJBVENIIFNJWkU8L3RleHQ+CiAgIDxyZWN0IHg9IjE3NyIgeT0iNDg3IiB3aWR0aD0iOTAiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjE3NSIgeT0iNDg1IiB3aWR0aD0iOTAiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIxODUiIHk9IjUwNSI+YmF0Y2hfc2l6ZTwvdGV4dD4KICAgPHBhdGggY2xhc3M9ImxpbmUiIGQ9Im0xNyAxNyBoMiBtMjAgMCBoMTAgbTEyOCAwIGgxMCBtMCAwIGgxMCBtNjggMCBoMTAgbTAgMCBoMjAyIG0tNDU4IDAgaDIwIG00MzggMCBoMjAgbS00NzggMCBxMTAgMCAxMCAxMCBtNDU4IDAgcTAgLTEwIDEwIC0xMCBtLTQ2OCAxMCB2MjQgbTQ1OCAwIHYtMjQgbS00NTggMjQgcTAgMTAgMTAgMTAgbTQzOCAwIHExMCAwIDEwIC0xMCBtLTQyOCAxMCBoMTAgbTYyIDAgaDEwIG0wIDAgaDIgbS0xMDQgMCBoMjAgbTg0IDAgaDIwIG0tMTI0IDAgcTEwIDAgMTAgMTAgbTEwNCAwIHEwIC0xMCAxMCAtMTAgbS0xMTQgMTAgdjI0IG0xMDQgMCB2LTI0IG0tMTA0IDI0IHEwIDEwIDEwIDEwIG04NCAwIHExMCAwIDEwIC0xMCBtLTk0IDEwIGgxMCBtNjQgMCBoMTAgbTIwIC00NCBoMTAgbTQ0IDAgaDEwIG0wIDAgaDI1MCBtLTQ0OCAtMTAgdjIwIG00NTggMCB2LTIwIG0tNDU4IDIwIHY2OCBtNDU4IDAgdi02OCBtLTQ1OCA2OCBxMCAxMCAxMCAxMCBtNDM4IDAgcTEwIDAgMTAgLTEwIG0tNDQ4IDEwIGgxMCBtMTMwIDAgaDEwIG0wIDAgaDEwIG0xMDAgMCBoMTAgbTAgMCBoMTY4IG0tNDQ4IC0xMCB2MjAgbTQ1OCAwIHYtMjAgbS00NTggMjAgdjI0IG00NTggMCB2LTI0IG0tNDU4IDI0IHEwIDEwIDEwIDEwIG00MzggMCBxMTAgMCAxMCAtMTAgbS00NDggMTAgaDEwIG0xNTQgMCBoMTAgbTAgMCBoMTAgbTEyMiAwIGgxMCBtMCAwIGgxMjIgbS00NDggLTEwIHYyMCBtNDU4IDAgdi0yMCBtLTQ1OCAyMCB2MjQgbTQ1OCAwIHYtMjQgbS00NTggMjQgcTAgMTAgMTAgMTAgbTQzOCAwIHExMCAwIDEwIC0xMCBtLTQ0OCAxMCBoMTAgbTU4IDAgaDEwIG0wIDAgaDEwIG01MCAwIGgxMCBtMCAwIGgyOTAgbS00NDggLTEwIHYyMCBtNDU4IDAgdi0yMCBtLTQ1OCAyMCB2MjQgbTQ1OCAwIHYtMjQgbS00NTggMjQgcTAgMTAgMTAgMTAgbTQzOCAwIHExMCAwIDEwIC0xMCBtLTQ0OCAxMCBoMTAgbTE2MiAwIGgxMCBtMCAwIGgxMCBtMTMyIDAgaDEwIG0wIDAgaDEwNCBtLTQ0OCAtMTAgdjIwIG00NTggMCB2LTIwIG0tNDU4IDIwIHYyNCBtNDU4IDAgdi0yNCBtLTQ1OCAyNCBxMCAxMCAxMCAxMCBtNDM4IDAgcTEwIDAgMTAgLTEwIG0tNDQ4IDEwIGgxMCBtMjI2IDAgaDEwIG0wIDAgaDEwIG0xNzIgMCBoMTAgbS00NDggLTEwIHYyMCBtNDU4IDAgdi0yMCBtLTQ1OCAyMCB2MjQgbTQ1OCAwIHYtMjQgbS00NTggMjQgcTAgMTAgMTAgMTAgbTQzOCAwIHExMCAwIDEwIC0xMCBtLTQ0OCAxMCBoMTAgbTEwMiAwIGgxMCBtMCAwIGgxMCBtODggMCBoMTAgbTAgMCBoMjA4IG0tNDQ4IC0xMCB2MjAgbTQ1OCAwIHYtMjAgbS00NTggMjAgdjI0IG00NTggMCB2LTI0IG0tNDU4IDI0IHEwIDEwIDEwIDEwIG00MzggMCBxMTAgMCAxMCAtMTAgbS00NDggMTAgaDEwIG01NiAwIGgxMCBtMCAwIGgxMCBtNTIgMCBoMTAgbTAgMCBoMjkwIG0tNDQ4IC0xMCB2MjAgbTQ1OCAwIHYtMjAgbS00NTggMjAgdjI0IG00NTggMCB2LTI0IG0tNDU4IDI0IHEwIDEwIDEwIDEwIG00MzggMCBxMTAgMCAxMCAtMTAgbS00NDggMTAgaDEwIG0xMDYgMCBoMTAgbTAgMCBoMTAgbTgyIDAgaDEwIG0wIDAgaDIxMCBtLTQ0OCAtMTAgdjIwIG00NTggMCB2LTIwIG0tNDU4IDIwIHYyNCBtNDU4IDAgdi0yNCBtLTQ1OCAyNCBxMCAxMCAxMCAxMCBtNDM4IDAgcTEwIDAgMTAgLTEwIG0tNDQ4IDEwIGgxMCBtMTA2IDAgaDEwIG0wIDAgaDEwIG05MCAwIGgxMCBtMCAwIGgyMDIgbTIzIC00ODQgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjUwNyAxNyA1MTUgMTMgNTE1IDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTA3IDE3IDQ5OSAxMyA0OTkgMjEiPjwvcG9seWdvbj4KPC9zdmc+)

</div>

#### `with_options`

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
<td><strong>IN CLUSTER</strong> <em>cluster_name</em></td>
<td>The <a href="/docs/sql/create-cluster">cluster</a> to maintain this
source.</td>
</tr>
<tr>
<td><strong>AUCTION</strong></td>
<td>Use the <a href="#auction">auction</a> load generator.</td>
</tr>
<tr>
<td><strong>MARKETING</strong></td>
<td>Use the <a href="#marketing">marketing</a> load generator.</td>
</tr>
<tr>
<td><strong>TPCH</strong></td>
<td>Use the <a href="#tpch">tpch</a> load generator.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Do nothing (except issuing a notice) if a source with the same name
already exists.</td>
</tr>
<tr>
<td><strong>TICK INTERVAL</strong></td>
<td>The interval at which the next datum should be emitted. Defaults to
one second.</td>
</tr>
<tr>
<td><strong>AS OF</strong></td>
<td>The tick at which to start producing data. Defaults to 0.</td>
</tr>
<tr>
<td><strong>UP TO</strong></td>
<td>The tick before which to stop producing data. Defaults to
infinite.</td>
</tr>
<tr>
<td><strong>SCALE FACTOR</strong></td>
<td>The scale factor for the <code>TPCH</code> generator. Defaults to
<code>0.01</code> (~ 10MB).</td>
</tr>
<tr>
<td><strong>FOR ALL TABLES</strong></td>
<td>Creates subsources for all tables in the load generator.</td>
</tr>
<tr>
<td><strong>EXPOSE PROGRESS AS</strong>
<em>progress_subsource_name</em></td>
<td>The name of the progress subsource for the source. If this is not
specified, the subsource will be named
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

## Description

Materialize has several built-in load generators, which provide a quick
way to get up and running with no external dependencies before plugging
in your own data sources. If you would like to see an additional load
generator, please submit a [feature
request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests).

### Auction

The auction load generator simulates an auction house, where users are
bidding on an ongoing series of auctions. The auction source will be
automatically demuxed into multiple subsources when the `CREATE SOURCE`
command is executed. This will create the following subsources:

- `organizations` describes the organizations known to the auction
  house.

  | Field | Type | Description |
  |----|----|----|
  | id | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the organization. |
  | name | [`text`](/docs/sql/types/text) | The organization’s name. |

- `users` describes the users that belong to each organization.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the user. |
  | `org_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the organization to which the user belongs. References `organizations.id`. |
  | `name` | [`text`](/docs/sql/types/text) | The user’s name. |

- `accounts` describes the account associated with each organization.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the account. |
  | `org_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the organization to which the account belongs. References `organizations.id`. |
  | `balance` | [`bigint`](/docs/sql/types/bigint) | The balance of the account in dollars. |

- `auctions` describes all past and ongoing auctions.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the auction. |
  | `seller` | [`bigint`](/docs/sql/types/bigint) | The identifier of the user selling the item. References `users.id`. |
  | `item` | [`text`](/docs/sql/types/text) | The name of the item being sold. |
  | `end_time` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the auction closes. |

- `bids` describes the bids placed in each auction.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the bid. |
  | `buyer` | [`bigint`](/docs/sql/types/bigint) | The identifier vof the user placing the bid. References `users.id`. |
  | `auction_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the auction in which the bid is placed. References `auctions.id`. |
  | `amount` | [`bigint`](/docs/sql/types/bigint) | The bid amount in dollars. |
  | `bid_time` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the bid was placed. |

The organizations, users, and accounts are fixed at the time the source
is created. Each tick interval, either a new auction is started, or a
new bid is placed in the currently ongoing auction.

### Marketing

The marketing load generator simulates a marketing organization that is
using a machine learning model to send coupons to potential leads. The
marketing source will be automatically demuxed into multiple subsources
when the `CREATE SOURCE` command is executed. This will create the
following subsources:

- `customers` describes the customers that the marketing team may
  target.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the customer. |
  | `email` | [`text`](/docs/sql/types/text) | The customer’s email. |
  | `income` | [`bigint`](/docs/sql/types/bigint) | The customer’s income in pennies. |

- `impressions` describes online ads that have been seen by a customer.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the impression. |
  | `customer_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the customer that saw the ad. References `customers.id`. |
  | `impression_time` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the ad was seen. |

- `clicks` describes clicks of ads.

  | Field | Type | Description |
  |----|----|----|
  | `impression_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the impression that was clicked. References `impressions.id`. |
  | `click_time` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the impression was clicked. |

- `leads` describes a potential lead for a purchase.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the lead. |
  | `customer_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the customer we’d like to convert. References `customers.id`. |
  | `created_at` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the lead was created. |
  | `converted_at` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the lead was converted. |
  | `conversion_amount` | [`bigint`](/docs/sql/types/bigint) | The amount the lead converted for in pennies. |

- `coupons` describes coupons given to leads.

  | Field | Type | Description |
  |----|----|----|
  | `id` | [`bigint`](/docs/sql/types/bigint) | A unique identifier for the coupon. |
  | `lead_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the lead we’re attempting to convert. References `leads.id`. |
  | `created_at` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the coupon was created. |
  | `amount` | [`bigint`](/docs/sql/types/bigint) | The amount the coupon is for in pennies. |

- `conversion_predictions` describes the predictions made by a highly
  sophisticated machine learning model.

  | Field | Type | Description |
  |----|----|----|
  | `lead_id` | [`bigint`](/docs/sql/types/bigint) | The identifier of the lead we’re attempting to convert. References `leads.id`. |
  | `experiment_bucket` | [`text`](/docs/sql/types/text) | Whether the lead is a control or experiment. |
  | `created_at` | [`timestamp with time zone`](/docs/sql/types/timestamp) | The time at which the prediction was made. |
  | `score` | [`numeric`](/docs/sql/types/numeric) | The predicted likelihood the lead will convert. |

### TPCH

The TPCH load generator implements the [TPC-H benchmark
specification](https://www.tpc.org/tpch/default5.asp). The TPCH source
must be used with `FOR ALL TABLES`, which will create the standard TPCH
relations. If `TICK INTERVAL` is specified, after the initial data load,
an order and its lineitems will be changed at this interval. If not
specified, the dataset will not change over time.

### Monitoring source progress

By default, load generator sources expose progress metadata as a
subsource that you can use to monitor source **ingestion progress**. The
name of the progress subsource can be specified when creating a source
using the `EXPOSE PROGRESS AS` clause; otherwise, it will be named
`<src_name>_progress`.

The following metadata is available for each source as a progress
subsource:

| Field | Type | Meaning |
|----|----|----|
| `offset` | [`uint8`](/docs/sql/types/uint/#uint8-info) | The minimum offset for which updates to this sources are still undetermined. |

And can be queried using:

<div class="highlight">

``` chroma
SELECT "offset"
FROM <src_name>_progress;
```

</div>

As long as the offset continues increasing, Materialize is generating
data. For more details on monitoring source ingestion progress and
debugging related issues, see
[Troubleshooting](/docs/ops/troubleshooting/).

## Examples

### Creating an auction load generator

To create a load generator source that simulates an auction house and
emits new data every second:

<div class="highlight">

``` chroma
CREATE SOURCE auction_house
  FROM LOAD GENERATOR AUCTION
  (TICK INTERVAL '1s')
  FOR ALL TABLES;
```

</div>

To display the created subsources:

<div class="highlight">

``` chroma
SHOW SOURCES;
```

</div>

```
          name          |      type
------------------------+----------------
 accounts               | subsource
 auction_house          | load-generator
 auction_house_progress | progress
 auctions               | subsource
 bids                   | subsource
 organizations          | subsource
 users                  | subsource
```

To examine the simulated bids:

<div class="highlight">

``` chroma
SELECT * from bids;
```

</div>

```
 id | buyer | auction_id | amount |          bid_time
----+-------+------------+--------+----------------------------
 10 |  3844 |          1 |     59 | 2022-09-16 23:24:07.332+00
 11 |  1861 |          1 |     40 | 2022-09-16 23:24:08.332+00
 12 |  3338 |          1 |     97 | 2022-09-16 23:24:09.332+00
```

### Creating a marketing load generator

To create a load generator source that simulates an online marketing
campaign:

<div class="highlight">

``` chroma
CREATE SOURCE marketing
  FROM LOAD GENERATOR MARKETING
  FOR ALL TABLES;
```

</div>

To display the created subsources:

<div class="highlight">

``` chroma
SHOW SOURCES;
```

</div>

```
          name          |      type
------------------------+---------------
 clicks                 | subsource
 conversion_predictions | subsource
 coupons                | subsource
 customers              | subsource
 impressions            | subsource
 leads                  | subsource
 marketing              | load-generator
 marketing_progress     | progress
```

To find all impressions and clicks associated with a campaign over the
last 30 days:

<div class="highlight">

``` chroma
WITH
    click_rollup AS
    (
        SELECT impression_id AS id, count(*) AS clicks
        FROM clicks
        WHERE click_time - INTERVAL '30' DAY <= mz_now()
        GROUP BY impression_id
    ),
    impression_rollup AS
    (
        SELECT id, campaign_id, count(*) AS impressions
        FROM impressions
        WHERE impression_time - INTERVAL '30' DAY <= mz_now()
        GROUP BY id, campaign_id
    )
SELECT campaign_id, sum(impressions) AS impressions, sum(clicks) AS clicks
FROM impression_rollup LEFT JOIN click_rollup USING(id)
GROUP BY campaign_id;
```

</div>

```
 campaign_id | impressions | clicks
-------------+-------------+--------
           0 |         350 |     33
           1 |         325 |     28
           2 |         319 |     24
           3 |         315 |     38
           4 |         305 |     28
           5 |         354 |     31
           6 |         346 |     25
           7 |         337 |     36
           8 |         329 |     38
           9 |         305 |     24
          10 |         345 |     27
          11 |         323 |     30
          12 |         320 |     29
          13 |         331 |     27
          14 |         310 |     22
          15 |         324 |     28
          16 |         315 |     32
          17 |         329 |     36
          18 |         329 |     28
```

### Creating a TPCH load generator

To create the load generator source and its associated subsources:

<div class="highlight">

``` chroma
CREATE SOURCE tpch
  FROM LOAD GENERATOR TPCH (SCALE FACTOR 1)
  FOR ALL TABLES;
```

</div>

To display the created subsources:

<div class="highlight">

``` chroma
SHOW SOURCES;
```

</div>

```
      name     |      type
---------------+---------------
 tpch          | load-generator
 tpch_progress | progress
 supplier      | subsource
 region        | subsource
 partsupp      | subsource
 part          | subsource
 orders        | subsource
 nation        | subsource
 lineitem      | subsource
 customer      | subsource
```

To run the Pricing Summary Report Query (Q1), which reports the amount
of billed, shipped, and returned items:

<div class="highlight">

``` chroma
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
```

</div>

```
 l_returnflag | l_linestatus | sum_qty  | sum_base_price | sum_disc_price  |    sum_charge     |      avg_qty       |     avg_price      |      avg_disc       | count_order
--------------+--------------+----------+----------------+-----------------+-------------------+--------------------+--------------------+---------------------+-------------
 A            | F            | 37772997 |    56604341792 |  54338346989.17 |  57053313118.2657 | 25.490380624798817 | 38198.351517998075 | 0.04003729114831228 |     1481853
 N            | F            |   986796 |     1477585066 |   1418531782.89 |   1489171757.0798 | 25.463731840115603 |  38128.27564317601 | 0.04007431682708436 |       38753
 N            | O            | 74281600 |   111337230039 | 106883023012.04 | 112227399730.9018 |  25.49430183051871 | 38212.221432873834 | 0.03999775539657235 |     2913655
 R            | F            | 37770949 |    56610551077 |   54347734573.7 |  57066196254.4557 | 25.496431466814634 |  38213.68205054471 | 0.03997848687172654 |     1481421
```

## Related pages

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-source/load-generator.md"
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
