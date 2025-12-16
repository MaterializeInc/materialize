<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)
 /  [SELECT](/docs/self-managed/v25.2/sql/select/)

</div>

# Recursive CTEs

Recursive CTEs operate on the recursively-defined structures like trees
or graphs implied from queries over your data.

## Syntax

### with_recursive_cte

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1NDkiIGhlaWdodD0iMzA1Ij4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iMyIgd2lkdGg9IjIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyOSIgeT0iMSIgd2lkdGg9IjIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzkiIHk9IjIxIj5XSVRIIE1VVFVBTExZIFJFQ1VSU0lWRTwvdGV4dD4KICAgPHJlY3QgeD0iNDUiIHk9Ijg1IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDMiIHk9IjgzIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjUzIiB5PSIxMDMiPig8L3RleHQ+CiAgIDxyZWN0IHg9IjExMSIgeT0iMTE3IiB3aWR0aD0iMTAwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjEwOSIgeT0iMTE1IiB3aWR0aD0iMTAwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMTkiIHk9IjEzNSI+UkVUVVJOIEFUPC90ZXh0PgogICA8cmVjdCB4PSIxMTEiIHk9IjE2MSIgd2lkdGg9IjkwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjEwOSIgeT0iMTU5IiB3aWR0aD0iOTAiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjExOSIgeT0iMTc5Ij5FUlJPUiBBVDwvdGV4dD4KICAgPHJlY3QgeD0iMjUxIiB5PSI4NSIgd2lkdGg9IjE0NCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyNDkiIHk9IjgzIiB3aWR0aD0iMTQ0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyNTkiIHk9IjEwMyI+UkVDVVJTSU9OIExJTUlUPC90ZXh0PgogICA8cmVjdCB4PSI0MTUiIHk9Ijg1IiB3aWR0aD0iNDYiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjQxMyIgeT0iODMiIHdpZHRoPSI0NiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjQyMyIgeT0iMTAzIj5saW1pdDwvdGV4dD4KICAgPHJlY3QgeD0iNDgxIiB5PSI4NSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjQ3OSIgeT0iODMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNDg5IiB5PSIxMDMiPik8L3RleHQ+CiAgIDxyZWN0IHg9IjIyMyIgeT0iMjcxIiB3aWR0aD0iMTYyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyMjEiIHk9IjI2OSIgd2lkdGg9IjE2MiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjIzMSIgeT0iMjg5Ij5yZWN1cnNpdmVfY3RlX2JpbmRpbmc8L3RleHQ+CiAgIDxyZWN0IHg9IjIyMyIgeT0iMjI3IiB3aWR0aD0iMjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjIxIiB5PSIyMjUiIHdpZHRoPSIyNCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjMxIiB5PSIyNDUiPiw8L3RleHQ+CiAgIDxyZWN0IHg9IjQyNSIgeT0iMjcxIiB3aWR0aD0iOTYiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjQyMyIgeT0iMjY5IiB3aWR0aD0iOTYiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI0MzMiIHk9IjI4OSI+c2VsZWN0X3N0bXQ8L3RleHQ+CiAgIDxwYXRoIGNsYXNzPSJsaW5lIiBkPSJtMTcgMTcgaDIgbTAgMCBoMTAgbTIyNiAwIGgxMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtLTI3NiA1MCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIyIDAgaDEwIG0wIDAgaDQ3MiBtLTUwMiAwIGgyMCBtNDgyIDAgaDIwIG0tNTIyIDAgcTEwIDAgMTAgMTAgbTUwMiAwIHEwIC0xMCAxMCAtMTAgbS01MTIgMTAgdjEyIG01MDIgMCB2LTEyIG0tNTAyIDEyIHEwIDEwIDEwIDEwIG00ODIgMCBxMTAgMCAxMCAtMTAgbS00OTIgMTAgaDEwIG0yNiAwIGgxMCBtMjAgMCBoMTAgbTAgMCBoMTEwIG0tMTQwIDAgaDIwIG0xMjAgMCBoMjAgbS0xNjAgMCBxMTAgMCAxMCAxMCBtMTQwIDAgcTAgLTEwIDEwIC0xMCBtLTE1MCAxMCB2MTIgbTE0MCAwIHYtMTIgbS0xNDAgMTIgcTAgMTAgMTAgMTAgbTEyMCAwIHExMCAwIDEwIC0xMCBtLTEzMCAxMCBoMTAgbTEwMCAwIGgxMCBtLTEzMCAtMTAgdjIwIG0xNDAgMCB2LTIwIG0tMTQwIDIwIHYyNCBtMTQwIDAgdi0yNCBtLTE0MCAyNCBxMCAxMCAxMCAxMCBtMTIwIDAgcTEwIDAgMTAgLTEwIG0tMTMwIDEwIGgxMCBtOTAgMCBoMTAgbTAgMCBoMTAgbTIwIC03NiBoMTAgbTE0NCAwIGgxMCBtMCAwIGgxMCBtNDYgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0yMiAtMzIgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tMzY4IDIxOCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIyIDAgaDEwIG0xNjIgMCBoMTAgbS0yMDIgMCBsMjAgMCBtLTEgMCBxLTkgMCAtOSAtMTAgbDAgLTI0IHEwIC0xMCAxMCAtMTAgbTE4MiA0NCBsMjAgMCBtLTIwIDAgcTEwIDAgMTAgLTEwIGwwIC0yNCBxMCAtMTAgLTEwIC0xMCBtLTE4MiAwIGgxMCBtMjQgMCBoMTAgbTAgMCBoMTM4IG0yMCA0NCBoMTAgbTk2IDAgaDEwIG0zIDAgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjUzOSAyODUgNTQ3IDI4MSA1NDcgMjg5Ij48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTM5IDI4NSA1MzEgMjgxIDUzMSAyODkiPjwvcG9seWdvbj4KPC9zdmc+)

</div>

### recursive_cte_binding

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1NjMiIGhlaWdodD0iMTQ3Ij4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDYxIDEgNTcgMSA2NSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDYxIDkgNTcgOSA2NSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iNDciIHdpZHRoPSI4MCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjkiIHk9IjQ1IiB3aWR0aD0iODAiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIzOSIgeT0iNjUiPmN0ZV9pZGVudDwvdGV4dD4KICAgPHJlY3QgeD0iMTMxIiB5PSI0NyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjEyOSIgeT0iNDUiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMTM5IiB5PSI2NSI+KDwvdGV4dD4KICAgPHJlY3QgeD0iMTk3IiB5PSI0NyIgd2lkdGg9Ijc4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIxOTUiIHk9IjQ1IiB3aWR0aD0iNzgiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSIyMDUiIHk9IjY1Ij5jb2xfaWRlbnQ8L3RleHQ+CiAgIDxyZWN0IHg9IjI5NSIgeT0iNDciIHdpZHRoPSI3NCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMjkzIiB5PSI0NSIgd2lkdGg9Ijc0IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMzAzIiB5PSI2NSI+Y29sX3R5cGU8L3RleHQ+CiAgIDxyZWN0IHg9IjE5NyIgeT0iMyIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjE5NSIgeT0iMSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyMDUiIHk9IjIxIj4sPC90ZXh0PgogICA8cmVjdCB4PSI0MDkiIHk9IjQ3IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDA3IiB5PSI0NSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0MTciIHk9IjY1Ij4pPC90ZXh0PgogICA8cmVjdCB4PSI0NTUiIHk9IjQ3IiB3aWR0aD0iNDAiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNDUzIiB5PSI0NSIgd2lkdGg9IjQwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI0NjMiIHk9IjY1Ij5BUzwvdGV4dD4KICAgPHJlY3QgeD0iNTE1IiB5PSI0NyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjUxMyIgeT0iNDUiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNTIzIiB5PSI2NSI+KDwvdGV4dD4KICAgPHJlY3QgeD0iMzkzIiB5PSIxMTMiIHdpZHRoPSI5NiIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMzkxIiB5PSIxMTEiIHdpZHRoPSI5NiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjQwMSIgeT0iMTMxIj5zZWxlY3Rfc3RtdDwvdGV4dD4KICAgPHJlY3QgeD0iNTA5IiB5PSIxMTMiIHdpZHRoPSIyNiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI1MDciIHk9IjExMSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI1MTciIHk9IjEzMSI+KTwvdGV4dD4KICAgPHBhdGggY2xhc3M9ImxpbmUiIGQ9Im0xNyA2MSBoMiBtMCAwIGgxMCBtODAgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0yMCAwIGgxMCBtNzggMCBoMTAgbTAgMCBoMTAgbTc0IDAgaDEwIG0tMjEyIDAgbDIwIDAgbS0xIDAgcS05IDAgLTkgLTEwIGwwIC0yNCBxMCAtMTAgMTAgLTEwIG0xOTIgNDQgbDIwIDAgbS0yMCAwIHExMCAwIDEwIC0xMCBsMCAtMjQgcTAgLTEwIC0xMCAtMTAgbS0xOTIgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDE0OCBtMjAgNDQgaDEwIG0yNiAwIGgxMCBtMCAwIGgxMCBtNDAgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbTIgMCBsMiAwIG0tMTkyIDY2IGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMiAwIGgxMCBtOTYgMCBoMTAgbTAgMCBoMTAgbTI2IDAgaDEwIG0zIDAgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjU1MyAxMjcgNTYxIDEyMyA1NjEgMTMxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iNTUzIDEyNyA1NDUgMTIzIDU0NSAxMzEiPjwvcG9seWdvbj4KPC9zdmc+)

</div>

| Field | Use |
|----|----|
| **RETURN AT RECURSION LIMIT \$n** | An optional clause indicating that the fixpoint computation should stop after `$n` iterations and use the current values computed for each `recursive_cte_binding` in the `select_stmt`. This could be useful when debugging and validating the correctness of recursive queries, or when you know exactly how many iterations you want to have, regardless of reaching a fixpoint. See the [Examples](#examples) section for an example. |
| **ERROR AT RECURSION LIMIT \$n** | An optional clause indicating that the fixpoint computation should stop after `$n` iterations and fail the query with an error. Adding this clause with a reasonably high limit is a good safeguard against accidentally running a non-terminating dataflow in your production clusters. |
| **recursive_cte_binding** | A binding that gives the SQL fragment defined under `select_stmt` a `cte_ident` alias. This alias can be used in the same binding or in all other (preceding and subsequent) bindings in the enclosing recursive CTE block. In contrast to [the `cte_binding` definition](/docs/self-managed/v25.2/sql/select/#cte_binding), a `recursive_cte_binding` needs to explicitly state its type as a comma-separated list of (`col_ident` `col_type`) pairs. |

## Details

Within a recursive CTEs block, any `cte_ident` alias can be referenced
in all `recursive_cte_binding` definitions that live under the same
block, as well as in the final `select_stmt` for that block.

A `WITH MUTUALLY RECURSIVE` block with a general form

<div class="highlight">

``` chroma
WITH MUTUALLY RECURSIVE
  -- A sequence of bindings, all in scope for all definitions.
  $R_1(...) AS ( $sql_cte_1 ),
  ...
  $R_n(...) AS ( $sql_cte_n )
  -- Compute the result from the final values of all bindings.
  $sql_body
```

</div>

is evaluated as if it was performing the following steps:

1.  Initially, bind `$R_1, ..., $R_n` to the empty collection.
2.  Repeat in a loop:
    1.  Update `$R_1` using the current values bound to
        `$R_1, ..., $R_n` in `$sql_cte_1`.
    2.  Update `$R_2` using the current values bound to
        `$R_1, ..., $R_n` in `$sql_cte_2`. Note that this includes the
        new value of `$R_1` bound above.
    3.  …
    4.  Update `$R_n` using the current values bound to
        `$R_1, ..., $R_n` in `$sql_cte_n`. Note that this includes the
        new values of `$R_1, ..., $R_{n-1}` bound above.
3.  Exit the loop when one of the following conditions is met:
    1.  The values bound to all CTEs have stopped changing.
    2.  The optional early exit condition to `RETURN` or
        `ERROR AT ITERATION $i` was set and we have reached iteration
        `$i`.

Note that Materialize’s ability to [efficiently handle incremental
changes to your
inputs](https://materialize.com/guides/incremental-computation/) extends
across loop iterations. For each iteration, Materialize performs work
resulting only from the input changes for this iteration and feeds back
the resulting output changes to the next iteration. When the set of
changes for all bindings becomes empty, the recursive computation stops
and the final `select_stmt` is evaluated.

<div class="warning">

**WARNING!**

In the absence of recursive CTEs, every `SELECT` query is guaranteed to
compute its result or fail with an error within a finite amount of time.
However, introducing recursive CTEs complicates the situation as
follows:

1.  The query might not converge (and may never terminate).
    Non-terminating queries never return a result and can consume a lot
    of your cluster resources. See [an
    example](#non-terminating-queries) below.
2.  A small update to a few (or even one) data points in your input
    might cascade in big updates in your recursive computation. This
    most likely will manifest in spikes of the cluster resources
    allocated to your recursive dataflows. See [an
    example](#queries-with-update-locality) below.

</div>

## Examples

Let’s consider a very simple schema consisting of `users` that belong to
a hierarchy of geographical `areas` and exchange `transfers` between
each other. Use the [SQL Shell](/docs/self-managed/v25.2/console/) to
run the sequence of commands below.

### Example schema

<div class="highlight">

``` chroma
-- A hierarchy of geographical locations with various levels of granularity.
CREATE TABLE areas(id int not null, parent int, name text);
-- A collection of users.
CREATE TABLE users(id char(1) not null, area_id int not null, name text);
-- A collection of transfers between these users.
CREATE TABLE transfers(src_id char(1), tgt_id char(1), amount numeric, ts timestamp);
```

</div>

### Example data

<div class="highlight">

``` chroma
DELETE FROM areas;
DELETE FROM users;
DELETE FROM transfers;

INSERT INTO areas VALUES
  (1, 2    , 'Brooklyn'),
  (2, 3    , 'New York'),
  (3, 7    , 'United States of America'),
  (4, 5    , 'Kreuzberg'),
  (5, 6    , 'Berlin'),
  (6, 7    , 'Germany'),
  (7, null , 'Earth');
INSERT INTO users VALUES
  ('A', 1, 'Alice'),
  ('B', 4, 'Bob'),
  ('C', 2, 'Carol'),
  ('D', 5, 'Dan');
INSERT INTO transfers VALUES
  ('B', 'C', 20.0 , now()),
  ('A', 'D', 15.0 , now() + '05 seconds'),
  ('C', 'D', 25.0 , now() + '10 seconds'),
  ('A', 'B', 10.0 , now() + '15 seconds'),
  ('C', 'A', 35.0 , now() + '20 seconds');
```

</div>

### Transitive closure

The following view will compute `connected` as the transitive closure of
a graph where:

- each `user` is a graph vertex, and
- a graph edge between users `x` and `y` exists only if a transfer from
  `x` to `y` was made recently (using the rather small `10 seconds`
  period here for the sake of illustration):

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW connected AS
WITH MUTUALLY RECURSIVE
  connected(src_id char(1), dst_id char(1)) AS (
    SELECT DISTINCT src_id, tgt_id FROM transfers WHERE mz_now() <= ts + interval '10s'
    UNION
    SELECT c1.src_id, c2.dst_id FROM connected c1 JOIN connected c2 ON c1.dst_id = c2.src_id
  )
SELECT src_id, dst_id FROM connected;
```

</div>

To see results change over time, you can
[`SUBSCRIBE`](/docs/self-managed/v25.2/sql/subscribe/) to the
materialized view and then use a different SQL Shell session to insert
some sample data into the base tables used in the view:

<div class="highlight">

``` chroma
SUBSCRIBE(SELECT * FROM connected) WITH (SNAPSHOT = FALSE);
```

</div>

You’ll see results change as new data is inserted. When you’re done,
cancel out of the `SUBSCRIBE` using **Stop streaming**.

<div class="note">

**NOTE:** Depending on your base data, the number of records in the
`connected` result might get close to the square of the number of
`users`.

</div>

### Strongly connected components

Another thing that you might be interested in is identifying maximal
sub-graphs where every pair of `users` are `connected` (the so-called
[*strongly connected components
(SCCs)*](https://en.wikipedia.org/wiki/Strongly_connected_component)) of
the graph defined above. This information might be useful to identify
clusters of closely-tight users in your application.

Since you already have `connected` defined as a `MATERIALIZED VIEW`, you
can piggy-back on that information to derive the SCCs of your graph. Two
`users` will be in the same SCC only if they are `connected` in both
directions. Consequently, given the `connected` contents, we can:

1.  Restrict `connected` to the subset of `symmetric` connections that
    go in both directions.
2.  Identify the `scc` of each `users` entry with the lowest `dst_id` of
    all `symmetric` neighbors and its own `id`.

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW strongly_connected_components AS
  WITH
    symmetric(src_id, dst_id) AS (
      SELECT src_id, dst_id FROM connected
      INTERSECT ALL
      SELECT dst_id, src_id FROM connected
    )
  SELECT u.id, least(min(c.dst_id), u.id)
  FROM users u
  LEFT JOIN symmetric c ON(u.id = c.src_id)
  GROUP BY u.id;
```

</div>

Again, you can insert some sample data into the base tables and observe
how the materialized view contents change over time using `SUBSCRIBE`:

<div class="highlight">

``` chroma
SUBSCRIBE(SELECT * FROM strongly_connected_components) WITH (SNAPSHOT = FALSE);
```

</div>

When you’re done, cancel out of the `SUBSCRIBE` using **Stop
streaming**.

<div class="note">

**NOTE:** The `strongly_connected_components` definition given above is
not recursive, but relies on the recursive CTEs from the `connected`
definition. If you don’t need to keep track of the `connected` contents
for other reasons, you can use [this alternative SCC
definition](https://twitter.com/frankmcsherry/status/1628519795971727366)
which computes SCCs directly using repeated forward and backward label
propagation.

</div>

### Aggregations over a hierarchy

You might want to keep track of the aggregated net balance per area for
a recent period of time. This can be achieved in three steps:

1.  Sum up the aggregated net balance for the set period for each user
    in an `user_balances` CTE.
2.  Sum up the `user_balances` of users directly associated with an area
    in a `direct_balances` CTE.
3.  Recursively add to `direct_balances` the `indirect_balances` sum of
    all child areas.

A materialized view that does the above three steps in three CTEs (of
which the last one is recursive) can be defined as follows:

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW area_balances AS
  WITH MUTUALLY RECURSIVE
    user_balances(id char(1), balance numeric) AS (
      WITH
        credits AS (
          SELECT src_id as id, sum(amount) credit
          FROM transfers
          WHERE mz_now() <= ts + interval '10s'
          GROUP BY src_id
        ),
        debits AS (
          SELECT tgt_id as id, sum(amount) debit
          FROM transfers
          WHERE mz_now() <= ts + interval '10s'
          GROUP BY tgt_id
        )
      SELECT
        id,
        coalesce(debit, 0) - coalesce(credit, 0) as balance
      FROM
        users
        LEFT JOIN credits USING(id)
        LEFT JOIN debits USING(id)
    ),
    direct_balances(id int, parent int, balance numeric) AS (
      SELECT
        a.id as id,
        a.parent as parent,
        coalesce(sum(ub.balance), 0) as balance
      FROM
        areas a
        LEFT JOIN users u ON (a.id = u.area_id)
        LEFT JOIN user_balances ub ON (u.id = ub.id)
      GROUP BY
        a.id, a.parent
    ),
    indirect_balances(id int, parent int, balance numeric) AS (
      SELECT
        db.id as id,
        db.parent as parent,
        db.balance + coalesce(sum(ib.balance), 0) as balance
      FROM
        direct_balances db
        LEFT JOIN indirect_balances ib ON (db.id = ib.parent)
      GROUP BY
        db.id, db.parent, db.balance
    )
  SELECT
    id, balance
  FROM
    indirect_balances;
```

</div>

As before, you can insert [the example data](#example-data) and observe
how the materialized view contents change over time from the `psql` with
the `\watch` command:

<div class="highlight">

``` chroma
SELECT id, name, balance FROM area_balances JOIN areas USING(id) ORDER BY id;
\watch 1
```

</div>

### Non-terminating queries

Let’s look at a slight variation of the [transitive closure
example](#transitive-closure) from above with the following changes:

1.  All `UNION` operators are replaced with `UNION ALL`.
2.  The `mz_now() < ts + $period` predicate from the first `UNION`
    branch is omitted.
3.  The `WITH MUTUALLY RECURSIVE` clause has an optional
    `ERROR AT RECURSION LIMIT 100`.
4.  The final result in this example is ordered by `src_id, dst_id`.

<div class="highlight">

``` chroma
WITH MUTUALLY RECURSIVE (ERROR AT RECURSION LIMIT 100)
  connected(src_id char(1), dst_id char(1)) AS (
    SELECT DISTINCT src_id, tgt_id FROM transfers
    UNION ALL
    SELECT src_id, dst_id FROM connected
    UNION ALL
    SELECT c1.src_id, c2.dst_id FROM connected c1 JOIN connected c2 ON c1.dst_id = c2.src_id
  )
SELECT src_id, dst_id FROM connected ORDER BY src_id, dst_id;
```

</div>

After inserting [the example data](#example-data) you can observe that
executing the above statement returns the following error:

<div class="highlight">

``` chroma
ERROR:  Evaluation error: Recursive query exceeded the recursion limit 100. (Use RETURN AT RECURSION LIMIT to not error, but return the current state as the final result when reaching the limit.)
```

</div>

The recursive CTE `connected` has not converged to a fixpoint within the
first 100 iterations! To see why, you can run variants of the same query
where the

<div class="highlight">

``` chroma
ERROR AT RECURSION LIMIT 100
```

</div>

clause is replaced by

<div class="highlight">

``` chroma
RETURN AT RECURSION LIMIT $n -- where $n = 1, 2, 3, ...
```

</div>

and observe how the result changes after `$n` iterations.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-after-1-iteration" class="tab-pane"
title="After 1 iteration">

<div class="highlight">

``` chroma
 src_id | dst_id
--------+--------
 A      | B
 ...
```

</div>

</div>

<div id="tab-after-2-iterations" class="tab-pane"
title="After 2 iterations">

<div class="highlight">

``` chroma
 src_id | dst_id
--------+--------
 A      | B
 A      | B
 ...
```

</div>

</div>

<div id="tab-after-3-iterations" class="tab-pane"
title="After 3 iterations">

<div class="highlight">

``` chroma
 src_id | dst_id
--------+--------
 A      | B
 A      | B
 A      | B
 ...
```

</div>

</div>

</div>

</div>

Changing the `UNION` to `UNION ALL` in the `connected` definition caused
a full copy of `transfer` to be added to the current value of
`connected` in each iteration! Consequently, `connected` never stops
growing and the recursive CTE computation never reaches a fixpoint.

### Queries with “update locality”

The examples presented so far have the following “update locality”
property:

<div class="note">

**NOTE:** A change in a source collection will usually cause a *bounded
amount* of changes to the contents of the recursive CTE bindings derived
after each iteration.

</div>

For example:

- Most of the time, inserting or removing `transfers` will not change
  the contents of the `connected` collection. This is true because:
  - An alternative path from `x` to `y` already existed before the
    insertion, or
  - An alternative path from `x` to `y` will exist after the removal.
- Inserting or removing `transfers` will not change most of the contents
  of the `area_balances` collection. This is true because:
  - Areas are organized in a hierarchy with a maximum height `h`.
  - A single transfer contributes directly only to the `area_balances`
    of the areas where the `src_id` and `tgt_id` belong.
  - A single transfer contributes indirectly only to the `area_balances`
    of the areas ancestor areas of the above two areas.
  - Consequently, each `transfer` change will affect at most `2` areas
    in each iteration and at most `2*h` areas in total.

However, note that not all iterative algorithms have this property. For
example:

- In a naive PageRank implementation, the introduction of a link between
  two pages `x` and `y` will change the PageRank of these two pages and
  every page `z` transitively connected to either `x` or `y`. Since most
  graphs exhibit [small-world
  properties](https://en.wikipedia.org/wiki/Small-world_network), this
  might represent most of the existing pages.
- In a [naive k-means clustering
  algorithm](https://en.wikipedia.org/wiki/K-means_clustering),
  inserting or removing a new data point will affect the positions of
  the `k` mean points after each iteration.

Depending on the size and update frequency of your input collections,
expressing algorithms that violate the “update locality” property (such
as the examples above) using recursive CTEs in Materialize might lead to
excessive CPU and memory consumption in the clusters that compute these
recursive queries.

## Related pages

- [Regular CTEs](/docs/self-managed/v25.2/sql/select/#regular-ctes)
- [`SELECT`](/docs/self-managed/v25.2/sql/select)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/select/recursive-ctes.md"
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
