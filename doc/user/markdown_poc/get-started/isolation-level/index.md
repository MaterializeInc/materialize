<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [What is Materialize?](/docs/get-started/)

</div>

# Consistency guarantees

The SQL standard defines four levels of transaction isolation. In order
of least strict to most strict they are:

- Read Uncommitted
- Read Committed
- Repeatable Read
- Serializable

In Materialize, you can request any of these isolation levels, but they
all behave the same as the Serializable isolation level. In addition to
the four levels defined in the SQL Standard, Materialize also defines a
[Strict Serializable](#strict-serializable) isolation level.

Isolation level is a configuration parameter that can be set by the user
on a session-by-session basis. The default isolation level is [Strict
Serializable](#strict-serializable).

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1NjUiIGhlaWdodD0iODEiPgogICA8cG9seWdvbiBwb2ludHM9IjkgMTcgMSAxMyAxIDIxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iMTcgMTcgOSAxMyA5IDIxIj48L3BvbHlnb24+CiAgIDxyZWN0IHg9IjMxIiB5PSIzIiB3aWR0aD0iNDYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMjkiIHk9IjEiIHdpZHRoPSI0NiIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzkiIHk9IjIxIj5TRVQ8L3RleHQ+CiAgIDxyZWN0IHg9Ijk3IiB5PSIzIiB3aWR0aD0iMjA4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9Ijk1IiB5PSIxIiB3aWR0aD0iMjA4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMDUiIHk9IjIxIj5UUkFOU0FDVElPTl9JU09MQVRJT048L3RleHQ+CiAgIDxyZWN0IHg9IjM0NSIgeT0iMyIgd2lkdGg9IjQwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjM0MyIgeT0iMSIgd2lkdGg9IjQwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzNTMiIHk9IjIxIj5UTzwvdGV4dD4KICAgPHJlY3QgeD0iMzQ1IiB5PSI0NyIgd2lkdGg9IjI4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjM0MyIgeT0iNDUiIHdpZHRoPSIyOCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzUzIiB5PSI2NSI+PTwvdGV4dD4KICAgPHJlY3QgeD0iNDI1IiB5PSIzIiB3aWR0aD0iMTEyIiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSI0MjMiIHk9IjEiIHdpZHRoPSIxMTIiIGhlaWdodD0iMzIiIGNsYXNzPSJub250ZXJtaW5hbCIgLz4KICAgPHRleHQgY2xhc3M9Im5vbnRlcm1pbmFsIiB4PSI0MzMiIHk9IjIxIj5pc29sYXRpb25fbGV2ZWw8L3RleHQ+CiAgIDxwYXRoIGNsYXNzPSJsaW5lIiBkPSJtMTcgMTcgaDIgbTAgMCBoMTAgbTQ2IDAgaDEwIG0wIDAgaDEwIG0yMDggMCBoMTAgbTIwIDAgaDEwIG00MCAwIGgxMCBtLTgwIDAgaDIwIG02MCAwIGgyMCBtLTEwMCAwIHExMCAwIDEwIDEwIG04MCAwIHEwIC0xMCAxMCAtMTAgbS05MCAxMCB2MjQgbTgwIDAgdi0yNCBtLTgwIDI0IHEwIDEwIDEwIDEwIG02MCAwIHExMCAwIDEwIC0xMCBtLTcwIDEwIGgxMCBtMjggMCBoMTAgbTAgMCBoMTIgbTIwIC00NCBoMTAgbTExMiAwIGgxMCBtMyAwIGgtMyIgLz4KICAgPHBvbHlnb24gcG9pbnRzPSI1NTUgMTcgNTYzIDEzIDU2MyAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjU1NSAxNyA1NDcgMTMgNTQ3IDIxIj48L3BvbHlnb24+Cjwvc3ZnPg==)

</div>

| Valid Isolation Levels                      |
|---------------------------------------------|
| [Read Uncommitted](#serializable)           |
| [Read Committed](#serializable)             |
| [Repeatable Read](#serializable)            |
| [Serializable](#serializable)               |
| [Strict Serializable](#strict-serializable) |

## Examples

<div class="highlight">

``` chroma
SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
```

</div>

<div class="highlight">

``` chroma
SET TRANSACTION_ISOLATION TO 'STRICT SERIALIZABLE';
```

</div>

## Serializable

The SQL standard defines the Serializable isolation level as preventing
the following three phenomenons:

- **P1 (”Dirty Read”):**

  > “SQL-transaction T1 modifies a row. SQL-transaction T2 then reads
  > that row before T1 performs a COMMIT. If T1 then performs a
  > ROLLBACK, T2 will have read a row that was never committed and that
  > may thus be considered to have never existed.” (ISO/IEC
  > 9075-2:1999 (E) 4.32 SQL-transactions)

- **P2 (”Non-repeatable read”):**

  > “SQL-transaction T1 reads a row. SQL-transaction T2 then modifies or
  > deletes that row and performs a COMMIT. If T1 then attempts to
  > reread the row, it may receive the modified value or discover that
  > the row has been deleted.” (ISO/IEC 9075-2:1999 (E) 4.32
  > SQL-transactions)

- **P3 (”Phantom”):**

  > “SQL-transaction T1 reads the set of rows N that satisfy some
  > \<search condition\>. SQL-transaction T2 then executes
  > SQL-statements that generate one or more rows that satisfy the
  > \<search condition\> used by SQL-transaction T1. If SQL-transaction
  > T1 then repeats the initial read with the same \<search condition\>,
  > it obtains a different collection of rows.” (ISO/IEC 9075-2:1999 (E)
  > 4.32 SQL-transactions)

Furthermore, Serializable also guarantees that the result of executing a
group of concurrent SQL-transactions produces the same effect as some
serial execution of those same transactions. A serial execution is one
where each SQL-transaction executes to completion before the next one
begins. There is no guarantee that this serial ordering is consistent
with the real time ordering of the transactions, in other words
transactions are not
[linearizable](https://jepsen.io/consistency/models/linearizable) under
the Serializable isolation level. For example if SQL-transaction T1
happens before SQL-transaction T2 in real time, then the result may be
equivalent to a serial order where T2 was executed first.

Non-linearizable orderings are more likely to surface when querying from
indexes and materialized views with large propagation delays. For
example, if SQL-transaction T1 happens before SQL-transaction T2 in real
time, T1 queries table t, and T2 queries materialized view mv where mv
is an expensive materialized view including t, then T2 may not see all
the rows that were seen by T1 if they are executed close enough together
in real time.

If a consistent snapshot is not available across all objects in a query
and all other objects in the current transaction, then the query will be
blocked until one becomes available. On the other hand, if a consistent
snapshot is available, then the query will be executed immediately. A
consistent snapshot is guaranteed to be available for transactions that
are known ahead of time to involve a single object (which includes
transactions against a single materialized view that was created using
multiple objects). Such transactions will therefore never block, and
always be executed immediately. A transaction can only be known ahead of
time to involve a single object when using auto-commit (i.e. omitting
`BEGIN` and `COMMIT`) or when using `SUBSCRIBE`. When using explicit
transactions (i.e. starting a transaction with `BEGIN`) with `SELECT`,
then it is assumed that all objects that share a schema with any object
mentioned in the first query of the transaction may be used later in the
transaction. Therefore, we use a consistent snapshot that is available
across all such objects.

## Strict serializable

The Strict Serializable isolation level provides all the same guarantees
as Serializable, with the addition that transactions are linearizable.
That means that if SQL-transaction T1 happens before SQL-transaction T2
in real time, then the execution is equivalent to a serial execution
where T1 comes before T2.

For example, if SQL-transaction T1 happens before SQL-transaction T2 in
real time, T1 queries table t, and T2 queries materialized view mv where
mv is an expensive materialized view including t, then T2 is guaranteed
to see all of the rows that were seen by T1.

It’s important to note that the linearizable guarantee only applies to
transactions (including single statement SQL queries which are
implicitly single statement transactions), not to data written while
ingesting from upstream sources. So if some piece of data has been fully
ingested from an upstream source, then it is not guaranteed to appear in
the next read transaction. See [real-time recency](#real-time-recency)
for more details. If some piece of data has been fully ingested from an
upstream source AND is included in the results of some read transaction
THEN all subsequent read transactions are guaranteed to see that piece
of data.

### Real-time recency

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

Materialize offers a form of “end-to-end linearizability” known as
real-time recency. When using real-time recency, all client-issued
`SELECT` statements include at least all data visible to Materialize in
any external source (i.e., sources created with `CREATE SOURCE` that use
`CONNECTION`s, such as Kafka, MySQL, and PostgreSQL sources) after
Materialize receives the query. This is what we mean by
linearizable––the results are guaranteed to contain all visible data
according to physical time.

For example, real-time recency ensures that if you have just performed
an `INSERT` into a PostgreSQL database that Materialize ingests as a
source, all of your real-time recency queries will include the
just-written data in their results.

Note that real-time recency only guarantees that the results will
contain *at least* the data visible to Materialize when we receive the
query. We cannot guarantee that the results will contain *only* the data
visible when Materialize receives the query. For instance, the rate at
which Materialize ingests data might include additional data made
visible after the timestamp we determined to be “real-time recent.”
Another example is that, due to network latency, the timestamp from the
external system that we determine to be “real-time recent” might be
later (i.e. include more data) than you expected.

Because Materialize waits until it ingests the data from the external
system, real-time recency queries can have additional latency. This
latency is introduced by both the time it takes us to ingest and commit
data from the source and the time spent communicating with it to
determine what data it has made available to us (e.g., querying
PostgreSQL for the replication slot’s LSN).

**Details**

- Real-time recency is only available with sessions running at the
  [strict serializable isolation level](#strict-serializable).
- Enable this feature per session using `SET real_time_recency = true`.
- Control the timeout for connecting to the external source to determine
  the timestamp with the `real_time_recency_timeout` session variable.
- Real-time recency queries only guarantee visibility of data from
  external systems (e.g., sources like Kafka, MySQL, and PostgreSQL).
  Real-time recency queries do not offer any form of guarantee when
  querying Materialize-local objects, such as
  [`LOAD GENERATOR`sources](/docs/sql/create-source/load-generator/)
  sources or system tables.
- Each real-time recency query connects to each external source
  transitively referenced in the query. The more external sources that
  are referenced, the greater the likelihood of latency caused by the
  network or ingestion rates.
- Materialize doesn’t currently offer a mechanism to provide a “lower
  bound” on the data we consider to be “real-time recent” in an external
  source. Real-time recency queries return at least all data visible to
  Materialize when our client connection communicates with the external
  system.

## Choosing the right isolation level

It may not be immediately clear which isolation level is right for you.
Materialize recommends to start with the default Strict Serializable
isolation level. If you are noticing performance issues on reads, and
your application does not need linearizable transactions, then you
should downgrade to the Serializable isolation level.

Strict Serializable provides stronger consistency guarantees but may
have slower reads than Serializable. This is because Strict Serializable
may need to wait for writes to propagate through materialized views and
indexes, while Serializable does not. For details about this behavior,
consult the documentation on [logical timestamp
selection](/docs/sql/functions/now_and_mz_now#logical-timestamp-selection).

In Serializable mode, a single auto-committed `SELECT` statement or a
`SUBSCRIBE` statement that references a single object (which includes
transactions against a single materialized view that was created using
multiple objects) will be executed immediately. Otherwise, the statement
may block until a consistent snapshot is available. If you know you will
be executing single `SELECT` statement transactions in Serializable
mode, then it is strongly recommended to use auto-commit instead of
explicit transactions.

## Learn more

Check out:

- [PostgreSQL
  documentation](https://www.postgresql.org/docs/current/transaction-iso.html)
  for more information on isolation levels.
- [Jepsen Consistency Models
  documentation](https://jepsen.io/consistency) for more information on
  consistency models.

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/get-started/isolation-level.md"
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
