---
title: "PostgreSQL"
description: "How to export results from Materialize to PostgreSQL using the Materialize sink SDK."
menu:
  main:
    parent: build-your-own-sink
    name: "PostgreSQL"
    weight: 10
---

{{< private-preview enabled-by-default="true" />}}

This guide shows how to keep a PostgreSQL table in sync with a Materialize
materialized view, table, or source, using the [sink
SDK](https://github.com/MaterializeIncLabs/mz-sink-sdk). The SDK subscribes to a
query and applies each change to PostgreSQL as an ordinary `INSERT`, `UPDATE`,
or `DELETE`, so your application reads a plain relational table.

Each batch of changes and the sink's checkpoint commit in the same PostgreSQL
transaction. A sink that crashes and restarts resumes from its last checkpoint
without duplicating or losing rows.

## Before you begin

- Python 3.11 or later on the host that runs the sink.

- Materialize SQL credentials with `SELECT` on the object you want to sink.

- PostgreSQL 13 or later, reachable from that host, and a role that can create
  tables in the destination schema. The sink creates the destination tables and
  its checkpoint table (`_mz_sink_meta`) itself:

  ```sql
  GRANT CONNECT ON DATABASE app TO sink;
  GRANT USAGE, CREATE ON SCHEMA public TO sink;
  ```

{{< note >}}
`CREATE` on the schema is required on every run, not just the first: the sink
reissues `CREATE TABLE IF NOT EXISTS` inside each transaction. On PostgreSQL 15
and later, `public` no longer grants `CREATE` to `PUBLIC`, so the grant above is
mandatory.

The sink must also own its checkpoint table, so two roles cannot share one. To
run a second sink under a different role in the same schema, give it its own
table with `PostgresSink(..., metadata_table="_mz_sink_meta_reporting")`.
{{< /note >}}

## Step 1. Retain enough history

A restarting sink resumes its subscription `AS OF` the last checkpoint. If
Materialize has already compacted past that timestamp, the subscription fails
with `could not find a valid timestamp for the query`, and the sink has to be
rebootstrapped from a fresh snapshot.

Set a history retention window longer than your worst-case sink downtime,
including deploys and incident response:

```mzsql
ALTER MATERIALIZED VIEW winning_bids SET (RETAIN HISTORY FOR '1hr');
```

For the semantics of history retention, see [Durable
subscriptions](/serve-results/durable-subscriptions/).

## Step 2. Install the SDK

```sh
pip install "mz-sink-sdk @ git+https://github.com/MaterializeIncLabs/mz-sink-sdk"
```

## Step 3. Write the sink

The sink is an ordinary Python program. Save the following as `sink.py`,
replacing the connection strings and the query:

```python
from mz_sink_sdk import MaterializeSubscription, SinkRunner
from mz_sink_sdk.postgres import PostgresSink

QUERY = "SELECT auction_id, bid_id, amount FROM winning_bids"

source = MaterializeSubscription(
    "postgresql://<user>:<password>@<host>:6875/materialize?sslmode=require",
    QUERY,
)
sink = PostgresSink(
    "postgresql://<user>:<password>@<host>/<database>",
    table="public.winning_bids",
    primary_key="auction_id",
)

SinkRunner(source, sink, sink_id="winning-bids-to-pg", query=QUERY).run_forever()
```

`primary_key` declares a uniqueness contract. The destination table gets a real
PostgreSQL `PRIMARY KEY`, and the sink fails the transaction rather than
overwriting a row it did not expect. Omit `primary_key` only if the query has no
key; see [Tables without a primary key](#tables-without-a-primary-key).

`sink_id` is the identity of the deployment. Two processes that share a
`sink_id` are the same logical sink: the one that starts later fences off the
earlier one, whose next commit fails instead of corrupting state. Give each
pipeline its own `sink_id`.

## Step 4. Run the sink

```sh
python sink.py
```

The sink creates `public.winning_bids` if it does not exist, writes the initial
snapshot, then commits each subsequent batch of changes about once a second.
Query the destination from PostgreSQL to verify:

```sql
SELECT * FROM public.winning_bids ORDER BY auction_id;
```

```nofmt
 auction_id | bid_id | amount
------------+--------+--------
          1 |     11 |  11.75
          3 |     30 |  30.25
          4 |     40 |  40.00
```

Column types come from the query's result types: `bigint` to `bigint`, `text[]`
to `text[]`, `uuid` to `uuid`, `jsonb` to `jsonb`. Materialize types with no
faithful PostgreSQL equivalent, such as `list`, `map`, and `record`, become
`jsonb`. A `numeric` column is created one digit wider than its Materialize
declaration, so `numeric(38,2)` arrives as `numeric(39,2)`.

## Sink into several tables

One subscription can feed several tables in a single transaction. A `Route`
picks the rows and columns each table receives:

```python
from mz_sink_sdk import MaterializeSubscription, Route, SinkRunner
from mz_sink_sdk.postgres import PostgresSink

QUERY = "SELECT kind, id, name, amount FROM live_objects"

source = MaterializeSubscription(MZ_DSN, QUERY)
sink = PostgresSink(PG_DSN)
orders = sink.table("public.orders", id="orders", primary_key="id")
customers = sink.table("public.customers", id="customers", primary_key="id")

SinkRunner(
    source,
    sink,
    sink_id="live-objects-to-pg",
    query=QUERY,
    routes=[
        Route(to=orders, columns=("id", "name", "amount"),
              where=lambda r: r["kind"] == "order", version="orders-v1"),
        Route(to=customers, columns=("id", "name"),
              where=lambda r: r["kind"] == "customer", version="customers-v1"),
    ],
).run_forever()
```

Both tables and the checkpoint commit together, so the two tables are never
observed at different points in the stream.

A `where` predicate runs on both additions and retractions, so it must be a pure
function of the row. The SDK cannot fingerprint a Python function, so bump
`version=` whenever you change what a predicate means. That string is part of
the sink's identity, and changing it forces an explicit rebootstrap instead of a
silently inconsistent table.

## Tables without a primary key

A table created without `primary_key=` keeps a lossless multiset
representation: your columns plus `__mz_row_hash` (the table's primary key),
`__mz_row_json`, and `__mz_multiplicity`. A row that reaches multiplicity zero
is deleted. Use this shape when the query has no key, or when duplicate rows
are meaningful and must be preserved exactly.

## Operational guidelines

- **Commit frequency.** The sink commits on every progress message from
  Materialize, about once a second. Pass
  `commit_policy=CommitInterval(<seconds>)` to `SinkRunner` for fewer, larger
  commits. The interval affects commit granularity and replay after a crash,
  never correctness.

- **Changing a running pipeline.** The checkpoint stores fingerprints of the
  query, its result schema, and the destination layout. Changing any of them
  fails startup with `QueryIdentityMismatch`, `SchemaMismatch`, or
  `DestinationPlanMismatch`, because a changed contract over an existing
  checkpoint has no safe interpretation. To make such a change, rebootstrap
  under a new `sink_id`, or drop the old checkpoint and destination state.
  Adding a column to a `SELECT *`, or changing a column's type, changes the
  schema fingerprint too.

  The destination's `id` is part of that layout. Moving an existing sink from
  the `table=` form in [Step 3](#step-3-write-the-sink) to an explicit
  `routes=` list changes the `id` from `default` to the table name, which is a
  `DestinationPlanMismatch`. Pass `id="default"` to keep the same identity.

- **Static relations.** The sink commits at progress boundaries, and a relation
  with no upstream dependencies, such as a materialized view over constants,
  never advances past its snapshot. Such a sink writes nothing and does not
  exit, which looks like a hang. Sink from a relation that tracks a table or a
  source.

- **Writing to the destination table.** The sink expects to be the only writer.
  If another process changes a row the sink is about to update, the transaction
  fails with `TargetStateMismatch`.

- **Errors.** Error classes other than `PrimaryKeyViolation` and
  `TargetStateMismatch` are importable from `mz_sink_sdk.sink`, not from the
  top-level package:

  ```python
  from mz_sink_sdk.sink import FenceLost, QueryIdentityMismatch, SinkError
  ```

  `run_forever()` reopens from the last durable checkpoint after any failure,
  so a fail-closed error retries in a loop rather than exiting the process.
  `FenceLost` means another process took over the `sink_id` and resolves
  itself. The mismatch errors above and `PrimaryKeyViolation` are configuration
  problems: watch the sink's logs, because they recur until you fix them.

## Related pages

- [`SUBSCRIBE`](/sql/subscribe/)
- [Durable subscriptions](/serve-results/durable-subscriptions/)
- [Sink results](/export-data/)
