---
title: "Serve results during source stalls"
description: "Which queries Materialize keeps serving while an upstream source is stalled or errored, and how to design for availability during upstream outages."
menu:
  main:
    name: "Serve during source stalls"
    parent: serve-results
    identifier: 'serve-results-source-stalls'
    weight: 16
---

When an upstream system becomes unavailable, for example a Kafka broker outage,
a database restart, or an ingestion cluster with no replicas, the affected
source stops ingesting. Your application does not have to stop reading. In many
cases, Materialize keeps serving results from the last data it ingested.

Whether a query keeps serving depends on the session's [isolation
level](/serve-results/isolation-level/) and on which inputs the query reads.
This guide covers how to design your queries so that they keep serving through
an upstream outage, and how to fail fast when they can't.

## Understand stalls and errors

### Stalls and errors

A source that stops ingesting is in one of two situations. Use
[`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses) to
check its status, and see [Understand the lifecycle of a
source](/ingest-data/lifecycle-of-a-source/) for what each status means.

| Situation | Status | Examples | Recovery |
|-----------|--------|----------|----------|
| **Stall** | `paused`, or `stalled` with a retryable error | Upstream unreachable, no replica running the source | Resumes on its own once the cause clears |
| **Definite error** | `stalled`, with an error that says the source or table must be dropped and recreated | Dropped publication, incompatible upstream schema change | [Recreate the affected objects](/ingest-data/lifecycle-of-a-source/#which-stalls-clear-on-their-own) |

A source can also stop ingesting without changing its status, for example when
the upstream connection hangs. Check whether its write frontier still advances
in [`mz_frontiers`](/sql/system-catalog/mz_internal/#mz_frontiers).

### What happens to timestamps

Materialize serves every query at a single timestamp. Each collection, whether
a source table, index, or materialized view, can answer queries at timestamps
between its **read frontier** and its **write frontier**. While a source
ingests, its write frontier keeps advancing.

- **During a stall**, the write frontier freezes at the last timestamp the
  source ingested. The data up to that timestamp stays intact and readable.
- **After a definite error**, the error is written into the collection, and the
  collection closes. Every read of it at or after the error's timestamp
  returns the error, at every isolation level. None of the patterns in this
  guide apply. Recreate the affected objects instead.

The rest of this guide is about stalls.

### How a stall affects queries

A query can only be served at a timestamp that every one of its inputs can
answer. When source A stalls, its write frontier freezes, while the read
frontier of a healthy source B keeps advancing. Once B's read frontier moves
past A's frozen write frontier, no timestamp works for both, and a query that
reads A and B together blocks until A resumes.

The isolation level adds its own constraint. Under **strict serializable** (the
default), a query must read at the current time, so any query that reads stalled
data blocks. Under **serializable**, a query can read at an earlier timestamp,
so a query whose inputs all stalled together serves results as of the stall.

Blocked queries wait. They do not error, and they complete once the source
resumes.

## Behavior by query shape and isolation level

| Query shape | Serializable | Strict serializable | Bounded staleness |
|-------------|--------------|---------------------|-------------------|
| Reads only healthy inputs | Serves | Serves | Serves |
| Reads only the stalled source's tables, or indexes and views over them | Serves stale | Blocks | Serves stale, then errors once the stall exceeds the bound |
| Reads an indexed or materialized view created before the stall that joins the stalled and healthy sources | Serves stale | Blocks | Serves stale, then errors once the stall exceeds the bound |
| Ad hoc query that joins a stalled input with a healthy one, including user-writable tables | Blocks | Blocks | Errors |
| `SUBSCRIBE` to the stalled source's tables | Emits the snapshot as of the stall, then waits | Blocks before the snapshot | Errors once the stall exceeds the bound |
| Explicit transaction that reads stalled data | Blocks | Blocks | Errors |

{{< note >}}
An ad hoc query that joins stalled and healthy inputs can serve under
serializable if an existing index or materialized view also depends on the same
healthy inputs, because it holds back their read frontier. Don't rely on this.
Maintain the query instead, as described below.
{{< /note >}}

To see why a specific query does or doesn't serve, run [`EXPLAIN
TIMESTAMP`](/sql/explain-timestamp/). It reports the read and write frontier of
each input and whether the query `can respond immediately`.

## Serve across sources

To keep serving a query that combines several sources, maintain it as an
[indexed view](/fundamentals/concepts/views/#indexes-on-views) or a
[materialized view](/fundamentals/concepts/views/#materialized-views), and query
that object under the serializable isolation level:

```mzsql
CREATE VIEW order_details AS
  SELECT o.id, o.total, c.name, c.region
  FROM orders o
  JOIN customers c ON o.customer_id = c.id;

CREATE INDEX order_details_idx ON order_details (id);
```

```mzsql
SET TRANSACTION_ISOLATION TO 'serializable';

SELECT * FROM order_details WHERE id = 42;
```

The view's write frontier follows its slowest input. When one source stalls,
the whole view freezes at a consistent point, and reading it is a query against
a single collection. It keeps serving results as of the stall for as long as
the stall lasts.

Create the view before the outage. A view that joins a stalled and a healthy
source and is created during the stall blocks, like the equivalent ad hoc query.

## Fail fast instead of blocking

If your application would rather get an error than wait, or than serve results
beyond a staleness threshold, use the [bounded
staleness](/serve-results/isolation-level/#bounded-staleness) isolation level:

```mzsql
SET TRANSACTION_ISOLATION TO 'bounded staleness 1m';
```

Bounded staleness never blocks. Queries that serve stale under serializable
keep serving until the stall is older than the bound. After that, they fail
with a retryable serialization failure (`SQLSTATE 40001`):

```nofmt
ERROR:  cannot serve query under bounded staleness 1m
DETAIL:  Freshest available timestamp is 104029ms older than the bound.
```

Queries whose inputs no longer share a valid timestamp, such as an ad hoc join
of a stalled and a healthy input, fail with a different error (`SQLSTATE
22000`). Retrying doesn't help until the source resumes:

```nofmt
ERROR:  could not find a valid timestamp for the query
```

Handle both errors in your application, for example by showing a "data
unavailable" state.

## Best practices

### Avoid explicit transactions

Inside an explicit transaction (`BEGIN ... COMMIT`), Materialize picks one
timestamp that is valid for every object in the schemas the first statement
references, plus the system catalog. During a stall, a transaction that reads
stalled data blocks even under serializable, because the other objects in its
[time domain](/sql/begin/#same-timedomain-error) keep advancing.

Issue single-statement queries instead. Some SQL clients and BI tools open
transactions implicitly, so check your client's autocommit setting.

## Related pages

- [Understand the lifecycle of a source](/ingest-data/lifecycle-of-a-source/)
- [Isolation levels](/serve-results/isolation-level/)
- [`EXPLAIN TIMESTAMP`](/sql/explain-timestamp/)
- [Troubleshoot unresponsive queries](/serve-results/troubleshooting/unresponsive-queries/)
