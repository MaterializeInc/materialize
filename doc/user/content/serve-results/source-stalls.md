---
title: "Serving during source stalls"
description: "Which queries Materialize can keep serving while an upstream source is stalled, and how to design for availability during upstream outages."
menu:
  main:
    name: "Serve during source stalls"
    parent: serve-results
    identifier: 'serve-results-source-stalls'
    weight: 16
---

When an upstream system becomes unavailable (a Kafka broker outage, a paused
replication slot, an ingestion cluster with no running replicas), the affected
source's write frontier stops advancing, or **stalls**. This is one of three
things "stalled" can mean, and the only one this page is about:

* **Write frontier stalled (this page).** The last consistently ingested data
  is intact and nothing new is written. Reads below the frozen write frontier
  keep serving.
* **[`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
  reports `stalled`.** A transient status carrying an error string, for
  example a connectivity hiccup. It does not poison the collection; ingestion
  resumes on its own once the underlying issue clears.
* **A durable ingestion error**, such as a decoding or replication error,
  written into the collection's own error stream. From that timestamp
  forward, every read of the collection returns the error, at every isolation
  level, and none of the patterns on this page recover from it.

A stalled write frontier does not have to stop your application from reading:
in many cases, Materialize keeps serving queries from the last consistently
ingested data.

Whether a given query keeps serving depends on two things: the
[isolation level](/serve-results/isolation-level/) of the session, and the
**shape of the query**. This page describes the behavior and the patterns that
keep serving available through an upstream outage.

## How a stall affects queries

Materialize serves every query at a single logical timestamp. This means that
each input must have overlapping timestamps. Imagine you have two sources, A
and B. When source A stalls, its timestamp is frozen at the point of the
stall, but source B will continue advancing.

{{< note >}}
Every collection has a **read frontier** (the earliest timestamp it can still
answer correctly, advanced by compaction) and a **write frontier** (all data
before this point has been fully processed). A query's timestamp must fall
between the [read and write frontiers](/sql/explain-timestamp/#details) of
every input it reads.
{{< /note >}}

Queries confined to the stalled data can still be served at the frozen
timestamp. The results are stale, but consistent. Queries that mix stalled and
still-advancing inputs may find that no common timestamp exists, in which case
the query **blocks** until one does (typically, when the source resumes).

## Behavior by query shape and isolation level

Under the **serializable** isolation level, queries whose inputs stalled
together keep serving; queries that mix stalled and live inputs block. Under
**strict serializable**, any query that reads stalled data blocks, because the
query timestamp must also reflect real-time recent writes. Queries that read
no stalled data at all are unaffected under either isolation level.

| Query shape | Serializable | Strict serializable |
| ----------- | ------------ | ------------------- |
| Point lookup on an index over stalled data | Serves stale | Blocks |
| Direct read of a table created from the stalled source (`CREATE TABLE ... FROM SOURCE`) | Serves stale | Blocks |
| Aggregation or full scan over one stalled collection | Serves stale | Blocks |
| Query whose inputs all stalled together (e.g., views over the same source) | Serves stale | Blocks |
| `SUBSCRIBE` to a single stalled collection | Serves stale | Blocks |
| Join between a stalled collection and a healthy one | Blocks | Blocks |
| Query mixing a stalled collection and a user-writable table | Blocks | Blocks |
| Explicit transaction, if a stalled collection shares a schema with anything the transaction reads | Blocks | Blocks |

Blocked queries **wait**; they do not error, and a client-side cancellation
(a driver-level statement timeout, or `Ctrl-C`) still reaches them right
away. A blocked query otherwise completes once the source resumes and its
write frontier passes the query's timestamp, with no work lost.

Note that server-side `statement_timeout` does not bound this wait: it is
existing, general behavior scoped to the read portion of write statements
(`INSERT ... SELECT`, and the `WHERE` of `UPDATE`/`DELETE`), not to blocked
reads, so it never fires while a query is waiting for its timestamp to
become available. Use a client-side timeout to bound the wait on a plain
`SELECT`.

To see why a specific query does or does not serve, use [`EXPLAIN
TIMESTAMP`](/sql/explain-timestamp/): it reports `can respond immediately:
true/false` along with the read and write frontiers of every input. To check
frontiers across every object at once,
[`mz_internal.mz_frontiers`](/sql/system-catalog/mz_internal/#mz_frontiers)
reports the read and write frontier of every source, sink, table, index, and
materialized view, and
[`mz_internal.mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
reports whether a source is merely `stalled` or has hit a durable error.

## Keep serving across sources: maintain the query

The recommended pattern for queries that span sources is to maintain the query
as an [indexed view](/fundamentals/concepts/views/#indexes-on-views) or
[materialized view](/fundamentals/concepts/views/#materialized-views), and
have the application read from that object directly:

```mzsql
CREATE VIEW order_enrichment AS
  SELECT o.id, o.total, c.name, c.region
  FROM kafka_orders o
  JOIN pg_customers c ON o.customer_id = c.id;

CREATE INDEX order_enrichment_idx ON order_enrichment (id);
```

The maintained object's write frontier follows its *slowest* input, so when
one of the sources stalls, the object as a whole freezes consistently. Reading
it is then a single-collection query, so it keeps serving stale results for as
long as the stall lasts. This is also the recommended pattern for query
latency in general, since point lookups on the index are served directly from
the index.

## Ad hoc queries across a stalled and a live source

An ad hoc query that joins a stalled collection to one still advancing (for
example, a generated query from a BI tool) has no supported way to keep
serving: Materialize can only wait for the two collections' timestamp ranges
to overlap again, or fail.

It is possible to hand-roll an overlap by maintaining an unrelated object
that reads the same inputs, since a maintained object holds back its inputs'
read frontiers for as long as it stays readable. This is not a supported
pattern: for a materialized view, the held-back window is exactly one step
behind the object's own write frontier, not a window you control, and the
optimizer can prune an input it proves unused, silently narrowing what the
trick actually covers. Treat it as a last resort, not a technique to build on.

If your application issues this kind of ad hoc query, convert it to the
maintained-query pattern above instead of trying to align frontiers after
the fact.

## Avoid explicit transactions during a stall

Materialize picks one timestamp for an entire transaction, valid across
every object in every schema referenced by its first statement, not just the
objects the transaction actually reads. If a stalled collection shares a
schema with a healthy table you query inside `BEGIN`, the transaction blocks
even though it never reads the stalled collection. Issue single-statement
queries instead of wrapping reads in an explicit transaction during a stall.

{{< if-released "v26.29" >}}
## Fail fast instead of blocking

If your application would rather receive an error than wait, or than serve
data past a staleness threshold, use the [bounded
staleness](/serve-results/isolation-level/#bounded-staleness) isolation
level. During a stall:

- Query shapes that serve stale under serializable also serve stale under
  bounded staleness, as long as the stall is younger than the configured
  bound.

- A query that would block under serializable instead errors once the data
  available is too stale for your bound. When a valid, if stale, timestamp
  exists, Materialize raises a serialization failure (`SQLSTATE 40001`) so
  your application gets a clean, retryable signal. When the inputs' timestamp
  ranges never overlap at all, it raises a different, non-retryable error
  instead. Design your fallback to handle both.

```mzsql
SET TRANSACTION_ISOLATION TO 'bounded staleness 1m';
```
{{< /if-released >}}

## Related pages

- [Isolation levels](/serve-results/isolation-level/)
- [`EXPLAIN TIMESTAMP`](/sql/explain-timestamp/)
- [`mz_internal.mz_frontiers`](/sql/system-catalog/mz_internal/#mz_frontiers)
- [`mz_internal.mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
- [`BEGIN`](/sql/begin/)
- [`SUBSCRIBE`](/sql/subscribe/)
- [Troubleshooting serving](/serve-results/troubleshooting/)
- [Ingest data: troubleshooting](/ingest-data/troubleshooting/)
