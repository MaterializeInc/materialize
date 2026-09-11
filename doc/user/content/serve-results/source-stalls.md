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
sources stop making progress, or **stall**. A stalled source does not have to
stop your application from reading: in many cases, Materialize keeps serving
queries from the last consistently ingested data.

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
| Read of stalled data inside an explicit transaction | Blocks | Blocks |

Blocked queries **wait**; they do not error. A blocked query completes once
the source resumes and its write frontier passes the query's timestamp, with
no work lost. Note that `statement_timeout` does not interrupt this wait: it
cancels queries that are executing, but does not fire while a query is waiting
for its timestamp to become available. Use client-side timeouts to bound the
wait.

To see why a specific query does or does not serve, use [`EXPLAIN
TIMESTAMP`](/sql/explain-timestamp/): it reports `can respond immediately:
true/false` along with the read and write frontiers of every input.

## Keep serving across sources: maintain the query

The recommended pattern for queries that span sources is to maintain the query
as an [indexed view](/concepts/views/#indexes-on-views) or [materialized
view](/concepts/views/#materialized-views), and have the application read from
that object directly:

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
memory.

## Keep ad hoc queries serving: align frontiers with a maintained object

If your application must issue **ad hoc** queries that reference multiple
collections (for example, generated queries from a BI tool joining a stalled
source to a live one), you can keep those queries servable during a stall by
maintaining *any* object that reads the same set of inputs:

```mzsql
-- A small maintained object whose only purpose is to hold the read
-- frontiers of its inputs together.
CREATE MATERIALIZED VIEW frontier_alignment AS
  SELECT max(id) FROM (
    SELECT id FROM kafka_orders
    UNION ALL
    SELECT id FROM pg_customers
  );
```

Because the maintained object must remain readable at its own (frozen) write
frontier, Materialize holds back the read frontiers of **all** of its inputs.
That keeps the stalled and live collections' frontier intervals overlapping,
so an ad hoc query over any subset of those inputs can still select a valid
timestamp and serve stale results instead of blocking.

For this to work, note:

- **The object must exist before the stall.** Once the live inputs' read
  frontiers have advanced past the stalled collection's write frontier,
  compaction has already discarded the historical data. Creating the aligning
  object after the fact does not help, and the new object itself cannot serve
  until the source resumes.

- **The object must genuinely read every input you want covered.** The
  optimizer removes inputs it can prove are unused (for example, behind
  `WHERE false`). Check `EXPLAIN` on the object's definition to confirm all
  intended inputs appear in the plan.

- **Holding back read frontiers has a cost.** For the duration of the stall,
  Materialize retains historical data for the covered inputs that it would
  otherwise compact away.

- **Reads of stalled data inside explicit transactions are not rescued**
  (see below).

## Don't use transactions

Explicit read transactions that touch stalled data are unable to serve
during a stall. Issue single-statement queries instead.

{{< if-released "v26.29" >}}
## Fail fast instead of blocking

If your application would rather receive an error than wait, or than serve
data past a staleness threshold, use the [bounded
staleness](/serve-results/isolation-level/#bounded-staleness) isolation
level. During a stall:

- Query shapes that serve stale under serializable also serve stale under
  bounded staleness, as long as the stall is younger than the configured
  bound.

- Query shapes that would block under serializable, and any query reading
  stalled data once the stall exceeds the bound, **error immediately** with
  `SQLSTATE 40001` instead of blocking, giving the application a clean signal
  to retry or fall back.

```mzsql
SET TRANSACTION_ISOLATION TO 'bounded staleness 1m';
```
{{< /if-released >}}

## Related pages

- [Isolation levels](/serve-results/isolation-level/)
- [`EXPLAIN TIMESTAMP`](/sql/explain-timestamp/)
- [`BEGIN`](/sql/begin/)
- [`SUBSCRIBE`](/sql/subscribe/)
- [Troubleshooting serving](/serve-results/troubleshooting/)
- [Ingest data: troubleshooting](/ingest-data/troubleshooting/)
