---
title: "Understand the lifecycle of a source"
description: "The states a source and its tables move through, how to monitor each one, and the error states you might encounter."
menu:
  main:
    parent: ingest-data
    name: "Understand the lifecycle of a source"
    identifier: ingest-source-lifecycle
    weight: 91
---

A source and the tables created from it move through a sequence of states
before they continuously serve up-to-date data. Knowing which state an object
is in tells you whether it is making progress or is stuck.

This page covers sources created with the [`CREATE SOURCE`](/sql/create-source/)
and [`CREATE TABLE ... FROM SOURCE`](/sql/create-table/) syntax. All source
types report through the same
[`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
view and move through the same states, so the queries below apply whether you
ingest from Kafka, PostgreSQL, MySQL, SQL Server, or a load generator.

The source and each of its tables report their own status. The source tracks
the connection to the upstream system, while each table tracks the ingestion of
one upstream relation.

## States

| State      | Meaning                                                                      |
|------------|------------------------------------------------------------------------------|
| `created`  | The object exists but is not ingesting yet.                                   |
| `starting` | The object is connecting to the upstream system and initializing.             |
| `running`  | The object is ingesting: first the initial snapshot, then upstream changes.   |
| `paused`   | No cluster replica is running the object. It makes no progress until one is.  |
| `stalled`  | The object hit an error. The `error` column reports the cause.                |
| `dropped`  | The object was dropped. Terminal.                                             |

The examples below use a PostgreSQL source and a Kafka source on a dedicated
cluster. Substitute your own object names.

```mzsql
CREATE CLUSTER ingest_demo SIZE '25cc';

CREATE SOURCE pg_src IN CLUSTER ingest_demo
  FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_orders');

CREATE SOURCE kafka_src IN CLUSTER ingest_demo
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'clicks');
```

## Created

A source that has no tables yet reports `created`. At this point Materialize
has recorded the source and its upstream connection, but it is not ingesting
anything and consumes no cluster resources:

```mzsql
SELECT name, type, status
FROM mz_internal.mz_source_statuses
ORDER BY name;
```

```nofmt
   name    |   type   | status
-----------+----------+---------
 kafka_src | kafka    | created
 pg_src    | postgres | created
(2 rows)
```

A source stays in `created` for as long as it has no tables. Ingestion begins
only when you attach a table with `CREATE TABLE ... FROM SOURCE`:

```mzsql
CREATE TABLE orders FROM SOURCE pg_src (REFERENCE orders);
CREATE TABLE clicks FROM SOURCE kafka_src (REFERENCE clicks) FORMAT JSON;
```

## Starting and running

Once a table is attached, the source and the table connect to the upstream
system (`starting`), then begin ingesting (`running`):

```mzsql
SELECT o.name, s.status, s.error
FROM mz_internal.mz_source_statuses s
JOIN mz_objects o ON o.id = s.id
ORDER BY o.name;
```

```nofmt
   name    | status  | error
-----------+---------+-------
 clicks    | running |
 kafka_src | running |
 orders    | running |
 pg_src    | running |
(4 rows)
```

`starting` is usually brief. If an object stays in `starting` for more than a
few minutes, see [Troubleshooting: Why isn't my source ingesting
data?](/ingest-data/troubleshooting/#why-isnt-my-source-ingesting-data).

## Snapshotting

`running` covers both the initial [snapshot](/fundamentals/concepts/snapshotting/)
and steady-state ingestion, so the status alone does not tell you whether the
initial snapshot is still in progress. Use
[`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics)
instead:

```mzsql
SELECT o.name, s.snapshot_records_known, s.snapshot_records_staged,
       s.snapshot_committed
FROM mz_internal.mz_source_statistics s
JOIN mz_objects o ON o.id = s.id
WHERE o.name IN ('orders', 'clicks')
ORDER BY o.name;
```

```nofmt
  name  | snapshot_records_known | snapshot_records_staged | snapshot_committed
--------+------------------------+-------------------------+--------------------
 clicks | 200                    | 200                     | t
 orders | 501                    | 501                     | t
(2 rows)
```

While the snapshot is in progress, `snapshot_records_staged` climbs toward
`snapshot_records_known` and `snapshot_committed` is `f`. A table cannot serve
queries until its snapshot completes: queries against it block until then.

{{< note >}}
These statistics are collected periodically, so for a window after an object
starts running they can read `NULL` and `f` even though the data is already
ingested and queryable. They also reset when a replica restarts. Track how they
evolve rather than reading them at a single moment.
{{< /note >}}

Snapshot duration and upstream impact vary by source type. CDC sources
(PostgreSQL, MySQL, SQL Server) require the upstream system to retain its
change log until the snapshot completes, so a long snapshot increases upstream
disk usage. Kafka sources have no equivalent retention requirement. See
[Snapshotting](/fundamentals/concepts/snapshotting/) and [Monitoring the
snapshotting
progress](/ingest-data/monitoring-data-ingestion/#monitoring-the-snapshotting-progress).

## Steady state

Once the snapshot is committed, the object continually ingests upstream changes
and `status` stays `running`. To confirm it is keeping up, compare the offset
Materialize has committed against the offset it knows about upstream:

```mzsql
SELECT o.name, s.offset_known, s.offset_committed,
       s.offset_known - s.offset_committed AS offset_delta
FROM mz_internal.mz_source_statistics s
JOIN mz_objects o ON o.id = s.id
WHERE o.name IN ('orders', 'clicks')
ORDER BY o.name;
```

```nofmt
  name  | offset_known | offset_committed | offset_delta
--------+--------------+------------------+--------------
 clicks | 200          | 200              | 0
 orders | 22526904     | 22526904         | 0
(2 rows)
```

You want `offset_delta` close to `0`. The unit depends on the source type: for
Kafka sources an offset is a Kafka offset, while for PostgreSQL sources it is a
log sequence number (LSN), which is why the two rows above differ by orders of
magnitude. Compare each object against itself over time rather than against
other objects. See [Monitoring data
lag](/ingest-data/monitoring-data-ingestion/#monitoring-data-lag).

{{< note >}}
A cluster replica restart or resize triggers
[hydration](/fundamentals/concepts/hydration/). For Kafka upsert sources, this
rebuilds the table's internal upsert index from storage; for other source
types, hydration is negligible or not applicable.
{{< /note >}}

## Paused

An object whose cluster has no replicas reports `paused` and makes no progress.
The `details` column reports why:

```mzsql
SELECT o.name, s.status, s.details
FROM mz_internal.mz_source_statuses s
JOIN mz_objects o ON o.id = s.id
ORDER BY o.name;
```

```nofmt
   name    | status |                             details
-----------+--------+-----------------------------------------------------------------
 clicks    | paused | {"hints":["There is currently no replica running this source"]}
 kafka_src | paused | {"hints":["There is currently no replica running this source"]}
 orders    | paused | {"hints":["There is currently no replica running this source"]}
 pg_src    | paused | {"hints":["There is currently no replica running this source"]}
(4 rows)
```

A cluster that had a replica and lost it reports a different hint, `The replica
running this source has been dropped`. Either way, ingestion resumes when the
cluster has a replica again, so [increase the replication
factor](/sql/alter-cluster/#replication-factor-1) of the cluster hosting the
source.

## Stalled

An object that hits an error reports `stalled`, with the cause in `error`:

```mzsql
SELECT o.name, s.status, s.error
FROM mz_internal.mz_source_statuses s
JOIN mz_objects o ON o.id = s.id
ORDER BY o.name;
```

In the output below, the publication backing the PostgreSQL source was dropped
upstream. Both `pg_src` and its table `orders` stall, because neither can make
progress without it, while the unrelated Kafka source keeps running:

```nofmt
   name    | status  |                      error
-----------+---------+--------------------------------------------------
 clicks    | running |
 kafka_src | running |
 orders    | stalled | postgres: publication "mz_orders" does not exist
 pg_src    | stalled | postgres: publication "mz_orders" does not exist
(4 rows)
```

Sources stall independently of one another, so a stall is scoped to the source
that hit the error and the tables that depend on it.

The `details` column carries the same error tagged with the subsystem that
reported it, which tells you which part of the pipeline failed:

```nofmt
{"namespaced":{"postgres":"publication \"mz_orders\" does not exist"}}
```

For causes and fixes, see [Troubleshooting data
ingestion](/ingest-data/troubleshooting/) for any source type, and the
CDC-specific guides for [PostgreSQL](/ingest-data/postgres/troubleshooting/)
and [MySQL](/ingest-data/mysql/troubleshooting/), which cover replication slot,
WAL, and GTID errors unique to those connectors.

## Dropped

Dropping an object is terminal. Once dropped, it no longer appears in
`mz_source_statuses`, but its final `dropped` status remains in
[`mz_source_status_history`](/sql/system-catalog/mz_internal/#mz_source_status_history).

## Reviewing the full history

`mz_source_statuses` reports only the current state. To see every transition an
object has gone through, which is the fastest way to understand how it reached
its current state, query the history:

```mzsql
SELECT o.name, h.occurred_at, h.status
FROM mz_internal.mz_source_status_history h
JOIN mz_objects o ON o.id = h.source_id
WHERE o.name IN ('orders', 'clicks')
ORDER BY h.occurred_at;
```

```nofmt
  name  |        occurred_at         |  status
--------+----------------------------+----------
 orders | 2026-09-11 15:04:24.384+00 | starting
 orders | 2026-09-11 15:04:24.385+00 | running
 clicks | 2026-09-11 15:04:24.515+00 | starting
 clicks | 2026-09-11 15:04:24.56+00  | running
(4 rows)
```

`created` is not a recorded transition, so it never appears in the history.
Here both tables moved from `starting` to `running` within milliseconds, since
each had only a few hundred rows to snapshot.

## Related pages

- [Sources](/fundamentals/concepts/sources/)
- [Snapshotting](/fundamentals/concepts/snapshotting/)
- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion/)
- [Troubleshooting data ingestion](/ingest-data/troubleshooting/)
