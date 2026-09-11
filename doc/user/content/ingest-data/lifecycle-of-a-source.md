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

Both the source and each of its tables report their own status. The source
tracks the connection to the upstream system, while each table tracks the
ingestion of one upstream relation.

## States

| State      | Meaning                                                                      |
|------------|------------------------------------------------------------------------------|
| `created`  | The object exists but is not ingesting yet.                                   |
| `starting` | The object is connecting to the upstream system and initializing.             |
| `running`  | The object is ingesting: first the initial snapshot, then upstream changes.   |
| `paused`   | No cluster replica is running the object. It makes no progress until one is.  |
| `stalled`  | The object hit an error. The `error` column reports the cause.                |
| `dropped`  | The object was dropped. Terminal.                                             |

The examples below use a PostgreSQL source and a Kafka source created on a
dedicated cluster. Substitute your own object names.

```mzsql
CREATE CLUSTER ingest_demo SIZE '25cc';

CREATE SOURCE pg_src IN CLUSTER ingest_demo
  FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_orders');
CREATE TABLE orders FROM SOURCE pg_src (REFERENCE orders);

CREATE SOURCE kafka_src IN CLUSTER ingest_demo
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'clicks');
CREATE TABLE clicks FROM SOURCE kafka_src (REFERENCE clicks) FORMAT JSON;
```

## Starting and running

Once a replica is available, an object connects to the upstream system
(`starting`), then begins ingesting (`running`). To check the current state of
a source and its tables:

```mzsql
SELECT o.name, s.status, s.error
FROM mz_internal.mz_source_statuses s
JOIN mz_objects o ON o.id = s.id
WHERE o.name IN ('pg_src', 'orders', 'kafka_src', 'clicks')
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

The `starting` state is usually brief. If an object stays in `starting` for
more than a few minutes, see [Troubleshooting: Why isn't my source ingesting
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
 clicks |                    200 |                     200 | t
 orders |                    500 |                     500 | t
(2 rows)
```

While the snapshot is in progress, `snapshot_records_staged` climbs toward
`snapshot_records_known` and `snapshot_committed` is `f`. The table cannot
serve queries until the snapshot completes: queries block until it does.

{{< note >}}
These statistics are collected periodically, so they can read `NULL` or `f`
for a short window after an object starts running, even once the data is
queryable. They also reset when a replica restarts, so track how they evolve
rather than their absolute values at a single moment.
{{< /note >}}

Snapshot duration and upstream impact vary by source type. CDC sources
(PostgreSQL, MySQL, SQL Server) require the upstream system to retain its
change log until the snapshot completes, so a long snapshot increases upstream
disk usage. Kafka sources have no equivalent retention requirement. See
[Snapshotting](/fundamentals/concepts/snapshotting/) and [Monitoring the
snapshotting
progress](/ingest-data/monitoring-data-ingestion/#monitoring-the-snapshotting-progress).

## Steady state

Once the snapshot is committed, the object continually ingests upstream
changes and `status` stays `running`. To confirm it is keeping up, compare the
offset Materialize has committed against the offset it knows about upstream:

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
 clicks |          200 |              200 |            0
 orders |     22510192 |         22510192 |            0
(2 rows)
```

You want `offset_delta` close to `0`. The unit depends on the source type: for
Kafka sources an offset is a Kafka offset, and for PostgreSQL sources it is a
log sequence number (LSN), which is why the two rows above differ by orders of
magnitude. See [Monitoring data
lag](/ingest-data/monitoring-data-ingestion/#monitoring-data-lag).

{{< note >}}
A cluster replica restart or resize triggers
[hydration](/fundamentals/concepts/hydration/). For Kafka upsert sources, this
rebuilds the table's internal upsert index from storage; for other source
types, hydration is negligible or not applicable.
{{< /note >}}

## Paused

An object whose cluster has no replicas reports `paused` and makes no progress:

```mzsql
SELECT o.name, s.status, s.details
FROM mz_internal.mz_source_statuses s
JOIN mz_objects o ON o.id = s.id
WHERE o.name IN ('pg_src', 'orders', 'kafka_src', 'clicks')
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

To resolve, [increase the replication
factor](/sql/alter-cluster/#replication-factor-1) of the cluster hosting the
source. The `details` column distinguishes the two ways an object becomes
paused: the hint above means the cluster has no replicas, while `The replica
running this source has been dropped` means the specific replica it was
running on went away.

## Stalled

An object that hits an error reports `stalled`, with the cause in `error` and
often a suggested fix in `details`:

```mzsql
SELECT o.name, s.status, s.error
FROM mz_internal.mz_source_statuses s
JOIN mz_objects o ON o.id = s.id
WHERE o.name = 'pg_src';
```

Errors surface on the object that owns the failing work: a connection problem
stalls the source, while an error specific to one upstream relation stalls
that table.

For causes and fixes, see [Troubleshooting data
ingestion](/ingest-data/troubleshooting/) for any source type, and the
CDC-specific guides for [PostgreSQL](/ingest-data/postgres/troubleshooting/)
and [MySQL](/ingest-data/mysql/troubleshooting/), which cover replication
slot, WAL, and GTID errors unique to those connectors.

## Dropped

Dropping an object is terminal. Once dropped, it no longer appears in
`mz_source_statuses`, but its final `dropped` status remains in
[`mz_source_status_history`](/sql/system-catalog/mz_internal/#mz_source_status_history).

## Reviewing the full history

`mz_source_statuses` reports only the current state. To see every transition an
object has gone through, which is the fastest way to understand how it reached
its current state, query the history:

```mzsql
SELECT o.name, h.occurred_at, h.status, h.error
FROM mz_internal.mz_source_status_history h
JOIN mz_objects o ON o.id = h.source_id
WHERE o.name IN ('orders', 'clicks')
ORDER BY h.occurred_at;
```

```nofmt
  name  |        occurred_at         |  status  | error
--------+----------------------------+----------+-------
 orders | 2026-09-11 14:58:14.908+00 | paused   |
 clicks | 2026-09-11 14:58:14.973+00 | paused   |
 orders | 2026-09-11 14:58:29.899+00 | starting |
 orders | 2026-09-11 14:58:29.901+00 | running  |
 clicks | 2026-09-11 14:58:29.992+00 | starting |
 clicks | 2026-09-11 14:58:29.997+00 | running  |
(6 rows)
```

These objects were created on a cluster with a replication factor of `0`, so
they started out `paused`. Raising the replication factor moved them through
`starting` to `running`, in this case within milliseconds.

## Related pages

- [Sources](/fundamentals/concepts/sources/)
- [Snapshotting](/fundamentals/concepts/snapshotting/)
- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion/)
- [Troubleshooting data ingestion](/ingest-data/troubleshooting/)
