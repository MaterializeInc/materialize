---
title: "Understand the lifecycle of a sink"
description: "The states a sink moves through, how to monitor each one, and the error states you might encounter."
menu:
  main:
    parent: sink
    name: "Understand the lifecycle of a sink"
    identifier: sink-lifecycle
    weight: 80
---

A sink moves through a sequence of states as it writes data to an external
system. Knowing which state a sink is in tells you whether it is making
progress or is stuck.

This page covers sinks created with [`CREATE SINK ... INTO
KAFKA`](/sql/create-sink/kafka/) and [`CREATE SINK ... INTO
ICEBERG`](/sql/create-sink/iceberg/). Both report through the same
[`mz_sink_statuses`](/sql/system-catalog/mz_internal/#mz_sink_statuses) view
and move through the same states, so the queries below apply to either sink
type. Differences between the two are called out where they matter.

## States

| State      | Meaning                                                                      |
|------------|--------------------------------------------------------------------------------|
| `created`  | The default shown before any status has been recorded, cleared almost immediately. See [Created](#created). |
| `starting` | The sink is connecting to the external system and initializing.               |
| `running`  | The sink is writing: first its input snapshot, then incremental changes.      |
| `paused`   | No cluster replica is running the sink. It makes no progress until one is.    |
| `stalled`  | The sink hit an error. The `error` column reports the cause.                  |
| `dropped`  | The sink was dropped. Terminal.                                               |

The examples below use a Kafka sink on a dedicated cluster. Substitute your own
object names.

```mzsql
CREATE CLUSTER export_demo SIZE '25cc';

CREATE CONNECTION kafka_conn TO KAFKA (BROKER 'redpanda:9092', SECURITY PROTOCOL PLAINTEXT);

CREATE SINK orders_sink IN CLUSTER export_demo
  FROM orders
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'orders_sink')
  FORMAT JSON
  ENVELOPE DEBEZIUM;
```

## Created

Unlike a source, a sink has no upstream connection to wait for and nothing
else to attach before it can run, so it has no separate resting `created`
state. `created` is the default `mz_sink_statuses` reports before any status
has been recorded for the sink, not a state a sink lingers in: creating a sink
against a running cluster moves it to `starting` and then `running`
essentially immediately, often within the same reporting interval:

```mzsql
SELECT name, type, status
FROM mz_internal.mz_sink_statuses
WHERE name = 'orders_sink';
```

```nofmt
    name     | type  | status
-------------+-------+---------
 orders_sink | kafka | running
(1 row)
```

If the sink's cluster has no replicas when you create it, it moves to
`paused` instead. See [Paused](#paused).

## Starting and running

`running` covers both the sink's initial write of its input snapshot and
steady-state writing of incremental changes, so the status alone does not
tell you which phase a sink is in. See [Hydration](#hydration) and [Steady
state](#steady-state).

A [Kafka connection](/sql/create-connection/#kafka) and a `CREATE SINK`
statement both validate that the target broker or catalog is reachable at
statement time, so most connectivity problems surface as an error on the
`CREATE SINK` statement itself rather than a `stalled` sink. `stalled`
generally means the sink was running and then lost its connection, or the
external system rejected a write. See [Stalled](#stalled).

## Hydration

{{% include-from-yaml data="hydration-details" name="sink" %}}

Unlike a source snapshot, a sink's initial write is not separately observable
in [`mz_sink_statistics`](/sql/system-catalog/mz_internal/#mz_sink_statistics):
the counters described in [Steady state](#steady-state) accumulate the same
way whether the sink is still writing its snapshot or has moved on to
incremental changes. To tell whether a sink has finished hydrating, use
[`mz_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_hydration_statuses)
instead:

```mzsql
SELECT o.name, h.hydrated
FROM mz_internal.mz_hydration_statuses h
JOIN mz_objects o ON o.id = h.object_id
WHERE o.name = 'orders_sink';
```

```nofmt
    name     | hydrated
-------------+----------
 orders_sink | t
(1 row)
```

A sink runs on exactly one cluster replica at a time, even on a cluster with
several. `replica_id` in `mz_sink_statistics` and
`mz_sink_status_history` identifies that replica; the others sit idle for
this sink.

For general hydration behavior, memory impact, and strategies such as a burst
replica during hydration, see [Hydration](/fundamentals/concepts/hydration/).

## Steady state

Once the sink is writing steadily, track its progress with
[`mz_sink_statistics`](/sql/system-catalog/mz_internal/#mz_sink_statistics):

```mzsql
SELECT messages_staged, messages_committed, bytes_staged, bytes_committed
FROM mz_internal.mz_sink_statistics
WHERE id = (SELECT id FROM mz_sinks WHERE name = 'orders_sink');
```

```nofmt
 messages_staged | messages_committed | bytes_staged | bytes_committed
-----------------+--------------------+--------------+------------------
               2 |                  2 |           78 |               78
(1 row)
```

These counters monotonically increase, so compare them over time rather than
reading a single snapshot: what matters is that all four keep climbing
together. `messages_staged`/`bytes_staged` count what the sink has written but
not necessarily committed; `messages_committed`/`bytes_committed` count what
the external system has durably accepted, and can lag behind the staged
counters when a write is retried. In practice, a Kafka sink's transactional
writes land in both counters together, so the two rarely differ at query
time. The counters reset to zero whenever the sink's dataflow restarts, for
example after [`stalled`](#stalled) recovers, so a drop to `0` on its own does
not mean data was lost.

## Paused

A sink whose cluster has no replicas reports `paused` and makes no progress.
The `details` column reports why:

```mzsql
SELECT name, status, details
FROM mz_internal.mz_sink_statuses
WHERE name = 'orders_sink';
```

```nofmt
    name     | status |                            details
-------------+--------+-----------------------------------------------------------------
 orders_sink | paused | {"hints":["There is currently no replica running this sink"]}
(1 row)
```

A cluster that had a replica and lost it reports a different hint, `The
replica running this sink has been dropped`. Either way, writing resumes when
the cluster has a replica again, so [increase the replication
factor](/sql/alter-cluster/#replication-factor-1) of the cluster hosting the
sink.

## Stalled

A sink that hits an error reports `stalled`, with the cause in `error`. For
example, a Kafka sink whose broker becomes unreachable after the sink was
already running:

```mzsql
SELECT name, status, error
FROM mz_internal.mz_sink_statuses
WHERE name = 'orders_sink';
```

```nofmt
    name     | status  |                                    error
-------------+---------+-------------------------------------------------------------
 orders_sink | stalled | kafka: Transaction error: Timed out waiting for operation to
             |         | finish, retry call to resume
(1 row)
```

The `details` column carries a hint alongside the error:

```nofmt
{"hints":["If you're running a single Kafka broker, ensure that the configs
transaction.state.log.replication.factor, transaction.state.log.min.isr, and
offsets.topic.replication.factor are set to 1 on the broker"]}
```

Common causes are the broker or Iceberg catalog becoming unreachable, a Kafka
topic being deleted out from under the sink, or an Iceberg table's schema
changing in a way the sink cannot reconcile with what it is writing.
Materialize restarts the sink's dataflow to retry, so `stalled` alternates
with `starting` and `running` in
[`mz_sink_status_history`](#reviewing-the-full-history) as it does; if the
underlying cause has not cleared, the retry fails and the sink stalls again.
A cause that will not clear on its own, such as an incompatible schema change
on the Iceberg side, needs to be fixed in the external system before the sink
can make progress again.

For Iceberg sinks specifically, an unreachable or unhealthy catalog is a
common stall cause in practice: Iceberg tables need periodic maintenance
(compacting data and delete files, expiring old snapshots) on the catalog
side, and a table whose maintenance has fallen behind can start rejecting the
sink's writes even though nothing changed in Materialize.

For causes and fixes, see [Troubleshooting
sinks](/export-data/sink-troubleshooting/).

## Dropped

Dropping a sink is terminal. Once dropped, it no longer appears in
`mz_sink_statuses`, but its final `dropped` status remains in
[`mz_sink_status_history`](/sql/system-catalog/mz_internal/#mz_sink_status_history).

## Reviewing the full history

`mz_sink_statuses` reports only the current state. To see every transition a
sink has gone through, query the history:

```mzsql
SELECT h.occurred_at, h.status
FROM mz_internal.mz_sink_status_history h
JOIN mz_sinks s ON s.id = h.sink_id
WHERE s.name = 'orders_sink'
ORDER BY h.occurred_at;
```

```nofmt
        occurred_at         |  status
-----------------------------+----------
 2026-09-11 21:25:14.616+00  | starting
 2026-09-11 21:25:14.616+00  | running
(2 rows)
```

Unlike a source, which can dwell in `starting` for a noticeable stretch, a
sink typically shows `starting` and `running` recorded at essentially the same
timestamp, since a sink has no upstream connection step to wait on.

## Related pages

- [Sinks](/fundamentals/concepts/sinks/)
- [Hydration](/fundamentals/concepts/hydration/)
- [Troubleshooting sinks](/export-data/sink-troubleshooting/)
- [`CREATE SINK`: Kafka/Redpanda](/sql/create-sink/kafka/)
- [`CREATE SINK`: Iceberg](/sql/create-sink/iceberg/)
