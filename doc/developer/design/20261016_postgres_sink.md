# Postgres sink

- Associated: [#39163](https://github.com/MaterializeInc/materialize/pull/39163)

## The problem

Users want a materialized view kept up to date in a PostgreSQL table, with
the same exactly-once guarantees the Kafka sink gives. The first cut of the
sink used three operators, a persistent staging table per sink, and
`pg_terminate_backend` to fence earlier incarnations. This document records
the simpler design that replaced it.

## Success criteria

- A `CREATE SINK ... INTO POSTGRES CONNECTION` keeps the target table equal to
  the sinked relation, applying each Materialize timestamp exactly once across
  restarts, replica changes and `ALTER SINK`.
- A superseded incarnation of the sink (a zombie replica, or two replicas
  running by mistake) can never commit to the target.
- The sink leaves nothing in the target database beyond the target table and
  one bookkeeping row.

## Solution proposal

The dataflow has two operators, matching the Kafka sink:

1. `encode` runs on every worker and turns each `(row, time, diff)` into a
   `COPY ... BINARY` staging row (the source columns, `mz_timestamp`,
   `mz_diff = ±1`). Encoding is the CPU-heavy part, so it is sharded.
2. `write` runs on one worker, chosen by hashing the sink id, and holds the
   only connection to the target database.

The write operator is the whole protocol. At startup it:

- creates `materialize.sink_progress` and the target table if needed. The
  sink's `KEY` becomes the primary key, so the per-window delete is an index
  lookup. A key with a nullable column gets a plain index instead.
- claims the sink's progress row: `UPDATE ... SET epoch = epoch + 1,
  sink_version = $v WHERE sink_id = $id AND sink_version <= $v RETURNING
  epoch, mz_frontier`. No row returned means a newer version of the sink owns
  the table, and the operator halts.
- reconciles the persist progress shard with the recorded frontier. The row is
  the source of truth because it moves in the same transaction as the data.
  A shard behind the row is pulled forward. A shard ahead of the row means the
  target lost committed writes, and the sink halts with that message.
- creates a session temporary table `mz_sink_stage ... ON COMMIT DELETE
  ROWS`.

Then, for each window `[lower, upper)` the input frontier closes, in one
transaction:

```
BEGIN ISOLATION LEVEL SERIALIZABLE
SELECT epoch, mz_frontier FROM materialize.sink_progress WHERE sink_id = $id
  -- halt unless epoch is ours and mz_frontier = lower
COPY pg_temp.mz_sink_stage FROM STDIN (FORMAT BINARY)
  -- streamed as rows arrive; rows exactly at `lower` go straight in,
  -- which is what streams a snapshot without buffering it
WITH latest AS (SELECT DISTINCT ON (key) ... FROM pg_temp.mz_sink_stage
                ORDER BY key, mz_timestamp DESC, mz_diff DESC),
     deleted AS (DELETE FROM target t USING latest l WHERE t.key = l.key),
     inserted AS (INSERT INTO target SELECT ... FROM latest WHERE mz_diff > 0)
SELECT counts
UPDATE materialize.sink_progress SET mz_frontier = upper
  WHERE sink_id = $id AND epoch = $epoch   -- halt unless one row
COMMIT
```

After the commit, and only then, the persist progress shard is advanced to
`upper`.

### Why this is correct

- **Exactly once.** A window is applied and its frontier recorded in one
  transaction, so a crash leaves either both or neither. On restart the
  recorded frontier is the resume point, and the as-of handling mirrors the
  Kafka sink's: progress is only recorded once strictly beyond the as-of.
- **Fencing.** Every incarnation bumps the epoch before writing, and every
  window checks the epoch at `BEGIN` and again in the final `UPDATE`. Under
  `SERIALIZABLE` a zombie whose window straddles a newer claim fails at
  commit, and a zombie that starts a window after the claim fails at
  `BEGIN`. `sink_version` fences the same way across `ALTER SINK`, as the
  Kafka and Iceberg sinks do.
- **Multiple timestamps per key in one window.** `DISTINCT ON` keeps the
  latest event for each key, and at that timestamp the insertion over the
  retraction, so an update lands as its new value and a delete as nothing.
  Every touched key is deleted and the survivors reinserted. This is why the
  sink requires a unique key: with duplicates a key-matched retraction cannot
  say which row to remove. The planner enforces it exactly as it does for
  Kafka upsert sinks, `KEY (...)` must be a unique key of the relation unless
  `NOT ENFORCED` is given.
- **Nothing persistent to clean.** The staging table is session temporary.
  A dropped connection drops it and rolls back the open window.

### Comparison with the other sinks

| | Kafka | Iceberg | Postgres |
| --- | --- | --- | --- |
| Encoding | all workers | all workers | all workers |
| Writing | one worker, transactional producer | all workers write files, one commits | one worker, one connection |
| Progress store | progress topic record | snapshot property | `materialize.sink_progress` row |
| Fence | producer epoch via `init_transactions` + `sink_version` | `sink_version` in snapshot | `epoch` + `sink_version` in progress row |
| Key required | yes for `ENVELOPE UPSERT` | yes for upsert mode | always |

The single writer bounds throughput by one connection's `COPY` rate, the
same bound the Kafka sink has on one producer. PostgreSQL executes a `COPY`
and the apply statement single-threaded, so a very large snapshot is
applied by one backend. Parallel writers would need per-writer temporary
tables and a two-phase commit, which is not worth it until a workload shows
the bound.

## Alternatives

- **Persistent staging table and a truncate on restart.** Needs
  `pg_terminate_backend` to clear out a zombie's open `COPY` before the
  truncate, and leaves a table and two indexes per sink in the user's schema.
- **Row locks instead of `SERIALIZABLE`.** `SELECT ... FOR UPDATE` on the
  progress row gives the same compare-and-set and avoids serialization
  failures from the user's own serializable transactions on the target table.
  Kept as the fallback if those failures turn up in practice.
- **Keyless sinks with `ctid` deletes.** Correct, but every retraction is a
  full-column join with no index, and it doubles the apply logic. Dropped in
  favor of requiring a key.

## Open questions

- Whether to record progress on every input frontier advance (as now, and as
  Kafka does) or on a `COMMIT INTERVAL` like Iceberg, to reduce write traffic
  to the user's database when the sink is idle.
