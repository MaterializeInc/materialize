---
source: src/adapter/src/coord/hydration_history.rs
revision: 46f729653a
---

# `adapter::coord::hydration_history`

Durable history collection for completed object and replica hydration episodes.

## Overview

Each sweep visits one user replica, installs a replica-targeted subscribe that diffs that replica's live hydration timestamps against the durable history tables, and appends missing rows through the timestamped OCC write path. Including each history table in its read expression makes the write idempotent across concurrent `environmentd` processes: two collectors that compute the same row race for one write timestamp, and the loser observes the winner's append through its own subscribe and finds nothing left to write.

One replica is sampled per interval, so an environment with N eligible replicas revisits each one approximately every `N * interval`. Collection is sampling, not an event log. Replica history records only the latest completed episode visible in a sweep. Intermediate episodes and intervals retracted before collection leave no evidence and are not recorded.

## Key Types

**`ReplicaTarget`** — A user replica eligible for one collection step, carrying `cluster_id`, `replica_id`, and `process_count`. The `process_count` is used by `replica_collection_sql` to gate the write on all configured processes having reported resource usage.

**`Sweep`** — Context for one sweep run, holding the `PeekClient`, catalog reference, `object_history_id` and `replica_history_id` table IDs, metrics handle, wall time, and a `cutoff` string (RFC 3339 timestamp). The two operations are:
- `collect` — appends one replica's completed object and replica episodes that their respective history tables are missing
- `retain` — retracts one bounded batch of rows from each history table that have aged out of the retention window

## Scheduling

`Coordinator::schedule_hydration_history_collection` aligns sweep fires to interval boundaries, shifted per-environment by a SHA-256-derived offset so a fleet-wide interval does not create a fleet-wide burst. Each sleep is capped at `SCHEDULE_RECHECK_CAP` (5 s) so dynamic configuration changes take effect promptly. Sweeps do not overlap: the next one is scheduled only after the previous completes or fails.

`Coordinator::run_hydration_history_collection` dispatches the sweep as a background task. The task runs `collect` against the selected user replica and `retain` against the catalog server cluster, then reschedules. The sweep handle is stored on the `Coordinator` so it is aborted when the coordinator drops.

## Collection Queries

`object_collection_sql` builds a `SELECT` that joins `mz_compute_hydration_times_per_worker` against `mz_object_hydration_history` with an anti-join, filtering to fully-hydrated user indexes and materialized views (all workers have a `hydrated_at`) whose episodes are not yet recorded. The cutoff and the anti-join sit outside the aggregate so they do not interfere with the per-worker completeness check.

`replica_collection_sql` implements a gaps-and-islands algorithm over compute export hydration intervals to find the latest completed hydration episode that is disconnected from any still-open interval. Transient exports (those with IDs starting with `t`) are excluded. The query also waits until every configured replica process (`process_count`) has reported resource usage before committing the episode, capturing `peak_memory_bytes` (cgroup `memory_peak`) and `peak_disk_bytes` (statvfs `fs_used_peak`, falling back to cgroup `swap_peak`).

## Retention

`object_retention_sql` returns a bounded batch (`RETENTION_BATCH_SIZE` = 1000) of the oldest `mz_object_hydration_history` rows aged past the cutoff.
`replica_retention_sql` returns the same bounded batch for `mz_replica_hydration_history`.
Both use a subquery so the `LIMIT` applies inside the relation expression rather than as a top-level `RowSetFinishing` that the OCC path cannot apply.

## Constants

| Constant | Value | Purpose |
|---|---|---|
| `SCHEDULE_RECHECK_CAP` | 5 s | Max sleep before rechecking dyncfg |
| `DISABLED_RECHECK_INTERVAL` | 60 s | Poll interval when disabled |
| `MUTATION_TIMEOUT` | 300 s | Per-step wall clock bound |
| `RETENTION_BATCH_SIZE` | 1000 | Rows deleted per retention step |

## Helper Functions

- `next_replica` — advances a cursor through `ReplicaTarget` entries sorted by ID, wrapping at the end, so each sweep visits a different replica
- `environment_schedule_offset` — derives a stable per-environment offset from a SHA-256 hash of the environment ID
- `plan_mutation` — plans a `SELECT` statement as the read side of a `ReadThenWritePlan`, and validates that the selection's column types match the target table
