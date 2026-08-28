---
source: src/adapter/src/coord/hydration_history.rs
revision: a1bcaebfe6
---

# `adapter::coord::hydration_history`

Durable history collection for completed compute-object hydration episodes.

## Overview

Each sweep visits one user replica, installs a replica-targeted subscribe that diffs that replica's live hydration timestamps against the durable history table, and appends missing rows through the timestamped OCC write path. Including the history table in the read expression makes the write idempotent across concurrent `environmentd` processes: two collectors that compute the same row race for one write timestamp, and the loser observes the winner's append through its own subscribe and finds nothing left to write.

One replica is sampled per interval, so an environment with N eligible replicas revisits each one approximately every `N * interval`. Collection is sampling, not an event log. An episode whose live row is retracted before its replica's turn in the sweep is not recorded because the evidence is gone.

## Key Types

**`Sweep`** — Context for one sweep run, holding the `PeekClient`, catalog reference, history table ID, metrics handle, wall time, and a `cutoff` string (RFC 3339 timestamp). The two operations are:
- `collect` — appends one replica's completed episodes that the history table is missing
- `retain` — retracts one bounded batch of rows that have aged out of the retention window

## Scheduling

`Coordinator::schedule_hydration_history_collection` aligns sweep fires to interval boundaries, shifted per-environment by a SHA-256-derived offset so a fleet-wide interval does not create a fleet-wide burst. Each sleep is capped at `SCHEDULE_RECHECK_CAP` (5 s) so dynamic configuration changes take effect promptly. Sweeps do not overlap: the next one is scheduled only after the previous completes or fails.

`Coordinator::run_hydration_history_collection` dispatches the sweep as a background task. The task runs `collect` against the selected user replica and `retain` against the catalog server cluster, then reschedules. The sweep handle is stored on the `Coordinator` so it is aborted when the coordinator drops.

## Collection Query

`collect_sql` builds a `SELECT` that joins `mz_compute_hydration_times_per_worker` against `mz_object_hydration_history` with an anti-join, filtering to fully-hydrated objects (all workers have a `hydrated_at`) whose episodes are not yet recorded. The cutoff and the anti-join sit outside the aggregate so they do not interfere with the per-worker completeness check.

## Retention

`retention_sql` returns a bounded batch (`RETENTION_BATCH_SIZE` = 1000) of the oldest rows aged past the cutoff, using a subquery so the `LIMIT` applies inside the relation expression rather than as a top-level `RowSetFinishing` that the OCC path cannot apply.

## Constants

| Constant | Value | Purpose |
|---|---|---|
| `SCHEDULE_RECHECK_CAP` | 5 s | Max sleep before rechecking dyncfg |
| `DISABLED_RECHECK_INTERVAL` | 60 s | Poll interval when disabled |
| `MUTATION_TIMEOUT` | 300 s | Per-step wall clock bound |
| `RETENTION_BATCH_SIZE` | 1000 | Rows deleted per retention step |

## Helper Functions

- `next_replica` — advances a cursor through replicas sorted by ID, wrapping at the end, so each sweep visits a different replica
- `environment_schedule_offset` — derives a stable per-environment offset from a SHA-256 hash of the environment ID
- `plan_mutation` — plans a `SELECT` statement as the read side of a `ReadThenWritePlan`, and validates that the selection's column types match the target table
