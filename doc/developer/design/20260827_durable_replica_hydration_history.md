# Durable Replica Hydration History

## Context

Materialize durably records successful hydration of individual index and
materialized-view dataflows. Users also need the replica-wide episode that says
how many objects hydrated together, how long the replica was hydrating, and how
close any process came to its resource limit.

Compute now exposes two replica-local inputs:

- `mz_compute_hydration_times_per_worker` reports installation and hydration
  timestamps for every compute export and worker.
- `mz_cluster_replica_resource_usage` reports resource observations for every
  replica process, including kernel-maintained memory and swap high-water marks
  and a sampled scratch-filesystem high-water mark.

This design extends the existing hydration-history sweep to persist successful
replica episodes from those inputs.

## Goals

- Record one row whenever a replica transitions from fully hydrated to
  hydrating and back.
- Include every index or materialized-view dataflow installed while that
  transition is in progress.
- Record the process peak relevant to a per-process replica resource limit.
- Survive environmentd and replica restarts.
- Stay idempotent across concurrent environmentd processes.
- Share the object-history collection cadence and retention period.

## Non-goals

- Failed, canceled, or OOM-killed episodes. A replica cannot report its final
  retracting introspection state after its process exits. The controller also
  does not retain the process's final resource observations.
- Storage objects. Storage does not publish equivalent lifecycle timestamps.
- A resettable resource peak for an incremental episode. The available kernel
  peaks reset when the process restarts, not when a new object is installed.
- A public stable catalog contract. The table starts in `mz_internal` while its
  semantics settle.

## History table

```text
mz_internal.mz_replica_hydration_history
  replica_id         text         not null
  cluster_id         text         not null
  started_at         timestamptz  not null
  finished_at        timestamptz  null
  object_count       uint8        not null
  peak_memory_bytes  uint8        null
  peak_disk_bytes    uint8        null
  status             text         not null
```

An episode is identified operationally by `(replica_id, started_at)`. The table
does not declare this as a relation key. The anti-join enforces uniqueness, and
an accidental duplicate must remain visible rather than be optimized away.

Only successful episodes are recorded, so `finished_at` is populated and
`status` is `hydrated`. The nullable finish and explicit status reserve a
compatible shape for terminal failures once they become observable. Resource
columns are nullable because cgroup peak files depend on the host kernel and a
replica without disk reports no filesystem observation.

There is no index. Collection runs on the selected user replica, so it cannot
use an index arranged on the catalog server. Such an index would pin the whole
retained table without removing the recurring import and arrangement cost.

## Episode boundaries

Each live object contributes an interval from its earliest worker installation
to its latest worker hydration. The collector waits until every visible worker
of every relevant object has hydrated. This preserves the object collector's
rule that a materialized view only finishes after its persist sink's active
worker reports completion.

A replica episode is a connected component in the union of those object
intervals. Two intervals belong to one episode if they overlap directly or
through a chain of overlapping intervals. A gap means the replica was fully
hydrated before the next object was installed, so the next interval starts a
new episode.

The query sorts intervals by installation time and computes the running maximum
finish among preceding intervals. An installation after that maximum starts a
new component. The latest such start identifies the episode whose resources are
currently observable. Its finish is the maximum object finish and its object
count is the number of intervals in the component.

This definition handles both important cases without coordinator-local state:

- A replica restart replaces every live interval with fresh timestamps. Their
  connected component becomes a new episode.
- `CREATE INDEX` on a fully hydrated replica installs an interval after the
  preceding component's finish. It becomes a new episode. Objects installed
  while it hydrates join that episode if their intervals overlap.

Sampling still limits completeness. An object that disappears before the sweep
can be absent from the episode, and a process clock ahead of the read timestamp
can hide a worker. These are the same accepted limits as durable object history.

## Resource interpretation

`peak_memory_bytes` is the maximum `cgroup memory_peak` across replica
processes. This is the accounting that the cgroup memory limit and OOM killer
act on. `peak_disk_bytes` is the maximum sampled `statvfs fs_used_peak` when a
scratch filesystem is present. Otherwise it is the maximum kernel-maintained
`cgroup swap_peak`.

Replica memory and disk limits apply independently to each process. The maximum
process peak therefore answers whether any process needed a larger size. Adding
process maxima would produce a number that may never have existed because the
peaks need not be simultaneous.

The operating system's peaks cover the process lifetime through the collector's
observation. They are not bounded by `finished_at`, so work after hydration and
before collection can raise them even for the first episode after a process
starts. In a later episode they can additionally include an earlier high-water
mark. The table documents this instead of presenting a process-lifetime value as
episode-scoped. A true episode peak requires a reset or a separately retained
interval maximum at the replica.

The collector requires at least one resource observation from every configured
process before writing. Individual peak metrics can still be absent, which is
represented by `NULL` rather than a zero sentinel.

## Collection and concurrency

Collection is disabled by default. Setting
`hydration_history_collection_interval` to a nonzero duration enables both the
object and replica history sweeps.

The existing single-flight sweep visits one user replica per interval. It first
collects object rows, then collects the latest replica episode, then runs
retention for both tables on the catalog server. A failure in one step does not
prevent the later steps from running.

The replica query anti-joins against the table it writes. Concurrent
environmentd processes can compute the same candidate, but exact-timestamp OCC
allows one write to commit. A losing subscribe observes that row and retracts
its own candidate before retrying. The identity uses replica-stamped
`started_at`, so an environmentd restart does not create a duplicate.

The same background isolation rules apply as for object history. Collection is
replica-targeted, writes only a system table, depends only on system objects,
does not take the user-DML OCC permit, and has a bounded attempt timeout.

## Retention and durability

Replica history uses `hydration_history_retention_period`, which defaults to 30
days. Each sweep retracts one bounded batch of rows whose `finished_at` is older
than the cutoff. Collection applies the same cutoff, so a live introspection row
cannot resurrect an episode that retention removed.

The table is exempt from bootstrap system-table reset and forced shard
replacement. Schema evolution keeps the shard and its rows. A migration guard
rejects a replacement step unless the exemption and guard are deliberately
removed together.

Collection remains best effort. A future schema replacement can intentionally
clear it, and current-state introspection cannot recreate an episode after all
of its evidence has disappeared.

## Future work

- Publish episode-scoped resettable peaks from the replica.
- Retain lifecycle and resource events until environmentd acknowledges them.
- Finalize open episodes from replica lifecycle events as canceled, failed, or
  OOM-killed.
- Define equivalent lifecycle signals for storage objects.
- Promote the catalog surface after its semantics and rollout have settled.
