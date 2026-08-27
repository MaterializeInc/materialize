# Durable Replica Hydration History

## Context

Materialize durably records successful hydration of individual index and
materialized-view dataflows. Users also need a replica-wide summary that says
how many objects hydrated together, how long the replica was hydrating, and how
close any process came to its resource limit.

Compute now exposes two replica-local inputs:

- `mz_compute_hydration_times_per_worker` reports installation and hydration
  timestamps for every compute export and worker.
- `mz_cluster_replica_resource_usage` reports resource observations for every
  replica process, including kernel-maintained memory and swap high-water marks
  and a sampled scratch-filesystem high-water mark.

This design extends the existing hydration-history sweep to sample successful
replica hydration components from those inputs.

## Goals

- Record the latest completed replica hydration component visible in each
  replica's sweep.
- Include every index or materialized-view dataflow still visible from the
  latest component when it is sampled.
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
- A complete event log. Intermediate transitions and intervals that retract
  before collection leave no current-state evidence.
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

A component is identified operationally by `(replica_id, started_at)`. The table
does not declare this as a relation key. The anti-join enforces uniqueness, and
an accidental duplicate must remain visible rather than be optimized away.

Only components whose surviving intervals have all hydrated are recorded, so
`finished_at` is populated and `status` is `hydrated`. An interval that retracts
before collection is unknown to the sampler. This means a canceled transition
can leave a successful component of the surviving intervals. The nullable finish
and explicit status leave room for lifecycle-accurate terminal failures. A future
writer must also define monotonic-guard and retention behavior for a `NULL` finish
before it can record them. Resource columns are nullable because cgroup peak files
depend on the host kernel and a replica without disk reports no filesystem
observation.

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
new component. The latest such start identifies the component whose resources
are currently observable. Its finish is the maximum object finish and its
object count is the number of intervals in the component. Earlier components
that completed between sweeps are intentionally not recorded because the
current resource observations cannot be attributed between them.

This definition handles both important cases without coordinator-local state:

- A replica restart replaces every live interval with fresh timestamps. Their
  connected component becomes a new episode.
- `CREATE INDEX` on a fully hydrated replica installs an interval after the
  preceding component's finish. It becomes a new episode. Objects installed
  while it hydrates join that episode if their intervals overlap.

Sampling limits completeness and boundary accuracy. An object that disappears
before the sweep is absent from the component. Process-local wall clocks stamp
the intervals, so skew can hide a worker at the sampled logical timestamp or
merge components that did not overlap in real time. Replica processes are fate
shared and always restart together, so a snapshot cannot combine process
generations.

After recording a component, collection only accepts a candidate that starts
after every retained row for that replica has finished. This monotonic guard
prevents later retractions from splitting an already-recorded component into an
additional overlapping row. It cannot recover intervals that disappeared
before the first row was recorded.

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
object and replica history sweeps. Both histories depend on compute introspection.
A replica configured with `INTROSPECTION INTERVAL = 0` is skipped even when the
history collection interval is nonzero.

The existing single-flight sweep visits one user replica per interval. It first
collects object rows, then collects the latest replica component, then runs
retention for both tables on the catalog server. A failure in one step does not
prevent the later steps from running.

The replica query anti-joins against the table it writes. Concurrent
environmentd processes can compute the same candidate, but exact-timestamp OCC
allows one write to commit. A losing subscribe observes that row and retracts
its own candidate before retrying. The guard uses replica-stamped timestamps,
so an environmentd restart does not create a duplicate.

The same background isolation rules apply as for object history. Collection is
replica-targeted, writes only a system table, depends only on system objects,
does not take the user-DML OCC permit, and has a bounded attempt timeout.

## Retention and durability

Replica history uses `hydration_history_retention_period`, which defaults to 30
days. Each sweep retracts one bounded batch of rows whose `finished_at` is older
than the cutoff. Collection applies the same cutoff, so one sweep cannot
resurrect a component its own retention step removed. Concurrent environmentd
processes can use slightly different cutoffs. An older sweep can temporarily
reinsert a boundary-aged row deleted by a newer sweep. Retention is therefore
eventual once every collector's cutoff has passed the row.

The table is exempt from bootstrap system-table reset and forced shard
replacement. Schema evolution keeps the shard and its rows. A migration guard
rejects a replacement step unless the exemption and guard are deliberately
removed together.

Collection remains best effort. A future schema replacement can intentionally
clear it, and current-state introspection cannot recreate an episode after all
of its evidence has disappeared.

## Future work

- Publish episode-scoped resettable peaks from the replica.
- Publish causal episode identities and terminal membership from the replica.
- Retain lifecycle and resource events until environmentd acknowledges them.
- Finalize open episodes from replica lifecycle events as canceled, failed, or
  OOM-killed.
- Define equivalent lifecycle signals for storage objects.
- Promote the catalog surface after its semantics and rollout have settled.
