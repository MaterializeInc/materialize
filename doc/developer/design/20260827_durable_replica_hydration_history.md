# Durable Replica Hydration History

## Context

The [durable object hydration history](20260817_durable_object_hydration_history.md)
records successful hydration for individual dataflows. This design extends that
collector with replica-wide hydration episodes and the resource high-water marks
visible when each episode is recorded.

The extension reuses the object collector's scheduling, replica-targeted
read-then-write path, exact-timestamp OCC, retention, and migration protections.
This document describes only the replica-level additions.

## Episode boundaries

Each live non-transient compute export contributes an interval from its earliest
worker installation to its latest worker hydration. The collector waits until
every visible worker of every live non-transient export has hydrated. Transient
query dataflows are excluded because the collection query itself creates one.

A replica episode is a connected component in the union of those export
intervals. Two intervals belong to one episode if they overlap directly or
through a chain of overlapping intervals. A gap means the replica was fully
hydrated before the next export was installed, so the next interval starts a
new episode.

Each sweep records only the latest completed episode visible in its snapshot.
Episodes that complete between sweeps and exports that retract before a sweep
leave no evidence. The current inputs can therefore record successful episodes
only. Failed, canceled, and OOM-killed outcomes need an additional durable
replica signal.

Process-local clocks stamp both interval endpoints. Clock skew can merge
episodes that did not overlap in real time. Once an episode is recorded, a
monotonic history guard prevents a later snapshot from interpreting retracted
intervals as an earlier or overlapping episode.

## Resource interpretation

`peak_memory_bytes` is the maximum `cgroup memory_peak` across replica
processes. `peak_disk_bytes` is the maximum sampled `statvfs fs_used_peak` when a
scratch filesystem is present. Otherwise it is the maximum kernel-maintained
`cgroup swap_peak`.

Replica memory and disk limits apply independently to each process. The maximum
process peak therefore answers whether any process approached its limit. Adding
process maxima would combine peaks that may not have occurred simultaneously.

The operating system's peaks cover the process lifetime through the collector's
observation. They are not bounded by `finished_at`, so work after hydration and
before collection can raise them. Later episodes can also include an earlier
high-water mark. A true episode peak requires a reset or a separately retained
interval maximum at the replica.

The collector requires at least one resource observation from every configured
process before writing. Individual peak metrics can still be absent, which is
represented by `NULL` rather than a zero sentinel.

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
does not declare a key or index. Collection runs on the selected replica, so a
catalog-server index would not avoid importing and arranging the history there.

Rows currently have a populated `finished_at` and the status `hydrated`.
Resource columns are nullable because the available kernel and filesystem
observations depend on the replica platform.
