---
title: "Replica resource usage"
description: "The per-process memory, swap and disk observations a cluster replica reports about itself, and how to interpret them."
menu:
  main:
    parent: "monitor"
    identifier: "monitor-replica-resource-usage"
    weight: 18
---

Every process of a cluster replica reports its own resource usage through
[`mz_introspection.mz_cluster_replica_resource_usage`](/reference/system-catalog/mz_introspection/#mz_cluster_replica_resource_usage).
Each row is one measurement, taken from one source, reported as that source
gave it. Sources measure overlapping but distinct quantities, and the
differences between them are informative, so no row is a combination of two
others. Deciding which number is "the" memory usage of a replica, or how close
it is to its limit, is left to queries over the relation.

A metric whose name ends in `peak` is a high-water mark since the process
started, and the rest are instantaneous. Peaks the operating system maintains
itself are exact, and are unaffected by how often the replica reads them. Peaks
folded from samples are marked as such below and can miss a spike shorter than
the sampling interval, which makes them lower bounds. An observation the
replica could not read is absent rather than zero, so which metrics appear
depends on the platform and the kernel version.

## Sources

| Source        | Reads                                   | Measures                                                                                  |
|---------------|-----------------------------------------|-------------------------------------------------------------------------------------------|
| `cgroup`      | the process's cgroup v2 interface files | the whole container, and the accounting that limit enforcement and the OOM killer act on   |
| `proc_status` | `/proc/self/status`                     | this process only, with resident memory broken down by backing                             |
| `rusage`      | `getrusage(RUSAGE_SELF)`                | this process only                                                                          |
| `statvfs`     | the replica's scratch filesystem        | the filesystem as a whole, absent where disk is provided as swap                           |

## Metrics

| Source        | Metric            | Meaning                                                                                                                       |
|---------------|-------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `cgroup`      | `memory_current`  | Memory charged to the cgroup: anonymous, page cache, kernel and socket memory.                                                  |
| `cgroup`      | `memory_peak`     | High-water mark of `memory_current`, maintained by the kernel.                                                                  |
| `cgroup`      | `memory_max`      | The cgroup's memory limit.                                                                                                     |
| `cgroup`      | `swap_current`    | Swap charged to the cgroup, including pages already read back whose swap slot is still allocated.                               |
| `cgroup`      | `swap_peak`       | High-water mark of `swap_current`, maintained by the kernel.                                                                    |
| `cgroup`      | `swap_max`        | The cgroup's swap limit.                                                                                                       |
| `cgroup`      | `anon`            | The part of `memory_current` backed by no file.                                                                                |
| `cgroup`      | `file`            | Page cache charged to the cgroup.                                                                                              |
| `cgroup`      | `shmem`           | Shared memory and tmpfs pages.                                                                                                 |
| `cgroup`      | `swapcached`      | Pages resident in memory whose swap slot is still allocated. Counted in both `anon` and `swap_current`.                          |
| `cgroup`      | `kernel`          | Kernel memory charged to the cgroup.                                                                                           |
| `cgroup`      | `slab`            | Kernel slab allocations, part of `kernel`.                                                                                     |
| `cgroup`      | `sock`            | Socket buffer memory.                                                                                                          |
| `cgroup`      | `events_max`      | Times an allocation hit `memory_max`. See the caveat below before using this as a limit-hit signal.                             |
| `cgroup`      | `events_oom_kill` | Processes in the cgroup killed by the OOM killer.                                                                              |
| `proc_status` | `vm_rss`          | Resident set size of this process, the sum of `rss_anon`, `rss_file` and `rss_shmem`.                                           |
| `proc_status` | `rss_anon`        | Resident memory backed by no file. The replica's own memory.                                                                    |
| `proc_status` | `rss_file`        | Resident file-backed memory, largely this binary's text. Shared between replicas and charged to whichever cgroup first faulted it in. |
| `proc_status` | `rss_shmem`       | Resident shared memory.                                                                                                        |
| `proc_status` | `vm_swap`         | This process's pages currently in swap. Excludes swap-cached pages, so it reads below `cgroup` `swap_current`.                   |
| `proc_status` | `vm_swap_peak`    | Maximum `vm_swap` over samples, so a lower bound on the true peak.                                                              |
| `proc_status` | `heap`            | `vm_rss` plus `vm_swap`, the quantity a replica is limited on.                                                                  |
| `proc_status` | `heap_peak`       | Maximum `heap` over samples, so a lower bound on the true peak.                                                                 |
| `rusage`      | `max_rss`         | Peak resident set size. Maintained by the kernel, but refreshed only at internal checkpoints, so it can read below a concurrent `vm_rss`. |
| `statvfs`     | `fs_used`         | Used bytes of the filesystem, which on a shared filesystem counts writes this replica never made.                               |
| `statvfs`     | `fs_used_peak`    | Maximum `fs_used` over samples, so a lower bound on the true peak.                                                              |

## Interpreting

Values from different sources are not interchangeable, and adding them together
generally produces a number that means nothing. In particular:

* **How close is this replica to its memory limit?** Compare `cgroup`
  `memory_current` against `memory_max`. **Did it ever reach it?** Compare
  `memory_peak` against `memory_max`. A `memory_peak` at the limit means the
  replica ran out of RAM and spilled to swap, even if the current reading is
  comfortable.
* **How close is it to its heap limit?** Compare `proc_status` `heap` against
  [`mz_internal.mz_cluster_replica_metrics`](/reference/system-catalog/mz_internal/#mz_cluster_replica_metrics)'s
  `heap_limit`. `heap_peak` bounds the high-water mark from below, and no source
  bounds it from above: the `cgroup` peaks describe a smaller quantity, and
  `max_rss` lags.
* **Do not use `events_max` as a limit-hit signal.** Where swap is configured it
  stays at zero even for a replica pinned at its ceiling, because reclaim
  succeeds by swapping instead of failing. `events_oom_kill` does report kills.
* **How much memory does this replica itself account for?** Use `proc_status`
  `rss_anon`. Do not use `vm_rss`: it includes `rss_file`, which is charged to
  another cgroup and so runs a roughly constant amount above the replica's own
  charge.
* **Do not add `memory_current` and `swap_current`.** A page read back from swap
  is counted in both, and `swapcached` reports how much is in that state.
* **Where disk is provided as swap**, disk usage appears as `swap_current` and
  there are no `statvfs` rows at all.
* **These readings live and die with the replica process.** A restart resets
  every peak. For history that survives restarts, see
  [`mz_internal.mz_cluster_replica_metrics_history`](/reference/system-catalog/mz_internal/#mz_cluster_replica_metrics_history).

## Example

Introspection relations are replica-local: a query reads the replica that
serves it, so pin both the cluster and the replica. This reports how close each
process came to a memory-limiter kill, comparing the quantity the limiter
enforces against the limit it enforces:

```mzsql
SET cluster = <cluster_name>;
SET cluster_replica = <replica_name>;

SELECT
    u.process_id,
    round((max(u.value) FILTER (WHERE u.metric = 'heap'))::numeric      / 1073741824, 2) AS heap_gib,
    round((max(u.value) FILTER (WHERE u.metric = 'heap_peak'))::numeric / 1073741824, 2) AS heap_peak_gib,
    round(m.heap_limit::numeric / 1073741824, 2)                                         AS limit_gib,
    round(100 * (max(u.value) FILTER (WHERE u.metric = 'heap_peak'))::numeric / m.heap_limit, 1) AS peak_pct
FROM mz_introspection.mz_cluster_replica_resource_usage u
JOIN mz_cluster_replicas r
     ON r.name = current_setting('cluster_replica')
    AND r.cluster_id = (SELECT id FROM mz_clusters WHERE name = current_setting('cluster'))
JOIN mz_internal.mz_cluster_replica_metrics m
     ON m.replica_id = r.id AND m.process_id = u.process_id
WHERE u.source = 'proc_status'
GROUP BY u.process_id, m.heap_limit
ORDER BY u.process_id;
```

`peak_pct` is a lower bound, because `heap_peak` is: a spike shorter than the
sampling interval can slip through it. No source provides a matching upper
bound. The query stays within `proc_status` deliberately. The `cgroup` metrics
measure a different quantity, and `memory_current` plus `swap_current`
double-counts every swap-cached page.
