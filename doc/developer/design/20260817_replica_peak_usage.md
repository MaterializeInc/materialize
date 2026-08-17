# Design: Replica Peak Resource Usage

## Summary

Replica resource usage is visible only as periodic samples taken by the orchestrator, roughly one
per minute, landing in `mz_cluster_replica_metrics_history`. A memory spike between two samples is
invisible, so the usage of a hydration episode that starts and finishes inside one sampling gap
cannot be recovered at all.

This adds a measured high-water mark instead: each replica process tracks the peak memory, heap
and disk usage it has reached, and compute introspection exposes those peaks as
`mz_introspection.mz_cluster_peak_usage`.

## Semantics

Peaks are monotonic for the lifetime of the process, and are never reset.

The alternative, max-since-last-flush, makes a window's peak recoverable by taking the maximum of
the rows in that window. It also makes every reader destructive: whoever reads first consumes the
value, a retried read loses a peak, and two consumers cannot both see the same episode. In a
system where the peak is read by ad-hoc SQL, that is not workable.

Monotone peaks still answer the question hydration visibility asks. A replica starts fresh, so at
the moment it finishes hydrating, its since-start peak *is* its hydration peak. More generally,
for a monotone series `M`, the peak over a window `(t1, t2]` is `M(t2)` whenever `M(t2) > M(t1)`,
and is otherwise bounded above by `M(t1)`.

The cost is that peaks live and die with the process. A restarted replica reports the peaks of its
new process, and the old ones are gone. Persisting peaks across replica lifetimes is deliberately
not part of this work.

## Precision

`memory_bytes` is exact. `getrusage`'s `ru_maxrss` is a high-water mark the kernel maintains
itself, so no spike can pass between two of our observations unseen.

`heap_bytes` and `disk_bytes` have no such kernel-side counter, so they are maxima over samples
and are therefore lower bounds: a spike shorter than the sampling interval can be missed. The
sampling interval is `mz_metrics_peak_usage_refresh_interval` (5s by default), separate from
`memory_limiter_interval`, which governs OOM-kill behavior and must not be retuned for
introspection's sake.

`heap_bytes` folds in the `memory_bytes` peak, since peak heap is at least peak memory. That keeps
`heap_bytes >= memory_bytes` true even when `ru_maxrss` catches a spike that sampling missed.

## Where the peaks are measured

In `mz_metrics::usage`, on the periodic task that already samples `rusage` and lgalloc stats. That
task runs on the tokio runtime, independent of the timely workers.

The tempting alternative, folding the maximum inside the compute logging operator, samples exactly
where sampling is least reliable: a logging operator only runs when its worker schedules it, and a
worker saturated by hydration is precisely the case whose peak we want. Keeping the fold in the
sampler also means a slow reader cannot lose a peak, because it reads an already-monotone value
rather than a series of instantaneous ones.

## How the peaks reach SQL

A new `ComputeLog::PeakUsage` logging dataflow reads the process-global peaks and emits one row
per process, following `ComputeLog::PrometheusMetrics`: the usage is per-process, not per-worker,
so one worker per process reports and the rest drop their capability.

The peaks are also registered as Prometheus gauges (`mz_metrics_peak_*_bytes`), which costs
nothing extra since the sampler already holds the values, and which makes them scrapable without
going through SQL.

## Alternatives considered

- **Extend `/api/usage-metrics` and `mz_cluster_replica_metrics_history`.** Gets 30-day retention
  and after-the-fact queryability for free. Rejected as the primary surface because the transport
  is a poll of an HTTP endpoint by the orchestrator, so reporting a peak means either making the
  endpoint destructive or duplicating peaks across polls, and because the peaks then inherit the
  orchestrator's poll cadence. Worth revisiting as the persistence story.
- **Reuse `mz_cluster_prometheus_metrics`.** The gauges appear there automatically, so this needs
  no catalog change at all. Rejected as the primary surface: values are untyped `double`, the
  relation is a debugging escape hatch rather than a documented contract, and it is gated by its
  own scrape-interval dyncfg, so a config change would silently remove the peaks.
