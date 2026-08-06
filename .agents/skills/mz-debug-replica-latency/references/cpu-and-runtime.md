# CPU starvation and runtime stalls

A replica can stall even with healthy memory if its threads cannot get CPU, or
if the async runtime is blocked. The symptom overlaps with memory pressure
(every `.await` looks slow), so use these checks to distinguish.

## CPU throttling (cgroup CPU quota)

If the container hits its CPU limit, the kernel throttles it: runnable threads
are forcibly descheduled until the next quota period. Heavy throttling stalls
everything, including the futures running persist ops.

```promql
rate(container_cpu_cfs_throttled_seconds_total{pod="<pod>"}[1m])
rate(container_cpu_cfs_throttled_periods_total{pod="<pod>"}[1m])
/ rate(container_cpu_cfs_periods_total{pod="<pod>"}[1m])
```

The second expression is the fraction of scheduling periods that were throttled.
Sustained high values mean the container is CPU-starved by its own quota.

Involuntary context switches corroborate CPU contention at the process level:

```promql
rate(mz_metrics_libc_ru_nivcsw_total{pod="<pod>"}[1m])
```

A spike in involuntary switches means the scheduler is preempting the process
against its will, i.e. it wants CPU it cannot get. Voluntary switches
(`ru_nvcsw_total`) are the opposite, the process yielding (often blocking on
IO), which points back toward the memory/IO story.

## Async runtime blocked by synchronous work

A subtler cause: a blocking operation on a Tokio worker thread (a blocking
syscall, a long synchronous compute section, a contended std mutex held across
work) starves the runtime. Other tasks on that worker, including persist
heartbeats and consensus client futures, cannot make progress until it returns.

Signals:

* On-CPU profile shows a single subsystem dominating CPU for the whole window
  (real compute), while the stalled subsystem is absent. This differs from the
  memory-pressure case, where the on-CPU profile is near-idle overall because
  threads are off-CPU on page faults.
* No CPU throttling and no major-fault spike, yet ops are slow: look for a
  blocking section in the hot path on-CPU.

## Distinguishing the three runtime-stall causes

* Memory/swap: on-CPU near-idle, `ru_majflt` spiking, mem+swap over limit.
* CPU throttle: `cfs_throttled` high, `ru_nivcsw` spiking, on-CPU may be busy.
* Blocking work on runtime: on-CPU shows one subsystem burning CPU, no throttle,
  no fault spike.

## Fixes to recommend

* Throttling: raise the CPU limit, or reduce concurrent work on the replica.
* Blocking-on-runtime: move the offending work to a blocking thread pool
  (`spawn_blocking`), shorten the synchronous section, or remove the lock held
  across the await. Identify the exact frame from the on-CPU profile first.
