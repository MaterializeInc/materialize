# Memory pressure and swap thrashing

This is the most common root cause of a single replica whose persist reads and
writes are all slow while sibling replicas on the same backing store are fine.
The mechanism: the process working set exceeds physical RAM, the kernel pages
cold memory out to swap, and every later access to a swapped-out page is a major
page fault, a synchronous disk read that blocks the faulting thread. Under heavy
thrashing the runtime spends most of its wall-clock blocked on page-ins, so
every `.await` on an external call appears slow even though the store responded
instantly.

Production replicas may run on swap-enabled nodes (a dedicated nodepool, local
NVMe used as swap). Swap is meant as a soft cushion for transient spikes, not as
a place to hold a multiple of RAM resident. Sustained heavy swap is the incident.

## The decisive signal: major page faults

```promql
rate(mz_metrics_libc_ru_majflt_total{pod="<pod>"}[1m])
```

This is the process's own getrusage major-fault counter. Sustained rates on the
order of tens to hundreds of thousands per second mean the process is thrashing:
threads are continuously blocking on swap-in. Correlate the rate against the
incident window. It typically peaks during the stall and decays afterward,
matching when the slow operation finally completed.

NOTE: this metric is served on the same `/metrics` endpoint that blanks out
during a hard stall, so `rate()` over the exact gap can return empty. Query the
raw counter (`mz_metrics_libc_ru_majflt_total{pod="<pod>"}` as a range) and
compute deltas across the surrounding samples by hand. The jump across the gap
is what you want.

Sibling rusage metrics that corroborate: `mz_metrics_libc_ru_minflt_total`
(minor faults), `mz_metrics_libc_ru_nivcsw_total` (involuntary context switches,
also a CPU-pressure signal), `mz_metrics_libc_ru_maxrss_bytes`.

## Quantify the footprint

```promql
container_memory_rss{pod="<pod>", container="<container>"}
container_memory_swap{pod="<pod>", container="<container>"}
max(kube_pod_container_resource_limits{pod="<pod>", resource="memory"})
```

* `container_memory_rss` near the cgroup memory limit means the container is
  pinned at its RAM ceiling.
* `container_memory_swap` is the cgroup memory+swap total. When it greatly
  exceeds the RAM limit (and physical node RAM), the difference is resident in
  swap. Hundreds of GB in swap is unambiguous thrashing.
* Use `max()` on the limit metric. kube-state-metrics often exposes duplicate
  series (multiple scrape endpoints), which makes a bare `/` join fail with a
  many-to-many error.

## What it looks like together

* One replica's external ops all in seconds, including trivial point reads.
* Sibling replicas on the same store at milliseconds.
* `ru_majflt` rate spiking into the tens or hundreds of thousands per second.
* mem+swap far above the RAM limit.
* App `/metrics` and node cadvisor scrapes blanking out over the stall window.
* On-CPU profile near-idle for the stalled subsystem (time spent off-CPU).

## Fixes to recommend

* Relieve memory: size the replica up, or reduce the dataflow working set, so it
  fits in RAM with swap as headroom rather than primary storage.
* Alert on `rate(mz_metrics_libc_ru_majflt_total)` and `container_memory_swap`
  for swap-enabled replicas. A single downstream warning understates how long
  the replica was actually thrashing.
* Frame the original "slow op" warning as a symptom so responders fix memory,
  not the persist/consensus layer.
