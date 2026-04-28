# Swap ratio evaluation

Measures the latency cost of running Materialize cluster replicas with memory:disk ratios greater than 1:1, using Linux swap on local NVMe as the disk backing.

## Motivation

Staging and production replica sizes scale cpu, memory, and disk proportionally.
This prevents isolating the effect of memory pressure — every size change moves all three dimensions at once.
GCP offers cluster SKUs with extreme memory:disk ratios (1:12, 1:30), and we need an honest estimate of how hydration and point-query latency degrade as the working set exceeds available RAM.
Running Materialize on a single Linux host with user-defined replica sizes and system swap enabled lets us sweep the ratio while holding cpu, workers, and workload constant.

## How it works

Environmentd launches with a custom `--cluster-replica-sizes` map covering a baseline (large memory, no swap pressure) and four swap-targeted sizes (1:2, 1:5, 1:12, 1:30).
All sizes set `swap_enabled: true`, so clusterd gets `--heap-limit = memory_limit + disk_limit` via `src/controller/src/clusters.rs`.
The sweep script creates a user cluster `lg` at each size, runs the AuctionScenario setup SQL (a self-joining time-window load generator borrowed from `test/cluster-spec-sheet`), creates an index on `bids(id)` to force hydration, then runs 200 random point lookups under `serializable` isolation.
A background thread samples the clusterd PID's `/proc/$pid/status` (VmRSS, VmSwap) and `/proc/vmstat` (pswpin, pswpout) once per second, writing a per-size CSV.
The total memory+disk budget is held constant across swap sizes (~32 GiB heap-limit) so only the split between RAM and swap varies.

## Files

* `swap_sizes.json` — replica size map passed to `--cluster-replica-sizes`
* `sweep.py` — driver: setup, hydration, steady-state queries, /proc sampling

## Running

Prereqs on the host:

* Linux with swap enabled on fast storage (this work ran on `r8gd.16xlarge` with 3.5 TiB of NVMe swap — `bin/scratch create r8gd.16xlarge.json` provisions a matching instance)
* Cockroach reachable at `localhost:26257`
* A Materialize checkout with `bin/environmentd --optimized` built

Launch environmentd with the custom size map:

```bash
bin/environmentd --optimized -- \
  --cluster-replica-sizes="$(cat test/swap-eval/swap_sizes.json)" \
  --bootstrap-default-cluster-replica-size=swap_1-2 \
  --bootstrap-builtin-system-cluster-replica-size=tiny \
  --bootstrap-builtin-catalog-server-cluster-replica-size=tiny \
  --bootstrap-builtin-probe-cluster-replica-size=tiny \
  --bootstrap-builtin-support-cluster-replica-size=tiny \
  --bootstrap-builtin-analytics-cluster-replica-size=tiny
```

Run the sweep (requires `psycopg` — `misc/python/venv/bin/python` already has it):

```bash
for sz in baseline swap_1-2 swap_1-5 swap_1-12 swap_1-30; do
  misc/python/venv/bin/python test/swap-eval/sweep.py \
    --size "$sz" --scale 4 --hydration-wait 900 --query-count 200
done
```

Results land in `/home/ubuntu/swap_results/`:

* `summary.csv` — one row per run with hydration wall time, arrangement size, point-query percentiles
* `<size>_s<scale>.csv` — per-second /proc samples
* `<size>_s<scale>_lat.txt` — raw per-query latencies (ms)

## Caveats

* Local process orchestrator: `memory_limit` and `disk_limit` are advisory inside clusterd (drive `--announce-memory-limit` and jemalloc `--heap-limit`), but the OS does not cgroup-enforce the RAM cap.
  Kernel swap activates naturally as allocations exceed physical RAM, not because of a container limit.
* Blob storage is local filesystem (`file://`), not S3.
  Object-store latency is not part of the measurement.
* Scratch directory shares the root disk with the OS, not the NVMe (both NVMes are dedicated to swap).
  Spill-to-disk IO competes with root disk traffic.
* Single-node: no network effects, no multi-replica coordination.
* The AuctionScenario setup here is a lightly trimmed copy of `test/cluster-spec-sheet/mzcompose.py::AuctionScenario.setup()`.
  Keep the two in sync if the upstream scenario changes.

## Results snapshot

Collected on r8gd.16xlarge, scale=4, 200 queries per size, serializable isolation.

Hydration and memory traffic:

| size      | mem   | disk  | ratio | hydration | peak RSS | peak swap | pswpout | vs baseline |
|-----------|-------|-------|-------|-----------|----------|-----------|---------|-------------|
| baseline  | 128GiB| 64GiB | 1:0.5 | 81s       | 15.0 GiB | 0         | 0       | 1.00x       |
| swap_1-2  | 11GiB | 21GiB | 1:2   | 90s       | 11.5 GiB | 3.3 GiB   | 1.4M    | 1.11x       |
| swap_1-5  | 5GiB  | 27GiB | 1:5   | 166s      | 5.3 GiB  | 9.2 GiB   | 9.5M    | 2.05x       |
| swap_1-12 | 2.5GiB| 29.5GiB| 1:12 | 338s      | 2.7 GiB  | 13.3 GiB  | 27.3M   | 4.18x       |
| swap_1-30 | 1GiB  | 31GiB | 1:30  | 571s      | 1.1 GiB  | 13.2 GiB  | 49.1M   | 7.06x       |

Steady-state point-query latency (200 random `SELECT count(*) FROM bids WHERE id = ?` after hydration):

| size      | p50 ms | p95 ms | p99 ms  | mean ms |
|-----------|--------|--------|---------|---------|
| baseline  | 6.03   | 6.75   | 89.27   | 7.57    |
| swap_1-2  | 6.04   | 35.62  | 103.42  | 9.81    |
| swap_1-5  | 6.85   | 43.54  | 235.69  | 14.68   |
| swap_1-12 | 7.51   | 62.20  | 273.53  | 18.74   |
| swap_1-30 | 9.21   | 74.25  | 992.68  | 31.92   |

Takeaways:

* Baseline working set is 15 GiB. Ratios that cap RSS below the working set pay swap cost roughly proportional to `working_set − memory_limit`.
* Hydration slowdown grows steeply above ~1:5. 1:30 thrashes (~200 GiB of page-out traffic during hydration).
* Steady-state p50 barely moves across ratios — hot index pages stay resident.
* Tail latency grows with ratio: p95 climbs 5–11x, p99 explodes at 1:30 (993 ms).
* Baseline p99 of 89 ms is already elevated vs p95 of 6.75 ms; some outliers exist independent of swap and deserve separate investigation.

## Results 2026-04-28: instrumented sweep, MGLRU on vs off

Two back-to-back 5-size sweeps on the same r8gd.16xlarge host with the new instrumentation. The first was run with MGLRU at the kernel default (`lru_gen/enabled=0x0003`); the second after `echo 0 > /sys/kernel/mm/lru_gen/enabled` (`lru_gen/enabled=0x0000`). All other settings unchanged: scale=4, 200 queries per size, default `MALLOC_CONF`, kernel 6.17.0-1010-aws, 3.5 TiB NVMe swap split across both NVMes at priority 10.

### Hydration and direct-reclaim pressure

| size      | hydration (MGLRU on) | hydration (MGLRU off) | psi_full_avg10 (on) | psi_full_avg10 (off) |
|-----------|----------------------|------------------------|----------------------|------------------------|
| baseline  | 80s                  | 78s                    | 0.00                 | 0.00                   |
| swap_1-2  | 89s                  | 91s                    | 0.38                 | 13.90                  |
| swap_1-5  | 176s                 | 178s                   | 7.53                 | 15.54                  |
| swap_1-12 | 345s                 | 442s                   | 5.82                 | 10.80                  |
| swap_1-30 | 569s                 | 910s                   | 7.01                 | 10.05                  |

PSI `full_avg10` measures the share of the sampling window in which **all** runnable tasks were stalled on memory — by construction, kswapd does not block userspace, so any non-zero value is direct-reclaim or swap-IO wait on a user thread. With MGLRU on, swap_1-2 stalls 0.38% of the time; with MGLRU off, the same configuration stalls 13.9%. Hydration time at 1:30 grows from 569s → 910s when MGLRU is disabled, a 60% slowdown. This is the local reproduction of the production r8gd profile signature, which itself ran on a 6.12.73 kernel where MGLRU is shipped but defaults to off.

### Steady-state point queries

| size      | p50 (on) | p95 (on) | p99 (on) | p50 (off) | p95 (off) | p99 (off) |
|-----------|----------|----------|----------|-----------|-----------|-----------|
| baseline  | 6.09     | 7.71     | 145      | 6.03      | 6.94      | 140       |
| swap_1-2  | 6.09     | 7.70     | 125      | 6.12      | 7.52      | 152       |
| swap_1-5  | 6.92     | 43.89    | 199      | 7.05      | 55.53     | 305       |
| swap_1-12 | 8.23     | 99.08    | 592      | 8.03      | 98.17     | 133       |
| swap_1-30 | 12.08    | 103.69   | 913      | 11.63     | 98.97     | 962       |

p50 and p95 are stable across LRU policies; p99 is dominated by single outliers and varies between runs even at the same configuration.

### Memory traffic

| size      | peak RSS | peak swap (on) | peak swap (off) | majflt (on) | majflt (off) | pswpout (on) | pswpout (off) |
|-----------|----------|-----------------|------------------|-------------|--------------|---------------|----------------|
| baseline  | 15 GiB   | 0               | 0                | 0           | 0            | 0             | 0              |
| swap_1-2  | 11 GiB   | 2.7 GiB         | 3.3 GiB          | 0.7 M       | 0.8 M        | 1.2 M         | 1.4 M          |
| swap_1-5  | 5.1 GiB  | 9.8 GiB         | 9.3 GiB          | 6.3 M       | 6.7 M        | 9.8 M         | 10.1 M         |
| swap_1-12 | 2.6 GiB  | 12.6 GiB        | 3.3 GiB          | 20.0 M      | 1.8 M        | 27.3 M        | 30.9 M         |
| swap_1-30 | 1.1 GiB  | 12.1 GiB        | 5.1 GiB          | 37.8 M      | 5.6 M        | 47.8 M        | 69.3 M         |

The MGLRU-off run at 1:12 and 1:30 has significantly **lower** peak `vmswap_kb` despite higher `pswpout` totals — under traditional LRU the kernel evicts and re-faults the same pages more aggressively, so the same swap region churns without growing. The MGLRU-on run resident swap footprint stays larger because evicted pages are kept around longer.

### Takeaways

* PSI `full_avg10` is the direct-reclaim proxy on this host. The kswapd-vs-direct vmstat buckets (`pgscan_kswapd`, `pgscan_direct`, `pgsteal_kswapd`, `pgsteal_direct`, `pgrefill`, `pageoutrun`, `allocstall_normal`) stay at zero on kernel 6.17.0-1010-aws regardless of `lru_gen/enabled`. The toggle changes runtime behavior but not the attribution counters in this kernel build. Use PSI `full_avg10` plus `workingset_refault_anon` plus `pswpin/pswpout` instead.
* MGLRU is a real mitigation for the r8gd direct-reclaim chain. Switching from MGLRU to traditional LRU on this host raises `psi_full_avg10` 2-3× and adds 60% to hydration at 1:30. Production kernel 6.12.73 ships MGLRU off by default, which is the regime where the original r8gd profile was collected.
* Even with MGLRU on, the workload pays substantial PSI under swap: baseline 0% → 1:5 7.5% → 1:30 7.0% (saturating). The remaining stall budget is the swap-IO wait, not reclaim — addressing this needs a faster swap device, lower allocation rate (knob 5), or the userspace soft-throttle (knob 4) so allocations slow before hitting the hard ceiling.
* `workingset_refault_anon` only populates with MGLRU off (1.4 M at 1:2 → 75 M at 1:30). Treat it as a swap-thrash counter for traditional-LRU runs; for MGLRU runs use `pswpin` as the substitute.

### Caveats specific to this measurement

* Single sample per cell. Hydration time variance between back-to-back baseline runs was 80s → 78s (≈3%); other cells likely similar but not bounded.
* AuctionScenario hydration includes a `mz_now()`-keyed time window, so `arr_records` varies between runs (45 M to 108 M). Ratios are computed on relative metrics only.
* Monitor stop time (`hydration_wait + 30 = 930s`) was reached just before swap_1-30 finished hydration in the MGLRU-off run (910s hydration). Per-second samples for the last ~20s of that run may be truncated.
* MGLRU global toggle does not retroactively re-charge already-allocated memcgs, so the second sweep's clusterd processes (newly forked after the toggle) do see traditional LRU, but the rest of the system may carry MGLRU state from before. The `lg` cluster is dropped and recreated each iteration, so each clusterd is a fresh memcg.

## Experiment: r8gd vs r6gd hydration anomaly

Polar Signals on-CPU profiles (24h ending 2026-04-27, namespace `environment-a858fdc6-92f2-40d7-a30e-a231810f9c2a-0`, comm `clusterd`) show:

* **u92** (M.X1-large, r8gd, kernel 6.12.73): ~25.6% CPU in `el0_da` / `do_page_fault` / `handle_mm_fault`, with `do_swap_page` 9.9%, `swapin_readahead` 6.2%, and a direct-reclaim chain `try_charge_memcg` → `try_to_free_mem_cgroup_pages` → `shrink_node` at 10.0%.
* **u88** (M.1-large, r6gd, same kernel): ~10.8% on faults, no `do_swap_page`, no direct-reclaim chain in the top 80.

Cgroup config is identical (`memory_limit=62100MiB`, `disk_limit=372600MiB`, `swap_enabled=true`, 8 workers). Only the node selector differs. lgalloc is off in production (conflicts with swap). THP is `madvise` and Materialize never calls `MADV_HUGEPAGE`. K8s does not surface `memory.high`, so reclaim hits the hard ceiling and runs synchronously on the user thread.

**Working hypothesis.** r8gd's faster CPU drives a higher anonymous-allocation rate than the swap device can drain asynchronously, so the kernel falls back to direct reclaim on the timely worker thread, eating CPU that would otherwise do compute.

**Success signal for any mitigation.** `pgscan_direct` drops sharply while `pgscan_kswapd` stays flat or rises — i.e. user-thread reclaim converted into background reclaim.

### Diagnostic instrumentation (in `sweep.py` since 2026-04-28)

Per-second columns in `<tag>.csv`:

* `vmrss_kb`, `vmswap_kb` — `/proc/$pid/status`
* `minflt`, `majflt` — `/proc/$pid/stat` deltas (per-process fault counts)
* `psi_some_avg10`, `psi_full_avg10`, `psi_full_avg60` — `/proc/pressure/memory`
* `pswpin`, `pswpout`, `pgmajfault` — `/proc/vmstat` deltas (system-wide swap traffic)
* `pgscan_kswapd`, `pgscan_direct`, `pgsteal_kswapd`, `pgsteal_direct` — direct-vs-background reclaim attribution (the headline ratio; **stays at zero when MGLRU is enabled — see caveat below**)
* `pgscan_anon`, `pgscan_file`, `pgsteal_anon`, `pgsteal_file` — anon vs file scan/steal split (populated under both traditional LRU and MGLRU)
* `workingset_refault_anon`, `workingset_refault_file`, `workingset_activate_anon`, `workingset_restore_anon` — refault accounting; `workingset_refault_anon` directly measures pages re-faulted from swap and is the cleanest swap-thrash signal
* `pgrefill`, `pageoutrun`, `allocstall_normal` — kswapd loop count, direct-reclaim stall count (also zero under MGLRU)
* `thp_fault_alloc`, `thp_fault_fallback`, `thp_split_page`, `thp_collapse_alloc` — THP activity

Once-per-run snapshot in `<tag>_env.txt`:

* `uname -r`, `/sys/kernel/mm/lru_gen/enabled`, `/sys/kernel/mm/transparent_hugepage/enabled`, `/proc/sys/vm/swappiness`
* `/proc/swaps`
* `MALLOC_CONF` from `/proc/$clusterd_pid/environ`

CSV schema changed on 2026-04-28; pre-instrumentation runs (`baseline_s4.csv`, `swap_1-2_s4.csv` from the original snapshot) have only `ts,vmrss_kb,vmswap_kb,pswpin,pswpout`.

### Workloads

Run each knob across these scenarios:

* **TPCH hydration** (the colleague's harness): TPCH source cluster, then install indexes on TPCH queries (omit q20), measure time to all-indexes-hydrated. Scale factors 16, 32, 64. Both M.1-large and M.X1-large. SF=16 and SF=32 should not perturb (working set fits) and act as controls. SF=64 is where the regression appears.
* **PR 36252 sweep** (this directory's `sweep.py`): AuctionScenario hydration plus 200 random point lookups under serializable. Already samples `/proc/$pid/status` (VmRSS, VmSwap) and `/proc/vmstat` (full set since 2026-04-28).

For each (knob × workload × family) cell, capture: total wall time, peak `VmRSS`, peak `VmSwap`, integral of `pgscan_direct`, integral of `pgscan_kswapd`, peak `full avg10` PSI.

### Knobs (ordered by expected leverage)

Run each on M.X1-large SF=64 (TPCH harness) and on the local r8gd swap-eval host with the existing AuctionScenario sweep at scale=4. Record: hydration time, peak `VmRSS`, peak `VmSwap`, `pgscan_direct` integral, `pgscan_kswapd` integral, peak `psi_full_avg10`, `majflt` delta.

1. **`MALLOC_CONF=dirty_decay_ms:-1,muzzy_decay_ms:-1`** — stop jemalloc from `MADV_DONTNEED`-ing idle pages (theory: pages get re-faulted when next allocation reuses the address range, doubling fault work). Fallback: `dirty_decay_ms:60000` if peak RSS overshoots `memory_limit`.
2. **`MALLOC_CONF=...,narenas:8`** — pin arena count to worker count to reduce cross-thread frees and dirty-page churn. Run knob 1 alone, then 1+2, then 2 alone (knob 1 should dominate).
3. **Per-family ratio sweep** — re-run the existing 1:2/1:5/1:12/1:30 sweep on r6gd and r8gd separately (not testable on this single-host harness; needs k8s).
4. **Soft userspace throttle in `src/compute/src/memory_limiter.rs`** — patch in a soft pre-limit at 85% of `heap_limit` that pauses dataflow scheduling and forces compaction. K8s does not surface `memory.high`, so the only path to soft throttle is application-level.
5. **Allocation-rate reduction at hot sites** — `merge_batcher`, `TimelyStack::merge_from`, `persist_source::PendingWork::do_work`, `mz_join_core::Work::process`, `Vec::spec_extend`/`RawVecInner::finish_grow`. Per-file PRs.

Deprioritized (cheap but low expected leverage): `vm.swappiness`, `transparent_hugepage` mode, MGLRU.

### Caveat: kernel mismatch and MGLRU

The local r8gd swap-eval host runs kernel 6.17; production runs 6.12.73. The 6.17 default has MGLRU enabled (`/sys/kernel/mm/lru_gen/enabled = 0x0003`), and MGLRU bypasses the traditional reclaim counters: `pgscan_kswapd`, `pgscan_direct`, `pgsteal_kswapd`, `pgsteal_direct`, `pgrefill`, `pageoutrun`, and `allocstall_*` all stay at zero, regardless of how hard reclaim is working. The 2026-04-28 instrumented sweep on this host showed `pgscan_anon` climbing to 123 M and `majflt` to 38 M at 1:30, but every kswapd/direct counter at 0 — the headline ratio is unrecoverable from those counters.

Production kernel 6.12.73 ships MGLRU but defaults to **off**, so the kswapd/direct attribution is recoverable in prod. To match prod on this host, disable MGLRU before running the sweep:

```
sudo sh -c 'echo 0 > /sys/kernel/mm/lru_gen/enabled'
```

Cross-check the env.txt snapshot to confirm `lru_gen/enabled = 0x0000` before drawing conclusions about direct-vs-background reclaim. The `workingset_refault_anon` and `pgscan_anon`/`pgsteal_anon` counters work under both LRU policies and remain useful even with MGLRU on.

### What this does not test

* Off-CPU stalls. Polar Signals on this project exposes only on-CPU profiles, so the actual blocking-on-swap-in time is invisible. Use `bpftrace -e 'kprobe:do_swap_page'` or eBPF off-cpu profiles if needed.
* Per-operator attribution. Timely interleaves operators on a single thread; flame graphs lose op-level granularity. Try `query_source_report` filtered by `timely_scope=...`.

### Reporting template

Per run record:

* knob set (full env, sysctls, code patches applied)
* family (r6gd or r8gd), size (M.1-large or M.X1-large), scale factor
* hydration time, peak `VmRSS`, peak `VmSwap`
* integral `pgscan_direct`, integral `pgscan_kswapd`, ratio
* peak `psi_full_avg10`
* `majflt` delta, `minflt` delta
* one Polar Signals link with the time range pinned

A successful mitigation shows: lower hydration time, lower `pgscan_direct` integral, higher `pgscan_kswapd:direct` ratio, lower peak PSI.

### References

* Profile data: project `877cb2f3-7413-4755-8414-38231cb06bca` (Materialize/Cloud), `parca_agent:samples:count:cpu:nanoseconds:delta`
* Sweep harness: `test/swap-eval/sweep.py` (PR 36252)
* Replica size definitions: `Pulumi.staging.yaml` `M.1-large` and `M.X1-large`
* k8s memory wiring: `src/orchestrator-kubernetes/src/lib.rs:613-624`
* Swap heap-limit wiring: `src/controller/src/clusters.rs:661-772`
* Existing rusage exporter: `src/metrics/src/rusage.rs:113-114`
* Memory limiter (soft-throttle target): `src/compute/src/memory_limiter.rs`

## Next steps

Not yet done. Captured here so a follow-up session can pick up.

* **Larger workloads.** scale=4 gives a ~4 GiB arrangement and a 15 GiB clusterd RSS.
  The 1:2 size (11 GiB memory) already barely fits, while 1:12 and 1:30 are forced into swap for most of the working set.
  To exercise the larger-memory end of the spectrum, bump `--scale` to 8 or 16, or run multiple `lg` clusters side by side.
  Also worth trying the TPCH scenarios from `test/cluster-spec-sheet` (especially `tpch_mv_strong`) since they stress different access patterns than the self-joining interval expansion in AuctionScenario.
  Watch for clusterd OOMing at the `mem + disk` heap limit — increase `disk_limit` proportionally if that happens.
* **Profiling during swap.** Collect a CPU profile (samply or `perf record`) and a jemalloc heap profile on clusterd while under swap pressure.
  This answers whether the wall-time cost is dominated by page-fault handler work, by memory allocator churn, or by idle waits on swap IO.
  `mz_compute::arrangement` paths and persist reader decode are the first suspects at high ratios.
  The per-second VmRSS/VmSwap CSVs already give us the shape of the memory-traffic curve; pairing them with a flame graph closes the loop on where the time is actually going.
* **Outlier investigation.** Baseline p99 (89 ms) sits well above p95 (6.75 ms) even with zero swap activity.
  Likely candidates: persist compaction ticking on the index arrangement, a first-touch effect on a cold batch, or catalog maintenance.
  Worth confirming with a longer query phase (1000+ queries) or by excluding the first 10 queries from the percentile calculation.
* **Real object store.** `--persist-blob-url` points at the local filesystem here, so persist reads don't pay S3 latency.
  For an honest comparison to staging, point at a real bucket before re-running the sweep.
* **Ratio to keep comparing.** The current map fixes total heap budget (`memory_limit + disk_limit`) at 32 GiB across swap sizes, so only the split varies.
  If GCP publishes a different total budget at the 1:12/1:30 tier, update `swap_sizes.json` to match before drawing conclusions about those SKUs specifically.
