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

* Linux with swap enabled on fast storage (this work ran on `r8gd.16xlarge` with 3.5 TiB of NVMe swap)
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

| size      | mem   | disk  | ratio | hydration | peak RSS | peak swap | pswpout | vs baseline |
|-----------|-------|-------|-------|-----------|----------|-----------|---------|-------------|
| baseline  | 128GiB| 64GiB | 1:0.5 | 81s       | 15.0 GiB | 0         | 0       | 1.00x       |
| swap_1-2  | 11GiB | 21GiB | 1:2   | 90s       | 11.5 GiB | 3.3 GiB   | 1.4M    | 1.11x       |
| swap_1-5  | 5GiB  | 27GiB | 1:5   | 172s      | 5.3 GiB  | 9.2 GiB   | 9.5M    | 2.12x       |
| swap_1-12 | 2.5GiB| 29.5GiB| 1:12 | 342s      | 2.7 GiB  | 13.3 GiB  | 27.3M   | 4.22x       |
| swap_1-30 | 1GiB  | 31GiB | 1:30  | 576s      | 1.1 GiB  | 13.2 GiB  | 49.1M   | 7.11x       |

Baseline working set is 15 GiB. Ratios that cap RSS below the working set pay swap cost roughly proportional to (working_set − memory_limit).
Above ~1:5 the hydration slowdown grows steeply; 1:30 thrashes (~200 GiB of page-out traffic during hydration).
