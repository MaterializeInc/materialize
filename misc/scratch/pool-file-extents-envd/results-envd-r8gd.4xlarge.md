# Parked TPC-H on environmentd, r8gd.4xlarge

These are the results of `park-matrix.sh` on the instance in `misc/scratch/pool-file-extents.json` (128 GiB RAM, kernel 7.0.0-1006-aws, instance-store NVMe formatted ext4 at `/scratch`, swap off). `bin/environmentd --release` ran with `envd-start.sh` defaults: lz4 on, `column_paged_batcher_budget_fraction` 0.01 and `column_paged_batcher_pool_rss_target_fraction` 0.02 of physical RAM, which gives a slot budget of about 1.3 GiB and an RSS target of about 2.6 GiB. `setup-tpch.sh 10` ingested TPC-H at scale factor 10, and `mkempty.sh` created the empty table that keeps the parked view's frontier live.

`park-matrix.sh` turns on `enable_compute_temporal_bucketing`, sets `compute_dataflow_max_inflight_bytes_cc` to 512 MiB, and disables lgalloc. Each row is one `park-arm.sh` run on a fresh 8-worker replica, whose process runs under a systemd scope with `MemoryMax` set to the size's memory limit. `index` parks every row in the arrange site's chunk batcher, and `mv` parks every row in the MV sink's correction buffer. "No backing" runs with spill on and the file backend off, so on a swap-less host the compressed tier stays in RAM.

The settled columns are sampled 180 s after hydration, and pool metrics are scraped after that sample.

| Run | Kind | Memory limit | Backing | Outcome | Wall s | Max VmRSS MiB | memory.peak MiB | Settled VmRSS MiB | Extent bytes resident | Extent bytes on file | File writes | File reads |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| r-ind-noback8 | index | 8 GiB | none | exited | 25.4 | 8146 | | | 5255004160 | 0 | 0 | 0 |
| r-ind-file8 | index | 8 GiB | file | hydrated | 31.1 | 5635 | 5570 | 3022 | 1159118848 | 4103340032 | 6612 | 3142 |
| r-ind-noback4 | index | 4 GiB | none | exited | 7.9 | 3915 | | | 183173120 | 0 | 0 | 0 |
| r-ind-file4 | index | 4 GiB | file | exited | 5.7 | 3977 | | | | | | |
| r-mv-noback8 | mv | 8 GiB | none | exited | 33.5 | 8091 | | | 4773642240 | 0 | 0 | 0 |
| r-mv-file8 | mv | 8 GiB | file | hydrated | 36.3 | 5292 | 5239 | 2779 | 1160249344 | 4451467264 | 9979 | 7128 |
| r-mv-noback4 | mv | 4 GiB | none | exited | 3.7 | 4102 | | | | | | |
| r-mv-file4 | mv | 4 GiB | file | exited | 3.5 | 3680 | | | 0 | 0 | 0 | 0 |

Every exited run reached a VmRSS within 11% of its memory limit before the process disappeared, which matches a cgroup OOM kill, but the journal was not checked for these runs. `memory.peak` reads 0 once the cgroup is gone, so it is blank for them. For exited runs the script scrapes metrics about 30 s after the exit, through the dead process's socket path. The process orchestrator relaunches a replica 5 s after it exits, so these metrics most likely describe the relaunched process partway through its own hydration, not the moment of death. Blank cells are scrapes that returned nothing.

`mz_column_pool_resident_bytes`, the slot tier, sat between 1152 and 1292 MB in every run that reported it. The settled file runs' cgroup `memory.current` was 2951 MiB (index) and 2708 MiB (mv), below their VmRSS, so page cache was negligible.

## Swap and unspilled reference

`park-swap.sh` ran on the same environmentd before `park-matrix.sh`, without `compute_dataflow_max_inflight_bytes_cc`, so persist read-ahead is unbounded in these rows. The swap rows use a 64 GiB swapfile on the same NVMe, with the file store off. The `nospill` rows turn spilling off on a 32 GiB replica.

| Run | Kind | Memory limit | Backing | Outcome | Wall s | Max VmRSS MiB | memory.peak MiB | Settled VmRSS MiB | Settled swap MiB | Settled memory.current MiB | Extent bytes resident | Extent pageouts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| q-ind-nospill | index | 32 GiB | spill off | hydrated | 21.0 | 21856 | 21910 | 14458 | 0 | 14412 | 0 | 0 |
| q-ind-swap8 | index | 8 GiB | swap | hydrated | 33.0 | 8238 | 8192 | 2936 | 3373 | 4280 | 1149321216 | 7899 |
| q-ind-swap4 | index | 4 GiB | swap | hydrated | 52.6 | 4146 | 4096 | 2641 | 4422 | 2628 | 1159397376 | 8899 |
| q-mv-nospill | mv | 32 GiB | spill off | hydrated | 24.6 | 15598 | 15735 | 8373 | 0 | 8314 | 0 | 0 |
| q-mv-swap8 | mv | 8 GiB | swap | hydrated | 42.8 | 8239 | 8192 | 2634 | 3312 | 5170 | 1159725056 | 10027 |
| q-mv-swap4 | mv | 4 GiB | swap | hydrated | 62.3 | 4155 | 4096 | 2577 | 3739 | 2963 | 1160511488 | 9932 |

In the same series, the no-backing and file arms without the read-ahead bound exited within 1.2 to 3.9 s at both limits, which is what led to the bound in `park-matrix.sh`.
