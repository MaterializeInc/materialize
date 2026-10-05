# Parked TPC-H on environmentd, r8gd.4xlarge

These are the results of `run-fair.sh` on the instance in `misc/scratch/pool-file-extents.json` (123 GiB usable RAM, 16 vCPUs, kernel 7.0.0-1006-aws, instance-store NVMe formatted ext4 at `/scratch`), at commit 85cb1dc125. `bin/environmentd --release` ran with `envd-start.sh` defaults: lz4 on, `column_paged_batcher_budget_fraction` 0.01 and `column_paged_batcher_pool_rss_target_fraction` 0.02 of physical RAM, which gives a slot budget of about 1.2 GiB and an RSS target of about 2.5 GiB. `setup-tpch.sh` ingested TPC-H, which took 4 minutes at scale factor 10 and 30 minutes at 100, and `mkempty.sh` created the empty table that keeps the parked view's frontier live.

`park-fair.sh` turns on `enable_compute_temporal_bucketing`, sets `compute_dataflow_max_inflight_bytes_cc` to 512 MiB for every arm, and `park-arm.sh` disables lgalloc. Each row is one `park-arm.sh` run on a fresh 8-worker replica, whose process runs under a systemd scope with `MemoryMax` set to the size's memory limit. `ind` parks every row in the arrange site's chunk batcher, and `mv` parks every row in the MV sink's correction buffer. Arm suffixes name the backing and the memory limit in GiB: `nospill` turns spilling off, `noback` spills with the file store off and swap off, `file` uses the file store with swap off, and `swap` uses a swapfile on the same NVMe (64 GiB at scale factor 10, 200 GiB at 100) with the file store off.

The settled columns are sampled 180 s after hydration, and pool metrics are scraped after that sample. Swapped-out and swapped-in GiB are host-wide `pswpout` and `pswpin` deltas over the run, which `park-arm.sh` recorded only for the scale factor 100 runs. Only one replica ran at a time, but environmentd and the idle source cluster share the host.

| Run | Outcome | Wall s | Max VmRSS MiB | memory.peak MiB | Settled VmRSS MiB | Settled memory.current MiB | Settled swap MiB | Extent bytes resident | Extent bytes on file | File writes | File reads | Pageouts | Swapped out GiB | Swapped in GiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| f10-ind-nospill32 | hydrated | 28.9 | 16086 | 16247 | 13439 | 13391 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| f10-ind-noback8 | exited | 29.9 | 8137 |  |  |  |  | 5267177472 | 0 | 0 | 0 | 0 |  |  |
| f10-ind-file8 | hydrated | 30.6 | 5164 | 5258 | 3038 | 2967 | 0 | 1159315456 | 4112646144 | 7262 | 3774 | 7262 |  |  |
| f10-ind-swap8 | hydrated | 31.1 | 5209 | 8192 | 2915 | 5810 | 2965 | 1160183808 | 0 | 0 | 0 | 6788 |  |  |
| f10-ind-noback4 | exited | 5.4 | 4014 |  |  |  |  | 0 | 0 | 0 | 0 | 0 |  |  |
| f10-ind-file4 | exited | 7.2 | 3981 |  |  |  |  | 0 | 0 | 0 | 0 | 0 |  |  |
| f10-ind-swap4 | hydrated | 38.5 | 4148 | 4096 | 1997 | 1940 | 4135 | 1160118272 | 0 | 0 | 0 | 6928 |  |  |
| f10-mv-nospill32 | hydrated | 32.4 | 10567 | 10562 | 8117 | 8058 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| f10-mv-noback8 | hydrated | 34.5 | 7952 | 8018 | 5687 | 5626 | 0 | 5581357056 | 0 | 0 | 0 | 0 |  |  |
| f10-mv-file8 | hydrated | 39.0 | 5075 | 5069 | 2741 | 2671 | 0 | 1160249344 | 4426301440 | 10004 | 7168 | 10004 |  |  |
| f10-mv-swap8 | hydrated | 38.5 | 4872 | 8030 | 2683 | 5678 | 3056 | 1159725056 | 0 | 0 | 0 | 9324 |  |  |
| f10-mv-noback4 | exited | 7.9 | 4072 |  |  |  |  | 0 | 0 | 0 | 0 | 0 |  |  |
| f10-mv-file4 | exited | 5.5 | 3956 |  |  |  |  |  |  |  |  |  |  |  |
| f10-mv-swap4 | hydrated | 48.7 | 4157 | 4096 | 2020 | 1963 | 3844 | 1159266304 | 0 | 0 | 0 | 8542 |  |  |
| f100-ind-noback8 | exited | 59.8 | 8122 |  |  |  |  | 2220539904 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| f100-ind-file8 | hydrated | 837.6 | 6366 | 8192 | 3871 | 5678 | 0 | 1158250496 | 60862758912 | 268119 | 219026 | 268111 | 0.0 | 0.0 |
| f100-ind-swap8 | hydrated | 760.9 | 5990 | 8192 | 3576 | 5811 | 44496 | 1159331840 | 0 | 0 | 0 | 266217 | 237.7 | 138.7 |
| f100-mv-noback8 | exited | 45.4 | 8189 |  |  |  |  | 4391469056 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| f100-mv-file8 | hydrated | 823.2 | 5246 | 8192 | 2720 | 5731 | 0 | 1159200768 | 65081933824 | 277451 | 236029 | 277435 | 0.0 | 0.0 |
| f100-mv-swap8 | hydrated | 815.3 | 5007 | 8192 | 2619 | 5763 | 44501 | 1160118272 | 0 | 0 | 0 | 274401 | 287.2 | 175.1 |

Every exited run reached a VmRSS within 4% of its memory limit before the process disappeared, which matches a cgroup OOM kill, but the journal was not checked. `memory.peak` reads 0 once the cgroup is gone, so it is blank for exited runs. For exited runs the script scrapes metrics about 30 s after the exit, through the dead process's socket path. The process orchestrator relaunches a replica 5 s after it exits, so these metrics most likely describe the relaunched process partway through its own hydration, and blank cells are scrapes that returned nothing.

A cgroup's `memory.current` counts page cache, which here is mostly persist's local blob files. That is why the swap arms' and the scale factor 100 file arms' `memory.peak` reach or approach the limit while their VmRSS stays well below it. None of them was OOM-killed.
