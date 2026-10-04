# pool_extents on r8gd.4xlarge, file mode

These are the results of `pool-matrix.sh` on the instance in `misc/scratch/pool-file-extents.json`, run with an earlier revision of the `pool_extents` harness (`src/ore/examples/pool_extents.rs`) than the committed one. The machine runs kernel 7.0.0-1006-aws, and the store sits on the instance-store NVMe formatted ext4 at `/scratch`.

Every run inserts 16384 chunks of 2 MiB, 32 GiB in total. The pool budget is 4096 MiB with 4 spill threads. Bodies are half a repeating pattern and half random, which gives a stored-to-body ratio of 0.504. 30% of chunks die 4096 inserts after their own insert. A churn phase of 8192 replacements follows, then a read phase with 16 readers of 2000 reads each. Rows list only the arguments that differ from that base.

The demotion rate divides the bytes written to the store by the wall time of fill, drain, churn and the second drain. That earlier revision's "demotion rate" line divided by fill and drain only, which overstated it by about 1.5 times, so this table uses the recomputed value, and the committed harness divides by the same four phases.

| Run | Arguments | Tier cap MiB | VmHWM MiB | Demotion GiB/s | Elision rate | Inline share | Spill at max, fill | Read p50 / p99 ms | Reads/s |
|---|---|---|---|---|---|---|---|---|---|
| base | `--rss-target-mib 8192` | 3584 | 7018 | 0.82 | 0.325 | 0.000 | 0.74 | 8.29 / 8.96 | 3198 |
| rss-0.1 | `--rss-target-mib 4506` | 0 | 4174 | 0.88 | 0.000 | 0.142 | 0.07 | 8.86 / 8.95 | 2322 |
| rss-0.25 | `--rss-target-mib 5120` | 512 | 4818 | 0.88 | 0.030 | 0.054 | 0.89 | 8.85 / 8.96 | 2414 |
| rss-0.5 | `--rss-target-mib 6144` | 1536 | 6199 | 0.87 | 0.076 | 0.031 | 0.86 | 8.82 / 8.96 | 2629 |
| readers-1 | base, `--readers 1 --reads 20000` | 3584 | 6667 | 0.84 | 0.322 | 0.000 | 0.71 | 0.67 / 0.77 | 1892 |
| readers-64 | base, `--readers 64 --reads 1000` | 3584 | 6975 | 0.84 | 0.325 | 0.000 | 0.70 | 14.27 / 90.42 | 3039 |
| spill-1 | base, `--spill-threads 1` | 3584 | 6893 | 0.82 | 0.325 | 0.000 | 0.68 | 8.31 / 8.97 | 3174 |
| spill-2 | base, `--spill-threads 2` | 3584 | 6961 | 0.84 | 0.325 | 0.000 | 0.69 | 8.32 / 8.96 | 3171 |
| spill-8 | base, `--spill-threads 8` | 3584 | 7012 | 0.84 | 0.325 | 0.000 | 0.71 | 8.33 / 8.97 | 3168 |
| identity-0.2 | base, `--identity-fraction 0.2` | 3584 | 8467 | 0.85 | 0.326 | 0.000 | 0.76 | 9.42 / 12.65 | 2547 |
| capacity-8g | base, `--file-capacity-mib 8192` | 3584 | 9458 | 0.57 | 0.433 | 0.000 | 0.48 | 0.42 / 8.95 | 4306 |

How to read the columns:
* **Elision rate** is `extent_demotions_elided / (extent_demotions_elided + extent_pageouts)`.
* **Inline share** is `extent_file_writes_inline / extent_file_writes`.
* **Spill at max** is the share of 1 ms samples during fill with `spill_in_flight` at `SPILL_IN_FLIGHT_MAX` (64).
* **Read latencies** are closed-loop `read_into` service times and include resident hits.

Results that held in every run:
* **No errors:** there were no write errors.
* **Store full only in capacity-8g:** `extent_file_full` was nonzero only in that run.
* **Repeat reads:** they were 66% of file reads, 52% at 1 reader and 82% at 64. The harness reads random live chunks with plain reads, which never admit.
* **Allocated space:** after the churn phase it exceeded live slot bytes by 0.9 to 1.7 GiB. Nothing was punched, because the volume never ran short.
