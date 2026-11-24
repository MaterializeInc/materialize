# Pager hydration experiment

Measures the impact of the column-paged batcher's spill-to-disk mechanism (the
"pager") on hydration time, comparing a paged-batcher replica against the legacy
columnation batcher across TPCH scale factors.

Full design and rationale: `doc/developer/design/20260716_pager_hydration_experiment.md`.

## What it does

Per scale factor it builds, over a TPCH load-generator source, a workload of
18 base-table pk/fk indexes, 5 indexed views, and 5 materialized views for the
join-heavy queries Q03, Q05, Q09, Q18, Q21.
It then measures how long two configurations take to hydrate that workload.

* `pager_on`: the column-paged batcher with spill and lz4.
* `pager_off`: the legacy columnation batcher.

Hydration time comes from `mz_internal.mz_compute_hydration_times.time_ns` for
index and join or aggregation MV exports, with wall-clock via
`mz_internal.mz_hydration_statuses` as the cross-check and the fallback.

The source uses a `TICK INTERVAL` so its write frontier never goes final.
This is required. With a bounded source the frontier is final and
`mz_hydration_statuses` reports a freshly added replica as hydrated before it
has loaded anything, which makes re-hydration timing meaningless.
The TPCH tick inserts and retracts, so total data size stays roughly constant.

## Two runners

### `run.py`: staging or cloud region

Drives a region over pgwire via `bin/mz`.
The `pager_on` and `pager_off` axis is two replicas in one cluster that differ
only by name.
The name-to-flag mapping is a per-replica scoped system parameter override,
applied out of band by the delivery-service config sync, not by this script
(see `doc/developer/design/20260609_scoped_feature_flags.md`).
The script only creates replicas named `pager_on` and `pager_off`, and verifies
the override resolved via `mz_internal.mz_replica_system_parameters`.

Prerequisites.

* A writable `mz` config path. `~/.config/materialize/mz.toml` is read-only in
  some sandboxes, so copy it somewhere writable and pass `--config`.
* The environment-wide pager flags set to the intended `pager_on` state
  (`enable_column_paged_batcher`, `enable_column_paged_batcher_spill`,
  `column_paged_batcher_lz4`, and the tuning knobs
  `column_paged_batcher_budget_fraction`, `column_paged_batcher_swap_pageout`).
  `pager_on` inherits these; `pager_off` carries a scoped
  `enable_column_paged_batcher=false` override.
* Replica sizes that are in the region's `allowed_cluster_replica_sizes`.
  `M.1-8xlarge` (whole machine, large disk) is the default.

Example.

```
python3 run.py \
  --config /path/to/writable/mz.toml \
  --region aws/us-east-1 --profile staging \
  --scale-factors 1,10,30,100 --trials 3 --size M.1-8xlarge --tick 1s \
  --outdir results
```

### `run_local.py`: local `bin/environmentd`

For fast iteration without a region.
Scoped per-replica parameters are not reproducible locally, so the pager axis is
a global `ALTER SYSTEM SET` applied sequentially: set the flag, add one replica,
hydrate, measure, drop, then flip the flag.
Application DDL and queries go to the external SQL port; `ALTER SYSTEM` goes to
the internal SQL port as `mz_system`.

Prerequisites.

* `bin/environmentd` running (with CockroachDB).
* Sizes are local `scale=1,workers=N` strings, small by default.

Example.

```
python3 run_local.py --scale-factors 1,3 --trials 3
```

## Outputs

* `results.csv`: one row per object per condition per trial:
  `sf, flavor, object, kind, trial, hydration_seconds, time_ns`.
* `windows.csv` (staging runner): per-trial replica id and wall-clock window,
  for correlating Grafana resource metrics.

Both runners are idempotent and resumable: a completed cell is skipped on
relaunch, and an already-ingested source is reused rather than re-created.

## Grafana

The staging runner does not pull metrics; it records the window manifest so an
operator or an MCP-enabled agent can pull per-window resource metrics from the
region's Prometheus.
Useful series, filtered by
`cluster_environmentd_materialize_cloud_replica_name`:

* `mz_memory_limiter_memory_usage_bytes`: peak memory (RAM plus swap).
* `mz_column_pager_paged_bytes_out_total`: pager spill volume (pager path only,
  not OS swap).
* `container_cpu_usage_seconds_total`, `container_memory_usage_bytes`: per pod.

## Notes

* `time_ns` is NULL for bare single-reduce materialized views (CLU-175) but
  populated for indexes and for join or aggregation MVs.
* Both conditions can reach disk. `pager_on` via the managed lz4 path,
  `pager_off` via kernel swap on a swap-enabled size. The pager metrics count
  only the managed path.
* The column pager bounds transient merge-batcher memory, not the final
  arrangement, so it does not lower peak RSS and does not by itself prevent an
  out-of-memory kill when the final arrangements exceed the replica's ceiling.
