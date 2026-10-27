# Out-of-core hydration experiment

`run.py` measures hydration time of TPCH indexes and materialized views on a
staging region, with one arm per buffer-pool configuration. The module
docstring describes the phases and the cluster layout.

## Arms

Each arm owns the clusters `idx_<arm>` and `mv_<arm>`. LaunchDarkly rules on the
replica context's `cluster_name` set the flags that differ from the staging
fallthrough. The fallthrough itself is the `pool_lz4` arm.

| Arm | Deviation from the staging fallthrough |
|---|---|
| `columnation` | `enable_column_paged_batcher = false`, `enable_column_paged_batcher_spill = false`, `enable_compute_correction_v2_spill = false` |
| `chunked` | `enable_column_paged_batcher_spill = false`, `enable_compute_correction_v2_spill = false` |
| `pool_raw` | `column_chunk_compress_min_depth = 255` |
| `pool_lz4` | none |
| `pool_lz4_1` | `column_paged_batcher_budget_fraction = 0.01` |

`column_chunk_compress_min_depth = 255` disables compression in practice, because
no chunk reaches generation 255. `column_paged_batcher_lz4` only affects the old
pager and has no effect on the buffer pool.

## Running

```
python3 misc/experiments/ooc-hydration/run.py --mz-bin target/debug/mz \
  --config <writable mz.toml> --size M.1-3xlarge --outdir <dir>
```

The run is resumable: it skips (phase, trial, size) cells already present in
`windows.csv`. `--phases` and `--arms` select subsets, so the index and MV
phases can run as two concurrent processes with separate `--outdir`s.

## Memory

Read the peaks from Prometheus for each window in `windows.csv`:
`container_memory_working_set_bytes` and `container_memory_swap` for the
replica pod, and `mz_column_pool_*` to confirm the arm. The `columnation` and
`chunked` arms must write no pool extents, and `pool_raw` must record no
compressed evictions.
