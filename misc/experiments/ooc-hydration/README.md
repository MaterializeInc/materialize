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
| `pool_lz4_b50` | `column_paged_batcher_budget_fraction = 0.5` |
| `pool_lz4_rss50` | `column_paged_batcher_pool_rss_target_fraction = 0.5` |

`column_chunk_compress_min_depth = 255` disables compression in practice, because
no chunk reaches generation 255. With `pool_lz4_b50` the budget exceeds the default
RSS target of 0.25, which leaves the compressed-resident tier no capacity.
`pool_lz4_rss50` doubles that tier's ceiling at the default budget.
The default arm list omits these two, pass them with `--arms`. `column_paged_batcher_lz4` only affects the old
pager and has no effect on the buffer pool.

## Running

```
python3 misc/experiments/ooc-hydration/run.py --mz-bin target/debug/mz \
  --config <writable mz.toml> --size M.1-3xlarge --outdir <dir>
```

The run is resumable: it skips (phase, trial, size) cells already present in
`windows.csv`. `--phases` and `--arms` select subsets, so the index and MV
phases can run as two concurrent processes with separate `--outdir`s.

`--parallel-trials` runs all trials of a phase at once, trial n on clusters
suffixed `_t<n>`. Add those cluster names to the LaunchDarkly rules first, and
give `mv_first` at least as many trials as `mv_second`, because each
second-write trial rehydrates the MVs of the first-write trial in its slot.
More replicas share more nodes, so check pod placement when comparing arms.

## Memory

Read the peaks for each window in `windows.csv` from
`mz_internal.mz_cluster_replica_metrics_history`, sampled once a minute.
`memory_bytes` is RSS and `heap_bytes - memory_bytes` is swap; cast to
`numeric` before subtracting, since both are unsigned. Pool extents are
swap-backed, so swap includes them. Staging Prometheus had only a handful of
cAdvisor samples per window.

Confirm the arm from `mz_column_pool_extent_bytes_written_total`: the
`columnation` and `chunked` arms must write no extents, and `pool_raw` writes
more extent bytes than `pool_lz4`. `mz_column_pool_evictions_compress_total`
counts every extent-writing eviction regardless of codec, so it does not tell
`pool_raw` apart.
