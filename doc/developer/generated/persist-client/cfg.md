---
source: src/persist-client/src/cfg.rs
revision: 70f75e4f0e
---

# persist-client::cfg

Defines `PersistConfig`, the central configuration struct holding all tunable knobs for persist (blob target size, compaction heuristics, retry parameters, feature flags, etc.).
Most knobs are backed by `mz_dyncfg::Config` entries collected via `all_dyncfgs` that can be updated at runtime via `ConfigUpdates::apply_from`.
`PersistConfig` derefs to `ConfigSet` for convenient dynamic config access.
Static fields cover build version, hostname, compaction concurrency/queue limits, writer lease duration, critical downgrade interval, and isolated runtime thread count.
`RetryParameters` encapsulates initial backoff, multiplier, and clamp for retry loops throughout persist.
The file also documents the trade-offs between each tunable dimension (write amplification, read amplification, memory usage, S3 operation count).
Includes version-compatibility helpers `code_can_read_data` and `code_can_write_data` and CRDB connection pool configs.
Two dynamic configs support the persist source operators: `SOURCE_HYDRATION_FRONTIER_COALESCE_BYTES` (`persist_source_hydration_frontier_coalesce_bytes`, default 0) causes the persist source to coalesce frontier downgrades during hydration catch-up until the given number of encoded bytes have been emitted (0 disables coalescing); `SOURCE_FETCH_CONCURRENCY` (`persist_source_fetch_concurrency`, default 1) sets the maximum number of concurrent part fetches per worker in the persist source (1 preserves serial behavior).
