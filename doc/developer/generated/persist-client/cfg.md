---
source: src/persist-client/src/cfg.rs
revision: b33ffcb977
---

# persist-client::cfg

Defines `PersistConfig`, the central configuration struct holding all tunable knobs for persist (blob target size, compaction heuristics, retry parameters, feature flags, etc.).
Most knobs are backed by `mz_dyncfg::Config` entries collected via `all_dyncfgs` that can be updated at runtime via `ConfigUpdates::apply_from`.
`PersistConfig` derefs to `ConfigSet` for convenient dynamic config access.
Static fields cover build version, hostname, compaction concurrency/queue limits, writer lease duration, critical downgrade interval, and isolated runtime thread count.
`RetryParameters` encapsulates initial backoff, multiplier, and clamp for retry loops throughout persist.
The file also documents the trade-offs between each tunable dimension (write amplification, read amplification, memory usage, S3 operation count).
Includes version-compatibility helpers `code_can_read_data` and `code_can_write_data` and CRDB connection pool configs.
