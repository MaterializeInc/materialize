---
source: src/persist-client/src/cfg.rs
revision: b8f9449bec
---

# persist-client::cfg

Defines `PersistConfig`, the central configuration struct holding all tunable knobs for persist (blob target size, compaction heuristics, retry parameters, feature flags, etc.).
Most knobs are backed by `mz_dyncfg::Config` entries that can be updated at runtime via `ConfigUpdates`.
The file also documents the trade-offs between each tunable dimension (write amplification, read amplification, memory usage, S3 operation count).
