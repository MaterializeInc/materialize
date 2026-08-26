---
source: src/persist/src/cfg.rs
revision: db15d3b2dc
---

# persist::cfg

Provides `BlobConfig` and `ConsensusConfig` enums that enumerate all supported storage backends and carry the configuration needed to open them.
`BlobConfig::try_from` parses a URI into the appropriate variant (file, S3, Azure, mem, turmoil) and `BlobConfig::open` instantiates the corresponding `Blob` implementation.
`ConsensusConfig` mirrors this pattern for consensus backends (Postgres, mem, turmoil; FoundationDB is available behind the `foundationdb` feature flag).
`BlobKnobs` defines timeout and sizing parameters consumed by both S3 and Azure backends.
`all_dyn_configs` registers all `mz_persist` dynamic configs into a `ConfigSet`; it adds `PG_CONSENSUS_READ_COMMITTED` from `crate::postgres` and five hedge configs (`BLOB_HEDGED_GET_ENABLED`, `BLOB_HEDGED_GET_DELAY`, `BLOB_HEDGED_GET_MAX_CONCURRENT`, `BLOB_HEDGED_GET_BUDGET_RATIO`, `BLOB_HEDGED_GET_WARM_INTERVAL`) from `crate::hedge`.
`open_hedge_sibling` is an async function that attempts to open a second, connection-pool-isolated `Blob` handle on the same URL for use by `HedgedBlob`; backends where isolation is impossible (`File`, `Mem`, `Turmoil`) return `HedgeSibling::SharedWithPrimary`, and on failure it returns `HedgeSibling::Unavailable` with a warning rather than propagating the error.
