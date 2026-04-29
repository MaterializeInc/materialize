---
source: src/persist/src/cfg.rs
revision: fb595f2446
---

# persist::cfg

Provides `BlobConfig` and `ConsensusConfig` enums that enumerate all supported storage backends and carry the configuration needed to open them.
`BlobConfig::try_from` parses a URI into the appropriate variant (file, S3, Azure, mem, turmoil) and `BlobConfig::open` instantiates the corresponding `Blob` implementation.
`ConsensusConfig` mirrors this pattern for consensus backends (Postgres, FoundationDB, mem, turmoil).
`BlobKnobs` defines timeout and sizing parameters consumed by both S3 and Azure backends.
