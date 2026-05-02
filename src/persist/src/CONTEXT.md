# persist::src (mz-persist)

The Blob/Consensus abstraction layer: defines the two storage primitives that
`mz-persist-client` builds on, plus all backend implementations and test helpers.

## Subdirectories

| Path | What it owns |
|---|---|
| `indexed/` | Columnar batch representation and Parquet/Arrow encoding — see [`indexed/CONTEXT.md`](indexed/CONTEXT.md) |

## Files (LOC ≈ 6,729 excluding indexed/)

| File | What it owns |
|---|---|
| `location.rs` | `Blob` + `Consensus` traits; `SeqNo`, `VersionedData`, `CaSResult`, `ExternalError` (`Determinate`/`Indeterminate`); `Tasked<A>` wrapper; `SCAN_ALL` constant |
| `s3.rs` | `S3Blob` — Amazon S3 backend; `ENABLE_S3_LGALLOC_*` dyn configs |
| `postgres.rs` | `PostgresConsensus` — CockroachDB/Postgres CAS log backend; `USE_POSTGRES_TUNED_QUERIES` dyn config |
| `azure.rs` | `AzureBlob` — Azure Blob Storage backend |
| `mem.rs` | `MemBlob` + `MemConsensus` — in-memory backends for tests |
| `file.rs` | `FileBlob` — local-filesystem blob backend |
| `foundationdb.rs` | `FdbConsensus` — FoundationDB consensus backend (feature-gated) |
| `turmoil.rs` | Network-simulation backends for chaos testing (feature-gated) |
| `cfg.rs` | `BlobConfig` / `ConsensusConfig` enums; URI-based factory functions; dyn config registration |
| `metrics.rs` | Prometheus metrics structs for all backends |
| `retry.rs` | Exponential backoff with jitter |
| `intercept.rs` | Test wrapper: intercept calls to inject errors |
| `unreliable.rs` | Test wrapper: probabilistic fault injection |
| `workload.rs` | Synthetic data generator for benchmarks |
| `error.rs` | `Error` enum (persist-layer errors distinct from `ExternalError`) |
| `generated.rs` | Re-export of protobuf-generated types |

## Key interfaces (exported)

- **`Blob` trait** — async key/value blob store: `get`, `list_keys_and_metadata`, `set`, `delete`, `delete_key_values`, `restore`.
- **`Consensus` trait** — linearizable CAS log: `head`, `compare_and_set`, `scan`, `truncate`.
- **`SeqNo`** — monotone sequence counter threading the CAS log.
- **`ExternalError`** — two-variant error type distinguishing determinate (definite failure) from indeterminate (possible success) errors; used throughout retry logic in `mz-persist-client`.
- **`Tasked<A>`** — adapter wrapping any `Blob`/`Consensus` impl in isolated tokio tasks to guard against stalled futures.

## Cross-references

- Consumed exclusively by `mz-persist-client` (high-level API) and `mz-persist-cli` (tooling).
- `mz-persist-types` provides codec traits (`Codec`, `Codec64`) and columnar schema types.
- Generated developer docs: `doc/developer/generated/persist/`.
