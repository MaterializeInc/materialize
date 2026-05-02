# mz-persist

The lower-level Blob/Consensus seam definition for Materialize's persistence
system. Provides the two storage primitives and all concrete backend
implementations. `mz-persist-client` builds the full persist API on top of
these primitives.

## Structure

| Path | LOC | What it owns |
|---|---|---|
| `src/indexed/` | 2,119 | Columnar batch types and Parquet/Arrow codec — see [`src/indexed/CONTEXT.md`](src/indexed/CONTEXT.md) |
| `src/` | 8,848 | All modules — see [`src/CONTEXT.md`](src/CONTEXT.md) |

## Key interfaces (exported)

- **`Blob`** — async key/value object store trait (`get`, `set`, `delete`, list).
- **`Consensus`** — linearizable compare-and-set log trait (`head`, `compare_and_set`, `scan`, `truncate`).
- **`SeqNo`** — monotone `u64` sequence counter; the ordering primitive for the CAS log.
- **`ExternalError`** — `Determinate` vs `Indeterminate` error split; drives retry decisions in `mz-persist-client`.
- **`BlobConfig` / `ConsensusConfig`** — URI-based factory enums for backend selection.
- **`ColumnarRecords`** — Arrow-backed columnar batch; the data unit flowing between persist layers.

## Backend implementations

| Module | Backend | Role |
|---|---|---|
| `s3` | Amazon S3 | Production blob store |
| `azure` | Azure Blob Storage | Production blob store |
| `postgres` | CockroachDB/Postgres | Production consensus log |
| `foundationdb` *(feature)* | FoundationDB | Consensus log (alternative) |
| `file` | Local filesystem | Dev/bench blob |
| `mem` | In-memory | Test blob + consensus |
| `turmoil` *(feature)* | Network simulation | Chaos testing |

## What to bubble up to src/CONTEXT.md

- `mz-persist` is the storage primitive seam: `Blob` + `Consensus` are the only two contracts the rest of persist depends on; all backend complexity is isolated here.
- The `Determinate`/`Indeterminate` `ExternalError` split is the retry protocol boundary — any new backend must correctly classify its errors or `mz-persist-client`'s retry loops break.

## Cross-references

- Generated developer docs: `doc/developer/generated/persist/`.
- `mz-persist-types` — codec and columnar schema traits (sits below this crate).
- `mz-persist-client` — the only production consumer.
