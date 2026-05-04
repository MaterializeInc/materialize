# mz-txn-wal

Atomic multi-shard write-ahead log over persist. Enables transactional writes
spanning multiple data shards through a single coordinating txns shard.

## Subtree (≈ 6,988 LOC total)

| Path | LOC | What it owns |
|---|---|---|
| `src/txns.rs` | 1,736 | `TxnsHandle`: register/forget/begin/apply_le/compact_to |
| `src/txn_cache.rs` | 1,507 | `TxnsCache`/`TxnsCacheState`: in-memory index of txns shard |
| `src/operator.rs` | 973 | `txns_progress` Timely operator for logical frontier translation |
| `src/lib.rs` | 949 | Protocol docs, `TxnsCodecDefault`, low-level CaA helpers |
| `src/txn_write.rs` | 715 | `Txn`/`TxnWrite`/`TxnApply`/`Tidy`: commit pipeline |
| `src/txn_read.rs` | 897 | `DataSnapshot`/`TxnsRead`/`TxnsReadTask`/subscribe machinery |
| `src/metrics.rs` | 189 | `Metrics`, `FallibleOpMetrics`, `InfallibleOpMetrics` |

## Package identity

Crate name: `mz-txn-wal`. Key deps: `mz-persist-client`, `mz-persist-types`,
`mz-timely-util`, `differential-dataflow`, `timely`, `prost`.

## Purpose

Implements a WAL pattern over persist shards. Writers buffer updates per data
shard, then atomically commit a set of batch handles plus a txns-shard record
via compare-and-append. Readers translate physical data-shard frontiers to
logical ones by consulting the txns shard cache.

Two isolation modes: **serializable** (reads up to txns-shard frontier) and
**strict serializable** (reads wait for txns frontier to pass the requested ts).

## Key interfaces (exported)

- `TxnsHandle` — write entry point: `open`, `register`, `forget`, `begin`,
  `apply_le`, `compact_to`.
- `Txn` / `TxnApply` / `Tidy` — commit pipeline returned by `begin`.
- `TxnsCache` / `TxnsRead` / `TxnsReadTask` — read-side cache and async task.
- `txns_progress` — Timely operator that wraps `shard_source` outputs to
  translate physical to logical frontiers; required for all data shard reads.
- `TxnsCodecDefault` — standard codec for txns shard entries.
- Low-level helpers: `small_caa`, `empty_caa`, `apply_caa`, `cads`.

## Downstream consumers

`mz-storage-controller` (registers shards, commits table writes via
`PersistTableWriteWorker`), `mz-storage` / clusterd (reads via `txns_progress`),
`mz-storage-client` (`StorageCollectionsImpl` holds a `TxnsHandle`).

## Architecture notes

- **Write path serialization**: all txns are linearized through the single txns
  shard — contended workloads pay O(N) retries where N is concurrency. The
  design explicitly trades horizontal write scale for cross-shard atomicity.
- **Physical vs logical frontier gap**: data shards only advance physically when
  written to; `txns_progress` bridges this so consumers see a correct logical
  upper. Without it, a shard_source would stall after the last write.
- **`TxnsCache` is read-critical**: the in-memory index of all committed txns
  must be replayed from the beginning of the txns shard on new readers —
  compaction via `compact_to` is the mitigation; without it, startup cost grows
  unboundedly.
- **Codec uniformity restriction**: all data shards must share `K,V,T,D` codecs;
  only `K`/`V` schemas may differ per shard. This is a hard constraint from the
  persist layer.

## Bubbled findings for src/CONTEXT.md

- `mz-txn-wal` is the cross-shard atomicity layer; `mz-storage-controller` and
  `mz-storage-client` both depend on it for table writes.
- The `txns_progress` Timely operator is the **mandatory read adapter** — any
  consumer of a txn-wal-managed shard must wrap reads through it or risk seeing
  a stale upper.
- Write contention under concurrent table DML is an architectural cost: O(N)
  retries, no batching of competing txns. Throughput tuning is a known open
  area (see `lib.rs` design notes).
