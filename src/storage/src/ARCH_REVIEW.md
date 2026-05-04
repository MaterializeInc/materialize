# Architecture review — `storage::src`

Scope: `src/storage/src/` and all subdirs (≈ 36,651 LOC of Rust).

## 1. Upsert triplication — three co-existing implementations with a dyncfg dispatch shim

**Files**
- `src/storage/src/upsert.rs:220` — `pub(crate) fn upsert(...)` — classic path, delegates to `upsert_continual_feedback::upsert_inner`
- `src/storage/src/upsert.rs:354` — `pub(crate) fn upsert_v2(...)` — v2 path, delegates to `upsert_continual_feedback_v2::upsert_inner`
- `src/storage/src/upsert_continual_feedback.rs:102` — `pub fn upsert_inner(...)` — v1b implementation (persist feedback + RocksDB/memory backend)
- `src/storage/src/upsert_continual_feedback_v2.rs:176` — `pub fn upsert_inner(...)` — v2 implementation (differential-collection key state)
- `src/storage/src/render/sources.rs:337-362` — dyncfg dispatch: `if ENABLE_UPSERT_V2 { upsert_v2(...) } else { upsert(...) }`

**Problem**
There are ~3,350 LOC of upsert implementation spread across three modules, where two of them (`upsert_continual_feedback` and `upsert_continual_feedback_v2`) contain the actual logic and `upsert.rs` is a dispatch shim. The classic path (`upsert_continual_feedback`) also has a RocksDB/memory backend abstraction layer (`UpsertStateBackend` in `upsert/types.rs`) that the v2 path does not use. This is a *parallel implementations* pattern: both paths solve the same problem (key-value upsert with rehydration from persist) but with different internal architectures. Maintaining bug fixes or correctness changes requires auditing both code paths independently.

The Deletion Test: the two implementations are not equivalent Modules — `upsert_continual_feedback` supports RocksDB-backed state for large keyspaces, `upsert_continual_feedback_v2` uses differential collections. Collapsing them would require either dropping RocksDB support or adding it to v2. This is a genuine *capability difference*, not just redundant code.

**Risk framing (not a recommendation to delete)**
The `ENABLE_UPSERT_V2` dyncfg indicates this is an intentional migration-in-progress rather than accidental duplication. The review concern is: once the migration is validated and v2 is stable, the deletion of v1b requires explicitly enumerating:
1. RocksDB-backed state for arbitrarily large keyspaces (not provided by v2 today).
2. Snapshot-buffering prevention (`STORAGE_UPSERT_PREVENT_SNAPSHOT_BUFFERING`) — verify it exists in v2 before removing v1b.
3. `rocksdb_use_native_merge_operator` optimization — verify equivalent or intentional cut.

**Actionable now**
- Add a TODO comment at `src/storage/src/render/sources.rs:337` naming each v1b-only capability with a ticket reference, so the eventual deletion has an explicit checklist.
- The `upsert/types.rs` `UpsertStateBackend` trait (1,502 LOC) is entirely v1b-specific; it should be marked as such to prevent new code from building on it.

## 2. (Honest skip) Source-connector sprawl across `kafka.rs` / `postgres.rs` / `mysql.rs` / `sql_server.rs`

Each connector implements `SourceRender` independently. There is no shared framework for offset committing, partition assignment, or snapshot detection — each re-derives these. However, the *deletion test passes*: these connectors wrap fundamentally different external protocols (Kafka consumer groups vs. Postgres replication slots vs. MySQL binlog vs. SQL Server CDC). Abstracting them would be relocation (shared helper functions) not deepening. The `SourceRender` trait is already the correct seam. No friction found here beyond "each connector is complex".

## 3. (Honest skip) `auction.rs` at 4,347 LOC

Only ~183 lines are logic; the rest are static lookup tables (celebrity names, company names, bid items). The file is large by line count but contains no structural complexity. Keep as-is.

## What this review did not reach

- `persist_sink.rs` (1,401 LOC) — the Materialize→persist write path; a separate review should check whether the batch-description / write-data / commit three-operator split matches the Iceberg sink and whether they share an Interface.
- `healthcheck.rs` `SuspendAndRestart` delay logic — could be a source of subtle race conditions under rapid error/recovery cycling; worth a focused review.
