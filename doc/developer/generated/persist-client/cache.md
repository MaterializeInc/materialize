---
source: src/persist-client/src/cache.rs
revision: db15d3b2dc
---

# persist-client::cache

Provides `PersistClientCache`, the process-wide cache of `PersistClient` instances keyed by `PersistLocation`.
The cache shares Postgres/CRDB consensus connections and blob handles across clients, which is critical because the number of such connections is a primary resource constraint in production.
When opening a blob, the cache wraps it in `HedgedBlob` (from `mz_persist::hedge`), which transparently fires a hedge request on a connection-pool-isolated sibling after a configurable delay, so a stalled primary connection does not block the read. The sibling is opened best-effort via `open_hedge_sibling`; if unavailable, `HedgedBlob` simply does not hedge.
It also holds the `StateCache` (shared in-memory shard state) and the PubSub sender/receiver task.
