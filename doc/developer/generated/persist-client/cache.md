---
source: src/persist-client/src/cache.rs
revision: b89a9e0ec5
---

# persist-client::cache

Provides `PersistClientCache`, the process-wide cache of `PersistClient` instances keyed by `PersistLocation`.
The cache shares Postgres/CRDB consensus connections and blob handles across clients, which is critical because the number of such connections is a primary resource constraint in production.
It also holds the `StateCache` (shared in-memory shard state) and the PubSub sender/receiver task.
