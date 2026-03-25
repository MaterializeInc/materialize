---
source: src/catalog/src/lib.rs
revision: d7429ca6b9
---

# catalog

Materialize's catalog layer: persistent metadata storage for the coordinator, providing both a durable persist-backed store and a rich in-memory view of all catalog objects.

The crate is structured into four top-level modules: `durable` (the persist-backed storage engine with schema migrations and atomic transactions), `memory` (in-memory object representations consumed by the adapter), `builtin` (hardcoded system objects auto-installed on open), and `config` (catalog initialization parameters).
The `expr_cache` module provides a persist-backed cache for optimized query plans, invalidated when optimizer features change.

Key dependencies include `mz-persist-client`, `mz-catalog-protos`, `mz-storage-types`, `mz-sql`, `mz-repr`, `mz-compute-types`, `mz-controller`, `mz-audit-log`, and `mz-durable-cache`.
The crate is consumed almost exclusively by `mz-adapter` (the coordinator layer) and the `catalog-debug` CLI tool.
