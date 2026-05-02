---
source: src/adapter/src/lib.rs
revision: 9d0a7c3c6f
---

# adapter

The `mz-adapter` crate is the coordinator: the central component that receives SQL commands from clients, plans and optimizes them, assigns timestamps, drives catalog transactions, and dispatches work to the storage and compute layers.

The crate is organized into several major subsystems:
* `coord` — the `Coordinator` struct, its event loop (`serve`), and all sub-modules for command handling, message handling, SQL sequencing, appends, DDL, peek execution, timestamp selection, and cluster scheduling.
* `catalog` — the adapter's view of the catalog: in-memory state, durable transactions, delta application, bootstrap/open, and system-table row generation.
* `optimize` — the `Optimize` trait and per-statement optimizer pipelines (peek, index, materialized view, subscribe, COPY TO, view).
* `explain` — `EXPLAIN` support for all IR stages (HIR, MIR, LIR, fast-path) and the optimizer-trace capture mechanism.
* `client` — the external `Client` / `SessionClient` API consumed by pgwire.
* `session` — per-connection `Session` state including transaction management and prepared statements.
* `config` — system-parameter synchronisation with LaunchDarkly or a JSON file.

Key public types re-exported from `lib.rs`: `Client`, `SessionClient`, `Handle`, `ExecuteResponse`, `AdapterError`, `AuthenticationError`, `AdapterNotice`, `CollectionIdBundle`, `ReadHolds`, `TimestampContext`, `serve`, `Config`.

Key dependencies: `mz-catalog`, `mz-controller`, `mz-compute-client`, `mz-storage-client`, `mz-sql`, `mz-expr`, `mz-transform`, `mz-persist-client`, `mz-timestamp-oracle`.
Downstream consumers: `mz-environmentd` (calls `serve` to start the coordinator), `mz-pgwire` (uses `Client`/`SessionClient`), `mz-balancerd`.
