---
source: src/adapter/src/coord.rs
revision: 9d0a7c3c6f
---

# adapter::coord

The core coordinator: `coord.rs` defines the `Coordinator` struct (the central state machine), `ExecuteContext` (the per-statement execution handle), the internal `Message` enum, and the `serve` / `Config` entry points.
The coordinator owns the `Catalog`, active compute sinks, pending peeks, read-policy manager, timeline oracles, and all inter-subsystem handles (controller, storage collections, secrets, orchestrator).
The file also defines `IdPool`, a pre-allocated pool of user `GlobalId` integers that amortizes per-DDL persist writes by reserving batches of IDs at once; the pool is owned by the coordinator and access is serialized through its single-threaded event loop.
Child modules partition the coordinator's responsibilities: `command_handler` handles external `Command` messages; `message_handler` handles internal async `Message` responses; `sequencer` executes SQL plans; `appends` manages table and builtin-table writes; `catalog_implications` derives and applies downstream effects from catalog state changes; `ddl` wraps catalog transactions; `peek` and `read_policy` manage query execution and compaction; `timestamp_selection` and `timeline` handle temporal reasoning; `catalog_serving` serves catalog snapshots; and supporting modules cover cluster scheduling, introspection routing, consistency checking, index management, and statement logging.
Bootstrap handles derived builtin storage collections (builtin MVs) separately: after registering input-less collections in dependency order, it bumps their sinces based on transitive dependency frontiers to satisfy as-of selection invariants.
