---
source: src/adapter/src/coord.rs
revision: c2cb53b0d0
---

# adapter::coord

The core coordinator: `coord.rs` defines the `Coordinator` struct (the central state machine), `ExecuteContext` (the per-statement execution handle), the internal `Message` enum, and the `serve` / `Config` entry points.
The coordinator owns the `Catalog`, active compute sinks, pending peeks, read-policy manager, timeline oracles, and all inter-subsystem handles (controller, storage collections, secrets, orchestrator).
Child modules partition the coordinator's responsibilities: `command_handler` handles external `Command` messages; `message_handler` handles internal async `Message` responses; `sequencer` executes SQL plans; `appends` manages table and builtin-table writes; `catalog_implications` derives and applies downstream effects from catalog state changes; `ddl` wraps catalog transactions; `peek` and `read_policy` manage query execution and compaction; `timestamp_selection` and `timeline` handle temporal reasoning; `catalog_serving` serves catalog snapshots; and supporting modules cover cluster scheduling, introspection routing, consistency checking, index management, and statement logging.
