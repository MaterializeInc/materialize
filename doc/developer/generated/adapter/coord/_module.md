---
source: src/adapter/src/coord.rs
revision: 5b9fb22e87
---

# adapter::coord

The core coordinator: `coord.rs` defines the `Coordinator` struct (the central state machine), `ExecuteContext` (the per-statement execution handle), the internal `Message` enum, and the `serve` / `Config` entry points.
The coordinator owns the `Catalog`, active compute sinks, pending peeks, read-policy manager, timeline oracles, and all inter-subsystem handles (controller, storage collections, secrets, orchestrator).
Child modules partition the coordinator's responsibilities: `command_handler` handles external `Command` messages; `message_handler` handles internal async `Message` responses; `sequencer` executes SQL plans; `appends` manages table and builtin-table writes; `ddl` wraps catalog transactions with downstream effects; `peek` and `read_policy` manage query execution and compaction; `timestamp_selection` and `timeline` handle temporal reasoning; and supporting modules cover cluster scheduling, introspection routing, consistency checking, and statement logging.
