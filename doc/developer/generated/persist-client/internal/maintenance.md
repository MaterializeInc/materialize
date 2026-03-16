---
source: src/persist-client/src/internal/maintenance.rs
revision: 2a6ac3ab4c
---

# persist-client::internal::maintenance

Defines `RoutineMaintenance` and `WriterMaintenance`, structs that accumulate background work (GC requests, rollup writes, compaction requests) to be performed after a successful CaS operation.
Handles are responsible for initiating this maintenance; `RoutineMaintenance` may be skipped for one-shot operations but must be performed for regular heartbeat operations.
