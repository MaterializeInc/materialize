---
source: src/storage/src/internal_control.rs
revision: b0fa98e931
---

# mz-storage::internal_control

Defines the intra-cluster command bus used by storage workers to broadcast commands to each other in a consistent total order.
`InternalStorageCommand` enumerates all commands that can be so broadcast (suspend/restart a dataflow, create ingestion and sink dataflows, run oneshot ingestions, drop dataflows, update configuration, propagate statistics).
`InternalCommandSender` and `InternalCommandReceiver` wrap an mpsc channel; `setup_command_sequencer` builds a small timely dataflow that sequences commands arriving from any worker through worker 0 before broadcasting them to all workers, ensuring a deterministic global order.
`DataflowParameters` holds runtime-tunable dataflow knobs (currently RocksDB configuration).
