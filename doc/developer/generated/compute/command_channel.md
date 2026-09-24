---
source: src/compute/src/command_channel.rs
revision: 17b8b10438
---

# mz-compute::command_channel

Implements a Timely dataflow-based command broadcast channel that fans out `ComputeCommand`s from worker 0 to all other workers.
Using a dataflow rather than direct per-worker delivery ensures all workers observe the same sequence of commands across reconnects, which is required because Timely mandates that all workers render identical dataflows in the same order.
Commands are represented as `UnifiedCommand`, an enum with two variants: `Compute(ComputeCommand, Uuid)` (carrying the nonce that identifies the client protocol incarnation) and `Storage(InternalStorageCommand)` (for clusters that host storage objects alongside compute objects). `CreateDataflow` commands are partitioned among workers via `split_command`; all other compute commands are replicated in full.
For clusters hosting storage objects, the channel also carries storage-internal commands that may be injected from any worker (e.g., by health operators triggering a suspend-and-restart). To give these commands a single consistent ordering across all workers, the channel uses a two-hop structure: producers tag each storage command with a per-producer index, worker 0 assigns a definitive global index, and receivers restore that order before delivering commands to the storage worker.
