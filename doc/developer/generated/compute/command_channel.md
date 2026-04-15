---
source: src/compute/src/command_channel.rs
revision: b0fa98e931
---

# mz-compute::command_channel

Implements a Timely dataflow-based command broadcast channel that fans out `ComputeCommand`s from worker 0 to all other workers.
Using a dataflow rather than direct per-worker delivery ensures all workers observe the same sequence of commands across reconnects, which is required because Timely mandates that all workers render identical dataflows in the same order.
Commands carry a UUID nonce identifying the client protocol incarnation; `CreateDataflow` commands are partitioned among workers via `split_command`, while all other commands are replicated in full.
