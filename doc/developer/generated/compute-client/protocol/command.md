---
source: src/compute-client/src/protocol/command.rs
revision: b55d3dee25
---

# mz-compute-client::protocol::command

Defines `ComputeCommand`, the enum of all commands sent from the compute controller to replicas.
Commands cover the three protocol stages: creation (`Hello`, `CreateInstance`), initialization (`InitializationComplete`), and computation (`CreateDataflow`, `Schedule`, `AllowWrites`, `AllowCompaction`, `Peek`, `CancelPeek`, `UpdateConfiguration`).
Also defines supporting types `InstanceConfig`, `ComputeParameters`, `PeekTarget`, and `Peek`.
