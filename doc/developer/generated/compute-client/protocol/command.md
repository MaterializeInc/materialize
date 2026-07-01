---
source: src/compute-client/src/protocol/command.rs
revision: 141cb2a0a5
---

# mz-compute-client::protocol::command

Defines `ComputeCommand`, the enum of all commands sent from the compute controller to replicas.
Commands cover the three protocol stages: creation (`Hello`, `CreateInstance`), initialization (`InitializationComplete`), and computation (`CreateDataflow`, `Schedule`, `AllowWrites`, `AllowCompaction`, `Peek`, `CancelPeek`, `UpdateConfiguration`).
Also defines supporting types `InstanceConfig`, `ComputeParameters`, `PeekTarget`, and `Peek`.
`InstanceConfig` carries an `initial_config: ConfigUpdates` field — a snapshot of the controller's dyncfg (with any replica-scoped overrides applied) at replica creation. The replica applies it to its worker configuration before create-time setup, so work performed during `CreateInstance` (for example rendering introspection dataflows) observes controller-synced values rather than dyncfg defaults. The same values are re-delivered through the subsequent `UpdateConfiguration`. `initial_config` is excluded from `compatible_with` compatibility checks, since differences across reconnects are expected and do not require a restart.
