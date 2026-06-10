---
source: src/service/src/lib.rs
revision: 2bb8e26dbb
---

# mz-service

Common infrastructure for services orchestrated by `environmentd`, primarily `clusterd`.
Provides the `GenericClient`/`Partitioned` client abstraction (`client`), the Cluster Transport Protocol implementation (`transport`), in-process communication (`local`), boot diagnostics (`boot`), secrets reader CLI integration (`secrets`), retry constants (`retry`), tracing helpers (`tracing`), and gRPC connection parameters (`params`).
Key dependencies include `mz-ore`, `mz-secrets`, `mz-orchestrator-process`, `mz-orchestrator-kubernetes`, `mz-aws-secrets-controller`, `bincode`, and `semver`.
