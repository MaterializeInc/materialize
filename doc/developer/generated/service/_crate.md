---
source: src/service/src/lib.rs
revision: c0f89a5887
---

# mz-service

Common infrastructure for services orchestrated by `environmentd`, primarily `clusterd`.
Provides the `GenericClient`/`Partitioned` client abstraction (`client`), the Cluster Transport Protocol implementation (`transport`), in-process communication (`local`), boot diagnostics (`boot`), retry constants (`retry`), and gRPC connection parameters (`params`).
Key dependencies include `mz-ore`, `bincode`, and `semver`.
