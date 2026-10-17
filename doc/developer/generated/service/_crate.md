---
source: src/service/src/lib.rs
revision: 134006ec86
---

# mz-service

Common infrastructure for services orchestrated by `environmentd`, primarily `clusterd`.
Provides the `GenericClient`/`Partitioned` client abstraction (`client`), the Cluster Transport Protocol implementation (`transport`), in-process communication (`local`), boot diagnostics (`boot`), and retry constants (`retry`).
Key dependencies include `mz-ore`, `bincode`, and `semver`.
