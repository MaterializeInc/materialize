---
source: src/service/src/client.rs
revision: 82d92a7fad
---

# mz-service::client

Defines `GenericClient`, the core trait for command/response communication with worker servers, and `Partitioned`, a client that fans commands out across multiple worker partitions and merges their responses.
`Partitionable` and `PartitionedState` are the traits that callers must implement to describe how commands are split and responses are aggregated.
All `recv` methods are documented and required to be cancel-safe.
