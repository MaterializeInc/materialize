---
source: src/storage-types/src/connections.rs
revision: aa87532765
---

# storage-types::connections

Defines all connection types used to connect storage sources and sinks to external systems, along with the runtime infrastructure needed to instantiate them.
Key types include `Connection` (an enum over Kafka, Postgres, MySQL, SQL Server, SSH, AWS, CSR, and Iceberg REST connections), `ConnectionContext` (holds global runtime state such as SSH tunnel managers and secret readers), and `KafkaConnection`/`PostgresConnection` etc.
The submodules `aws`, `inline`, and `string_or_secret` provide supporting abstractions for AWS credential loading, reference/inlined connection polymorphism, and secret-backed string values respectively.
Connection types implement `AlterCompatible` to constrain which fields may change across an `ALTER CONNECTION`.
