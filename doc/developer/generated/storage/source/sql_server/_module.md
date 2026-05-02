---
source: src/storage/src/source/sql_server.rs
revision: 5427dc5764
---

# mz-storage::source::sql_server

Implements `SourceRender` for `SqlServerSourceConnection`, composing replication and progress operators into a SQL Server CDC ingestion dataflow.
Per-capture-instance `SourceOutputInfo` structs carry the decoder, resume LSN, initial LSN, and partition index needed by the replication operator.
Submodules `replication` and `progress` implement the two operator families.
