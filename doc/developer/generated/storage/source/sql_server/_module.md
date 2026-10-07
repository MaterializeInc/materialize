---
source: src/storage/src/source/sql_server.rs
revision: b382b15182
---

# mz-storage::source::sql_server

Implements `SourceRender` for `SqlServerSourceConnection`, composing replication and progress operators into a SQL Server CDC ingestion dataflow.
Per-capture-instance `SourceOutputInfo` structs carry the decoder, resume LSN, initial LSN, and partition index needed by the replication operator.
The `render` method receives a `ResumeUppers<Lsn>` stream and forwards it to the progress operator, which uses the per-export frontiers to report each export's `offset_committed` and to determine per-capture-instance change-table cleanup boundaries.
Submodules `replication` and `progress` implement the two operator families.
