---
source: src/sql-server-util/src/desc.rs
revision: b3a16a93da
---

# mz-sql-server-util::desc

Provides metadata types for SQL Server tables and columns, plus row-decoding logic.
`SqlServerTableDesc` and `SqlServerColumnDesc` describe upstream table structure; `SqlServerRowDecoder` (built from a `SqlServerTableDesc` and an `mz_repr::RelationDesc`) efficiently decodes `tiberius::Row` values into `mz_repr::Row` values, mapping SQL Server types to Materialize datums.
`SqlServerColumnDecodeType` enumerates the supported SQL Server column types; protobuf serialization is provided via `RustType` impls for persistence across restarts.
The module also defines `SqlServerTableRaw`, `SqlServerColumnRaw`, `SqlServerCaptureInstanceRaw`, and `SqlServerQualifiedTableName` for intermediate representations used during schema discovery.
