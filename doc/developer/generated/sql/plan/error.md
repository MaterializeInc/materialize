---
source: src/sql/src/plan/error.rs
revision: a5806f6b80
---

# mz-sql::plan::error

Defines `PlanError`, the central error type for all planning failures, with variants covering unsupported features, name resolution failures, type errors, ambiguous references, purification errors, and more.
`PlanError` wraps lower-level errors from parsing, catalog lookup, type conversion, and upstream connectors (Postgres, MySQL, SQL Server, CSR) and provides human-readable `Display` and `detail`/`hint` messages consumed by the adapter.
`IcebergSinkUnsupportedKeyType` is produced when a column with a non-primitive or floating-point type is used as an Iceberg equality delete key; its hint directs the user to use primitive, non-floating-point columns.
