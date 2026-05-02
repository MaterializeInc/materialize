---
source: src/sql/src/plan/error.rs
revision: 721951ce66
---

# mz-sql::plan::error

Defines `PlanError`, the central error type for all planning failures, with variants covering unsupported features, name resolution failures, type errors, ambiguous references, purification errors, and more.
`PlanError` wraps lower-level errors from parsing, catalog lookup, type conversion, and upstream connectors (Postgres, MySQL, SQL Server, CSR) and provides human-readable `Display` and `detail`/`hint` messages consumed by the adapter.
`PlanError::Internal(String)` represents an invariant violation that is a Materialize bug rather than a user error; it is constructed via the `bail_internal!` and `internal_err!` macros defined in `lib.rs`.
`IcebergSinkUnsupportedKeyType` is produced when a column with a non-primitive or floating-point type is used as an Iceberg equality delete key; its hint directs the user to use primitive, non-floating-point columns.
