---
source: src/sql/src/plan/error.rs
revision: 4267863081
---

# mz-sql::plan::error

Defines `PlanError`, the central error type for all planning failures, with variants covering unsupported features, name resolution failures, type errors, ambiguous references, purification errors, and more.
`PlanError` wraps lower-level errors from parsing, catalog lookup, type conversion, and upstream connectors (Postgres, MySQL, SQL Server, CSR) and provides human-readable `Display` and `detail`/`hint` messages consumed by the adapter.
