---
source: src/adapter/src/error.rs
revision: 00cc513fa5
---

# adapter::error

Defines `AdapterError`, the central error type for the coordinator, with variants covering planning errors, catalog errors, storage/compute controller errors, RBAC violations, timestamp errors, and miscellaneous unstructured errors.
Implements `From` conversions from many downstream error types (`PlanError`, `StorageError`, `AdapterError`, etc.) and maps each variant to a PostgreSQL `SqlState` error code and severity via `Into<ErrorResponse>`.
The `ShouldTerminateGracefully` trait (private) identifies errors — notably catalog fencing — that should cause a clean process exit rather than a panic.
