---
source: src/adapter/src/error.rs
revision: 2bd0f58824
---

# adapter::error

Defines `AdapterError`, the central error type for the coordinator, with variants covering planning errors, catalog errors, storage/compute controller errors, RBAC violations, timestamp errors, authentication errors, and miscellaneous unstructured errors.
Notable variants include `ConcurrentDependencyDrop { dependency_kind, dependency_id }` for mid-flight dependency drops, `AuthenticationError` (with an inner `AuthenticationError` enum covering `InvalidCredentials`, `NonLogin`, `RoleNotFound`, `PasswordRequired`), `ReplacementSchemaMismatch(RelationDescDiff)` for schema-incompatible replacements, `ReplaceMaterializedViewSealed`, `CollectionUnreadable`, `AlterClusterWhilePendingReplicas`, `ImpossibleTimestampConstraints`, `OidcGroupSyncFailed(String)` for OIDC group-to-role sync failures in strict mode (maps to `SqlState::INTERNAL_ERROR`), and four bounded-staleness variants: `BoundedStalenessExceeded { bound, gap_ms, slowest_input }` (maps to `SqlState::T_R_SERIALIZATION_FAILURE`), `BoundedStalenessReadOnly` (maps to `SqlState::READ_ONLY_SQL_TRANSACTION`), `BoundedStalenessRealTimeRecencyConflict` (maps to `SqlState::FEATURE_NOT_SUPPORTED`), and `BoundedStalenessTimelineUnsupported` (maps to `SqlState::FEATURE_NOT_SUPPORTED`).
Implements `From` conversions from many downstream error types (`PlanError`, `StorageError`, `OptimizerError`, `VarError`, etc.) and maps each variant to a PostgreSQL `SqlState` error code via `code()`; error details and hints are provided through `detail()` and `hint()`.
The `From<OptimizerError>` conversion maps `OptimizerError::RestrictedFunction` to `AdapterError::Unauthorized(UnauthorizedError::RestrictedSystemObject)` rather than the generic `Optimizer` variant; within `code()`, `OptimizerError::RestrictedFunction` maps to `SqlState::INSUFFICIENT_PRIVILEGE`.
Several constructor helpers on `AdapterError` (e.g. `concurrent_dependency_drop_from_instance_missing`, `concurrent_dependency_drop_from_peek_error`) perform explicit error-kind conversions that are intentionally not automatic `From` impls.
The `ShouldTerminateGracefully` trait (private) identifies errors — such as `FenceError::DeployGeneration` — that should cause a clean process exit rather than a panic.
`RecursionLimitError` (from `mz_ore::stack`) carries a `std::backtrace::Backtrace` field and does not implement `Clone`; as a result, `AdapterError` is also not `Clone`.
The `eval_error_code` helper maps `EvalError` variants to SQLSTATE codes exhaustively (no wildcard fallthrough to `XX000`). `InvalidRangeError::InvalidRangeData` maps to `SqlState::DATA_EXCEPTION`.
