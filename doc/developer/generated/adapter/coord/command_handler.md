---
source: src/adapter/src/coord/command_handler.rs
revision: 3df8ae2fd8
---

# adapter::coord::command_handler

Implements the coordinator's `handle_command` method, which dispatches each `Command` variant to the appropriate handler.
Covers session startup and teardown (`Startup`, `Terminate`), authentication, SQL execution (`Execute`, `Commit`), COPY FROM STDIN setup (`StartCopyFromStdin`), webhook lookup, system variable get/set, consistency checks, and frontend-offload variants (`ExecuteSlowPathPeek`, `ExecuteSubscribe`, `ExecuteCopyTo`, `CopyToPreflight`, `ExecuteSideEffectingFunc`, `RegisterFrontendPeek`, `UnregisterFrontendPeek`, `ExplainTimestamp`, `FrontendStatementLogging`) that reduce coordinator bottleneck.
`handle_startup` performs privilege checks, triggers JWT group-to-role membership sync via `maybe_sync_jwt_groups`, sets session defaults, and emits the initial builtin-table writes for session tracking.
`CheckRoleCanLogin` (`handle_role_can_login`) handles pre-authentication role existence and login-attribute checks; `InjectAuditEvents` inserts manually injected audit log entries via a catalog transaction.
A module-private `RoleLoginStatus` enum (`NotFound`, `CanLogin`, `NonLogin`) and `role_login_status` helper are shared among authentication handlers (`handle_authenticate_password`, `handle_generate_sasl_challenge`, `handle_authenticate_verify_sasl_proof`).
When handling `Command::DetermineRealTimeRecentTimestamp`, the RTR future is awaited via `Coordinator::await_real_time_recent_timestamp` so that `StorageError::RtrTimeout` and `StorageError::RtrDropFailure` are converted to humanized `AdapterError` variants before propagating.
