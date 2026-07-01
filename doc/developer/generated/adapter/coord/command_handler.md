---
source: src/adapter/src/coord/command_handler.rs
revision: 277b33e9c0
---

# adapter::coord::command_handler

Implements the coordinator's `handle_command` method, which dispatches each `Command` variant to the appropriate handler.
Covers session startup and teardown (`Startup`, `Terminate`), authentication, SQL execution (`Execute`, `Commit`), COPY FROM STDIN setup (`StartCopyFromStdin`), webhook lookup, system variable get/set, consistency checks, frontend-offload variants (`ExecuteSlowPathPeek`, `ExecuteSubscribe`, `ExecuteCopyTo`, `CopyToPreflight`, `ExecuteSideEffectingFunc`, `RegisterFrontendPeek`, `UnregisterFrontendPeek`, `ExplainTimestamp`, `FrontendStatementLogging`) that reduce coordinator bottleneck, and scoped feature-flag management (`UpdateScopedSystemParameters` dispatches to `Coordinator::reconcile_scoped_system_parameters`; `InstallScopedSystemParameterFrontend` stores the shared frontend into `Coordinator::scoped_frontend`).
`handle_startup` performs privilege checks, triggers JWT group-to-role membership sync via `maybe_sync_jwt_groups`, sets session defaults, and emits the initial builtin-table writes for session tracking. During session default resolution, if the resolved `transaction_isolation` default names a feature-flagged isolation level whose flag is disabled (e.g. a role default set while `bounded staleness` was enabled, with the flag later turned off), that default is dropped so the session falls back to the built-in default.
`CheckRoleCanLogin` (`handle_role_can_login`) handles pre-authentication role existence and login-attribute checks; `InjectAuditEvents` inserts manually injected audit log entries via a catalog transaction.
A module-private `RoleLoginStatus` enum (`NotFound`, `CanLogin`, `NonLogin`) and `role_login_status` helper are shared among authentication handlers (`handle_authenticate_password`, `handle_generate_sasl_challenge`, `handle_authenticate_verify_sasl_proof`).
When handling `Command::DetermineRealTimeRecentTimestamp`, the RTR future is awaited via `Coordinator::await_real_time_recent_timestamp` so that `StorageError::RtrTimeout` and `StorageError::RtrDropFailure` are converted to humanized `AdapterError` variants before propagating.
