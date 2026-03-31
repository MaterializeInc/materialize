---
source: src/adapter/src/coord/command_handler.rs
revision: f7c755e1ed
---

# adapter::coord::command_handler

Implements the coordinator's `handle_command` method, which dispatches each `Command` variant to the appropriate handler.
Covers session startup and teardown (`Startup`, `Terminate`), authentication, SQL execution (`Execute`, `Commit`), webhook lookup, system variable get/set, consistency checks, and all the frontend-offload variants (`ExecuteSlowPathPeek`, `ExecuteSubscribe`, `ExecuteCopyTo`, `CopyToPreflight`, `ExecuteSideEffectingFunc`, `RegisterFrontendPeek`, `UnregisterFrontendPeek`, `ExplainTimestamp`, `FrontendStatementLogging`) added to reduce coordinator bottleneck.
`handle_startup` performs privilege checks, sets session defaults, and emits the initial builtin-table writes for session tracking.
`CheckRoleCanLogin` (`handle_role_can_login`) handles pre-authentication role existence and login-attribute checks; `InjectAuditEvents` inserts manually injected audit log entries via a catalog transaction.
A module-private `RoleLoginStatus` enum (`NotFound`, `CanLogin`, `NonLogin`) and `role_login_status` helper are shared among authentication handlers (`handle_authenticate_password`, `handle_generate_sasl_challenge`, `handle_authenticate_verify_sasl_proof`).
