---
source: src/adapter/src/coord/command_handler.rs
revision: aa7a1afd31
---

# adapter::coord::command_handler

Implements the coordinator's `handle_command` method, which dispatches each `Command` variant to the appropriate handler.
Covers session startup and teardown (`Startup`, `Terminate`), authentication, SQL execution (`Execute`, `Commit`), webhook lookup, system variable get/set, consistency checks, and all the fast-path peek variants (`ExecuteSlowPathPeek`, `ExecuteCopyTo`, `RegisterFrontendPeek`, etc.) added to reduce coordinator bottleneck.
`handle_startup` performs privilege checks, sets session defaults, and emits the initial builtin-table writes for session tracking.
`CheckRoleCanLogin` (`handle_role_can_login`) handles pre-authentication role existence and login-attribute checks; `InjectAuditEvents` inserts manually injected audit log entries via a catalog transaction.
Authentication handlers (`handle_authenticate_verify_sasl_proof`, `handle_authenticate_password`) use a shared `RoleLoginStatus` enum to classify role login eligibility.
