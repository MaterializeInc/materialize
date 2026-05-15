---
source: src/expr/src/scalar/func/unmaterializable.rs
revision: 3df8ae2fd8
---

# mz-expr::scalar::func::unmaterializable

Defines the `UnmaterializableFunc` enum, representing session- and environment-dependent SQL functions that cannot be folded at planning time.
Variants include `CurrentDatabase`, `CurrentTimestamp`, `MzNow`, `MzVersion`, `MzRoleOidMemberships`, `MzSessionRoleMemberships`, `SessionUser`, `ViewableVariables`, and similar functions; evaluation is deferred to `mz-adapter`.
Each variant implements `output_type` to report its return type for type inference purposes.
`allowed_in_restricted_session() -> bool` classifies each variant for use when `restrict_to_user_objects` is active: session-identity, time, and session-config variants return `true`; system-information variants (`MzVersion`, `MzVersionNum`, `MzUptime`, `MzRoleOidMemberships`, `MzEnvironmentId`, `MzIsSuperuser`, `PgBackendPid`, `PgPostmasterStartTime`, `Version`) return `false`. The adapter's unmaterializable-function evaluator rejects `false` variants with `OptimizerError::RestrictedFunction` in restricted sessions.
