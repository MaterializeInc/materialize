---
source: src/sql/src/session/metadata.rs
revision: 3df8ae2fd8
---

# mz-sql::session::metadata

Defines the `SessionMetadata` trait, the minimal read-only view of a session that the SQL layer needs during planning and RBAC checks: connection ID, client IP, current `PlanContext`, `RoleMetadata`, and `SessionVars`.
The adapter implements this trait on its full `Session` type; the SQL layer depends only on the trait.
The trait provides `restrict_to_user_objects() -> bool` as a convenience method that delegates to `self.vars().restrict_to_user_objects()`; this is consumed by `rbac::check_restrict_to_user_objects` and the optimizer's unmaterializable-function evaluator.
