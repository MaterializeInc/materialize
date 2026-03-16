---
source: src/sql/src/session/metadata.rs
revision: 3e680d68fc
---

# mz-sql::session::metadata

Defines the `SessionMetadata` trait, the minimal read-only view of a session that the SQL layer needs during planning and RBAC checks: connection ID, client IP, current `PlanContext`, `RoleMetadata`, and `SessionVars`.
The adapter implements this trait on its full `Session` type; the SQL layer depends only on the trait.
