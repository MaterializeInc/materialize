---
source: src/sql/src/plan/statement/acl.rs
revision: daff501451
---

# mz-sql::plan::statement::acl

Plans access-control statements: `GRANT`/`REVOKE` privileges, `GRANT`/`REVOKE` role membership, `ALTER DEFAULT PRIVILEGES`, `ALTER OWNER`, and `REASSIGN OWNED`.
Each statement has a `describe_*` function (returns `StatementDesc`) and a `plan_*` function (returns a `Plan` variant such as `GrantPrivilegesPlan` or `AlterOwnerPlan`).
