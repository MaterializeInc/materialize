---
source: src/sql/src/plan/statement/acl.rs
revision: 39dcae2fba
---

# mz-sql::plan::statement::acl

Plans access-control statements: `GRANT`/`REVOKE` privileges, `GRANT`/`REVOKE` role membership, `ALTER DEFAULT PRIVILEGES`, `ALTER OWNER`, and `REASSIGN OWNED`.
Each statement has a `describe_*` function (returns `StatementDesc`) and a `plan_*` function (returns a `Plan` variant such as `GrantPrivilegesPlan` or `AlterOwnerPlan`).
`GRANT/REVOKE ALL` now includes network policies: `plan_update_privilege` iterates over `scx.catalog.get_network_policies()` and maps each to `ObjectId::NetworkPolicy`.
`ObjectType::MetricSink` is grouped with `Sink`, `ClusterReplica`, `Role`, and `Func` in the `ALTER DEFAULT PRIVILEGES` validation: metric sinks do not have privileges and produce an error if named in that context.
