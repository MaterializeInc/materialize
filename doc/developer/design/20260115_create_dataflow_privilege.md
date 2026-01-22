# CREATE_DATAFLOW Permission

- Associated: (TBD - GitHub issue to be created)

## The Problem

Currently, all users who have SELECT privileges on objects can execute any query against those objects, regardless of the computational cost. Some queries can be answered quickly using existing indexes (via the "fast path"), while others require rendering a full dataflow on a compute cluster (the "slow path").

In multi-tenant or cost-sensitive environments, organizations may want to restrict certain users or roles to only execute queries that can be satisfied by existing indexes, preventing them from triggering expensive dataflow rendering operations.

## Success Criteria

1. A new privilege called `CREATE_DATAFLOW` can be granted/revoked on clusters
2. Users without this privilege can still execute SELECT queries that qualify for the fast path:
   - Queries that target an existing index
   - Queries that only apply map-filter-project (MFP) operations without temporal filters
   - Constant queries (e.g., `SELECT 1 + 1`)
3. Users without this privilege receive a clear error message when attempting queries that would require dataflow rendering
4. The existing RBAC infrastructure is extended cleanly without breaking changes
5. Superusers and privileged roles retain full access

## Out of Scope

- Restricting fast-path queries based on index usage patterns
- Cost-based query restrictions (e.g., limiting based on estimated resource consumption)
- Per-query dataflow rendering quotas
- Restricting access to specific indexes

## Solution Proposal

### High-Level Summary

Introduce a new `CREATE_DATAFLOW` privilege (similar to existing privileges like `SELECT`, `USAGE`, `CREATE`) that controls whether a user can execute queries requiring dataflow rendering. This privilege is checked AFTER the optimizer determines the query execution path.

### Detailed Design

#### 1. New Privilege Definition

Add a new `CREATE_DATAFLOW` privilege to the `AclMode` bitflags in `src/repr/src/adt/mz_acl_item.rs`:

```rust
const CREATE_DATAFLOW = 1 << 28; // 'D' character representation
```

This follows the pattern of other system privileges like `CREATE_ROLE` (1 << 31), `CREATE_DB` (1 << 30), `CREATE_CLUSTER` (1 << 29).

This privilege will be applicable to **clusters**, since dataflow rendering happens on compute clusters.

#### 2. Privilege Check Location

Unlike other privileges that are checked before optimization, the `CREATE_DATAFLOW` check must happen AFTER the optimizer determines whether a query can use the fast path.

**File:** `src/adapter/src/coord/sequencer/inner/peek.rs`

In the `peek_finish()` function, after extracting the `peek_plan` from `global_lir_plan.unapply()`, insert the check BEFORE calling `implement_peek_plan()`:

```rust
let (peek_plan, df_meta, typ) = global_lir_plan.unapply();

// Check CREATE_DATAFLOW privilege for slow path queries
if matches!(&peek_plan, PeekPlan::SlowPath(_)) {
    rbac::check_create_dataflow_privilege(
        &self.catalog().for_session(session),
        session,
        cluster_id,
    )?;
}
```

The check happens here because:
- At this point, the optimizer has determined whether a fast path is possible
- `PeekPlan::FastPath` variants (Constant, PeekExisting, PeekPersist) don't need the privilege
- `PeekPlan::SlowPath` means a dataflow will be created, requiring the privilege

#### 3. SQL Syntax

```sql
-- Grant dataflow rendering permission
GRANT CREATEDATAFLOW ON CLUSTER <cluster_name> TO <role>;

-- Revoke dataflow rendering permission
REVOKE CREATEDATAFLOW ON CLUSTER <cluster_name> FROM <role>;

-- Check current privileges
SHOW PRIVILEGES ON CLUSTER <cluster_name>;
```

#### 4. Default Behavior

- By default, `CREATEDATAFLOW` is NOT automatically granted - cluster owners have it via ownership
- Superusers always have `CREATEDATAFLOW` regardless of explicit grants
- System clusters (mz_catalog_server, mz_introspection, etc.) are exempt from this check - all users can execute queries on system clusters without CREATEDATAFLOW

#### 5. Error Messages

When a user without `CREATEDATAFLOW` attempts a query requiring dataflow rendering:

```
ERROR: permission denied for CLUSTER <cluster_name>
DETAIL: The '<role>' role needs CREATEDATAFLOW privileges on CLUSTER <cluster_name>
```

#### 6. Files Modified

| File | Changes |
|------|---------|
| `src/repr/src/adt/mz_acl_item.rs` | Add `CREATE_DATAFLOW` constant, char, string, and update all match arms |
| `src/sql/src/rbac.rs` | Add `check_create_dataflow_privilege()` function, update `all_object_privileges()` |
| `src/adapter/src/coord/sequencer/inner/peek.rs` | Insert privilege check after optimization in `peek_finish()` |
| `src/sql/src/plan/statement/acl.rs` | Support GRANT/REVOKE for CREATEDATAFLOW on clusters |
| `src/sql-parser/src/ast/defs/statement.rs` | Add `CREATEDATAFLOW` to `Privilege` enum |
| `src/sql-parser/src/parser.rs` | Parse CREATEDATAFLOW keyword |
| `src/sql-lexer/src/keywords.txt` | Add `Createdataflow` keyword |

## Alternatives

### Alternative 1: Query-level annotation

Instead of a privilege, use a session variable or query hint:
```sql
SET disallow_dataflow_rendering = true;
SELECT * FROM indexed_view; -- Works if fast path
```

**Rejected because:** This puts control in the wrong hands - users could simply unset the variable.

### Alternative 2: Cluster-level setting

Make dataflow rendering a cluster configuration:
```sql
ALTER CLUSTER analytics SET (allow_dataflow_rendering = false);
```

**Rejected because:** This affects ALL users on the cluster, not per-role control.

### Alternative 3: Separate "read-only index" role

Create a special role type that can only read from indexes.

**Rejected because:** This doesn't integrate with the existing RBAC model and creates a parallel permission system.

## Open Questions (Resolved)

1. ~~**Privilege name:**~~ Decided: `CREATEDATAFLOW`
2. ~~**Scope:**~~ Decided: Cluster-scoped only (where dataflows are rendered)
3. ~~**EXPLAIN handling:**~~ Decided: Allow EXPLAIN even for slow-path queries (users can see plans without executing)
4. ~~**Subscribe queries:**~~ Decided: Not included initially - only SELECT/peek queries are subject to this check

## Testing

Integration tests are located at `test/sqllogictest/createdataflow_privilege.slt`. The tests cover:

- GRANT/REVOKE CREATEDATAFLOW syntax
- Privilege enforcement: users without CREATEDATAFLOW are blocked from slow-path queries
- Fast-path queries (constants, index lookups) succeed without CREATEDATAFLOW
- REVOKE ALL removes CREATEDATAFLOW privilege
