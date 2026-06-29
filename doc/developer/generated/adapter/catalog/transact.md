---
source: src/adapter/src/catalog/transact.rs
revision: 3762081e1d
---

# adapter::catalog::transact

Implements catalog transactions: `Catalog::transact` opens a durable `Transaction`, executes a sequence of `Op` (catalog operations such as CreateItem, DropObject, RenameItem, GrantPrivilege, etc.), flushes to durable storage, and returns the resulting `StateUpdate` diffs for the coordinator to apply.
The module enforces referential integrity, privilege checks, and naming constraints within the transaction, and writes audit log events as side effects.
Zero-downtime deployment logic (0dt) is also managed here; DDL operations can be gated behind deployment-readiness checks during the cutover window.
`InjectedAuditEvent` struct and `Op::InjectAuditEvents` variant allow manually appending audit log entries at the current oracle write timestamp. `CREATE ROLE` audit events use `EventDetails::CreateRoleV1` which includes the `auto_provision_source` field.
`Op::AlterAddColumn` emits an `AlterAddColumnV1` audit log event recording the table ID, column name, column type, and nullability. `Op::AlterTimestampInterval` emits an `AlterSourceTimestampIntervalV1` audit log event recording the item ID, old interval, and new interval.
`Op::WeirdStorageUsageUpdates` has been removed; storage usage id allocation is handled outside the catalog transaction via `Catalog::allocate_storage_usage_id`.
`Op::UpdateScopedSystemParameters { scoped }` persists the durable cache of scoped (per-cluster and per-replica) system parameters to match the complete desired state supplied by the system-parameter sync loop. The handler diffs against the current durable contents: it upserts changed or added entries, removes entries no longer present in the desired state, and lazily prunes entries whose owning object id is absent from the catalog (object ids are never reused, so such entries are inert).
`Op::CreateClusterReplica` carries a required `replica_id: ReplicaId` field that must be pre-allocated out-of-band by the caller via the durable allocator (mirroring how cluster and item IDs are allocated). The apply path always uses `insert_cluster_replica_with_id` and never allocates a replica ID in-apply.
When dropping a cluster replica, the code expands dependent objects (including materialized views that target the replica) using `state.cluster_replica_dependents(cluster_id, replica_id, &mut seen)`, records their comments for cleanup, and adds them to the drop list; `seen` is seeded from already-collected items so the plan-driven path (which processes dependents before the replica in reverse-dependency order) does not re-add them.
When dropping items, all storage collections associated with a dropped entry are scheduled for removal unconditionally; whether the item was a replacement target is not considered at this stage.
