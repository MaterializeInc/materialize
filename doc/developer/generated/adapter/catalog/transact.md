---
source: src/adapter/src/catalog/transact.rs
revision: 3a1c877be6
---

# adapter::catalog::transact

Implements catalog transactions: `Catalog::transact` opens a durable `Transaction`, executes a sequence of `Op` (catalog operations such as CreateItem, DropObject, RenameItem, GrantPrivilege, etc.), flushes to durable storage, and returns the resulting `StateUpdate` diffs for the coordinator to apply.
The module enforces referential integrity, privilege checks, and naming constraints within the transaction, and writes audit log events as side effects.
Zero-downtime deployment logic (0dt) is also managed here; DDL operations can be gated behind deployment-readiness checks during the cutover window.
`InjectedAuditEvent` struct and `Op::InjectAuditEvents` variant allow manually appending audit log entries at the current oracle write timestamp. `CREATE ROLE` audit events use `EventDetails::CreateRoleV1` which includes the `auto_provision_source` field.
`Op::AlterAddColumn` emits an `AlterAddColumnV1` audit log event recording the table ID, column name, column type, and nullability. `Op::AlterTimestampInterval` emits an `AlterSourceTimestampIntervalV1` audit log event recording the item ID, old interval, and new interval.
`Op::WeirdStorageUsageUpdates` has been removed; storage usage id allocation is handled outside the catalog transaction via `Catalog::allocate_storage_usage_id`.
When dropping items, all storage collections associated with a dropped entry are scheduled for removal unconditionally; whether the item was a replacement target is not considered at this stage.
