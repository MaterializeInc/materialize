---
source: src/adapter/src/catalog/transact.rs
revision: 9d0a7c3c6f
---

# adapter::catalog::transact

Implements catalog transactions: `Catalog::transact` opens a durable `Transaction`, executes a sequence of `Op` (catalog operations such as CreateItem, DropObject, RenameItem, GrantPrivilege, etc.), flushes to durable storage, and returns the resulting `StateUpdate` diffs for the coordinator to apply.
The module enforces referential integrity, privilege checks, and naming constraints within the transaction, and writes audit log events and storage-usage records as side effects.
Zero-downtime deployment logic (0dt) is also managed here; DDL operations can be gated behind deployment-readiness checks during the cutover window.
`InjectedAuditEvent` struct and `Op::InjectAuditEvents` variant allow manually appending audit log entries at the current oracle write timestamp. `CREATE ROLE` audit events use `EventDetails::CreateRoleV1` which includes the `auto_provision_source` field.
When dropping items, all storage collections associated with a dropped entry are scheduled for removal unconditionally; whether the item was a replacement target is not considered at this stage.
