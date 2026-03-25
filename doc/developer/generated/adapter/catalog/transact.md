---
source: src/adapter/src/catalog/transact.rs
revision: 892cf626bc
---

# adapter::catalog::transact

Implements catalog transactions: `Catalog::transact` opens a durable `Transaction`, executes a sequence of `Op` (catalog operations such as CreateItem, DropObject, RenameItem, GrantPrivilege, etc.), flushes to durable storage, and returns the resulting `StateUpdate` diffs for the coordinator to apply.
The module enforces referential integrity, privilege checks, and naming constraints within the transaction, and writes audit log events and storage-usage records as side effects.
Zero-downtime deployment logic (0dt) is also managed here; DDL operations can be gated behind deployment-readiness checks during the cutover window.
