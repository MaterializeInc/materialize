---
source: src/adapter/src/coord/appends.rs
revision: 6d5a5b088e
---

# adapter::coord::appends

Implements all append operations executed by the coordinator: group-commit of user table writes, builtin-table updates, and the write-lock machinery that prevents concurrent conflicting writes.
`group_commit_initiate` coalesces pending table write requests into a single timestamped batch; `write_and_append_builtin_table_updates` drives updates to system catalog tables.
`BuiltinTableAppendNotify` and `GroupCommitWriteLocks` manage the notification and locking primitives used to serialise group commits.
`group_commit` always performs the append call even when there are no user writes, relying on the storage layer to periodically advance the upper of all tables; it does not iterate catalog entries to inject empty advancement entries per table.
`UserWriteResponder` is an enum with a single `Session(PendingTxn)` variant that wraps the per-session `PendingTxn` for `PendingWriteTxn::User`; the `User` variant's `pending_txn` field is replaced by `responder: UserWriteResponder`. The write timestamp for `User` writes is picked by the oracle during group commit; the write lock is either handed off from the submitting session (`write_locks: Some(..)`) or acquired during group commit (`write_locks: None`).
