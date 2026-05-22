---
source: src/adapter/src/coord/appends.rs
revision: 967672afc3
---

# adapter::coord::appends

Implements all append operations executed by the coordinator: group-commit of user table writes, builtin-table updates, and the write-lock machinery that prevents concurrent conflicting writes.
`group_commit_initiate` coalesces pending table write requests into a single timestamped batch; `write_and_append_builtin_table_updates` drives updates to system catalog tables.
`BuiltinTableAppendNotify` and `GroupCommitWriteLocks` manage the notification and locking primitives used to serialise group commits.
`group_commit` always performs the append call even when there are no user writes, relying on the storage layer to periodically advance the upper of all tables; it does not iterate catalog entries to inject empty advancement entries per table.
