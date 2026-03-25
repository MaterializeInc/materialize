---
source: src/adapter/src/coord/appends.rs
revision: af5783aa4f
---

# adapter::coord::appends

Implements all append operations executed by the coordinator: group-commit of user table writes, builtin-table updates, and the write-lock machinery that prevents concurrent conflicting writes.
`group_commit_initiate` coalesces pending table write requests into a single timestamped batch; `write_and_append_builtin_table_updates` drives updates to system catalog tables.
`BuiltinTableAppendNotify` and `GroupCommitWriteLocks` manage the notification and locking primitives used to serialise group commits.
