---
source: src/persist-client/src/write.rs
revision: 4267863081
---

# persist-client::write

Defines `WriteHandle`, the primary public API for writing to a shard, and `WriterId` (a UUID identifying a writer session).
`WriteHandle` exposes `compare_and_append` (atomic upper-check-then-append), `append` (unconditional retry loop), and `batch` (build a `Batch` without appending it yet).
Writer sessions heartbeat to register their presence; the upper of the shard advances only through successful `compare_and_append` operations.
