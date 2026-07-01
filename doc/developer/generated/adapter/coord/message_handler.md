---
source: src/adapter/src/coord/message_handler.rs
revision: 9d0b66c63c
---

# adapter::coord::message_handler

Implements `Coordinator::handle_message`, the main dispatch for internal `Message` variants flowing through the coordinator's event loop.
Handles controller responses (compute peek results, subscribe batches, copy-to responses, watch-set notifications), timer ticks (group commit, timeline advancement, cluster scheduling, storage usage collection and pruning), staged-pipeline continuations (peek, create index, create view, create materialized view, subscribe, introspection subscribe, explain timestamp, secret, cluster), linearized read delivery, deferred statement execution, and private-link VPC endpoint events.
This is the heart of the coordinator's reactive loop; every asynchronous response from the storage/compute layers arrives here.
`storage_usage_update` obtains a write timestamp from the oracle, allocates a single durable batch id via `Catalog::allocate_storage_usage_id`, builds `BuiltinTableUpdate` rows via `pack_storage_usage_update` for each shard, and submits them via `builtin_table_update().execute()` without going through `catalog_transact_inner`.
In read-only mode, `storage_usage_fetch` logs an info message and reschedules via `Message::StorageUsageSchedule` without performing any shard scan or writes.
