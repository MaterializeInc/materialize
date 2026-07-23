---
source: src/adapter/src/coord/message_handler.rs
revision: a60edac7f1
---

# adapter::coord::message_handler

Implements `Coordinator::handle_message`, the main dispatch for internal `Message` variants flowing through the coordinator's event loop.
Handles controller responses (compute peek results, subscribe batches, copy-to responses, watch-set notifications), timer ticks (group commit, timeline advancement, cluster scheduling, storage usage collection and pruning), staged-pipeline continuations (peek, create index, create view, create materialized view, subscribe, introspection subscribe, explain timestamp, secret, cluster), linearized read delivery, deferred statement execution, private-link VPC endpoint events, and cluster controller requests (`Message::ClusterControllerRequest`, dispatched to `handle_cluster_controller_request`).
This is the heart of the coordinator's reactive loop; every asynchronous response from the storage/compute layers arrives here.
`storage_usage_update` obtains a write timestamp from the oracle, allocates a single durable batch id via `Catalog::allocate_storage_usage_id`, builds `BuiltinTableUpdate` rows via `pack_storage_usage_update` for each shard, and submits them via `builtin_table_update().execute()` without going through `catalog_transact_inner`.
In read-only mode, `storage_usage_fetch` logs an info message and reschedules via `Message::StorageUsageSchedule` without performing any shard scan or writes.
`ARRANGEMENT_SIZES_FRESHNESS_MARGIN` (10 seconds) is the minimum age of a replica's introspection-subscribe delivery before `arrangement_sizes_snapshot` trusts its data; the margin covers the collection manager's batched write flush and the oracle read timestamp trailing the wall clock. `arrangement_sizes_snapshot` resolves catalog IDs on the main loop, then spawns a task that takes a read timestamp and snapshots `mz_object_arrangement_sizes_unified` and `mz_compute_hydration_times`; the task calls `arrangement_sizes_records` to prepare `Vec<ArrangementSizeRecord>` and sends them back as `Message::ArrangementSizesWrite` (or reschedules directly if empty or on failure). `arrangement_sizes_write` receives the prepared records, re-validates replica freshness (cluster events between snapshot and write can invalidate previously-fresh replicas), obtains a write timestamp, packs and appends the rows to `mz_object_arrangement_size_history`, and reschedules the next collection. In read-only mode, `arrangement_sizes_snapshot` reschedules without spawning a task.
