---
source: src/storage-controller/src/persist_handles/read_only_table_worker.rs
revision: 82d92a7fad
---

# storage-controller::persist_handles::read_only_table_worker

Implements `read_only_mode_table_worker`, a Tokio task that writes to migrated builtin table shards directly (bypassing the txn-wal system) while the controller is in read-only mode during zero-downtime upgrades.
It follows the upper of the txns shard and continuously advances registered table uppers to match, ensuring dataflows dependent on those tables can make progress.
This is an intentional short-term hack: it will be deleted once native persist schema migrations replace the need for builtin table migration in read-only mode.
