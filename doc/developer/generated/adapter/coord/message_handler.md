---
source: src/adapter/src/coord/message_handler.rs
revision: 4267863081
---

# adapter::coord::message_handler

Implements `Coordinator::handle_message`, the main dispatch for internal `Message` variants flowing through the coordinator's event loop.
Handles controller responses (compute peek results, subscribe batches, storage status changes), timer ticks (group commit, cluster scheduling, storage usage collection, parameter sync), statement-logging watch-set notifications, and replication-factor adjustments.
This is the heart of the coordinator's reactive loop; every asynchronous response from the storage/compute layers arrives here.
