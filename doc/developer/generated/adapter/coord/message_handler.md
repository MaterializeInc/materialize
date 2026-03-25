---
source: src/adapter/src/coord/message_handler.rs
revision: a29f0a64ed
---

# adapter::coord::message_handler

Implements `Coordinator::handle_message`, the main dispatch for internal `Message` variants flowing through the coordinator's event loop.
Handles controller responses (compute peek results, subscribe batches, copy-to responses, watch-set notifications), timer ticks (group commit, timeline advancement, cluster scheduling, storage usage collection and pruning), staged-pipeline continuations (peek, create index, create view, create materialized view, subscribe, introspection subscribe, explain timestamp, secret, cluster), linearized read delivery, deferred statement execution, and private-link VPC endpoint events.
This is the heart of the coordinator's reactive loop; every asynchronous response from the storage/compute layers arrives here.
