---
source: src/storage-types/src/dyncfgs.rs
revision: 39acefa861
---

# storage-types::dyncfgs

Declares `mz_dyncfg::Config` constants for all storage-layer dynamic configuration parameters.
Covers flow-control (backpressure, source suspension), controller behaviour (shard finalisation), Kafka client settings (reconnect/retry backoff, client-ID enrichment, PrivateLink endpoint ID algorithm, sink producer message size and batching limits via `KAFKA_SINK_MESSAGE_MAX_BYTES`, `KAFKA_SINK_BATCH_SIZE`, and `KAFKA_SINK_BATCH_NUM_MESSAGES`), statistics retention windows, and upsert operator selection (`ENABLE_UPSERT_V2`, `STORAGE_USE_CONTINUAL_FEEDBACK_UPSERT`).
These constants are registered into a `ConfigSet` and can be read both statically during dataflow rendering and dynamically at runtime.
