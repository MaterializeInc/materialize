---
source: src/storage/src/sink/kafka.rs
revision: ba3c29ef7b
---

# mz-storage::sink::kafka

Renders a Kafka sink dataflow comprising two operators: `encode_collection` walks the input `SinkBatchStream` via `for_each_diff_pair`, emitting one encoded `KafkaMessage` per `DiffPair` at each `(key, timestamp)` (initialising schema-registry entries on startup for Avro), and a single-worker `sink_collection` that transactionally commits encoded records plus frontier progress markers to data and progress topics using librdkafka transactions.
Implements the `SinkRender` trait for `KafkaSinkConnection`.
The encoder resolves `headers_index` and evaluates the optional `partition_by` `MirScalarExpr` to determine per-message partition assignments; user-specified headers whose keys begin with `materialize-` are silently dropped.
Avro schema registration for both key and value encoders is handled by `build_avro_encoder`, which dispatches to either `publish_kafka_schema` (Confluent Schema Registry) or `publish_glue_schema` (AWS Glue Schema Registry) based on the `WireFormat`, and builds an `AvroEncoder` framing records with the resulting schema id. A missing registry is treated as unreachable since the sink planner always supplies one.
`TransactionalProducer` wraps a `ThreadedProducer` and manages the full transaction lifecycle: `init_transactions` (which fences out prior producers), `begin_transaction`, per-message `send`, and `commit_transaction` (which also writes a `ProgressRecord` to the progress topic).
The producer is configured with `message.max.bytes`, `batch.size`, and `batch.num.messages` drawn from the dyncfg constants `KAFKA_SINK_MESSAGE_MAX_BYTES`, `KAFKA_SINK_BATCH_SIZE`, and `KAFKA_SINK_BATCH_NUM_MESSAGES` respectively, allowing these librdkafka limits to be adjusted at runtime.
Progress records carry the current frontier antichain and the sink version, enabling fencing of older sink instances on restart.
A background task (`fetch_partition_count_loop`) keeps the cached partition count up to date across the topic's lifetime.
A separate background task (`collect_statistics`) forwards librdkafka broker-level statistics to Prometheus metrics via a `watch` channel. The per-broker accumulator variables are re-initialized to zero on each statistics interval so that both running totals (counters) and point-in-time values (gauges) are aggregated from scratch across all brokers, avoiding stale accumulation from previous intervals.
