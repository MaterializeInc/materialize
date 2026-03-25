---
source: src/storage/src/sink/kafka.rs
revision: e79a6d96d9
---

# mz-storage::sink::kafka

Renders a Kafka sink dataflow comprising two operators: a `row_encoder` that encodes `Row` values to Avro/JSON (initialising schema-registry entries on startup) and a single-worker `kafka_sink` that transactionally commits encoded records plus frontier progress markers to data and progress topics using librdkafka transactions.
Implements the `SinkRender` trait for `KafkaSinkConnection`.
