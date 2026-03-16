---
source: src/storage-types/src/sinks.rs
revision: 2775b554ae
---

# storage-types::sinks

Defines `StorageSinkDesc`, the full description of a storage sink dataflow, parameterised over metadata type `S` and timestamp `T`.
`StorageSinkConnection` enumerates the supported sink backends: `Kafka` and `S3Oneshot` (copy-to).
Also defines `SinkEnvelope` (Debezium or Upsert), `KafkaSinkConnection`, `KafkaSinkCompressionType`, and `S3UploadInfo`.
The `s3_oneshot_sink` submodule provides the S3 preflight logic used before a copy-to sink begins writing.
