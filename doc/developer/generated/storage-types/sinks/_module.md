---
source: src/storage-types/src/sinks.rs
revision: 0c2379c49c
---

# storage-types::sinks

Defines `StorageSinkDesc`, the full description of a storage sink dataflow, parameterised over metadata type `S` and timestamp `T`.
`StorageSinkConnection` enumerates the supported sink backends: `Kafka` and `Iceberg`.
`SinkEnvelope` covers three variants: `Debezium` (Kafka only), `Upsert`, and `Append` (Iceberg only).
`KafkaSinkConnection` carries topic, format, compression, key/value descriptors, partition-by expression, and ID style fields; `IcebergSinkConnection` carries catalog and AWS connection references, namespace, table, and key descriptors.
`KafkaSinkFormat` and `KafkaSinkFormatType` describe the key/value encoding (Avro, JSON, Text, or Bytes).
`KafkaSinkCompressionType` maps to librdkafka compression options (none, gzip, snappy, lz4, zstd).
`KafkaIdStyle` distinguishes prefixed (new-style) from legacy IDs for progress group and transactional IDs.
Constants `ICEBERG_APPEND_DIFF_COLUMN` (`_mz_diff`) and `ICEBERG_APPEND_TIMESTAMP_COLUMN` (`_mz_timestamp`) name the extra columns appended by `MODE APPEND` Iceberg sinks.
`S3UploadInfo` and `S3SinkFormat` support the copy-to S3 path; file size bounds are `MIN_S3_SINK_FILE_SIZE` (16 MiB) and `MAX_S3_SINK_FILE_SIZE` (4 GiB).
The `s3_oneshot_sink` submodule provides the S3 preflight logic used before a copy-to sink begins writing.
