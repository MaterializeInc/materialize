---
source: src/rocksdb-types/src/config.rs
revision: 5eaf0598f0
---

# mz-rocksdb-types::config

Defines `RocksDBTuningParameters`, the plain-old-data struct that carries every knob used to tune a RocksDB instance for Materialize's `UPSERT` workload (high write rate, no durability requirement, low space amplification).
Key supporting types are `CompactionStyle` (`Level` or `Universal`) and `CompressionType` (`Zstd`, `Snappy`, `Lz4`, `None`), both serializable and parseable from case-insensitive strings for use as LaunchDarkly dynamic configuration parameters.
The `defaults` submodule exposes named constants for every field's default value, and `RocksDbWriteBufferManagerConfig` carries write-buffer-manager settings that span multiple RocksDB instances.
