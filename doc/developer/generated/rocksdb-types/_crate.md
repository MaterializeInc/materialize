---
source: src/rocksdb-types/src/lib.rs
revision: e757b4d11b
---

# mz-rocksdb-types

Provides the shared configuration types for `mz-rocksdb`, decoupled into a separate crate so that consumers can depend on the types without pulling in the full RocksDB native library.

## Module structure

* `lib.rs` — crate root; re-exports `RocksDBTuningParameters` and `defaults` from `config`.
* `config` — all tuning types: `RocksDBTuningParameters`, `CompactionStyle`, `CompressionType`, `RocksDbWriteBufferManagerConfig`, and the `defaults` submodule.

## Key dependencies

* `serde` — serialization of tuning parameters for LaunchDarkly delivery.
* `uncased` — case-insensitive string parsing for `CompactionStyle` and `CompressionType`.
* `mz-ore` — test and metrics utilities (pulled in for test support).

## Downstream consumers

Consumed by `mz-rocksdb` (which applies the parameters to a live RocksDB instance) and by any crate that needs to represent or pass RocksDB configuration without a direct RocksDB dependency.
