---
source: src/storage/src/source/generator/key_value.rs
revision: e79a6d96d9
---

# mz-storage::source::generator::key_value

Implements the `KeyValueLoadGenerator`, a configurable high-throughput load generator that emits key-value updates across multiple output partitions using parallel async operators.
Supports configurable key and value sizes, snapshot batch sizes, transactional groups, and a tick-rate, and uses seeded randomness for reproducibility.
