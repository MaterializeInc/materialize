---
source: src/storage/src/source/generator/key_value.rs
revision: 5427dc5764
---

# mz-storage::source::generator::key_value

Implements the `KeyValueLoadGenerator`, a configurable high-throughput load generator that emits key-value updates across multiple output partitions using parallel async operators.
Supports configurable key and value sizes, snapshot batch sizes, transactional groups, and a tick-rate, and uses seeded randomness for reproducibility.
