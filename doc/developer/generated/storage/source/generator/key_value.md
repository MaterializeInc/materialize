---
source: src/storage/src/source/generator/key_value.rs
revision: b382b15182
---

# mz-storage::source::generator::key_value

Implements the `KeyValueLoadGenerator`, a configurable high-throughput load generator that emits key-value updates across multiple output partitions using parallel async operators.
Supports configurable key and value sizes, snapshot batch sizes, transactional groups, and a tick-rate, and uses seeded randomness for reproducibility.
The `render_statistics_operator` accepts a `ResumeUppers<MzOffset>` stream and reports each export's `offset_committed` and `offset_known` individually from its per-export frontier.
