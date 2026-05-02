---
source: src/storage/src/source/generator.rs
revision: 5427dc5764
---

# mz-storage::source::generator

Implements `SourceRender` for `LoadGeneratorSourceConnection`, dispatching to one of seven built-in generators (Auction, Clock, Counter, Datums, KeyValue, Marketing, Tpch) based on the connection description.
The module drives each `Generator` implementation via a tokio interval and emits `SourceMessage` records partitioned across workers; generators are seeded for reproducibility and support resumption from a saved offset.
