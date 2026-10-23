---
source: src/storage/src/source/generator.rs
revision: b382b15182
---

# mz-storage::source::generator

Implements `SourceRender` for `LoadGeneratorSourceConnection`, dispatching to one of seven built-in generators (Auction, Clock, Counter, Datums, KeyValue, Marketing, Tpch) based on the connection description.
The module drives each `Generator` implementation via a tokio interval and emits `SourceMessage` records partitioned across workers; generators are seeded for reproducibility and support resumption from a saved offset.
The `render` method and internal helpers accept a `ResumeUppers<MzOffset>` stream; each export's `offset_committed` and `offset_known` are reported individually using the per-export frontier from `ResumeUppers::exports`.
