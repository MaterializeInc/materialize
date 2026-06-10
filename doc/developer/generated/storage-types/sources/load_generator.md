---
source: src/storage-types/src/sources/load_generator.rs
revision: 4267863081
---

# storage-types::sources::load_generator

Defines `LoadGeneratorSourceConnection` and the `LoadGenerator` enum covering the built-in synthetic data generators (Auction, Counter, Marketing, TPCH, KeyValue, Clock).
Each generator variant carries its own configuration and implements the `Generator` trait to produce an `Event` stream of (timestamp, row) pairs.
Provides static `RelationDesc`s for each generator's output relations and progress subsource.
