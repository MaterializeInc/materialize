---
source: src/storage/src/source/generator/tpch.rs
revision: 16d7fb2b2d
---

# mz-storage::source::generator::tpch

Implements the `Tpch` load-generator, which produces a TPC-H benchmark dataset (customer, orders, lineitem, part, partsupp, supplier, nation, region tables) at a configurable scale factor, with ongoing order refresh cycles that insert new orders and retract old ones.
