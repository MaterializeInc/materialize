---
source: src/repr/src/adt/jsonb.rs
revision: 844ad57e4b
---

# mz-repr::adt::jsonb

Defines `Jsonb` (owned) and `JsonbRef` (borrowed) for representing PostgreSQL-compatible JSONB values stored as a `Row` of datums.
Provides `JsonbPacker` for building JSONB values datum-by-datum and `JsonbRef` for tree-traversal and serialization to/from `serde_json::Value`.
