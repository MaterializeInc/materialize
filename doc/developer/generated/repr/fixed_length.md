---
source: src/repr/src/fixed_length.rs
revision: 31e0aab020
---

# mz-repr::fixed_length

Defines `ExtendDatums`, a trait for types that can append their datums to a `Vec<Datum>`, abstracting over `Row` and alternative trace representations (e.g., `DatumSeq` in `row_spine`). The single required method `extend_datums(arena, target, max)` takes a `RowArena` for representations that need to decode into borrowed storage, a target `Vec<Datum>`, and an optional maximum datum count. Representations backed directly by packed bytes (such as `Row`) ignore the arena. A blanket impl forwards through shared references. The previous `ToDatumIter` trait (which produced an iterator) has been removed in favor of this push-based design.
