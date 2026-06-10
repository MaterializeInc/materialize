---
source: src/persist-types/src/arrow.rs
revision: fa066df3e5
---

# persist-types::arrow

Provides a compact protobuf representation of Apache Arrow `ArrayData` (`ProtoArrayData`) for space-efficient storage at the consensus layer.
Also defines `ArrayOrd` (a typed enum over all supported array types) and `ArrayIdx` (an index into an `ArrayOrd` with total order), which together enable lexicographic comparison of columnar rows across `Part`s.
`ArrayBound` wraps a single-element array as a lower bound that can be trimmed to a configurable byte budget while preserving its ordering guarantee.
