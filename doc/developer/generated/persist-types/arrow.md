---
source: src/persist-types/src/arrow.rs
revision: 25802a51f9
---

# persist-types::arrow

Provides a compact protobuf representation of Apache Arrow `ArrayData` (`ProtoArrayData`) for space-efficient storage at the consensus layer.
Also defines `ArrayOrd` (a typed enum over all supported array types) and `ArrayIdx` (an index into an `ArrayOrd` with total order), which together enable lexicographic comparison of columnar rows across `Part`s.
`ArrayBound` wraps a single-element array as a lower bound that can be trimmed to a configurable byte budget while preserving its ordering guarantee.
`from_proto_with_type` validates that the number of children in a deserialized `ProtoArrayData` matches the count expected by its declared data type, returning a `TryFromProtoError` on mismatch rather than panicking.
