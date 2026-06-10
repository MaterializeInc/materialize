---
source: src/repr/src/relation_and_scalar.rs
revision: 1f45b1f21e
---

# mz-repr::relation_and_scalar

Contains protobuf-generated code (`include!` of the build output) for `ScalarType`, `ColumnType`, `RelationDesc`, `RelationType`, and related structs, along with `RustType` conversion implementations.
This file is the bridge between the in-memory representation and the protobuf wire format used in catalog persistence and cross-service communication.
