---
source: src/repr/src/adt/array.rs
revision: fe91a762d1
---

# mz-repr::adt::array

Defines `Array<'a, T>` and `ArrayDimension` types representing SQL arrays, along with `InvalidArrayError` for constraint violations.
`Array` is generic over an element type parameter `T` (defaulting to `Datum<'a>`) propagated through its inner `DatumList<'a, T>`; the parameter is a phantom marker and is not enforced at runtime.
`Array` stores elements as serialized bytes paired with dimension metadata (`ArrayDimensions`) and supports multi-dimensional arrays consistent with PostgreSQL's array semantics (up to `MAX_ARRAY_DIMENSIONS = 6`).
`Array<'a, Datum<'a>>` implements the `SqlContainerType` trait, enabling generic unwrap/wrap of `SqlScalarType::Array` element types at compile time.
