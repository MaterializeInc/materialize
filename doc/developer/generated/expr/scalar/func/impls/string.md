---
source: src/expr/src/scalar/func/impls/string.rs
revision: 61475c0097
---

# mz-expr::scalar::func::impls::string

The largest impls file, providing scalar function implementations for `text` datums.
Includes type-cast functions from string to nearly every other scalar type (`CastStringToBool`, `CastStringToInt16/32/64`, `CastStringToFloat32/64`, `CastStringToUint16/32/64`, `CastStringToOid`, `CastStringToNumeric`, `CastStringToDate`, `CastStringToTimestamp/Tz`, `CastStringToTime`, `CastStringToInterval`, `CastStringToUuid`, `CastStringToChar`, `CastStringToVarChar`, `CastStringToInt2Vector`, `CastStringToPgLegacyChar`, `CastStringToPgLegacyName`, `CastStringToBytes`).
Also provides `Reverse`, `IsLikeMatch`, `IsRegexpMatch`, `RegexpSplitToArray`, and various parameterized casts that manually implement `EagerUnaryFunc` or `LazyUnaryFunc`.
Text manipulation is central to SQL, making this the broadest single-type impl module.
