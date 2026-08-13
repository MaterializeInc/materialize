---
source: src/expr/src/scalar/func/impls/string.rs
revision: b28da7d561
---

# mz-expr::scalar::func::impls::string

The largest impls file, providing scalar function implementations for `text` datums.
Includes type-cast functions from string to nearly every other scalar type (`CastStringToBool`, `CastStringToInt16/32/64`, `CastStringToFloat32/64`, `CastStringToUint16/32/64`, `CastStringToOid`, `CastStringToNumeric`, `CastStringToDate`, `CastStringToTimestamp/Tz`, `CastStringToTime`, `CastStringToInterval`, `CastStringToUuid`, `CastStringToChar`, `CastStringToVarChar`, `CastStringToInt2Vector`, `CastStringToPgLegacyChar`, `CastStringToPgLegacyName`, `CastStringToBytes`).
Also provides `Reverse`, `IsLikeMatch`, `IsRegexpMatch`, `RegexpSplitToArray`, and various parameterized casts that manually implement `EagerUnaryFunc` or `LazyUnaryFunc`. Several casts that carry a sub-expression field are generic over an expression type parameter `E` (defaulting to `MirScalarExpr`): `CastStringToArray<E>`, `CastStringToList<E>`, `CastStringToMap<E>`, and `CastStringToRange<E>`. Each exposes `try_map_expr` and `map_expr` methods for converting the inner expression type (used when lowering from MIR to LIR).
Text manipulation is central to SQL, making this the broadest single-type impl module.
`text_to_name` (`cast_string_to_pg_legacy_name`) returns `preserves_uniqueness = false` because `parse_pg_legacy_name` truncates to 63 bytes, collapsing distinct inputs. `CastStringToVarChar` returns `preserves_uniqueness = self.length.is_none()`: when a length bound is present, truncation makes the cast non-injective regardless of `fail_on_len`; only the unbounded case is injective.
`Reverse` declares `preserves_uniqueness = true` and `inverse = to_unary!(Reverse)` (the function is its own left inverse), enabling the unary reducer to elide `reverse(reverse(x))` to `x`.
