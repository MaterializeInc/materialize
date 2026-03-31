---
source: src/ore/src/cast.rs
revision: 5f785f23fd
---

# mz-ore::cast

Defines a family of cast traits that replace unsafe or silent uses of the `as` operator with semantically explicit, compile-time-checked alternatives.

`CastFrom`/`CastInto` cover widening and platform-specific casts that the standard library omits (e.g. `u32 → usize` on 64-bit targets); `ReinterpretCast` makes bit-reinterpretation explicit (e.g. `u32 → i32`); `TryCastFrom` returns `Option` for casts that can only be round-tripped for a subset of values (notably float↔integer); `CastLossy` covers intentionally inexact conversions such as large integers to floating point.
Each trait is implemented via macro for all applicable primitive-type pairs, and the `cast_from!` macro also generates `const fn` free functions (e.g. `u64_to_usize`) for use in constant contexts.
