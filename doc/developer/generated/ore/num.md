---
source: src/ore/src/num.rs
revision: 4267863081
---

# mz-ore::num

Defines `NonNeg<T>`, a transparent wrapper over signed numeric types that enforces non-negativity at construction time.
`NonNeg::try_from` returns `NonNegError` when the value is negative; the type derefs to its inner value and implements standard traits including `Serialize`/`Deserialize`, `Ord`, and (with the `proptest` feature) `Arbitrary`.
Conversions to `u64` and `usize` (on 64-bit targets) are provided for `NonNeg<i64>`.
