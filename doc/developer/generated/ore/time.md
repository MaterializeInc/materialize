---
source: src/ore/src/time.rs
revision: f38003ddc8
---

# mz-ore::time

Provides the `DurationExt` trait, which adds two extension methods to `std::time::Duration`.
`try_from_secs_i64` constructs a `Duration` from a signed integer number of seconds, returning an error for negative values.
`saturating_mul_f64` multiplies a `Duration` by an `f64` factor, saturating at zero or `u64::MAX` seconds instead of panicking on overflow, NaN, or negative input.
