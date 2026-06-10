---
source: src/expr/src/scalar/func.rs
revision: 2a36076f3c
---

# mz-expr::scalar::func

The central module for all scalar function definitions in `mz-expr`.
The root `func.rs` file defines many arithmetic, string, regex, timezone, and cryptographic functions directly using `#[sqlfunc]`, plus shared helpers like `parse_timezone`, `build_regex`, `stringify_datum`, `regexp_match_static`, `regexp_split_to_array_re`, and `array_create_scalar`.
Submodules define the function enum infrastructure and per-type implementations:
- `macros` -- `derive_unary\!`, `derive_binary\!`, `derive_variadic\!`, and `to_unary\!` code-generation macros.
- `unary` -- `LazyUnaryFunc`, `EagerUnaryFunc` traits and the `UnaryFunc` enum.
- `binary` -- `LazyBinaryFunc`, `EagerBinaryFunc` traits and the `BinaryFunc` enum.
- `variadic` -- `LazyVariadicFunc`, `EagerVariadicFunc` traits and the `VariadicFunc` enum.
- `impls` -- per-type function implementations (one submodule per SQL scalar type).
- `unmaterializable` -- `UnmaterializableFunc` for session-context functions.
- `format` -- date/time formatting.
- `encoding` -- binary encoding.

`UnaryFunc`, `BinaryFunc`, `VariadicFunc`, and `UnmaterializableFunc` are re-exported and referenced by `MirScalarExpr`.
Several timestamp/date/interval arithmetic functions have corrected `is_monotone` annotations: `add_timestamp_interval`, `add_timestamp_tz_interval`, `sub_timestamp_interval`, `sub_timestamp_tz_interval` are `(false, false)`; `add_date_interval` and `sub_date_interval` are `(true, false)` (monotone in the date argument but not in the interval argument); `date_bin_timestamp` and `date_bin_timestamp_tz` are `(false, true)` (not monotone in stride, but monotone in source). These corrections reflect that `Interval` lex order (months, days, micros) does not respect calendar-aware arithmetic with day-clamping.
`age_timestamp` and `age_timestamp_tz` carry no `is_monotone` annotation (non-monotone in both arguments): as `a` increases past a month boundary, `months` increments while `days` drops, producing a lex-smaller `Interval`; holding `a` fixed and varying `b` produces a V-shape at `a == b`.
`date_bin` computes `origin + floor((source - origin) / stride) * stride`. When `source < origin` and `(source - origin)` is an exact multiple of `stride` (i.e., the source lands exactly on a bin boundary before the origin), the remainder is 0 and no extra stride subtraction is applied, so the result is exactly `source` (the correct bin start). Overflow at the `i64` nanosecond boundary surfaces as `EvalError::DateBinOutOfRange` via `checked_sub`.
`array_lower` returns the lower bound of the requested dimension (which may differ from 1 for arrays with custom lower bounds); `array_upper` returns the upper bound (`lower_bound + length - 1`), not the raw length.
