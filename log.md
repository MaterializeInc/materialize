# Materialize Optimization Log

## Session 1: Numeric-to-float casting - stack-allocated buffer

**Date:** 2026-02-18

**Problem:** `cast_numeric_to_float32` and `cast_numeric_to_float64` in
`src/expr/src/scalar/func/impls/numeric.rs` used `a.to_string().parse::<f64>()` which
heap-allocates a String for every conversion. This is called during `NUMERIC::float4`
and `NUMERIC::float8` casts, and also from JSONB float extraction.

**Fix:** Replaced the heap-allocated `String` with a stack-allocated 64-byte `NumericBuf`
that implements `fmt::Write`. The Decimal formatting + float parsing algorithm is
identical (correctly-rounded Eisel-Lemire), just without the malloc/free overhead.

Note: We also investigated using `f32/f64::try_from(Decimal)` from the dec crate which
is 5-6x faster for simple values, but it uses `powf` internally which introduces ~1 ULP
precision errors for values with large exponents (e.g., `9E-39` produces
`8.999999999999999e-39` instead of `9e-39`). This would break SLT tests, so we stuck
with the stack buffer approach which is bit-identical to the original.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench numeric_cast`):

### Per-value f64 conversion (ns/iter)

| Value               | Old (String) | New (StackBuf) | Speedup |
|---------------------|-------------|----------------|---------|
| small_int (42)      | 31          | 26             | 1.19x   |
| decimal (123.456789)| 43          | 35             | 1.23x   |
| large (99999...)    | 56          | 50             | 1.12x   |
| negative (-3.14...) | 49          | 42             | 1.17x   |
| tiny (0.000001)     | 36          | 29             | 1.24x   |
| zero                | 32          | 21             | 1.52x   |
| one                 | 31          | 21             | 1.48x   |
| pi (3.14159...)     | 55          | 49             | 1.12x   |

### Per-value f32 conversion (ns/iter)

| Value               | Old (String) | New (StackBuf) | Speedup |
|---------------------|-------------|----------------|---------|
| small_int (42)      | 30          | 25             | 1.20x   |
| zero                | 30          | 20             | 1.50x   |
| one                 | 31          | 20             | 1.55x   |
| pi (3.14159...)     | 56          | 49             | 1.14x   |

### Batch f64 conversion (10k values, ns/iter)

| Approach   | Time      | Speedup |
|------------|-----------|---------|
| Old String | 416,648   | -       |
| StackBuf   | 361,384   | 1.15x   |

**Summary:** ~15-55% faster per-value, ~15% faster in batch. Improvement comes entirely
from eliminating the heap allocation. The formatting + parsing algorithm is the dominant
cost.

**Files changed:**
- `src/expr/src/scalar/func/impls/numeric.rs` - Added `NumericBuf` stack buffer, updated
  `cast_numeric_to_float32` and `cast_numeric_to_float64`
- `src/repr/benches/numeric_cast.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- The dec crate's `TryFrom<Decimal> for f64` is 5-6x faster but has precision issues
  with `powf`. If the dec crate's conversion were fixed to use correctly-rounded
  arithmetic, it would be a bigger win.
- `cx_datum()` and `cx_agg()` clone a `Context` struct (24 bytes) on every call.
  The clone is cheap (~1ns) so this is NOT a meaningful bottleneck.
- `RowArena` is created per-row in `peek_result_iterator.rs:210` during peek execution.
  A TODO comment suggests reusing it, which could reduce allocation pressure for large
  result sets.
- Trace bundle is cloned in `compute_state.rs:582` for every peek - could use Arc.
- `SafeMfpPlan` is cloned multiple times during peek setup - could use Arc.

## Session 2: Zero-allocation numeric formatting for pgwire output

**Date:** 2026-02-19

**Problem:** Every time a `Numeric` (decimal) value is formatted for output—via pgwire
text encoding (`format_numeric` in `strconv.rs`) or `Datum::Display`—the code called
`Decimal::to_standard_notation_string()` which performs **two heap allocations** per value:
1. `coefficient_digits()` allocates a `Vec<u8>` (up to 39 bytes) via the C function
   `decNumberGetBCD`
2. `to_standard_notation_string()` allocates a `String` for the formatted result

The formatted String is then copied into the output buffer and immediately dropped. For a
query returning 100k numeric values, this means 200k unnecessary heap allocations.

**Fix:** Added `write_numeric_standard_notation()` in `src/repr/src/adt/numeric.rs` that
writes directly to any `fmt::Write` implementation with **zero heap allocations**:
- Uses `coefficient_units()` (returns `&[u16]` slice from the internal representation—no
  allocation) to extract digits into a stack-allocated `[u8; 39]` array
- Builds the complete output (sign, digits, decimal point, leading zeros) in a
  stack-allocated `[u8; 80]` buffer
- Writes the entire result with a single `write_str()` call

For `format_numeric` (pgwire path), a thin `FmtWriteFormatBuffer` adapter bridges the
`FormatBuffer` trait to `fmt::Write`.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench numeric_format`):

### Write-only: pure formatting cost to pre-allocated buffer (ns/iter)

| Value                      | Old (to_standard_notation_string) | New (write_numeric) | Speedup |
|----------------------------|----------------------------------|---------------------|---------|
| zero                       | 14.4                             | 2.4                 | 6.0x    |
| one                        | 14.5                             | 5.3                 | 2.7x    |
| small_int (42)             | 15.5                             | 6.3                 | 2.5x    |
| decimal (123.456789)       | 22.3                             | 11.1                | 2.0x    |
| negative (-3.14...)        | 28.3                             | 14.5                | 1.9x    |
| tiny (0.000001)            | 24.9                             | 7.1                 | 3.5x    |
| large_int (99999...)       | 30.3                             | 15.7                | 1.9x    |
| large_dec (99999.999...)   | 29.9                             | 16.5                | 1.8x    |
| max_precision (39 digits)  | 50.3                             | 21.0                | 2.4x    |
| small_exp (1e10)           | 36.3                             | 8.1                 | 4.5x    |
| neg_exp (1e-10)            | 31.9                             | 7.5                 | 4.3x    |
| many_decimals (0.123...39) | 56.3                             | 21.4                | 2.6x    |

### Per-value formatting to new String (ns/iter)

| Value                      | Old  | New  | Speedup |
|----------------------------|------|------|---------|
| zero                       | 16.5 | 14.5 | 1.1x    |
| small_int (42)             | 17.6 | 17.0 | 1.0x    |
| negative (-3.14...)        | 27.6 | 23.2 | 1.2x    |
| tiny (0.000001)            | 25.5 | 16.1 | 1.6x    |
| max_precision (39 digits)  | 49.5 | 30.5 | 1.6x    |
| small_exp (1e10)           | 35.5 | 16.8 | 2.1x    |
| many_decimals              | 50.2 | 30.3 | 1.7x    |

### Batch formatting (10k values to shared buffer, µs/iter)

| Approach                        | Time (µs) | Speedup |
|---------------------------------|-----------|---------|
| Old (to_standard_notation_string) | 306       | -       |
| New (write_numeric)             | 118       | 2.6x    |

**Summary:** 1.8-6.0x faster per value depending on the value, **2.6x faster in the
realistic batch scenario** (pgwire formatting many values to a shared buffer). The speedup
comes from eliminating both heap allocations (coefficient Vec + result String) and using a
single `write_str` call instead of per-character writes.

**Files changed:**
- `src/repr/src/adt/numeric.rs` - Added `write_numeric_standard_notation()` + tests
- `src/repr/src/strconv.rs` - Updated `format_numeric()` to use new function, added
  `FmtWriteFormatBuffer` adapter
- `src/repr/src/scalar.rs` - Updated `Datum::Numeric` Display to use new function
- `src/repr/benches/numeric_format.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `format_timestamp`/`format_timestamptz` in `strconv.rs` use `ts.format("%m-%d %H:%M:%S")`
  which creates a chrono `DelayedFormat` with format string parsing overhead. Could be
  replaced with direct `month()`, `day()`, `hour()`, `minute()`, `second()` extraction.
- `values_from_row()` in `src/pgrepr/src/value.rs:896` allocates a new `Vec<Option<Value>>`
  per row during pgwire encoding. Encoding directly from Datum to BytesMut would eliminate
  this allocation.
- `zero_diffs.clone()` in `src/compute/src/render/reduce.rs:1313` clones a `Vec<Accum>`
  per input row during accumulable reductions. Could use SmallVec for common case.
- JSONB serialization in `src/repr/src/adt/jsonb.rs:604` and JSON interchange in
  `src/interchange/src/json.rs:131` also use `to_standard_notation_string()` and could be
  updated to use `write_numeric_standard_notation()` for the same benefit.
