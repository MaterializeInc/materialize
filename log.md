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
