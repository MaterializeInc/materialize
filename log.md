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

## Session 3: Direct timestamp/date/time formatting - eliminate chrono format overhead

**Date:** 2026-02-19

**Problem:** Every time a timestamp, date, or time value is formatted for output—via pgwire
text encoding or `Display`—the code used chrono's `ts.format("%m-%d %H:%M:%S")` which:
1. Parses the format string on **every call** (no caching)
2. Creates a `DelayedFormat` struct with internal state
3. Iterates through format directives with pattern matching
4. Calls accessor methods indirectly through the formatting machinery

This affects `format_timestamp`, `format_timestamptz`, `format_date`, and `format_time`
in `strconv.rs`, all of which are called for every value sent over pgwire. The
`format_nanos_to_micros` helper also used `write!` with dynamic width formatting.

**Fix:** Replaced all chrono `format()` calls with direct field extraction and stack-buffer
byte writing:
- `write_year()` handles the year with a fast path for 4-digit years (the common case)
  and a fallback for years > 9999
- `write_u2()` writes 2-digit zero-padded values directly via arithmetic
- Date/time components ("-MM-DD HH:MM:SS") are built in a stack-allocated `[u8; 15]` buffer
  and written with a single `write_str()` call
- `format_nanos_to_micros` builds ".NNNNNN" in a stack `[u8; 8]` buffer and trims trailing
  zeros, avoiding `write!` with dynamic width

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench timestamp_format`):

### Per-value timestamp formatting (ns/iter)

| Value               | Old (chrono format) | New (direct) | Speedup |
|---------------------|--------------------:|-------------:|--------:|
| epoch               | 158                 | 8.5          | 18.6x   |
| typical (2024-06-15)| 160                 | 8.5          | 18.9x   |
| with_micros         | 170                 | 13.8         | 12.3x   |
| small_micros        | 181                 | 13.4         | 13.5x   |
| year_0001           | 163                 | 8.3          | 19.6x   |
| year_9999           | 173                 | 13.6         | 12.7x   |

### Per-value timestamptz formatting (ns/iter)

| Value               | Old (chrono format) | New (direct) | Speedup |
|---------------------|--------------------:|-------------:|--------:|
| epoch               | 169                 | 10.8         | 15.6x   |
| typical             | 169                 | 10.9         | 15.5x   |
| with_micros         | 182                 | 17.3         | 10.5x   |
| small_micros        | 190                 | 16.9         | 11.2x   |
| year_0001           | 174                 | 10.9         | 16.0x   |
| year_9999           | 181                 | 18.3         | 9.9x    |

### Per-value date formatting (ns/iter)

| Value               | Old (chrono format) | New (direct) | Speedup |
|---------------------|--------------------:|-------------:|--------:|
| epoch               | 84.5                | 11.1         | 7.6x    |
| typical             | 85.8                | 11.0         | 7.8x    |
| year_end            | 84.6                | 11.3         | 7.5x    |

### Per-value time formatting (ns/iter)

| Value               | Old (chrono format) | New (direct) | Speedup |
|---------------------|--------------------:|-------------:|--------:|
| midnight            | 97.4                | 4.4          | 22.1x   |
| typical             | 96.5                | 4.4          | 21.9x   |
| with_micros         | 109                 | 10.1         | 10.8x   |

### Batch formatting (10k values to shared buffer)

| Approach                     | Time       | Speedup |
|------------------------------|------------|---------|
| Old chrono 10k timestamps    | 1,680 µs   | -       |
| New direct 10k timestamps    | 115 µs     | **14.6x** |
| Old chrono 10k timestamptz   | 1,790 µs   | -       |
| New direct 10k timestamptz   | 141 µs     | **12.7x** |

**Summary:** **10-22x faster per value, 13-15x faster in batch**. The enormous speedup
comes from eliminating chrono's format-string parsing overhead entirely. The old code
spent most of its time parsing "%m-%d %H:%M:%S" on every call; the new code extracts
fields directly and writes them through simple arithmetic into a stack buffer. Zero heap
allocations in the new path.

**Files changed:**
- `src/repr/src/strconv.rs` - Rewrote `format_date`, `format_time`, `format_timestamp`,
  `format_timestamptz`, and `format_nanos_to_micros` to use direct field extraction with
  stack buffers. Added `write_year()` and `write_u2()` helper functions.
- `src/repr/benches/timestamp_format.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `values_from_row()` in `src/pgrepr/src/value.rs:896` allocates a new `Vec<Option<Value>>`
  per row during pgwire encoding. Encoding directly from Datum to BytesMut would eliminate
  this per-row allocation.
- `Row::unpack()` in `src/repr/src/row.rs:656` iterates the row twice: once to count datums,
  once to collect them. A single-pass collector could give ~1.8-2x improvement.
- Top-K sorting in `src/compute/src/render/top_k.rs:908` unpacks entire rows just to compare
  a few columns. Indexed datum access (without full unpack) could give 2-4x improvement.
- `RowArena::push_unary_row()` calls `into_vec()` on `CompactBytes`, forcing heap allocation
  even for small rows that fit inline. Storing `CompactBytes` directly would help.
- JSONB/JSON interchange still use `to_standard_notation_string()` for numeric formatting.

## Session 4: Direct integer formatting - eliminate fmt machinery overhead

**Date:** 2026-02-19

**Problem:** Every time an integer value (i16, i32, i64, u16, u32, u64) is formatted for
pgwire output, the code used `write!(buf, "{}", i)` which goes through Rust's `fmt`
machinery:
1. Constructs a `fmt::Arguments` struct
2. Calls `FormatBuffer::write_fmt` which dispatches to `fmt::Write::write_fmt`
3. `write_fmt` invokes `Display::fmt` through dynamic dispatch
4. `Display::fmt` creates a `Formatter` with many fields (fill, align, width, precision, flags)
5. Formats the number into an internal buffer and writes it out

This affects `format_int16`, `format_int32`, `format_int64`, `format_uint16`,
`format_uint32`, `format_uint64`, and `format_mz_timestamp` in `strconv.rs`—all called for
every integer value sent over pgwire. Integer columns are among the most common column types
in practice.

**Fix:** Replaced all `write!` calls with direct digit extraction into a stack-allocated
`[u8; 20]` buffer (20 bytes covers the longest i64/u64 representation) and a single
`write_str()` call:
- Uses a 200-byte compile-time lookup table (`DIGIT_PAIRS`) that maps 0-99 to their two
  ASCII digit bytes, allowing two digits to be processed per division
- `write_i64_buf()` uses negative-number arithmetic internally to handle `i64::MIN`
  (-9223372036854775808) without overflow
- `write_u64_buf()` handles all unsigned types
- Both return a `&str` slice into the stack buffer, written with a single `write_str` call

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench int_format`):

### Per-value i64 formatting (ns/iter)

| Value               | Old (write! macro) | New (stack buf) | Speedup |
|---------------------|-------------------:|----------------:|--------:|
| zero                | 9.4                | 3.5             | 2.7x    |
| one                 | 9.2                | 3.0             | 3.1x    |
| small (42)          | 9.6                | 3.2             | 3.0x    |
| hundred (100)       | 10.2               | 3.2             | 3.2x    |
| thousand (9999)     | 10.0               | 4.6             | 2.2x    |
| million (1000000)   | 10.6               | 5.3             | 2.0x    |
| typical_id (1.2e9)  | 11.3               | 5.9             | 1.9x    |
| negative (-42)      | 11.4               | 3.1             | 3.6x    |
| neg_large (-1.2e9)  | 12.9               | 5.9             | 2.2x    |
| i32_max             | 11.3               | 5.9             | 1.9x    |
| i32_min             | 13.0               | 6.0             | 2.2x    |
| i64_max             | 13.4               | 8.3             | 1.6x    |
| i64_min             | 14.6               | 8.4             | 1.7x    |

### Per-value u64 formatting (ns/iter)

| Value               | Old (write! macro) | New (stack buf) | Speedup |
|---------------------|-------------------:|----------------:|--------:|
| zero                | 9.0                | 3.1             | 2.9x    |
| one                 | 9.0                | 4.1             | 2.2x    |
| small (42)          | 9.5                | 5.2             | 1.8x    |
| thousand (9999)     | 9.8                | 5.2             | 1.9x    |
| typical_id (1.2e9)  | 11.0               | 6.3             | 1.7x    |
| u32_max             | 11.2               | 6.3             | 1.8x    |
| u64_max             | 13.1               | 8.0             | 1.6x    |

### Batch formatting (10k values to shared buffer)

| Approach                  | Time (µs) | Speedup   |
|---------------------------|-----------|-----------|
| Old write! 10k i64        | 117       | -         |
| New stack_buf 10k i64     | 59        | **2.0x**  |
| Old write! 10k i32        | 110       | -         |
| New stack_buf 10k i32     | 52        | **2.1x**  |

**Summary:** 1.6-3.6x faster per value, **2.0-2.1x faster in batch**. The speedup comes
from eliminating the `fmt` machinery entirely (Arguments construction, Formatter setup,
trait dispatch). The 2-digit lookup table keeps performance high even for 19-20 digit
numbers. Zero heap allocations in the new path.

**Files changed:**
- `src/repr/src/strconv.rs` - Added `DIGIT_PAIRS` lookup table, `write_i64_buf()`, and
  `write_u64_buf()` helper functions. Updated `format_int16`, `format_int32`,
  `format_int64`, `format_uint16`, `format_uint32`, `format_uint64`, and
  `format_mz_timestamp` to use direct stack-buffer formatting.
- `src/repr/benches/int_format.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `values_from_row()` in `src/pgrepr/src/value.rs:896` allocates a `Vec<Option<Value>>`
  per row during pgwire encoding. Encoding directly from Datum to BytesMut would eliminate
  this per-row allocation (N+1 allocations per row for N string/bytes columns).
- `Row::unpack()` in `src/repr/src/row.rs:656` iterates the row twice: once to count datums,
  once to collect them. A single-pass collector could give ~1.8-2x improvement.
- Top-K sorting in `src/compute/src/render/top_k.rs:908` unpacks entire rows just to compare
  a few columns (Top1Monoid). The code itself acknowledges this ("It might be nice to cache
  this row decoding"). Top1MonoidLocal already shows the better pattern with shared DatumVec
  buffers.
- `RowArena` is allocated per-row in `peek_result_iterator.rs:210` during peek execution.
  A pooled/reusable arena would reduce allocation pressure.
- `format_uuid`, `format_jsonb`, `format_interval` in strconv.rs still use `write!` macro.
- JSONB/JSON interchange still use `to_standard_notation_string()` for numeric formatting.
