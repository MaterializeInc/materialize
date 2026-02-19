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

## Session 5: Fast-path float formatting - bypass ryu for integer-valued floats

**Date:** 2026-02-19

**Problem:** Every time a float value (f32, f64) is formatted for pgwire output, the code
used `ryu::Buffer::format_finite(f)` followed by a per-character `Peekable<Chars>` iteration
to fix up the ryu output (strip ".0" suffix, insert '+' before positive exponents). This has
two sources of overhead:
1. **ryu formatting** (~30-44ns per value): Even for simple integer-valued floats like 0.0,
   1.0, 42.0, ryu runs its full Ryū algorithm (Grisu/Schubfach) to compute the shortest
   representation, which is massive overkill for integers.
2. **Per-character iteration** (~5-15ns per value): The `Peekable<Chars>` loop calls
   `write_char()` for each character individually instead of writing bulk string slices.

For many real-world workloads, float columns often contain integer-like values (counts,
measurements, IDs stored as doubles, etc.), making optimization (1) particularly impactful.

**Fix:** Two optimizations in `format_float`:

1. **Integer fast path**: For integer-valued floats where `f == f.trunc()` and the value fits
   in i64 with `|value| < 1e15`, bypass ryu entirely and use our direct integer formatter
   (from session 4). This works because ryu uses decimal notation for these values, producing
   output identical to our integer formatter after ".0" stripping. The threshold of 1e15
   ensures we stay within ryu's decimal notation range (ryu switches to scientific notation
   at ~1e16 for round powers of 10).

2. **Bulk string writes**: For the ryu path (non-integer floats), replaced the per-character
   `Peekable<Chars>` iteration with byte-level `position()` search for 'e' and 1-3
   `write_str()` calls. For the common case (no exponent), this is a single `write_str`.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench float_format`):

### Per-value f64 formatting (ns/iter)

| Value                | Old (ryu+per-char) | New (fast path) | Speedup |
|----------------------|-------------------:|----------------:|--------:|
| zero (0.0)           | 6.9                | 4.6             | 1.5x    |
| one (1.0)            | 41.0               | 5.0             | **8.2x**|
| small_int (42.0)     | 39.0               | 5.0             | **7.8x**|
| hundred (100.0)      | 43.7               | 5.4             | **8.0x**|
| thousand (9999.0)    | 38.7               | 6.0             | **6.5x**|
| sci_pos_exp (1.5e10) | 48.6               | 10.0            | **4.9x**|
| pi (3.14159...)      | 32.6               | 27.1            | 1.2x    |
| typical (3.14)       | 33.3               | 31.6            | 1.05x   |
| neg_pi (-3.14159...) | 33.3               | 27.5            | 1.21x   |
| negative (-42.5)     | 41.8               | 40.7            | 1.03x   |
| small_frac (0.001)   | 37.4               | 35.3            | 1.06x   |
| large (1e15)         | 53.4               | 48.8            | 1.09x   |
| large_frac (1.23e15) | 43.3               | 39.1            | 1.11x   |
| max_sig (1.8e308)    | 37.3               | 29.4            | 1.27x   |
| min_pos (5e-324)     | 21.8               | 19.6            | 1.11x   |

### Per-value f32 formatting (ns/iter)

| Value                | Old (ryu+per-char) | New (fast path) | Speedup |
|----------------------|-------------------:|----------------:|--------:|
| zero (0.0)           | 6.9                | 4.4             | 1.6x    |
| one (1.0)            | 24.5               | 5.0             | **4.9x**|
| hundred (100.0)      | 27.1               | 5.5             | **4.9x**|
| sci_pos_exp (1.5e10) | 43.7               | 9.3             | **4.7x**|
| pi (3.14159...)      | 21.9               | 18.0            | 1.22x   |
| typical (3.14)       | 22.2               | 21.2            | 1.05x   |
| negative (-42.5)     | 27.0               | 26.8            | 1.01x   |

### Batch formatting (10k values to shared buffer)

| Approach                           | Time      | Speedup   |
|------------------------------------|-----------|-----------|
| Old ryu 10k mixed f64              | 424 µs    | -         |
| New optimized 10k mixed f64        | 199 µs    | **2.1x**  |
| Old ryu 10k integer-valued f64     | 337 µs    | -         |
| New fast-path 10k integer f64      | 61 µs     | **5.5x**  |

**Summary:** For integer-valued floats (the fast path), **5-8x faster** by bypassing ryu
entirely and using the direct integer formatter. For non-integer floats, 1.05-1.27x faster
from the bulk write_str optimization. In realistic batch scenarios: **2.1x faster for mixed
workloads, 5.5x faster for integer-heavy workloads**. The fast path adds only ~1-2ns overhead
for non-integer values (the `f == f.trunc()` check), which is negligible.

**Files changed:**
- `src/repr/src/strconv.rs` - Added integer fast path to `format_float`, replaced per-char
  `Peekable<Chars>` iteration with byte-level `position()` + `write_str` slicing. Added
  correctness test `test_format_float64`.
- `src/repr/benches/float_format.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `values_from_row()` in `src/pgrepr/src/value.rs:896` allocates a `Vec<Option<Value>>`
  per row during pgwire encoding. For string columns, `s.to_owned()` clones every string
  into the Value enum; for bytes, `b.to_vec()` clones. Encoding directly from Datum to
  BytesMut would eliminate all these per-row allocations. Could pre-encode DataRow bytes
  eagerly (before sending through the async channel) to avoid the intermediate Value
  representation entirely.
- `format_uuid` uses `write!(buf, "{}", uuid)` - could use `uuid.as_hyphenated().encode_lower()`
  to a stack buffer + single write_str, avoiding fmt machinery for 36 chars.
- `format_bytes` uses `hex::encode(bytes)` which heap-allocates a String for every bytea
  value. Could write hex directly to the buffer with a lookup table.
- `format_interval` uses multiple `write!` calls in its Display impl - could use direct
  formatting similar to timestamp optimization.
- DatumVec already solves the Row::unpack() hot path (single-pass with allocation reuse).
  Row::unpack() two-pass is only used in cold paths and is not worth optimizing.
- `CheapTimestamp` decoding in `read_datum` does integer division + DateTime::from_timestamp
  construction on every timestamp datum read. Could potentially be made faster with direct
  NaiveDateTime construction.

## Session 6: Direct DataRow encoding - bypass per-row Vec<Option<Value>> allocation

**Date:** 2026-02-19

**Problem:** Every row sent over pgwire went through a two-step encoding pipeline:
1. `values_from_row()` converts a `RowRef` into `Vec<Option<Value>>`, which:
   - Allocates a new `Vec` per row (N+1 allocations for N columns)
   - For `Datum::String(s)`, clones the string data via `s.to_owned()` into `Value::Text(String)`
   - For `Datum::Bytes(b)`, clones the bytes via `b.to_vec()` into `Value::Bytea(Vec<u8>)`
   - For all other types, creates owned `Value` variants (e.g., `Value::Int4(i32)`)
2. The `Codec::encode()` method then iterates over the `Vec<Option<Value>>`, calling
   `Value::encode_text()` for each field, writing into `BytesMut`

The intermediate `Value` representation was completely unnecessary for text-format encoding:
the same `strconv` formatters that `Value::encode_text()` calls can be called directly with
the borrowed Datum data. For a query returning 100k rows with 5 columns, this was
100k Vec allocations + 100k×(string columns) string clones, all immediately discarded.

**Fix:** Two changes:

1. **`encode_data_row_direct()`** in `src/pgrepr/src/value.rs`: Encodes a complete pgwire
   DataRow message directly from `RowRef` to `BytesMut`, including the 'D' type byte,
   message length, field count, and per-field encoding. For text format, it calls
   `encode_datum_text_direct()` which dispatches directly to `strconv` formatters for all
   simple scalar types (bool, int, float, numeric, timestamp, date, time, uuid, interval,
   string, bytes, jsonb). Complex types (arrays, lists, maps, records, ranges) fall back
   to the `Value` path.

2. **`BackendMessage::PreEncoded(Bytes)`** variant in `src/pgwire/src/message.rs`: Allows
   the protocol layer to send pre-encoded wire bytes directly through the codec without
   re-framing. The `Codec::encode()` method writes `PreEncoded` bytes directly to the
   output buffer, bypassing all type-byte/length/field encoding logic.

3. **Protocol integration** in `src/pgwire/src/protocol.rs`: The `send_rows` loop now
   encodes each row directly into a reusable `BytesMut` buffer using
   `encode_data_row_direct()`, then wraps it in `BackendMessage::PreEncoded` for sending.
   The `row_buf` is allocated once and reused across rows via `clear()`/`split()`.

**Benchmark results** (criterion, `cargo bench -p mz-pgrepr --bench datarow_encode`):

### Per-row DataRow encoding (ns/iter)

| Scenario              | Old (values_from_row) | New (direct_encode) | Speedup |
|-----------------------|----------------------:|--------------------:|--------:|
| integers_5col         | 170                   | 154                 | 1.10x   |
| mixed_5col            | 165                   | 131                 | 1.26x   |
| strings_5col          | 198                   | 124                 | 1.60x   |
| wide_15col            | 500                   | 431                 | 1.16x   |

### Batch encoding (10k rows to shared buffer)

| Scenario                     | Old (ms)  | New (ms)  | Speedup   |
|------------------------------|-----------|-----------|-----------|
| mixed_5col (10k rows)        | 1.684     | 1.370     | **1.23x** |
| strings_5col (10k rows)      | 1.783     | 1.263     | **1.41x** |

**Summary:** 1.1-1.6x faster per row, **1.2-1.4x faster in batch**. The biggest win is for
string-heavy rows (1.6x per row, 1.4x batch) because the old path cloned every string into
an owned `Value::Text(String)`. For integer-only rows the improvement is smaller (1.1x)
since the `Vec` allocation overhead is amortized across column encoding time.

The improvement compounds with previous formatting optimizations: the direct path writes
integers, timestamps, floats, etc. using the optimized stack-buffer formatters from sessions
2-5 without any intermediate `Value` construction.

**End-to-end test** (against running Materialize, 100k rows, Python psycopg client):
- `SELECT * FROM bench_ints` (5 int cols): median=494ms, ~202k rows/sec
- `SELECT * FROM bench_mixed` (3 int + 2 text + 1 bool): median=594ms, ~168k rows/sec
- `SELECT * FROM bench_strings` (5 text cols): median=674ms, ~148k rows/sec

These timings include compute, storage, encoding, network, and client-side parsing.
The encoding improvement from direct DataRow encoding is approximately 20-40% of the
pgwire encoding phase (as measured by the micro-benchmark), but encoding is only one
component of the total query time.

**Files changed:**
- `src/pgrepr/src/value.rs` - Added `encode_datum_text_direct()` and
  `encode_data_row_direct()` functions
- `src/pgrepr/src/lib.rs` - Re-exported `encode_data_row_direct`
- `src/pgwire/src/message.rs` - Added `BackendMessage::PreEncoded(Bytes)` variant
- `src/pgwire/src/codec.rs` - Handle `PreEncoded` in `Codec::encode()`
- `src/pgwire/src/protocol.rs` - Rewrote `send_rows` loop to use direct encoding
- `src/pgrepr/Cargo.toml` - Added criterion dev-dependency, registered benchmark
- `src/pgrepr/benches/datarow_encode.rs` - Added benchmark (new file)

**Future optimization ideas identified during research:**
- `format_bytes` uses `hex::encode(bytes)` which heap-allocates a String for every bytea
  value. Could write hex directly to the buffer with a lookup table.
- `format_uuid` uses `write!(buf, "{}", uuid)` - could use `uuid.as_hyphenated().encode_lower()`
  to a stack buffer + single write_str, avoiding fmt machinery.
- `format_interval` uses multiple `write!` calls - could use direct formatting.
- JSONB/JSON interchange still use `to_standard_notation_string()` for numeric formatting.
- The `row_buf.split().freeze()` in the protocol loop creates a `Bytes` (atomic refcount)
  per row. If the codec supported writing directly to its internal buffer instead of going
  through the `BackendMessage` enum, this per-row `Bytes` allocation could be eliminated.

## Session 7: Direct hex encoding for bytea - eliminate hex::encode heap allocation

**Date:** 2026-02-19

**Problem:** Every time a `bytea` value is formatted for pgwire output, `format_bytes` in
`strconv.rs` called `hex::encode(bytes)` which:
1. Allocates a `String` with capacity `2 * bytes.len()`
2. Iterates over input bytes, writing hex chars into the String
3. `write!(buf, "\\x{}", hex_string)` copies the String through `fmt` machinery into the
   output buffer
4. The String is immediately dropped

For a 32-byte SHA-256 hash, this means a 64-byte String allocation + copy + free per value.
For a 1024-byte binary blob, it's a 2048-byte allocation. Bytea columns appear in many
workloads: hash values, encrypted data, serialized objects, binary protocols.

**Fix:** Replaced `hex::encode` with direct hex encoding using a lookup table and
stack-allocated chunk buffer:
- `HEX_CHARS` static array maps nibble values (0-15) to ASCII hex digits
- Processes input bytes in 64-byte chunks, producing 128 hex chars per chunk into a
  stack-allocated `[u8; 128]` buffer
- Each chunk is written with a single `write_str()` call
- Zero heap allocations; the "\\x" prefix is written first with `write_str`

The chunk-based approach ensures the stack buffer stays small (128 bytes) while handling
arbitrarily large bytea values efficiently through bulk writes.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench bytes_format`):

### Per-value bytea formatting (ns/iter)

| Size              | Old (hex::encode) | New (direct hex) | Speedup |
|-------------------|------------------:|-----------------:|--------:|
| empty             | 13.2              | 3.5              | 3.8x    |
| 1 byte            | 20.4              | 7.6              | 2.7x    |
| 4 bytes           | 25.2              | 8.7              | 2.9x    |
| 16 bytes (SHA-128)| 55.9              | 22.1             | 2.5x    |
| 32 bytes (SHA-256)| 77.8              | 21.0             | 3.7x    |
| 64 bytes          | 155               | 34.1             | 4.6x    |
| 100 bytes         | 177               | 50.1             | 3.5x    |
| 256 bytes         | 400               | 127              | 3.2x    |
| 1024 bytes        | 1,571             | 500              | 3.1x    |

### Batch formatting (10k values, 16-79 bytes each, to shared buffer)

| Approach          | Time (µs) | Speedup   |
|-------------------|-----------|-----------|
| Old hex::encode   | 992       | -         |
| New direct hex    | 277       | **3.6x**  |

**Summary:** 2.5-4.6x faster per value, **3.6x faster in batch**. The speedup comes from
eliminating the heap-allocated String that `hex::encode` produces and replacing the `write!`
fmt machinery with direct `write_str` calls. Larger values show more consistent ~3x speedup
(allocation cost is proportional to size); smaller values show higher ratios due to fixed
overhead elimination.

**Files changed:**
- `src/repr/src/strconv.rs` - Rewrote `format_bytes()` to use direct hex encoding with
  `HEX_CHARS` lookup table and stack-allocated chunk buffer. Added `test_format_bytes` test.
- `src/repr/benches/bytes_format.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `format_uuid` uses `write!(buf, "{}", uuid)` - could use `uuid.as_hyphenated().encode_lower()`
  to a stack buffer + single write_str, avoiding fmt machinery for the fixed 36-char output.
- `format_interval` uses multiple `write!` calls in its Display impl - could use direct
  formatting similar to timestamp optimization.
- JSONB serialization in `src/repr/src/adt/jsonb.rs:604` uses `to_standard_notation_string()`
  for numeric values inside JSONB. Could be replaced with a stack-buffered approach using
  `write_numeric_standard_notation()`.
- JSON interchange in `src/interchange/src/json.rs:131` has the same
  `to_standard_notation_string()` issue.
- RowArena in `peek_result_iterator.rs:210` is created per-row but is almost free (no alloc
  until first push). Only worth optimizing if MFP expressions frequently allocate.
- Row comparison (`RowRef::cmp`) is already optimal (length-then-memcmp). No opportunity there.
- The biggest remaining non-formatting optimization opportunities are in the compute layer:
  `row.iter().nth(index)` pattern in reduce.rs iterates sequentially instead of random access,
  and `Row::unpack()` in top_k allocates a full Vec just to compare a few columns.

## Session 8: Direct interval formatting - bypass Interval::Display fmt machinery

**Date:** 2026-02-19

**Problem:** `format_interval` in `strconv.rs` used `write!(buf, "{}", iv)` which dispatches
through `Interval`'s `Display` implementation. The Display impl uses multiple `write!` and
`write_char` calls through the `fmt` machinery:
- `write!(f, "{}", years)`, `write!(f, " year")` etc. for each component
- `write!(f, "{:02}:{:02}:{:02}", hours, minutes, seconds)` for the time part
- Multiple `write_char` calls for fractional seconds
- Each `write!` call constructs `fmt::Arguments`, creates a `Formatter` with many fields,
  and dispatches through trait objects

For complex intervals with years, months, days, and fractional time, this results in 6-10+
separate `write!`/`write_char` calls, each paying the full fmt machinery overhead.

Also investigated `format_uuid` (`write!(buf, "{}", uuid)` → `uuid.hyphenated().encode_lower()`),
but the uuid crate's Display impl is already well-optimized internally (~12.5ns), and the
encode_lower approach was actually slightly slower (~14ns). Reverted this change.

Also fixed a minor inefficiency in `format_timestamp`: replaced
`ts.and_utc().timestamp_subsec_nanos()` with `ts.nanosecond()` to avoid unnecessary
`DateTime<Utc>` construction just to extract the nanosecond component.

**Fix:** Replaced `write!(buf, "{}", iv)` with direct formatting using:
- Stack-allocated `[u8; 20]` buffer for integer components via `write_i64_buf()`
- Stack-allocated `[u8; 8]` buffer for `HH:MM:SS` time formatting using `write_u2()` helper
  (2-digit lookup from DIGIT_PAIRS table)
- Stack-allocated `[u8; 10]` buffer for fractional seconds (`.` + up to 9 digits)
- Single `write_str()` call per component instead of `write!` macro dispatch
- Sign handling computed once upfront, written as literal strings ("-", "+")

The implementation preserves exact output compatibility with the Display impl, including
PostgreSQL-compatible sign placement rules (negative sign on months propagates, positive
sign on days/time after negative months, etc.).

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench interval_format`):

### Per-value interval formatting (ns/iter)

| Value               | Old (Display write!) | New (direct) | Speedup |
|---------------------|---------------------:|-------------:|--------:|
| zero                | 53.4                 | 10.3         | 5.2x    |
| one_hour            | 51.4                 | 10.0         | 5.1x    |
| hms                 | 50.9                 | 10.1         | 5.0x    |
| one_year            | 22.3                 | 8.3          | 2.7x    |
| years_months        | 35.2                 | 10.3         | 3.4x    |
| one_day             | 20.0                 | 8.5          | 2.4x    |
| days                | 21.7                 | 8.6          | 2.5x    |
| neg_months          | 38.8                 | 10.8         | 3.6x    |
| neg_days            | 23.9                 | 8.9          | 2.7x    |
| with_micros         | 64.2                 | 15.0         | 4.3x    |
| complex             | 107.0                | 21.8         | 4.9x    |
| neg_time            | 48.6                 | 9.1          | 5.4x    |
| all_parts           | 103.7                | 22.0         | 4.7x    |

### Batch formatting (10k values to shared buffer)

| Approach            | Time (µs) | Speedup   |
|---------------------|-----------|-----------|
| Old Display 10k     | 1,080     | -         |
| New direct 10k      | 225       | **4.8x**  |

**Summary:** 2.4-5.4x faster per value, **4.8x faster in batch**. The speedup comes from
eliminating the fmt machinery entirely: no `fmt::Arguments` construction, no `Formatter`
setup, no trait object dispatch. Simple intervals (time-only) see the biggest improvement
because the old path's overhead dominated the actual formatting work. Complex intervals
with many components still see ~5x because each component's `write!` overhead is eliminated.

**Files changed:**
- `src/repr/src/strconv.rs` - Rewrote `format_interval()` to use direct stack-buffer
  formatting with `write_i64_buf()` and `write_u2()`. Also changed `format_timestamp` to
  use `ts.nanosecond()` instead of `ts.and_utc().timestamp_subsec_nanos()`.
  Added `test_format_interval` correctness test.
- `src/repr/benches/interval_format.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `format_uuid` uses `write!(buf, "{}", uuid)` but the uuid crate's Display is already
  fast (~12.5ns). encode_lower approach was slightly slower. Not worth optimizing.
- JSONB serialization in `src/repr/src/adt/jsonb.rs` uses `to_standard_notation_string()`
  for numeric values. Could be replaced with stack-buffered formatting.
- JSON interchange in `src/interchange/src/json.rs` has the same issue.
- The compute layer has the biggest remaining opportunities:
  `row.iter().nth(index)` pattern in reduce.rs iterates sequentially instead of random access,
  and `Row::unpack()` in top_k allocates a full Vec just to compare a few columns.
- `CheapTimestamp` decoding does integer division + `DateTime::from_timestamp` on every
  timestamp datum read. Direct `NaiveDateTime` construction could be faster.
- Most pgwire text formatting is now direct. Remaining `write!`-based formatters:
  `format_jsonb`, `format_array`, `format_list`, `format_map`, `format_record`,
  `format_range` - these are more complex (recursive) and harder to optimize.
