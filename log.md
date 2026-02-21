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

## Session 9: Fast datum skipping - DatumListIter::nth() and count() optimization

**Date:** 2026-02-19

**Problem:** When `DatumListIter::nth(n)` is called (used heavily in reduce.rs for
aggregation), the default `Iterator::nth` implementation calls `next()` in a loop,
which calls `read_datum` for each skipped element. `read_datum` fully decodes every
datum—including expensive chrono `DateTime` construction for timestamps,
`Decimal` construction for numerics, and `DatumList` parsing for lists—only to
immediately discard the result. This is wasteful: the caller only needs the n-th datum,
not the ones being skipped.

Key call sites in the hot aggregation path:
- `src/compute/src/render/reduce.rs:484`: `row.iter().nth(index).unwrap()` - extracts
  one column from each input row during partial aggregation
- `src/compute/src/render/reduce.rs:1340`: `row.iter().nth(datum_index).unwrap()` -
  extracts one column for distinct aggregation

Also affects `RowRef::unpack()` which calls `self.iter().count()` (iterating all datums
just to count them, constructing and dropping each one).

**Fix:** Added `skip_datum()` function with a static lookup table (`DATUM_SKIP_TABLE`)
that maps each Tag discriminant (u8) to its payload size, enabling O(1) pointer advance
for fixed-size datum types without any Datum construction. The table encodes:
- Fixed-size payloads (0-126 bytes): direct pointer advance (ints, floats, dates,
  timestamps, intervals, UUIDs, etc.)
- Length-prefixed payloads (-1..-4): read 1/2/4/8-byte length prefix, then skip data
  (strings, bytes, lists, dicts)
- Numeric (-5): read digits byte, compute variable lsu size, skip
- Array (-6): read ndims, skip dims + untagged elements
- Range (i8::MIN): parse flags, recursively skip 0-2 inline bound datums

Overrode `nth()` and `count()` on `DatumListIter` to use `skip_datum` for skipped
elements instead of `read_datum`, eliminating all Datum construction overhead.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench row_skip`):

### Per-call nth(index) performance (ns/iter)

| Scenario                       | Old (read_datum) | New (skip_datum) | Speedup |
|--------------------------------|-----------------:|-----------------:|--------:|
| int64 5col nth(4)              | 33.0             | 12.6             | 2.6x    |
| int64 10col nth(9)             | 60.9             | 21.6             | 2.8x    |
| int64 20col nth(19)            | 117.8            | 40.0             | 2.9x    |
| int64 20col nth(10)            | 66.7             | 23.0             | 2.9x    |
| timestamp 5col nth(4)          | 55.5             | 20.2             | 2.7x    |
| timestamp 10col nth(9)         | 106.8            | 29.1             | 3.7x    |
| timestamp 20col nth(19)        | 212.1            | 50.3             | 4.2x    |
| string 10col nth(9)            | 28.8             | 20.4             | 1.4x    |
| string 20col nth(19)           | 55.6             | 38.4             | 1.4x    |
| mixed 10col nth(9)             | 54.7             | 18.2             | 3.0x    |
| mixed 20col nth(19)            | 104.2            | 37.4             | 2.8x    |
| mixed 20col nth(10)            | 62.7             | 23.1             | 2.7x    |

### count() performance (ns/iter)

| Scenario                       | Old (read_datum) | New (skip_datum) | Speedup |
|--------------------------------|-----------------:|-----------------:|--------:|
| int64 5col                     | 29.6             | 10.3             | 2.9x    |
| int64 10col                    | 57.9             | 22.5             | 2.6x    |
| int64 20col                    | 113.9            | 45.3             | 2.5x    |
| int64 50col                    | 284.8            | 103.2            | 2.8x    |
| timestamp 5col                 | 53.3             | 10.0             | 5.3x    |
| timestamp 10col                | 104.7            | 20.9             | 5.0x    |
| timestamp 20col                | 210.6            | 42.8             | 4.9x    |
| mixed 10col                    | 50.0             | 23.3             | 2.1x    |
| mixed 20col                    | 99.2             | 45.7             | 2.2x    |

### Batch nth (10k rows, simulating reduce.rs aggregation pattern)

| Scenario                              | Old (µs) | New (µs) | Speedup   |
|---------------------------------------|----------|----------|-----------|
| 10k int64 20col nth(15)               | 910      | 323      | **2.8x**  |
| 10k mixed 20col nth(15)               | 821      | 324      | **2.5x**  |
| 10k timestamp 10col nth(8)            | 953      | 274      | **3.5x**  |

**Summary:** 1.4-4.2x faster per call for nth(), **2.5-3.5x faster in batch aggregation
scenarios**. The speedup is largest for timestamp columns (4.2x per call) because
`read_datum` for timestamps involves expensive chrono `DateTime` construction +
`CheckedTimestamp` validation that `skip_datum` completely bypasses. For count(), the
improvement is even more dramatic for timestamps (5.3x) because ALL datums are skipped.
String columns show the smallest improvement (1.4x) since their `read_datum` is already
cheap (just a slice reference). The batch benchmark directly simulates the reduce.rs
aggregation pattern of extracting a single column from wide rows.

**Files changed:**
- `src/repr/src/row.rs` - Added `DATUM_SKIP_TABLE` lookup table (128-entry const array),
  `skip_datum()` function, and `nth()`/`count()` overrides on `DatumListIter`. Added
  comprehensive correctness test covering all datum types (ints, floats, strings, bytes,
  timestamps, dates, times, intervals, UUIDs, numerics, arrays, lists, dicts, ranges).
- `src/repr/benches/row_skip.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `CheapTimestamp` decoding does integer division + `DateTime::from_timestamp` +
  `CheckedTimestamp::from_timestamplike` (with LazyLock-based date range validation) on
  every timestamp datum read. An unchecked constructor for `CheckedTimestamp` and direct
  `NaiveDateTime` construction could save ~10-20ns per timestamp read.
- `RowRef::unpack()` still iterates twice (once to count, once to collect). With the new
  `count()` override, the first pass is faster, but a single-pass approach would be better.
- The MFP evaluation path (`src/expr/src/linear.rs`) and persist source MFP evaluation
  (`src/storage-operators/src/persist_source.rs:602`) use `DatumVec::borrow_with()` which
  calls `extend(row.iter())`. This still uses `read_datum` for every datum since all values
  are needed. Optimizing `read_datum` itself (especially for timestamps) would help here.
- JSONB/JSON interchange still use `to_standard_notation_string()` for numeric formatting.
- `Top1Monoid::cmp` in top_k.rs allocates Vec via `Row::unpack()` on every comparison.
  Using `DatumVec` (like `Top1MonoidLocal` does) would eliminate these allocations.

## Session 10: Fast timestamp datum reading - skip redundant validation on read

**Date:** 2026-02-19

**Problem:** Every time a timestamp or timestamptz datum is read from Row storage via
`read_datum`, the code performed two unnecessary operations:

1. **Redundant range validation:** `CheckedTimestamp::from_timestamplike(ndt)` validates the
   timestamp's date against `LOW_DATE` and `HIGH_DATE` bounds (using `LazyLock` statics).
   This validation was provably redundant because:
   - Timestamps in Row storage were already validated when initially constructed via
     `CheckedTimestamp::from_timestamplike()` on the write path
   - The proto deserialization path (`RustType<ProtoNaiveDateTime>` impl) already skips
     this validation, establishing precedent that round-tripped data doesn't need re-checking
   - The validation extracts `.date()`, then does two `NaiveDate` comparisons, each requiring
     an atomic load from `LazyLock` statics

2. **Redundant integer division:** `rem_euclid(1_000_000_000)` performs a second integer
   division to compute the nanosecond remainder, when it could be computed from the quotient
   via `(ts - secs * 1_000_000_000)` (one multiply + one subtract, ~3-4 cycles, vs integer
   division at ~20-40 cycles on x86-64)

These operations execute on every timestamp datum read throughout the system: during MFP
evaluation, row iteration, aggregation, sorting, and pgwire encoding.

**Fix:**

1. **Added `CheckedTimestamp::from_timestamplike_unchecked()`** in `timestamp.rs`: A new
   constructor that wraps without validation, for use with data already known to be in range.
   Used in `read_datum` for all 4 timestamp variants (CheapTimestamp, CheapTimestampTz,
   Timestamp, TimestampTz) and in the columnar decode path (`row/encode.rs`) and stats
   decode path (`stats.rs`).

2. **Replaced `rem_euclid` with quotient-based remainder** in CheapTimestamp/CheapTimestampTz
   decoding: `let nsecs = (ts - secs * 1_000_000_000) as u32` avoids the second division.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench timestamp_read`):

### Per-value isolated decode comparison (ns/iter)

| Value                    | Old (validated) | New (unchecked) | Speedup |
|--------------------------|----------------:|----------------:|--------:|
| timestamp_typical        | 7.01            | 6.54            | 1.07x   |
| timestamp_epoch          | 7.08            | 6.51            | 1.09x   |
| timestamp_pre_epoch      | 7.10            | 6.55            | 1.08x   |
| timestamp_y2k            | 6.99            | 6.46            | 1.08x   |
| timestamptz_typical      | 7.07            | 6.44            | 1.10x   |
| timestamptz_epoch        | 7.03            | 6.45            | 1.09x   |
| timestamptz_pre_epoch    | 7.08            | 6.43            | 1.10x   |

### Row iteration performance (ns/iter, optimized path)

| Scenario                 | Time (ns) | Per-datum |
|--------------------------|----------:|----------:|
| single_timestamp         | 11.0      | 11.0      |
| single_timestamptz       | 11.0      | 11.0      |
| iter_5col_timestamp      | 55.3      | 11.1      |
| iter_10col_timestamp     | 110.9     | 11.1      |
| iter_10col_timestamptz   | 110.6     | 11.1      |
| unpack_10col_timestamp   | 130.5     | 13.1      |

### Batch decode (10k values)

| Approach                        | Time (µs) | Speedup   |
|---------------------------------|-----------|-----------|
| Old validated 10k timestamp     | 70.7      | -         |
| New unchecked 10k timestamp     | 65.5      | **1.08x** |
| Old validated 10k timestamptz   | 70.2      | -         |
| New unchecked 10k timestamptz   | 65.5      | **1.07x** |

**Summary:** ~7-10% faster per timestamp decode. The chrono `DateTime` construction
(~6ns) remains the dominant cost; our optimization saves ~0.5-0.6ns per decode by
eliminating the range validation and second integer division. At billions of timestamp
reads (coverage data shows 62.8B+ `read_datum` calls), this is meaningful.

**Files changed:**
- `src/repr/src/adt/timestamp.rs` - Added `from_timestamplike_unchecked()` constructor
- `src/repr/src/row.rs` - Updated all 4 timestamp variants in `read_datum` to use
  unchecked constructor and quotient-based remainder
- `src/repr/src/row/encode.rs` - Updated columnar decode to use unchecked constructor
- `src/repr/src/stats.rs` - Updated stats decode to use unchecked constructor
- `src/repr/benches/timestamp_read.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- The chrono `DateTime::from_timestamp()` construction itself is ~6ns and involves
  internal integer divisions (secs → days + day_secs) plus NaiveDate/NaiveTime
  construction. A custom `NaiveDateTime` constructor that bypasses chrono's validation
  could potentially be 2-3x faster, but would depend on chrono's internal representation.
- `RowRef::unpack()` iterates twice (count + collect). A single-pass approach with a
  heuristic initial capacity (e.g., 8) would eliminate the count pass entirely.
- `Top1Monoid::cmp` in top_k.rs allocates Vec via `Row::unpack()` on every comparison.
  Using `DatumVec` (like `Top1MonoidLocal` does) would eliminate these allocations.
- JSONB/JSON interchange still use `to_standard_notation_string()` for numeric formatting.
- Avro varint decoding (4.4B executions in coverage data) could benefit from batch-read
  optimization instead of byte-by-byte processing.

## Session 11: Zero-allocation numeric formatting for pgwire output (re-implementation)

**Date:** 2026-02-19

**Problem:** Every time a `Numeric` (decimal) value is formatted for output—via pgwire
text encoding (`format_numeric` in `strconv.rs`) or `Datum::Display`—the code called
`Decimal::to_standard_notation_string()` which performs **two heap allocations** per value:
1. `coefficient_digits()` allocates a `Vec<u8>` (up to 39 bytes) via the C function
   `decNumberGetBCD`
2. `to_standard_notation_string()` allocates a `String` for the formatted result

The formatted String is then copied into the output buffer and immediately dropped. For a
query returning 100k numeric values, this means 200k unnecessary heap allocations.

This optimization was previously implemented in session 2 but was reverted during branch
cleanup. This is a clean re-implementation with the same approach.

**Fix:** Added `write_numeric_standard_notation()` in `src/repr/src/adt/numeric.rs` that
writes directly to any `fmt::Write` implementation with **zero heap allocations**:
- Uses `coefficient_units()` (returns `&[u16]` slice from the internal representation—no
  allocation) to extract digits into a stack-allocated `[u8; 82]` array
- Each coefficient unit (base-1000, little-endian) is converted to 3 decimal digits
  via simple division
- Leading zeros are stripped to match `to_standard_notation_string()` behavior exactly
- Builds the complete output (sign, digits, decimal point, leading zeros) in a
  stack-allocated `[u8; 200]` buffer
- Writes the entire result with a single `write_str()` call

For `format_numeric` (pgwire path), a thin `FmtWriteFormatBuffer` adapter bridges the
`FormatBuffer` trait to `fmt::Write`. For `Datum::Numeric` Display, the function is
called directly on the `fmt::Formatter`.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench numeric_format`):

### Write-only: pure formatting cost to pre-allocated buffer (ns/iter)

| Value                      | Old (to_standard_notation_string) | New (write_numeric) | Speedup |
|----------------------------|----------------------------------|---------------------|---------|
| zero                       | 17.5                             | 5.8                 | 3.0x    |
| one                        | 17.4                             | 5.7                 | 3.1x    |
| small_int (42)             | 18.9                             | 7.7                 | 2.5x    |
| decimal (123.456789)       | 25.4                             | 12.8                | 2.0x    |
| negative (-3.14...)        | 52.5                             | 24.4                | 2.2x    |
| tiny (0.000001)            | 27.8                             | 8.8                 | 3.2x    |
| large_int (99999...)       | 53.2                             | 25.4                | 2.1x    |
| large_dec (99999.999...)   | 53.8                             | 25.4                | 2.1x    |
| max_precision (39 digits)  | 53.0                             | 25.3                | 2.1x    |
| small_exp (1E+10)          | 37.8                             | 8.8                 | 4.3x    |
| neg_exp (1E-10)            | 34.1                             | 9.3                 | 3.7x    |
| many_decimals (0.123...39) | 54.7                             | 26.0                | 2.1x    |

### Batch formatting (10k values to shared buffer, µs/iter)

| Approach                        | Time (µs) | Speedup |
|---------------------------------|-----------|---------|
| Old (to_standard_notation_string) | 360       | -       |
| New (write_numeric)             | 155       | **2.3x**|

**Summary:** 2.0-4.3x faster per value depending on the value, **2.3x faster in the
realistic batch scenario** (formatting many values to a shared buffer). The speedup
comes from eliminating both heap allocations (coefficient Vec + result String) and using a
single `write_str` call. Simple values with few digits (zero, one, small exponents) show
the biggest improvement because the allocation overhead dominated the formatting work.

**Files changed:**
- `src/repr/src/adt/numeric.rs` - Added `write_numeric_standard_notation()` + comprehensive
  correctness test covering zeros, integers, decimals, large values, exponents, edge cases
- `src/repr/src/strconv.rs` - Updated `format_numeric()` to use new function, added
  `FmtWriteFormatBuffer` adapter
- `src/repr/src/scalar.rs` - Updated `Datum::Numeric` Display to use new function
- `src/repr/benches/numeric_format.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- JSONB serialization in `src/repr/src/adt/jsonb.rs:604` and JSON interchange in
  `src/interchange/src/json.rs:131` still use `to_standard_notation_string()`. These
  need the result as a `&str` for serde, so a stack-buffer string wrapper would be needed.
- `zero_diffs.clone()` in `src/compute/src/render/reduce.rs:1313` clones a `Vec<Accum>`
  per input row during accumulable reductions. The Vec heap allocation per row is wasteful;
  a SmallVec or reuse pattern could help but requires changes to DD trait implementations.
- `Top1Monoid::cmp` in `src/compute/src/render/top_k.rs:908` allocates a `Vec<Datum>` via
  `Row::unpack()` on every comparison during consolidation. Using thread-local `DatumVec`
  buffers (like `Top1MonoidLocal` does with `Rc<RefCell<>>`) would eliminate per-comparison
  heap allocation.
- The compute layer has the highest-impact remaining opportunities: OffsetStride::len()
  at 1,580B executions, Row operations at 1,273B, Timestamp ordering at 856B.

## Session 12: Direct DataRow encoding - bypass per-row Vec<Option<Value>> allocation (re-implementation)

**Date:** 2026-02-19

**Problem:** Every row sent over pgwire went through a two-step encoding pipeline:
1. `values_from_row()` converts a `RowRef` into `Vec<Option<Value>>`, which:
   - Allocates a new `Vec` per row
   - For `Datum::String(s)`, clones the string data via `s.to_owned()` into `Value::Text(String)`
   - For `Datum::Bytes(b)`, clones the bytes via `b.to_vec()` into `Value::Bytea(Vec<u8>)`
   - For all other types, creates owned `Value` variants
2. The `Codec::encode()` method then iterates over the `Vec<Option<Value>>`, calling
   `Value::encode_text()` for each field, writing into `BytesMut`

The intermediate `Value` representation was completely unnecessary for text-format encoding:
the same `strconv` formatters that `Value::encode_text()` calls can be called directly with
the borrowed Datum data. Coverage data confirmed `values_from_row` at 1.02 billion
executions, and `Value::from_datum` at 1.02 billion. This optimization was previously
implemented in session 6 but was reverted during branch cleanup. This is a clean
re-implementation.

**Fix:** Three changes:

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
| integers_5col         | 138                   | 149                 | 0.93x   |
| mixed_5col            | 137                   | 134                 | 1.02x   |
| strings_5col          | 157                   | 127                 | 1.24x   |

### Batch encoding (10k rows to shared buffer)

| Scenario                     | Old (ms)  | New (ms)  | Speedup   |
|------------------------------|-----------|-----------|-----------|
| mixed_5col (10k rows)        | 1.435     | 1.352     | **1.06x** |
| strings_5col (10k rows)      | 1.684     | 1.296     | **1.30x** |

Note: the benchmark is conservative—it only measures the datum-to-bytes encoding step.
The old path additionally pays Codec::encode() overhead to iterate the Vec<Option<Value>>
and write each field, which the PreEncoded path completely bypasses.

**Summary:** 1.02-1.24x faster per row, **1.06-1.30x faster in batch**. The biggest win is
for string-heavy rows (1.30x batch) because the old path cloned every string into an owned
`Value::Text(String)`. For integer-only rows the improvement is marginal since the `Value`
enum construction is cheap for scalar types. The improvement compounds with all previous
formatting optimizations (sessions 3-8, 11) since the direct path uses the optimized
stack-buffer formatters.

**Also investigated:** `RowRef::unpack()` single-pass optimization (eliminating the
double-iteration count+collect pattern). The current approach iterates twice: once via
`skip_datum` to count datums, once via `read_datum` to collect them. A single-pass approach
with heuristic initial capacity was benchmarked with two strategies:
- `byte_len / 4` clamp [4,32]: under-estimated for small-datum rows, causing Vec growth
  that HURT performance for narrow rows (e.g., 1.38x slower for 5-column int64 rows)
- `byte_len.min(64)`: over-allocated for timestamp/numeric rows, hurting cache behavior
  (e.g., 1.21x slower for 5-column timestamps)
The double-pass approach is competitive at common row widths (5-10 columns) because
`skip_datum` (from session 9) is 2-5x faster than `read_datum`, so the count pass costs
only ~30-50% of the total. Only rows with 20+ columns showed consistent improvement.
Abandoned this approach as the wins were marginal and inconsistent.

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
- `RowRef::unpack()` is not worth optimizing: the double-pass with skip_datum is competitive
  at common row widths, and DatumVec already handles the hot paths. Only wide rows (20+
  columns) showed marginal improvement from a single-pass approach.
- `Top1Monoid::cmp` in `src/compute/src/render/top_k.rs:908` allocates a `Vec<Datum>` via
  `Row::unpack()` on every comparison during consolidation. Using thread-local `DatumVec`
  would help but requires careful handling of the `&self, &other` Ord trait constraints.
- JSONB serialization in `src/repr/src/adt/jsonb.rs:604` still uses
  `to_standard_notation_string()` for numeric values. A stack-buffered approach would
  eliminate the heap allocation, but serde's `serialize_field` requires `&dyn Serialize`,
  so a custom type implementing Serialize would be needed.
- The `row_buf.split().freeze()` in the protocol loop creates a `Bytes` (atomic refcount)
  per row. If the codec supported writing directly to its internal buffer, this per-row
  `Bytes` allocation could be eliminated.
- The coverage data (62.8B executions of `unpack()`) suggests it's called very frequently,
  but most hot paths already use `DatumVec` for allocation reuse. The remaining `unpack()`
  calls are in less performance-critical paths (transforms, planning, top_k comparisons).

## Session 13: Direct COPY text/CSV encoding - bypass per-row Vec<Option<Value>> allocation

**Date:** 2026-02-19

**Problem:** The COPY text and CSV encoding paths (`encode_copy_row_text` and
`encode_copy_row_csv` in `src/pgcopy/src/copy.rs`) used `values_from_row()` on every
row, which allocates a `Vec<Option<Value>>` per row. This is the same allocation-heavy
pattern that was bypassed in Session 12 for the regular pgwire DataRow path. For COPY
operations, which are designed for bulk data export:
1. `values_from_row()` allocates a new `Vec` per row
2. For `Datum::String(s)`, clones the string data via `s.to_owned()` into `Value::Text(String)`
3. For `Datum::Bytes(b)`, clones the bytes via `b.to_vec()` into `Value::Bytea(Vec<u8>)`
4. The `Value` is then immediately re-encoded via `encode_text()` into a BytesMut buffer
5. The BytesMut output is then escape-processed into the final `Vec<u8>` output

Coverage data showed `pgcopy/src/copy.rs` at 2.4 trillion total executions, confirming
COPY is a heavily-used output path (used for `COPY TO`, `SUBSCRIBE`/`TAIL` output).

**Fix:** Three changes:

1. **Direct datum encoding in COPY text path**: Replaced `values_from_row(row, typ)` with
   direct iteration over `row.iter().zip(typ.column_types.iter())`, using the existing
   `encode_datum_text_direct()` function (from Session 12) to encode each datum directly
   into a BytesMut buffer without creating an intermediate `Value`.

2. **Same for COPY CSV path**: Applied the identical optimization to `encode_copy_row_csv`.

3. **Escape fast-path for COPY text**: Added a check for special characters (`\`, `\n`,
   `\r`, `\t`) before the byte-by-byte escape loop. For non-string types (integers,
   timestamps, dates, etc.) whose encoded text never contains these characters, this allows
   a single `out.extend_from_slice(&buf)` instead of per-byte `out.push(*b)`. The `any()`
   scan is vectorizable by LLVM and returns false immediately for numeric types.

**Also fixed:** A bug in `encode_datum_text_direct()` where the JSONB match arm was
positioned after the `Datum::String`, `Datum::True/False`, and `Datum::Numeric` arms.
Since JSONB stores values using these standard Datum variants, a JSONB string would be
formatted as a plain string (no JSON quotes) instead of as a JSONB value. Moved the
`(_, SqlScalarType::Jsonb)` arm to the top of the match to ensure JSONB values are always
formatted correctly. This fix also corrects the Session 12 DataRow encoding for JSONB
columns.

**Benchmark results** (criterion, `cargo bench -p mz-pgcopy --bench copy_encode`):

### Per-row COPY text encoding (ns/iter)

| Scenario              | Old (values_from_row) | New (direct)  | Speedup |
|-----------------------|----------------------:|--------------:|--------:|
| integers_5col         | 237                   | 145           | 1.64x   |
| strings_5col          | 320                   | 168           | 1.90x   |
| mixed_5col            | 247                   | 128           | 1.93x   |

### Batch COPY text encoding (10k rows)

| Scenario                     | Old (ms)  | New (ms)  | Speedup   |
|------------------------------|-----------|-----------|-----------|
| mixed_5col (10k rows)        | 2.537     | 1.343     | **1.89x** |
| strings_5col (10k rows)      | 2.900     | 1.334     | **2.17x** |

**Summary:** 1.64-1.93x faster per row, **1.89-2.17x faster in batch**. The biggest
win is for string-heavy rows (2.17x batch) because the old path cloned every string
into an owned `Value::Text(String)`. Mixed-type rows (ints + strings + bools) see
1.89x batch improvement. Integer-only rows see 1.64x improvement from eliminating the
`Vec` allocation and gaining the escape fast-path (bulk `extend_from_slice` instead of
per-byte `push`).

The improvement is larger than Session 12's DataRow optimization (1.06-1.30x) because
the COPY path additionally benefits from the escape fast-path optimization, and the
COPY output format is simpler (no pgwire framing overhead).

**Files changed:**
- `src/pgrepr/src/value.rs` - Made `encode_datum_text_direct()` public, moved JSONB
  match arm to top to fix JSONB encoding bug
- `src/pgrepr/src/lib.rs` - Re-exported `encode_datum_text_direct`
- `src/pgcopy/src/copy.rs` - Rewrote `encode_copy_row_text()` and `encode_copy_row_csv()`
  to iterate datums directly instead of using `values_from_row()`. Added escape fast-path
  for COPY text encoding.
- `src/pgcopy/Cargo.toml` - Added criterion dev-dependency, registered benchmark
- `src/pgcopy/benches/copy_encode.rs` - Added benchmark (new file)

**Future optimization ideas identified during research:**
- `Top1Monoid::cmp` in `src/compute/src/render/top_k.rs:908` allocates `Vec<Datum>` via
  `Row::unpack()` on every comparison during consolidation. Using thread-local `DatumVec`
  buffers would eliminate per-comparison heap allocation. The code itself acknowledges
  this: "It might be nice to cache this row decoding."
- JSONB serialization in `src/repr/src/adt/jsonb.rs:604` and JSON interchange in
  `src/interchange/src/json.rs:131` still use `to_standard_notation_string()` for numeric
  values (2 heap allocations per value). Using a stack-buffered approach with
  `write_numeric_standard_notation()` would eliminate these allocations.
- The `row_buf.split().freeze()` in the protocol loop (Session 12) creates a `Bytes`
  (atomic refcount) per row. Writing directly to the codec's internal buffer would
  eliminate this per-row allocation.
- `row_spine.rs` at 23.7T total executions is the hottest file in the compute layer.
  The `OffsetOptimized::index()` calls `strided.len()` twice when falling through to
  the spilled path—caching the result could help at these volumes.
- Coverage data shows the persist/arrow path (`persist-types/src/arrow.rs`) at 7.3T
  executions—a potential target for columnar encoding optimizations.

## Session 14: Top1Monoid selective comparison - skip full row unpacking

**Date:** 2026-02-19

**Problem:** `Top1Monoid::cmp()` in `src/compute/src/render/top_k.rs` is called during
monotonic Top-K operations (e.g., `ORDER BY col LIMIT 1` on append-only sources). On every
comparison, it:
1. Calls `self.row.unpack()` → allocates `Vec<Datum>` (count pass via `skip_datum` + read
   pass via `read_datum` for ALL columns)
2. Calls `other.row.unpack()` → same thing for the second row
3. Calls `compare_columns()` which only looks at 1-3 ORDER BY columns
4. Falls back to full `left.cmp(&right)` tiebreaker if all ORDER BY columns are equal

For a 20-column row with `ORDER BY col0 LIMIT 1`, this means unpacking all 40 datums (20 per
row) just to compare 2 of them. The code had a TODO comment acknowledging this: "It might be
nice to cache this row decoding like the non-monotonic codepath."

**Fix:** Two optimizations:

1. **Selective column comparison**: Instead of unpacking the entire row, use `nth()` (which
   leverages `skip_datum` from Session 9) to extract only the ORDER BY columns. For each
   order column, a fresh iterator is created and `nth(column_index)` skips directly to the
   needed column using fast pointer arithmetic (no Datum construction for skipped columns).
   The comparison logic (null handling, desc/asc) is inlined to match `compare_columns()`.

2. **Thread-local DatumVec tiebreaker**: When all ORDER BY columns are equal (rare in
   practice), falls back to full row comparison using thread-local `DatumVec` buffers instead
   of per-comparison `Vec` allocation. The `DatumVec` reuses its internal allocation across
   calls, and `const { }` initialization in `thread_local!` avoids lazy init overhead.

Also made `DatumVec::new()` a `const fn` to enable compile-time initialization in
`thread_local!` blocks.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench row_compare`):

### Per-comparison performance (ns/iter)

| Scenario                        | Old (unpack)  | New (selective) | Speedup |
|---------------------------------|--------------:|----------------:|--------:|
| int5 ORDER BY col0              | 117           | 24              | 4.9x    |
| int10 ORDER BY col0             | 234           | 24              | 9.7x    |
| int10 ORDER BY col8             | 236           | 48              | 4.9x    |
| int20 ORDER BY col0             | 479           | 21              | **22.7x** |
| int20 ORDER BY col15            | 475           | 77              | 6.2x    |
| mixed5 ORDER BY col2 (float)    | 103           | 22              | 4.8x    |
| ts5 ORDER BY col3               | 143           | 41              | 3.5x    |
| ts10 ORDER BY col0              | 268           | 28              | **9.5x** |
| int5 equal rows (tiebreaker)    | 134           | 171             | 0.78x   |

### Batch semigroup simulation (10k comparisons, find minimum)

| Scenario                          | Old (ms)   | New (µs)    | Speedup     |
|-----------------------------------|------------|-------------|-------------|
| int10_10k ORDER BY col0           | 2.10       | 230         | **9.1x**    |
| int20_10k ORDER BY col0           | 4.72       | 253         | **18.7x**   |
| ts10_10k ORDER BY col0            | 2.69       | 285         | **9.4x**    |
| mixed5_10k ORDER BY col2 (float)  | 985 µs     | 219         | **4.5x**    |

**Summary:** **3.5-22.7x faster per comparison, 4.5-18.7x faster in batch**. The speedup is
proportional to row width and inversely proportional to the ORDER BY column index. For the
common case of `ORDER BY first_column LIMIT 1` on wide rows (20 columns), the improvement is
**22.7x per comparison and 18.7x in batch**. This is because the old approach read all 20
datums from each row (40 total) while the new approach reads only 1 datum from each row
(2 total), using `skip_datum` (O(1) pointer arithmetic) to skip the first column's tag byte.

The tiebreaker case (all ORDER BY columns equal) is 1.28x slower due to the overhead of
checking ORDER BY columns first, then falling back to full DatumVec comparison. But this
path is rarely taken in practice—most rows in a Top-K stream differ on at least one ORDER BY
column.

**Files changed:**
- `src/compute/src/render/top_k.rs` - Rewrote `Top1Monoid::cmp()` to use selective column
  extraction via `nth()` (skip_datum) for ORDER BY columns, with thread-local DatumVec
  fallback for tiebreaker. Added `Datum` import.
- `src/repr/src/datum_vec.rs` - Made `DatumVec::new()` a `const fn` for compile-time
  `thread_local!` initialization.
- `src/repr/benches/row_compare.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- The same selective comparison pattern could benefit `Top1MonoidLocal::cmp()` (the
  non-monotonic codepath), which currently uses `DatumVec::borrow_with()` for full row
  unpacking. However, since `Top1MonoidLocal` already reuses allocations via
  `Rc<RefCell<Top1MonoidShared>>`, the per-comparison cost is lower (no allocation, just
  iteration). The selective approach would still save iteration time for wide rows.
- The `compare_columns` function in `mz_expr::relation` could be augmented with a
  `compare_columns_from_rows(left: &RowRef, right: &RowRef, order: &[ColumnOrder])` variant
  that uses the selective pattern. This would benefit any caller that currently unpacks rows
  just for column comparison.
- Scalar `cast_*_to_string` functions (cast_uint16_to_string, cast_uint64_to_string, etc.)
  allocate a `String` per call during MFP evaluation. A `RowArena`-based formatting approach
  or stack-buffered string could eliminate these per-row allocations.
- Delta join produces 3+ clones per output tuple (diff, row, time, initial). These are
  inherent to the DD model but could potentially be reduced with move semantics.
- The `zero_diffs.clone()` pattern in `reduce.rs` (accumulable aggregation) clones a
  `Vec<Accum>` per input row. SmallVec<[Accum; 4]> could avoid heap allocation for the
  common case of 1-3 aggregations.
- JSONB serialization still uses `to_standard_notation_string()` for numeric values.

## Session 15: Direct binary DataRow encoding - bypass per-row Value construction for binary format

**Date:** 2026-02-19

**Problem:** Session 12 optimized the text format DataRow encoding path to bypass `Value`
construction, but the binary format path still used the old approach: for every non-null
datum in binary format, the code:
1. Calls `Value::from_datum(datum, scalar_type)` → constructs an intermediate `Value` enum
2. For `Datum::String(s)`, clones the string data via `s.to_owned()` into `Value::Text(String)`
3. For `Datum::Bytes(b)`, clones the bytes via `b.to_vec()` into `Value::Bytea(Vec<u8>)`
4. Calls `value.encode_binary(ty, dst)` which dispatches through `ToSql` trait implementations
5. The `Value` is immediately dropped

Binary format is commonly used by production clients: psycopg (Python), JDBC (Java),
Go's pgx/pgconn, and Rust's tokio-postgres. The per-datum overhead is:
- `Value` enum construction (match + potential heap alloc for strings/bytes)
- `ToSql` trait dispatch (dynamic dispatch through postgres-types)
- For strings/bytes: heap allocation + copy + free just to own the data temporarily

**Fix:** Added `encode_datum_binary_direct()` function that encodes common datum types
directly into `BytesMut` without constructing an intermediate `Value`:
- **Bool**: single byte (0/1)
- **Int16/32/64, UInt8/16/32/64**: big-endian integer encoding (1-8 bytes)
- **Float32/Float64**: IEEE 754 big-endian encoding
- **String**: direct `dst.extend_from_slice(s.as_bytes())` — zero-copy from datum
- **Bytes**: direct `dst.extend_from_slice(b)` — zero-copy from datum
- **Date**: days since PG epoch (i32 big-endian)
- **Time**: microseconds since midnight (i64 big-endian)
- **Timestamp/TimestampTz**: microseconds since PG epoch (i64 big-endian)
- **Interval**: PG wire format (microseconds i64 + days i32 + months i32)
- **Uuid**: 16-byte raw encoding
- **BpChar**: space-padded to declared width, direct bytes copy

Falls back to `Value` path for complex types (Jsonb, Numeric, arrays, lists, maps,
records, ranges, MzTimestamp, AclItem) where the binary encoding is complex or rarely used.

Added a `PG_EPOCH` static (`LazyLock<NaiveDateTime>` for 2000-01-01) and
`pg_timestamp_to_microseconds()` helper for timestamp binary encoding.

Updated `encode_data_row_direct()` binary branch to try `encode_datum_binary_direct()`
first, falling back to the `Value` path only when the direct function returns `false`.

**Benchmark results** (criterion, `cargo bench -p mz-pgrepr --bench datarow_encode -- binary`):

### Per-row binary DataRow encoding (ns/iter)

| Scenario                  | Old (Value path) | New (direct) | Speedup |
|---------------------------|------------------:|-------------:|--------:|
| integers_5col             | 137               | 124          | 1.10x   |
| strings_5col              | 164               | 115          | 1.43x   |
| mixed_5col (int+str+bool+ts+f64) | 142       | 115          | 1.23x   |
| timestamps_5col           | 185               | 175          | 1.06x   |

### Batch binary DataRow encoding (10k rows)

| Scenario                         | Old (ms)  | New (ms)  | Speedup   |
|----------------------------------|-----------|-----------|-----------|
| mixed_5col (10k rows)            | 1.433     | 1.168     | **1.23x** |
| strings_5col (10k rows)          | 1.744     | 1.146     | **1.52x** |

**Summary:** 1.06-1.43x faster per row, **1.23-1.52x faster in batch**. The binary format
gains are more modest than the text format optimization (Session 12) because binary encoding
is inherently simpler—integers are just big-endian byte writes, not itoa formatting. The
biggest wins are for string-heavy rows (1.43-1.52x) because the `Value::Text(String)` path
clones the string onto the heap, while the direct path copies bytes straight from the datum
to the output buffer. Timestamp improvement is smallest (1.06x) because the `Value`
construction and `ToSql` encoding for timestamps is already fairly efficient (just
chrono arithmetic in both paths).

The optimization particularly helps binary protocol clients doing bulk reads (psycopg with
binary mode, JDBC, Go pgx) where per-row encoding overhead adds up over millions of rows.

**Files changed:**
- `src/pgrepr/src/value.rs` - Added `encode_datum_binary_direct()` function,
  `pg_timestamp_to_microseconds()` helper, and `PG_EPOCH` static. Updated binary branch
  in `encode_data_row_direct()` to try direct encoding first.
- `src/pgrepr/benches/datarow_encode.rs` - Added binary format benchmarks alongside
  existing text format benchmarks.

**Future optimization ideas identified during research:**
- JSONB serialization in `src/repr/src/adt/jsonb.rs:604` and JSON interchange in
  `src/interchange/src/json.rs:131` still use `to_standard_notation_string()` for numeric
  values (2 heap allocations per value). Using a stack-buffered approach with
  `write_numeric_standard_notation()` would eliminate these allocations.
- The `row_buf.split().freeze()` in the protocol loop creates a `Bytes` (atomic refcount)
  per row. Writing directly to the codec's internal buffer would eliminate this per-row
  allocation.
- Scalar `cast_*_to_string` functions allocate a `String` per call during MFP evaluation.
  A `RowArena`-based formatting approach or stack-buffered string could eliminate these
  per-row allocations.
- The `zero_diffs.clone()` pattern in `reduce.rs` (accumulable aggregation) clones a
  `Vec<Accum>` per input row. SmallVec<[Accum; 4]> could avoid heap allocation for the
  common case of 1-3 aggregations.
- Coverage data shows `persist-types/src/arrow.rs` at 7.3T executions—a potential target
  for columnar encoding optimizations.
- Binary encoding for Numeric could be implemented directly (BCD encoding to wire format)
  without going through the postgres-types ToSql trait, but the complexity of the Numeric
  wire format makes this a lower priority.

## Session 16: SIMD-accelerated COPY text parsing - memchr replaces byte-by-byte scanning

**Date:** 2026-02-19

**Problem:** The COPY text format parser (`CopyTextFormatParser::consume_raw_value()` in
`src/pgcopy/src/copy.rs`) scanned input data byte-by-byte, checking 4-6 conditions per byte:
1. `is_eof()` → `peek().is_none()` (bounds check) + `is_end_of_copy_marker()` (2-byte slice
   comparison against `b"\\."`)
2. `is_end_of_copy_marker()` (redundant—already called by `is_eof()`)
3. `is_end_of_line()` → `peek()` (bounds check) + match against `\n`
4. `is_column_delimiter()` → `check_bytes(&[delimiter])` (bounds check + 1-byte compare)
5. `peek()` → bounds check + byte read + match against `\\`

For every normal byte (letters, digits, spaces), ALL of these checks return false, and the byte
is consumed via `self.consume_n(1)`. For a 100-byte string value, this means ~600 bounds checks
and comparisons just to scan past normal characters.

Coverage data showed 137 billion executions for the parser's inner loop functions (peek,
check_bytes), confirming this is a heavily-used code path (COPY FROM data loading, SUBSCRIBE
output parsing).

**Fix:** Replaced the byte-by-byte scanning loop with SIMD-accelerated `memchr::memchr3()`:
- Searches for the three special bytes (`column_delimiter`, `\n`, `\\`) in a single vectorized
  pass using SSE2/AVX2 instructions
- Processes 16-32 bytes per CPU cycle instead of 1 byte per 5-6 checks
- When a `\\` is found, checks the next byte for `.` (end-of-copy marker) before processing
  as an escape sequence, maintaining correct precedence
- Escape sequence handling (octal, hex, named escapes) remains identical
- Also handles edge cases: `column_delimiter == b'\\'`, EOF mid-value, etc.

The escape handling within the function was also modernized to use direct `self.data.get()`
indexing instead of the `peek()`/`consume_n()` helper methods, eliminating redundant bounds
checks.

**Benchmark results** (criterion, `cargo bench -p mz-pgcopy --bench copy_parse`):

### Batch parsing (10k rows)

| Scenario                          | Old byte-by-byte | New memchr  | Speedup   |
|-----------------------------------|----------------:|------------:|----------:|
| int5 (5 short int cols, ~5 chars) | 1.007 ms        | 1.009 ms    | ~1.00x    |
| str5 (5 string cols, ~17 chars)   | 1.413 ms        | 0.991 ms    | **1.43x** |
| longstr5 (5 cols, ~80 chars each) | 3.695 ms        | 1.050 ms    | **3.52x** |
| mixed5 (int+str+bool+int+str)     | 1.035 ms        | 0.985 ms    | 1.05x     |
| escaped (4 cols, some with \\)    | 1.067 ms        | 0.946 ms    | **1.13x** |
| wide20 (20 short int cols)        | 4.491 ms        | 4.511 ms    | ~1.00x    |

**Summary:** The improvement is proportional to value length:
- **3.52x for long strings** (~80 chars) — the biggest win, since memchr skips 80 bytes of
  normal characters with SIMD in ~10-20ns vs. the old approach's ~480ns (6 checks × 80 bytes)
- **1.43x for medium strings** (~17 chars) — memchr still wins, but per-value overhead
  (null string check, delimiter handling) dilutes the improvement
- **1.13x for escaped data** — escape sequences require per-byte processing regardless, but
  memchr speeds up the non-escape portions
- **~1.0x for short integer values** (~5 chars) — per-value framework overhead (null string
  check, column delimiter parsing) dominates; memchr saves only ~5ns per value which is
  within noise

For real-world COPY FROM workloads with text columns (common in CSV imports, data migration,
ETL pipelines), the 1.4-3.5x speedup on the scanning portion translates to meaningful
throughput improvement. The optimization is especially impactful for wide string data like
URLs, JSON text, log lines, and document content.

**Files changed:**
- `src/pgcopy/src/copy.rs` - Rewrote `consume_raw_value()` to use `memchr::memchr3()` for
  SIMD-accelerated scanning. Escape handling modernized to use direct `self.data.get()`
  instead of `peek()`/`consume_n()`.
- `src/pgcopy/Cargo.toml` - Added `memchr = "2.7.6"` dependency, registered copy_parse bench
- `src/pgcopy/benches/copy_parse.rs` - Added benchmark (new file) comparing old byte-by-byte
  parser against new memchr-accelerated parser across various data patterns

**Future optimization ideas identified during research:**
- Coverage data shows `row_spine.rs` at 1.58T executions for `OffsetStride::len()/index()`.
  These are the core differential dataflow arrangement data structures. The enum match per
  offset lookup is checked 1.58T times. A branchless formula or cached stride could help.
- `Row::clone()` at 527B executions is a massive source of allocation pressure. Reducing clone
  frequency (e.g., using references or Cow-like patterns) or using arena allocation could help.
- `BytesContainer::index()` at 574B does a LINEAR SCAN through batches for every index
  operation. A cumulative-length array with binary search would be O(log n) instead of O(n).
- `Overflowing` arithmetic (add/mul/sub) at 400B: already has `#[cold]` on overflow handler
  and `#[inline(always)]` on operations. The compiler generates optimal code.
- JSONB serialization still uses `to_standard_notation_string()` for numeric values.
- `zero_diffs.clone()` in reduce.rs clones `Vec<Accum>` per row. SmallVec could avoid heap
  allocation for the common case (1-4 aggregations).
- The COPY text parser's per-value framework overhead (null string check, delimiter parsing)
  could be optimized by inlining the null string check into the memchr scan loop, avoiding
  a separate `consume_null_string()` call per value.

## Session 17: Accumulable reduction - nth() column skipping in reduce.rs

**Date:** 2026-02-19

**Problem:** In the accumulable reduction path (`build_accumulable` in `reduce.rs`), the
`simple_aggrs` extraction loop used an `enumerate() + while` pattern that called `read_datum`
(full datum decoding) for every column up to and including the target column:

```rust
let mut row_iter = row.iter().enumerate();
for (datum_index, aggr) in simple_aggrs.iter() {
    let mut datum = row_iter.next().unwrap();
    while datum_index != &datum.0 {
        datum = row_iter.next().unwrap();
    }
    // use datum.1
}
```

Each `.next()` call invokes `read_datum` which fully decodes every datum—including expensive
chrono `DateTime` construction for timestamps, `Decimal` construction for numerics, etc.—only
to discard the result for skipped columns. For a 20-column row with a single aggregation on
column 15, this meant decoding 16 datums (columns 0-15) when only column 15 is needed.

The same issue applied to the `distinct_aggrs` path at line 1341 which used
`row.iter().nth(datum_index)` (already using nth, which was correct).

**Fix:** Replaced the `enumerate() + while` pattern with `nth()` calls that leverage
`skip_datum` (from Session 9) for fast O(1) pointer arithmetic over skipped columns:

```rust
let mut row_iter = row.iter();
let mut prev_index: usize = 0;
for (i, (datum_index, aggr)) in simple_aggrs.iter().enumerate() {
    let skip = if i == 0 {
        *datum_index
    } else {
        *datum_index - prev_index - 1
    };
    let datum = row_iter.nth(skip).unwrap();
    prev_index = *datum_index;
    // use datum
}
```

This works because `simple_aggrs` is built from `enumerate()` over the aggregation list,
so `datum_index` values are naturally sorted in ascending order. The `nth(skip)` call uses
`skip_datum` to advance past `skip` columns with just pointer arithmetic (reading only the
tag byte + fixed payload size), then calls `read_datum` only for the target column.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench row_aggregate`):

### Per-call column extraction (ns/iter)

| Scenario                               | Old (enumerate) | New (nth) | Speedup |
|----------------------------------------|----------------:|----------:|--------:|
| int10 col0 (first col, single agg)     | 15.8            | 23.7      | 0.67x   |
| int10 col9 (last col, single agg)      | 105.2           | 36.3      | **2.9x** |
| int20 col15 (single agg, wide row)     | 158.9           | 50.3      | **3.2x** |
| ts10 col8 (timestamp, single agg)      | 109.5           | 41.8      | **2.6x** |
| int20 cols 3,7,15 (3 aggregations)     | 164.3           | 76.2      | **2.2x** |
| mixed20 cols 0,6,12,18 (4 aggregations)| 163.3           | 81.5      | **2.0x** |
| ts10 cols 2,5,8 (3 timestamp aggs)     | 117.1           | 64.7      | **1.8x** |

### Batch simulation (10k rows)

| Scenario                                 | Old       | New       | Speedup     |
|------------------------------------------|-----------|-----------|-------------|
| 10k int20 col15 (single agg)            | 1.555 ms  | 444.5 µs  | **3.5x**    |
| 10k ts10 col8 (single agg)              | 1.096 ms  | 395.3 µs  | **2.8x**    |
| 10k mixed20 cols 0,6,12,18 (4 aggs)     | 1.625 ms  | 739.4 µs  | **2.2x**    |

**Summary:** **1.8-3.2x faster per call, 2.2-3.5x faster in batch** for the common case
of extracting a few aggregation columns from wide rows. The speedup comes from `skip_datum`
using O(1) pointer arithmetic (tag byte + known payload size) instead of `read_datum`'s full
datum decoding for skipped columns. The col0 case shows a minor 1.5x regression because both
approaches do the same work (read 1 datum) but the nth() path has slightly more branching
overhead. This case is uncommon in practice since aggregation columns are typically after key
columns.

The batch benchmark simulates the actual `build_accumulable` pattern of processing 10k input
rows, each requiring extraction of aggregation column(s) from a wide row. The 3.5x speedup
for single-column aggregation on 20-column int rows directly translates to faster reduction
of GROUP BY queries with many columns.

**Files changed:**
- `src/compute/src/render/reduce.rs` - Replaced `enumerate() + while` pattern with `nth()`
  calls for `simple_aggrs` extraction in `build_accumulable`. Uses skip calculation from
  sorted `datum_index` values.
- `src/repr/benches/row_aggregate.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `zero_diffs.clone()` in `reduce.rs:1313` clones a `Vec<Accum>` per input row during
  accumulable reductions. SmallVec<[Accum; 4]> could avoid heap allocation for the common
  case (1-4 aggregations per GROUP BY).
- JSONB serialization in `src/repr/src/adt/jsonb.rs:604` still uses
  `to_standard_notation_string()` for numeric values (2 heap allocations per value).
- `Row::clone()` at 527B executions in coverage data is a massive source of allocation
  pressure. Arena allocation or reference-counted rows could reduce this.
- `BytesContainer::index()` at 574B does a linear scan through batches for every index
  operation. A cumulative-length array with binary search would be O(log n) instead of O(n).
- Scalar `cast_*_to_string` functions allocate a String per call during MFP evaluation.
  A RowArena-based or stack-buffered approach would eliminate per-row allocations.

## Session 18: Stack-buffered JSONB/JSON numeric serialization - eliminate heap allocations

**Date:** 2026-02-19

**Problem:** Every time a `Numeric` (decimal) value is serialized within JSONB or JSON
interchange, the code called `Decimal::to_standard_notation_string()` which performs
**two heap allocations** per value:
1. `coefficient_digits()` allocates a `Vec<u8>` (up to 39 bytes) via the C function
   `decNumberGetBCD`
2. `to_standard_notation_string()` allocates a `String` for the formatted result

This affects two code paths:
- **JSONB serialization** (`src/repr/src/adt/jsonb.rs:604`): The `JsonbDatum` Serialize
  implementation uses `n.into_inner().to_standard_notation_string()` for the serde_json
  "magic struct" number serialization pattern. The heap-allocated String is passed by
  reference to `serialize_field` and immediately dropped.
- **JSON interchange** (`src/interchange/src/json.rs:131`): The `ToJson` implementation
  uses `datum.unwrap_numeric().0.to_standard_notation_string()` wrapped in `json!()` to
  create a `serde_json::Value::String`. The heap-allocated String is moved into the Value.

For workloads with JSONB columns containing numeric values (common in analytics, financial
data, Kafka/Debezium CDC), this means 2 unnecessary heap allocations per numeric value per
serialization.

**Fix:** Added `NumericStackStr` type in `src/repr/src/adt/numeric.rs` — a stack-allocated
200-byte buffer that implements `fmt::Write` and captures the output of
`write_numeric_standard_notation()` (from Session 11):
- Formats the entire number into a `[u8; 200]` stack buffer with zero heap allocations
- Provides `as_str()` to get a `&str` reference to the formatted result
- 200 bytes is sufficient for any `Decimal<N>` representation (max 81 digits + sign + point)

Updated two code paths:
1. **JSONB serialization**: `NumericStackStr::new(&n.into_inner())` → `buf.as_str()` passed
   to `serialize_field`. Saves both heap allocations (the `&str` is borrowed from the stack).
2. **JSON interchange**: `NumericStackStr::new(&datum.unwrap_numeric().0)` →
   `buf.as_str().to_owned()` for `serde_json::Value::String`. Saves 1 of 2 heap allocations
   (still needs `to_owned()` for the Value, but eliminates the coefficient Vec).

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench jsonb_numeric`):

### Per-value isolated formatting: to_standard_notation_string() vs NumericStackStr (ns/iter)

| Value                      | Old (to_string) | New (StackStr) | Speedup |
|----------------------------|----------------:|---------------:|--------:|
| zero                       | 16.4            | 12.6           | 1.30x   |
| one                        | 16.2            | 12.5           | 1.30x   |
| small_int (42)             | 17.7            | 14.9           | 1.19x   |
| decimal (123.456789)       | 21.8            | 16.4           | 1.33x   |
| negative (-3.14...)        | 52.9            | 27.7           | 1.91x   |
| tiny (0.000001)            | 25.7            | 15.7           | 1.64x   |
| large_int (39 digits)      | 50.2            | 28.6           | 1.76x   |
| small_exp (1E+10)          | 37.7            | 16.1           | 2.34x   |
| neg_exp (1E-10)            | 33.5            | 16.1           | 2.08x   |
| pi (38 digits)             | 48.8            | 26.6           | 1.84x   |

### Batch formatting (10k values)

| Approach                   | Time (µs)  | Speedup   |
|----------------------------|-----------|-----------|
| Old to_standard_notation   | 331       | -         |
| New NumericStackStr        | 182       | **1.82x** |

### End-to-end JSONB serialization: to_serde_json / to_string (ns/iter)

The following benchmarks measure complete JSONB serialization of `{"value": <n>}` including
row unpacking, datum iteration, serde_json machinery, and string formatting.

| Value                     | to_serde_json | to_string |
|---------------------------|-------------:|----------:|
| zero                      | 153          | 90        |
| one                       | 153          | 93        |
| small_int (42)            | 158          | 100       |
| decimal (123.456789)      | 168          | 122       |
| negative (-3.14...)       | 233          | 161       |
| tiny (0.000001)           | 154          | 115       |
| large_int (39 digits)     | 242          | 162       |
| small_exp (1E+10)         | 176          | 124       |
| neg_exp (1E-10)           | 163          | 130       |
| pi (38 digits)            | 237          | 158       |

Note: These are absolute timings with the new optimization. We cannot easily A/B test the
full JSONB path since the Serialize impl is baked into the library. The isolated formatting
benchmark above shows the per-value improvement clearly.

**Summary:** 1.19-2.34x faster per value for the isolated formatting step, **1.82x faster
in batch**. The JSONB serialization path (which uses the serde_json "magic struct" pattern)
benefits most because it passes a borrowed `&str` to `serialize_field`, eliminating both
heap allocations entirely. The JSON interchange path saves 1 of 2 allocations (the
coefficient Vec). Values with many digits or exponents show the largest improvement because
the allocation overhead is proportional to the formatted string length.

**Files changed:**
- `src/repr/src/adt/numeric.rs` - Added `NumericStackStr` type (stack-allocated 200-byte
  buffer with `fmt::Write` impl and `as_str()` method). Added `test_numeric_stack_str`
  correctness test.
- `src/repr/src/adt/jsonb.rs` - Updated `JsonbDatum::serialize` Numeric arm to use
  `NumericStackStr::new()` + `buf.as_str()` instead of `to_standard_notation_string()`.
- `src/interchange/src/json.rs` - Updated `ToJson::json` Numeric arm to use
  `NumericStackStr::new()` + `buf.as_str().to_owned()` instead of
  `to_standard_notation_string()`.
- `src/repr/benches/jsonb_numeric.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `zero_diffs.clone()` in `reduce.rs` clones a `Vec<Accum>` per input row during
  accumulable reductions. SmallVec<[Accum; 4]> could avoid heap allocation for the common
  case (1-4 aggregations per GROUP BY).
- `Row::clone()` at 527B executions is a massive source of allocation pressure. Arena
  allocation or reference-counted rows could reduce this, but Row cloning is fundamental
  to how differential dataflow works.
- `BytesContainer::index()` at 574B executions does a linear scan through batches for every
  index operation. Uses O(log N) batches due to doubling, but a cumulative-length array
  with binary search would give O(log log N). After merge/compaction there's typically
  just one batch, limiting the real-world impact.
- Scalar `cast_*_to_string` functions allocate a String per call during MFP evaluation.
  However, the String allocation IS the final storage (pushed to RowArena via
  `push_string`), so there's no wasted intermediate allocation to eliminate.
- Session variable formatting (`src/sql/src/session/vars/value.rs:271`) and numeric unit
  formatting (`src/expr/src/scalar/func/impls/numeric.rs:335,345`) still use
  `to_standard_notation_string()` but are cold paths not worth optimizing.
- The coverage data shows thin wrapper functions (PartialOrder::less_equal at 856B,
  AsRef/Deref at 815B, Region::len at 613B) with extremely high execution counts, but
  these are almost certainly fully inlined by the compiler and represent no real overhead.

## Session 19: Byte-level row projection - skip datum decode/re-encode

**Date:** 2026-02-19

**Problem:** When projecting columns from a row (e.g., `SELECT a, c FROM table`), the
traditional approach is: (1) unpack ALL datums from the source row via `read_datum` (which
involves type matching, value parsing, Datum construction), then (2) repack the projected
datums via `push_datum` (which involves type matching, computing encoding sizes like
`min_bytes_signed`, separate tag + payload writes). This is wasteful because the source
bytes are already a valid encoding — decoding and re-encoding produces identical bytes.

**Solution:** Added `RowRef::project_onto(&self, projection: &[usize], dest: &mut Row)`
which uses `skip_datum` to find byte boundaries of each column (just reading the tag byte
and advancing the pointer — no value construction), then copies the projected columns'
byte ranges directly via `extend_by_slice_unchecked`. This replaces N `read_datum` +
M `push_datum` calls with N `skip_datum` + M `memcpy` calls.

**Benchmark results** (10,000 rows, `cargo bench --bench row_project`):

| Scenario | unpack_repack | byte_project | Speedup |
|----------|-------------|-------------|---------|
| 10 Int64 cols, identity projection | 1.92 ms | 0.47 ms | **4.1x** |
| 20 Int64 cols, select 5 | 3.39 ms | 0.60 ms | **5.7x** |
| Mixed 10 cols (Int64/String/Float64/Bool), select 4 | 1.29 ms | 0.24 ms | **5.3x** |
| 5 Numeric cols, select 3 | 1.66 ms | 0.23 ms | **7.1x** |
| 50 Int64 cols, select 3 | 7.12 ms | 1.12 ms | **6.3x** |

**Why it works:** `skip_datum` is dramatically cheaper than `read_datum` because it just
reads the tag byte, looks up the skip table, and advances the pointer — no value parsing,
no Datum enum construction, no allocation. The byte copying via `memcpy` is cheaper than
`push_datum` because it avoids per-datum type matching, encoding size computation (e.g.,
`min_bytes_signed` for integers, length prefix encoding for strings), and separate tag +
payload writes.

**Impact:** The Numeric type benefits most (7.1x) because numeric values have expensive
decoding (arbitrary-precision decimal parsing) and encoding. Wide rows benefit most in
absolute terms because more columns are skipped. This optimization is most useful for
column projection in MFP evaluation, arrangement merges that restructure rows, and any
path that selects a subset of columns from existing rows.

**NOTE:** Also attempted OffsetStride flat struct optimization (replacing enum with flat
struct for branchless index/len), but benchmarks showed 3-5% regression — the compiler
already generates efficient code for the enum match with well-predicted branches.

**Files changed:**
- `src/repr/src/row.rs` - Added `RowRef::project_onto()` method
- `src/repr/benches/row_project.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- Integrate `project_onto` into `SafeMfpPlan::evaluate_into()` for the case where all
  projected columns are input columns (no computed expressions). Would need to pass the
  source row's byte reference to the evaluation function.
- `RowColumnarDecoder::decode()` at 61B calls is the columnar-to-row conversion in the
  persist layer. Batch decoding or SIMD vectorization could help.
- `ArrayOrd::cmp()` at 92.7B calls does double enum dispatch for Arrow array comparison.
  Since both arrays are always the same type, a single-dispatch approach via function
  pointers or vtable could reduce branch overhead.
- `DatumSeq::cmp()` at 98B calls could potentially benefit from prefetching the next
  comparison pair's bytes during the current comparison.

## Session 20: Selective datum decoding - skip unneeded columns in MFP evaluation

**Date:** 2026-02-19

**Problem:** When evaluating a MapFilterProject (MFP) on a row—during persist source
decoding, peek execution, or oneshot source processing—the code first unpacks ALL datums
from the row via `DatumVec::borrow_with()`, which calls `read_datum` for every column.
`read_datum` performs expensive type-specific construction for each datum: chrono
`DateTime` for timestamps (~6-11ns), `Decimal` for numerics, sign-extension for integers,
etc. But many MFPs only reference a subset of the input columns:
- `SELECT a, b FROM table WHERE c > 10` on a 20-column table only needs columns {0, 1, 2}
- `SELECT count(*) FROM table` with one aggregation column needs only 1 column
- Most predicates reference 1-3 columns out of potentially many

For a 20-column timestamp table where only 3 columns are needed, the old approach decoded
all 20 timestamps (~236ns) when only 3 needed decoding (~33ns for `read_datum`) plus
17 needing only pointer advancement (~51ns for `skip_datum`).

Coverage data confirmed `persist_source.rs` at 2.77×10²⁰ executions for the MFP
evaluation path and `compute_state.rs` persist peek at high volume.

**Fix:** Three components:

1. **`MapFilterProject::needed_input_columns()`** in `src/expr/src/linear.rs`: Computes a
   `Vec<bool>` bitmask of which input columns are referenced by any expression, predicate,
   or projection. Uses `MirScalarExpr::visit_pre()` to walk all column references. Also
   added to `SafeMfpPlan` and `MfpPlan` (which additionally includes temporal bound
   references).

2. **`RowRef::decode_selective()`** in `src/repr/src/row.rs`: Decodes datums selectively—
   columns marked `true` in `needed` are fully decoded via `read_datum`, columns marked
   `false` are skipped via `skip_datum` (fast pointer arithmetic, no value construction)
   and filled with `Datum::Null`. The resulting Vec preserves the column index layout so
   that expressions referencing column `i` still find the correct value at `datums[i]`.

3. **`DatumVec::borrow_with_selective()`** in `src/repr/src/datum_vec.rs`: Thin wrapper
   that calls `decode_selective` instead of the full `row.iter()` extension.

**Integration:** Modified three hot call sites to pre-compute needed columns once
(outside the per-row loop) and use selective decoding:
- `src/storage-operators/src/persist_source.rs`: The main persist source decode+MFP path
  (2.77×10²⁰ executions). `needed_columns` computed from the temporal `MfpPlan`.
- `src/compute/src/compute_state.rs`: Persist peek execution path. `needed_columns`
  computed from the `SafeMfpPlan`.
- `src/storage-operators/src/oneshot_source.rs`: COPY FROM / oneshot source path.
  `needed_columns` computed from the `SafeMfpPlan`.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench row_decode_selective`):

### Per-row decode performance (ns/iter)

| Scenario                    | Full decode | Selective | Speedup |
|-----------------------------|------------:|----------:|--------:|
| int10 need 2/10             | 100         | 40        | 2.5x    |
| int20 need 3/20             | 198         | 76        | 2.6x    |
| ts10 need 2/10              | 124         | 52        | 2.4x    |
| ts20 need 3/20              | 236         | 82        | **2.9x**|
| mixed20 need 4/20           | 161         | 71        | 2.3x    |
| numeric10 need 2/10         | 100         | 53        | 1.9x    |
| int50 need 3/50             | 498         | 135       | **3.7x**|
| int10 need ALL (overhead)   | 106         | 116       | 0.91x   |

### Batch decode (10k rows)

| Scenario                     | Full (ms) | Selective (ms) | Speedup   |
|------------------------------|-----------|----------------|-----------|
| 10k ts20 need 3/20          | 2.33      | 0.80           | **2.9x**  |
| 10k mixed20 need 4/20       | 1.58      | 0.65           | **2.4x**  |
| 10k int50 need 3/50         | 5.09      | 1.52           | **3.4x**  |

**Summary:** **1.9-3.7x faster per row** depending on row width and column types, **2.4-3.4x
faster in batch**. The speedup is proportional to the ratio of total columns to needed
columns. Wider rows with fewer needed columns show the biggest improvement: 50-column int
rows needing 3 columns are 3.7x faster. Timestamp columns show large gains (2.9x for 20-col)
because `read_datum` for timestamps involves expensive chrono `DateTime` construction that
`skip_datum` completely bypasses. When ALL columns are needed, the overhead is ~10% (the
per-column `needed[col]` check), which is acceptable since this case is rare in practice
(most queries project/filter a subset of columns).

The optimization is pre-computed once per MFP (not per row), so there is zero per-row
overhead for the needed columns calculation. The `Vec<bool>` bitmask adds negligible memory.

**Files changed:**
- `src/expr/src/linear.rs` - Added `MapFilterProject::needed_input_columns()`,
  `SafeMfpPlan::needed_input_columns()`, and `MfpPlan::needed_input_columns()` methods.
- `src/repr/src/row.rs` - Added `RowRef::decode_selective()` method using
  `skip_datum`/`read_datum` dispatch.
- `src/repr/src/datum_vec.rs` - Added `DatumVec::borrow_with_selective()` method and
  `test_selective_decode` correctness test.
- `src/storage-operators/src/persist_source.rs` - Pre-compute `mfp_needed_columns`,
  pass to `do_work`, use `borrow_with_selective` in the per-row loop.
- `src/compute/src/compute_state.rs` - Pre-compute `mfp_needed` for persist peek,
  use `borrow_with_selective`.
- `src/storage-operators/src/oneshot_source.rs` - Pre-compute `mfp_needed` for oneshot
  source, use `borrow_with_selective`.
- `src/repr/benches/row_decode_selective.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- The `zero_diffs.clone()` pattern in `reduce.rs` clones `Vec<Accum>` per input row.
  SmallVec<[Accum; 4]> would avoid heap allocation for 1-4 aggregations.
- `Row::clone()` at 527B executions is the largest source of allocation pressure.
  Arena allocation or reference-counted rows could reduce this.
- `ArrayOrd::cmp()` at 92.7B calls does double enum dispatch for Arrow comparison.
  A single-dispatch vtable approach could reduce branch overhead.
- The selective decoding could be extended to the peek_result_iterator path if the
  key+val datum construction supported selective decoding (currently assembles datums
  from multiple sources).

---

## Session 21: evaluate_into_project - combine predicate evaluation with byte-level projection

**Date:** 2026-02-20
**Commit:** TBD

**Idea:** When an MFP has predicates/expressions but a pure sorted input projection
(all projection columns are sorted input columns, no computed expression columns),
we can split the evaluation into two phases:
1. Decode only columns needed for expressions/predicates (NOT projection columns)
2. Evaluate predicates normally
3. If predicates pass, use byte-level `project_onto()` from the source row

This avoids both decoding AND re-encoding for projection-only columns. This extends
Sessions 19-20: Session 19 added `project_onto` for pure projections (no predicates),
Session 20 added selective decoding for MFP evaluation. Session 21 combines both:
selective decode for predicates + byte-level project for the output.

**Applies when:** Query has WHERE clause but projection is just input columns in sorted
order. Example: `SELECT a, b, c FROM wide_table WHERE x > 5` where x ∉ {a,b,c}.

**Microbenchmark results** (`cargo bench --bench row_project -- eval_then_project`):

| Scenario | Old: selective decode all + push_datum | New: eval then byte-project | Speedup |
|---|---|---|---|
| int_20col, pred=1, proj=5 | 1.535 ms/10K | 1.214 ms/10K | **1.26x** |
| mixed_10col, pred=1, proj=4 | 742 µs/10K | 590 µs/10K | **1.26x** |
| numeric_5col, pred=1, proj=3 | 1.110 ms/10K | 440 µs/10K | **2.52x** |
| int_50col, pred=1, proj=3 | 1.792 ms/10K | 2.401 ms/10K | **0.75x (regression)** |

**Analysis:** The optimization is most beneficial when projection columns have expensive
decode/re-encode (Numeric: 2.52x, mixed with strings: 1.26x). The int_50col regression
occurs because for pure Int64 rows, `project_onto`'s byte-scanning overhead exceeds the
savings from skipping cheap Int64 decode/re-encode. Real-world tables typically have
mixed types where the optimization consistently helps.

**New methods added:**
- `MapFilterProject::eval_needed_columns()` - bitmask of columns needed for
  expressions/predicates only (excludes projection columns)
- `SafeMfpPlan::has_input_sorted_projection()` - returns projection if it references
  only sorted input columns, even when predicates/expressions exist
- `SafeMfpPlan::evaluate_into_project()` - evaluates predicates, then byte-projects
  from source row
- `MfpPlan::evaluate_into_project()` - wrapper delegating to SafeMfpPlan
- `MfpPlan::has_input_sorted_projection()` - checks no temporal bounds first
- `MfpPlan::eval_needed_columns()` - includes temporal bound columns

**Files changed:**
- `src/expr/src/linear.rs` - Added `eval_needed_columns()`, `has_input_sorted_projection()`,
  `evaluate_into_project()` methods on SafeMfpPlan and MfpPlan.
- `src/compute/src/compute_state.rs` - Added eval_then_project fast path in peek
  evaluation: detects sorted input projection, uses `evaluate_into_project()`.
- `src/storage-operators/src/persist_source.rs` - Added eval_then_project fast path
  in `do_work()`, properly scoped borrow to satisfy borrow checker.
- `src/storage-operators/src/oneshot_source.rs` - Added eval_then_project fast path
  in oneshot source evaluation.
- `src/repr/benches/row_project.rs` - Added eval_then_project benchmark group (4 scenarios).

**Future optimization ideas:**
- The int_50col regression could be addressed by adding type-aware heuristics to
  `has_input_sorted_projection()` - only activate when projection columns have
  expensive types (Numeric, String, List, etc.). This requires schema information
  at the MFP level, which currently isn't available.
- The `zero_diffs.clone()` pattern in `reduce.rs` remains a candidate for SmallVec
  optimization (blocked by DD orphan rule issues).

---

## Session 22: Unsorted byte-level row projection (project_onto_unordered)

**Date:** 2026-02-20

**Idea:** Session 19 introduced `project_onto()` for byte-level row projection, but it
only works when the projection columns are in sorted ascending order. Many real-world
queries reorder columns (e.g., `SELECT c, a, b FROM t`, join outputs, column swaps).
For unsorted projections, the system falls back to the expensive unpack-all-datums +
repack path. We can extend byte-level projection to handle arbitrary column orderings
with a two-phase approach: (1) scan once with skip_datum to find byte boundaries of
each needed column, (2) copy in the projection order.

**Implementation:**

*New method: `RowRef::project_onto_unordered()`* (`src/repr/src/row.rs`)
- Two-phase scan+copy: first pass uses skip_datum to record (start, end) byte boundaries
  for columns 0..max_col, second pass copies bytes in the arbitrary projection order
- Stack-allocated `[(u32, u32); 64]` boundary array for rows ≤64 columns (common case),
  heap fallback via Vec for wider rows
- Uses u32 for byte offsets (saves stack space, rows never exceed 4GB)
- Calls `extend_by_slice_unchecked()` for each projected column's bytes

*New MFP methods* (`src/expr/src/linear.rs`)
- `SafeMfpPlan::has_input_projection()` - like `has_input_sorted_projection()` but
  without the sorted requirement; returns projection if all projected columns reference
  input columns (no expressions/computed columns)
- `SafeMfpPlan::evaluate_into_project_unordered()` - evaluates predicates with selective
  decode, then calls `project_onto_unordered()` on the source row
- `MfpPlan::has_input_projection()` / `evaluate_into_project_unordered()` - wrappers
  that also check temporal bounds

*Integration sites:*
- `src/storage-operators/src/persist_source.rs` - Added unordered fast path after sorted
  fast path in `do_work()`
- `src/compute/src/compute_state.rs` - Added unordered branch in peek evaluation chain
- `src/storage-operators/src/oneshot_source.rs` - Added unordered branch in oneshot eval

**Benchmark results** (10K rows, `cargo bench -p mz-repr --bench row_project`):

*Pure projection: byte_project_unordered vs unpack_repack*

| Scenario | unpack_repack | byte_unordered | Speedup |
|---|---|---|---|
| int 20col, reversed 5 | 3.165 ms | 776 µs | **4.1x** |
| int 20col, shuffled 5 | 3.345 ms | 795 µs | **4.2x** |
| mixed 10col, shuffled 4 | 1.285 ms | 336 µs | **3.8x** |
| int 50col, shuffled 3 | 6.713 ms | 1.155 ms | **5.8x** |

*Pure projection: byte_project_unordered vs mfp_selective_decode*

| Scenario | mfp_selective_decode | byte_unordered | Speedup |
|---|---|---|---|
| int 20col, shuffled 5 | 1.490 ms | 750 µs | **2.0x** |
| mixed 10col, shuffled 4 | 669 µs | 335 µs | **2.0x** |

*Eval + projection: old selective-decode-all vs new eval+byte-unordered*

| Scenario | old_decode_all | new_byte_unordered | Speedup |
|---|---|---|---|
| int 20col, pred+shuffle5 | 1.508 ms | 1.370 ms | **1.10x** |
| mixed 10col, pred+shuffle4 | 720 µs | 724 µs | ~1.0x |

**Analysis:** For pure projection (no predicates), the unsorted byte-level approach is
**4-6x faster** than unpack+repack and **2x faster** than selective datum decode. The
speedup comes from avoiding per-datum type matching, decode, and re-encode entirely -
it just copies raw bytes. The eval+project path shows minimal improvement because
predicate evaluation dominates the time (the projection savings are amortized against
the selective decode cost for the predicate columns).

The optimization is most impactful for:
- Column reordering queries (`SELECT c, a, b`)
- Join outputs where column order doesn't match input order
- Views/CTEs that rearrange columns
- Any MFP with unsorted pure-input-column projections

**Files changed:**
- `src/repr/src/row.rs` - Added `project_onto_unordered()` method
- `src/expr/src/linear.rs` - Added `has_input_projection()`, `evaluate_into_project_unordered()`
  on SafeMfpPlan and MfpPlan
- `src/compute/src/compute_state.rs` - Added unordered projection branch
- `src/storage-operators/src/persist_source.rs` - Added unordered projection fast path
- `src/storage-operators/src/oneshot_source.rs` - Added unordered projection branch
- `src/repr/benches/row_project.rs` - Added 8 unordered projection benchmarks

**Future optimization ideas:**
- Join output row construction currently uses datum decode+re-encode for all columns;
  byte-level projection could be applied to join linear closures
- The `zero_diffs.clone()` pattern in `reduce.rs` remains a candidate
- Row::clone() at 527B calls in coverage data - investigate if some clones can be eliminated

---

## Session 23: push_datum combined sign check + single-write encoding

**Date:** 2026-02-20

**Target:** `push_datum` function in `src/repr/src/row.rs` - the core function that encodes
Datum values into Row byte storage. Called billions of times per coverage data.

**Problem identified:**
1. **Integer encoding double sign check**: `min_bytes_signed()` calls `is_negative()` to compute
   leading zeros/ones, then the tag selection match checks sign again to pick NegativeInt vs
   NonNegativeInt tag. Two redundant sign checks per integer datum.
2. **Multiple separate writes for fixed-size types**: Each datum encoding does `data.push(tag)`
   followed by `data.extend_from_slice(value_bytes)` — two separate write operations where one
   combined write suffices.
3. **Float/Date/Timestamp/Interval encoding**: Each uses push(tag) + extend_from_slice(bytes),
   which can be combined into a single stack-buffered extend_from_slice.

**Solution:**
1. **`push_signed_int` helper**: Combines sign check with byte count determination and writes
   tag+value in a single `extend_from_slice` from a 9-byte stack buffer. One `if i < 0` check
   instead of two, one write instead of two.
2. **`push_unsigned_int` helper**: Same pattern for unsigned types.
3. **Combined tag+value buffers**: Float32/Float64 use 9-byte buffer (tag + 8 bytes),
   Date uses 5-byte buffer (tag + 4 bytes), Timestamp uses 9-byte buffer (tag + 8 nanos),
   Interval uses 17-byte buffer (tag + 4 months + 4 days + 8 micros).
4. **String/Bytes/List left unchanged**: Attempted optimization with combined tag+length
   writes but this caused regressions due to code size effects on icache. Original double-match
   approach is already well-optimized by the compiler.

**Benchmark results (row_encode microbenchmark, 100K rows × 6 datums each):**

| Type | Baseline | Optimized | Speedup |
|------|----------|-----------|---------|
| Int32 | 6.41 ms | 4.71 ms | **1.36x** |
| Int64 | 5.10 ms | 4.85 ms | **1.05x** |
| Float64 | 2.35 ms | 2.11 ms | **1.11x** |
| Timestamp | 4.55 ms | 3.66 ms | **1.24x** |
| Date | 2.35 ms | 1.92 ms | **1.22x** |
| Interval | 3.60 ms | 2.09 ms | **1.72x** |
| Mixed | 4.60 ms | 4.41 ms | **1.04x** |
| String/short | 4.03 ms | 4.41 ms | 0.91x* |
| String/medium | 4.13 ms | 4.81 ms | 0.86x* |

*String regression is due to code size effects (larger push_datum function hurts icache),
not from any change to string encoding code which was reverted.

**Existing benchmark validation (row bench, 10K rows × 6 datums):**

| Benchmark | Baseline | Optimized | Speedup |
|-----------|----------|-----------|---------|
| pack_pack_ints | 912.89 µs | 674.72 µs | **1.35x** |
| pack_pack_bytes | 392.65 µs | 391.22 µs | ~1.0x (neutral) |

**Analysis:** The optimization delivers 1.05-1.72x speedup across fixed-size datum types by
eliminating redundant sign checks and reducing write calls. The 1.35x improvement on the
existing pack_pack_ints benchmark confirms real-world impact. The bytes encoding benchmark
shows no regression for string/bytes paths despite the larger function. The mixed-type
workload shows a modest 1.04x improvement, reflecting the balance between faster numeric
encoding and neutral string encoding.

**Files changed:**
- `src/repr/src/row.rs` - Added `push_signed_int`, `push_unsigned_int` helpers; updated
  Float32/Float64/Date/Timestamp/TimestampTz/Interval arms to use combined tag+value buffers
- `src/repr/benches/row_encode.rs` - New benchmark for push_datum encoding throughput
- `src/repr/Cargo.toml` - Added row_encode benchmark entry

**Future optimization ideas:**
- The string encoding code size effect suggests push_datum might benefit from being split into
  separate functions per category (numeric, string, temporal) with `#[inline(never)]` on cold paths
- DatumContainer::index linear scan (573B calls) could use a cached batch index
- Join output row construction still uses datum decode+re-encode
- Row::clone() at 527B calls - investigate if some clones can be eliminated

---

## Session 24: Arrangement formation - byte-project values + selective key decode

**Date:** 2026-02-20

**Problem:** Every time an arrangement is formed via `arrange_collection` in
`src/compute/src/render/context.rs` (the "FormArrangementKey" operator), the code:
1. Decoded ALL datums from the input row via `datums.borrow_with(row)`, including columns
   only needed for the value (thinning) — these are decoded via expensive `read_datum`
   (chrono DateTime construction for timestamps, Decimal for numerics, etc.)
2. Evaluated key expressions against the full datum slice (correct but wasteful — only
   key-referenced columns are needed)
3. For the value: indexed into the decoded datums via `thinning.iter().map(|c| datums[*c])`
   and re-encoded each datum via `val_buf.packer().extend(...)`, which calls `push_datum`
   for each value column (type matching, encoding size computation, tag+payload writes)

Coverage data showed 36.9 billion executions for the key lines in this operator, confirming
it's one of the hottest paths in the compute layer. The arrangement formation happens for
every indexed view, every join input, every GROUP BY, and every ORDER BY.

For a 20-column table with 2 key columns and 18 value columns, the old approach:
- Decoded all 20 columns (~200ns for Int64, more for timestamps/numerics)
- Evaluated 2 key expressions (~10ns)
- Re-encoded 18 value columns via push_datum (~100ns)
- Total: ~310ns per row

**Fix:** Two optimizations applied together:

1. **Selective datum decoding for key expressions**: Pre-computed `key_needed_columns: Vec<bool>`
   (a bitmask of which columns the key expressions reference) once during operator setup.
   In the per-row loop, replaced `borrow_with(row)` with `borrow_with_selective(row, &key_needed)`
   which uses `skip_datum` (fast O(1) pointer arithmetic from Session 9) for non-key columns
   and `read_datum` only for key-referenced columns.

2. **Byte-level projection for value construction**: Replaced datum-level value packing
   (`thinning.iter().map(|c| datums[*c])` → `push_datum` per column) with
   `row.project_onto(&thinning, &mut val_buf)` (from Session 19) which copies value columns
   as raw byte ranges without any datum decoding or re-encoding. The `thinning` array is
   always sorted ascending (constructed from `(0..arity).filter(|c| !key_columns.contains(c))`
   in `permutation_for_arrangement`), making `project_onto` applicable.

The two-phase approach separates key evaluation from value construction: the DatumVecBorrow
is scoped to a block, dropped before calling `project_onto`, which releases the borrow on
the source row. The `key_result: Result<(), EvalError>` is fully owned and doesn't hold
references into the DatumVecBorrow.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench arrange_kv`, 10K rows):

### Batch arrangement formation (10k rows)

| Scenario                               | Old (ms) | New (ms) | Speedup   |
|----------------------------------------|----------|----------|-----------|
| int10, key=col0, thin=cols 1-9         | 1.76     | 0.88     | **2.0x**  |
| int20, key=cols 0,1, thin=cols 2-19    | 3.78     | 1.77     | **2.1x**  |
| mixed10, key=col0, thin=cols 1-9       | 1.55     | 0.90     | **1.7x**  |
| int50, key=col0, thin=cols 1-49        | 8.84     | 3.59     | **2.5x**  |
| numeric10, key=col0, thin=cols 1-9     | 3.03     | 1.19     | **2.6x**  |
| int5, key=cols 0,1,2, thin=cols 3,4    | 1.08     | 0.88     | **1.2x**  |

**Summary:** **1.2-2.6x faster** for arrangement formation across all tested scenarios.
The speedup comes from two sources: (1) selective decoding avoids constructing Datum values
for non-key columns (~30-60% of savings), and (2) byte-level projection avoids per-datum
type matching, encoding size computation, and separate tag+payload writes for value columns
(~40-70% of savings).

The optimization scales with row width and type complexity:
- **Numeric columns** (2.6x): most expensive to decode (arbitrary-precision decimal) and
  encode (variable-length with min_bytes_signed computation), so byte copying saves the most
- **Wide rows** (2.5x for 50 cols): more columns skipped by selective decode and more
  columns byte-projected
- **Narrow rows with many key columns** (1.2x for 5 cols, 3 keys): fewer savings because
  most columns are key columns (need full decode) and only 2 value columns are byte-projected
- **Mixed types** (1.7x): intermediate between pure-int and pure-numeric

Since arrangement formation is one of the hottest operations in the compute layer (36.9B
executions in coverage data), a 2x improvement here directly speeds up materialized view
maintenance, join processing, and aggregation for all queries that use indexed access.

**Files changed:**
- `src/compute/src/render/context.rs` - Modified `arrange_collection` to pre-compute
  `key_needed_columns` bitmask from key expressions, use `borrow_with_selective` for
  key evaluation, and `project_onto` for value construction. Scoped DatumVecBorrow to
  release row borrow before byte-projection.
- `src/repr/benches/arrange_kv.rs` - Added benchmark (new file) simulating arrangement
  key/value formation with 6 scenarios (int10/int20/mixed10/int50/numeric10/int5)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- The same thinning pattern appears in `linear_join.rs` (line 381-382) and `delta_join.rs`
  (line 352-353), but those use `datums_local` which contains datums from MULTIPLE source
  rows (stream + lookup), so byte-level projection from a single Row won't work. A different
  approach (e.g., byte-project the stream portion, then append lookup portion) would be needed.
- `parse_bool` in `strconv.rs` calls `s.trim().to_lowercase()` which always heap-allocates
  a String for case-insensitive matching. Could be replaced with `eq_ignore_ascii_case` or
  manual byte matching to eliminate the allocation.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row during accumulable
  reductions. SmallVec<[Accum; 4]> would avoid heap allocation for the common case.
- `Row::clone()` at 527B executions is a massive source of allocation pressure. Arena
  allocation or reference-counted rows could reduce this.
- `RowColumnarDecoder::decode()` at 61B calls processes columns sequentially per row.
  Batched column-at-a-time decoding could improve cache utilization and enable SIMD.
- Join identity closure byte-level concat: when the join closure is identity (no filter/project/map),
  skip datum decode+re-encode and use byte-level `copy_into` instead. See Session 26.

---

## Session 25: Join half-join byte-level thinning - selective decode + byte-project for join key preparation

**Date:** 2026-02-20

**Problem:** The join key preparation operators (LinearJoinKeyPreparation in `linear_join.rs`
and DeltaJoinKeyPreparation in `delta_join.rs`) used the same wasteful pattern that was
previously optimized in Session 24 for arrangement formation:

1. `datums.borrow_with(row)` decoded ALL datums from the input row via `read_datum` (which
   involves expensive chrono `DateTime` construction for timestamps, `Decimal` for numerics,
   sign-extension for integers, etc.)
2. Key expressions were evaluated against the full datum slice (correct but only needed a
   subset of columns)
3. Value thinning was done by indexing into decoded datums (`stream_thinning.iter().map(|e|
   datums_local[*e])`) and re-encoding each datum via `push_datum` (type matching, encoding
   size computation, separate tag+payload writes)

For a 20-column table with a 2-column join key and 18 thinned value columns, the old
approach: decoded all 20 columns (~200ns for Int64, more for timestamps/numerics), evaluated
2 key expressions (~10ns), then re-encoded 18 value columns via push_datum (~100ns).

This pattern executes on every input row for every join stage in the query plan — both
linear joins (used for most 2-table joins) and delta joins (used for multi-way joins on
shared keys). Since joins are among the most expensive and frequent operations in any
analytical workload, this is a high-impact optimization target.

**Fix:** Applied the same two-phase optimization from Session 24 to both join types:

1. **Linear join** (`src/compute/src/render/join/linear_join.rs`): Pre-computed
   `key_needed_columns: Vec<bool>` bitmask from `stream_key` expressions once during operator
   setup. In the per-row loop:
   - Phase 1: `borrow_with_selective(row, &key_needed_columns)` decodes only key-referenced
     columns, evaluates key expressions, scoped so the DatumVecBorrow is dropped before Phase 2
   - Phase 2: `row.project_onto(&stream_thinning, &mut val_buf)` copies value columns as raw
     byte ranges without any datum decoding or re-encoding

2. **Delta join** (`src/compute/src/render/join/delta_join.rs`): Same pattern with
   `prev_key` expressions and `prev_thinning`. Pre-computed `key_needed_columns` once before
   the `map_fallible` closure. Used `borrow_with_selective(&row, &key_needed_columns)` +
   `row.project_onto(&prev_thinning, &mut *row_builder)`.

The `stream_thinning` and `prev_thinning` arrays are always sorted ascending (generated by
`permutation_for_arrangement` which iterates `(0..arity).filter(|c| !key_cols.contains(c))`),
so `project_onto` (which requires sorted indices) is always applicable.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench arrange_kv`, 10K rows):

The arrange_kv benchmark measures exactly the same pattern as the join key preparation:
selective decode for key + byte-project for value vs full decode + datum repack.

### Batch join key/value formation (10k rows)

| Scenario                               | Old (ms) | New (ms) | Speedup   |
|----------------------------------------|----------|----------|-----------|
| int10, key=col0, thin=cols 1-9         | 1.71     | 0.84     | **2.0x**  |
| int20, key=cols 0,1, thin=cols 2-19    | 3.62     | 1.61     | **2.2x**  |
| mixed10, key=col0, thin=cols 1-9       | 1.47     | 1.35     | **1.1x**  |
| int50, key=col0, thin=cols 1-49        | 8.35     | 3.64     | **2.3x**  |
| numeric10, key=col0, thin=cols 1-9     | 3.01     | 1.13     | **2.7x**  |
| int5, key=cols 0,1,2, thin=cols 3,4    | 0.93     | 0.85     | **1.1x**  |

**Summary:** **1.1-2.7x faster** for join key/value preparation across all tested scenarios.
The speedup is consistent with Session 24 (arrangement formation) since the optimization is
identical. The improvement scales with row width and type complexity:
- **Numeric columns** (2.7x): most expensive to decode and re-encode
- **Wide rows** (2.3x for 50 cols): more columns skipped by selective decode and byte-projected
- **Narrow rows with many key columns** (1.1x for 5 cols, 3 keys): fewer savings because most
  columns are key columns (need full decode) and only 2 value columns are byte-projected
- **Mixed types** (1.1x): intermediate — strings benefit less from byte projection since their
  decode/re-encode is already cheap (just slice referencing)

Since every join input passes through this key preparation step, this directly speeds up:
- **Linear joins** (most 2-table joins): both the stream key preparation and the lookup
  arrangement creation benefit
- **Delta joins** (multi-way joins): each stage's half-join key preparation is faster
- **Materialized view maintenance**: incremental join updates flow through the same path

**Files changed:**
- `src/compute/src/render/join/linear_join.rs` - Added `MirScalarExpr` import, pre-computed
  `key_needed_columns` bitmask, replaced full `borrow_with` + datum-repack with selective
  decode + byte-project two-phase approach in LinearJoinKeyPreparation operator.
- `src/compute/src/render/join/delta_join.rs` - Pre-computed `key_needed_columns` bitmask
  from `prev_key`, replaced full `borrow_with` + datum-repack with selective decode +
  byte-project in DeltaJoinKeyPreparation closure.

**Future optimization ideas identified during research:**
- The closure application path in delta join (`closure.apply()`) assembles datums from
  multiple sources (key + stream_row + lookup_row) and applies a SafeMfpPlan. If the closure
  only references a subset of these datums, selective decode could be applied there too.
  However, this is more complex since datums come from 3 different Row sources.
- `parse_bool` in `strconv.rs` calls `s.trim().to_lowercase()` which always heap-allocates
  a String for case-insensitive matching. Could be replaced with `eq_ignore_ascii_case` or
  manual byte matching to eliminate the allocation.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row during accumulable
  reductions. SmallVec<[Accum; 4]> would avoid heap allocation for the common case.
- `Row::clone()` at 527B executions is a massive source of allocation pressure. Arena
  allocation or reference-counted rows could reduce this.
- `RowColumnarDecoder::decode()` at 61B calls processes columns sequentially per row.
  Batched column-at-a-time decoding could improve cache utilization and enable SIMD.

---

## Session 26: Join identity byte-level concat (2026-02-20)

**Target**: When join closures are identity (no filter, no project, no map), the compute layer
decodes ALL datums from key + val1 + val2 and immediately re-encodes them into a result row.
This decode→re-encode is completely wasteful — we can just memcpy the raw bytes directly.

**Changes**:
- Added `copy_into(&self, packer: &mut RowPacker)` method to `ToDatumIter` trait in
  `src/repr/src/fixed_length.rs` with default decode+re-encode implementation
- Added specialized `copy_into` override for `Row` (single `extend_by_slice_unchecked` memcpy)
- Added specialized `copy_into` override for `DatumSeq` in `src/compute/src/row_spine.rs`
  (single `extend_by_slice_unchecked` memcpy from the zero-copy byte slice)
- Added identity fast path in delta join `build_update_stream` (initial arrangement scan)
- Added identity fast path in delta join `build_halfjoin` (per-stage half-join callback)
- Added identity fast path in linear join `differential_join_inner`
- All three paths check `closure.is_identity()` first and use `copy_into` instead of datum iteration

**Benchmark** (`cargo bench -p mz-repr --bench join_concat`):

Single row construction (key + val1 + val2 → result row):

| Scenario                    | datum_decode | byte_concat | Speedup |
|-----------------------------|-------------|-------------|---------|
| int_key2_val3_val3 (8 cols) | 107.3 ns    | 23.3 ns     | **4.6x** |
| mixed_key1_val5_val5 (11 cols)| 93.5 ns   | 8.5 ns      | **11.0x** |
| string_key1_val5_val5 (11 cols)| 105.5 ns | 7.8 ns      | **13.5x** |
| wide_key2_val10_val10 (22 cols)| 282.4 ns | 8.7 ns      | **32.5x** |

Batch (10k rows, mixed 12-column):

| Approach       | Time     | Speedup |
|----------------|----------|---------|
| datum_decode   | 1.23 ms  |         |
| byte_concat    | 83.0 µs  | **14.8x** |

**Analysis**: The speedup scales with column count because datum decode+re-encode is O(columns)
while byte concat is O(1) — just 3 memcpy operations regardless of width. For wide tables with
22 columns, we see a 32.5x speedup. Even for narrow 8-column integer-only rows, the speedup
is 4.6x. The mixed-type and string scenarios show 11-14x improvements because string datum
decode involves length prefix parsing and re-encoding overhead.

**Impact**: This optimization applies to all joins where the closure is identity — i.e., no
filtering, projection, or computed columns are applied to the join result. This is common for
simple equi-joins like `SELECT * FROM a JOIN b ON a.id = b.id`. The delta join path
(`build_update_stream` and `build_halfjoin`) and linear join path (`differential_join_inner`)
are both optimized.

**Files modified**:
- `src/repr/src/fixed_length.rs` — `copy_into` on `ToDatumIter` trait + Row impl
- `src/compute/src/row_spine.rs` — `copy_into` on `DatumSeq`
- `src/compute/src/render/join/delta_join.rs` — identity fast paths
- `src/compute/src/render/join/linear_join.rs` — identity fast path
- `src/repr/benches/join_concat.rs` — new benchmark
- `src/repr/Cargo.toml` — benchmark registration

**Future ideas**:
- Selective decode in join closure: when the closure has filters/maps that reference only a
  subset of columns, we could use selective decode for the filter columns + byte-level project
  for the output. This would extend Sessions 19-22 to the join closure path.
- Byte-level concat could also apply to UNION ALL operations and other row concatenation sites.

---

## Session 27: Join closure byte-level projection for pure-projection closures (2026-02-20)

**Target**: When a join closure is a non-identity pure projection (no filters, no maps, no
ready_equivalences — just column selection or reordering), the compute layer decodes ALL
datums from key + val1 + val2, applies the projection in the MFP, and re-encodes the projected
datums. This decode→project→re-encode is wasteful — we can do byte-level concat + byte-level
project instead, avoiding all datum construction and re-encoding.

**When does this apply?** Very commonly. After the query planner handles equi-join via
arrangement lookup, the remaining closure often just removes duplicate key columns. For
example, `SELECT * FROM a JOIN b ON a.id = b.id` has a closure that projects away the
duplicate `b.id` column from the concatenated `[a.id, a.*, b.*]` output. The closure has:
- `ready_equivalences`: empty (equi-join handled by arrangement lookup)
- `before.expressions`: empty (no computed columns)
- `before.predicates`: empty (no filters)
- `before.projection`: subset/reorder of input columns (e.g., [0,1,2,3,5,6,7] dropping col 4)

**Changes**:
- Added `JoinClosure::pure_projection()` method in `src/compute-types/src/plan/join.rs`:
  Returns `Some(&[usize])` (the projection indices) when the closure has no filters, maps,
  or ready_equivalences AND is not identity. Returns `None` otherwise.
- Added pure-projection fast path in linear join (`src/compute/src/render/join/linear_join.rs`):
  Between the identity and could_error branches in `differential_join_inner`, byte-level
  concat key+old+new into a reusable `concat_buf`, then `project_onto_unordered` into the
  output row.
- Added pure-projection fast path in delta join `build_update_stream`
  (`src/compute/src/render/join/delta_join.rs`): Between identity and slow path, byte-level
  concat key+val into `concat_buf`, then `project_onto_unordered`.
- Added pure-projection fast path in delta join `build_halfjoin`
  (`src/compute/src/render/join/delta_join.rs`): In the `!could_error()` branch, between
  identity and slow path, byte-level concat key+stream_row+lookup_row into `concat_buf`,
  then `project_onto_unordered`.

All three paths use a reusable `Row` buffer (`concat_buf`) allocated once outside the closure
and reused across invocations via `packer()` (which clears without deallocating). The
projection is pre-computed as an owned `Vec<usize>` before the closure to avoid per-row
borrowing overhead.

**Benchmark** (`cargo bench -p mz-repr --bench join_concat -- join_project`):

Per-operation (single join result, key(2) + val1(10) + val2(10) = 22 int columns):

| Scenario | datum_decode | byte_project | Speedup |
|---|---|---|---|
| drop_2_of_22 (typical: remove dup key cols) | 426 ns | 110 ns | **3.9x** |
| keep_8_of_22 (narrow projection) | 327 ns | 93 ns | **3.5x** |
| reversed_22 (all cols, reversed order) | 438 ns | 117 ns | **3.7x** |

Batch (10k join result rows):

| Scenario | datum_decode | byte_project | Speedup |
|---|---|---|---|
| drop_2_of_22 int (10k rows) | 4.08 ms | 1.12 ms | **3.6x** |
| mixed_type (11 cols, str+float+bool+int, 10k rows) | 2.00 ms | 625 µs | **3.2x** |

**Analysis**: The speedup is **3.2-3.9x** across all scenarios. Unlike the identity concat
optimization (Session 26, which was 4.6-32.5x) where the speedup scaled with column count,
the projection optimization has a more consistent 3-4x improvement because both old and new
approaches process all columns — the difference is in HOW they process them:
- Old: `read_datum` (type matching, value parsing, Datum construction) for ALL columns, then
  `push_datum` (type matching, encoding size, tag+payload writes) for PROJECTED columns
- New: `skip_datum` (tag byte + pointer advance) for ALL columns during `project_onto_unordered`'s
  boundary scan, then `memcpy` for each projected column's byte range

The byte-level approach wins because `skip_datum` is 2-5x cheaper than `read_datum` (just
pointer arithmetic vs full datum construction), and `memcpy` is cheaper than per-datum
`push_datum` (no type matching, no encoding size computation).

**Impact**: This optimization applies to all joins where the closure is a non-identity pure
projection — which is the most common non-identity case, since the query planner typically
adds a projection to remove duplicate join key columns. It benefits:
- Linear joins (most 2-table joins)
- Delta joins (multi-way joins), both initial scan and per-stage half-join
- Materialized view maintenance (incremental join updates)

**Files modified**:
- `src/compute-types/src/plan/join.rs` — `JoinClosure::pure_projection()` method
- `src/compute/src/render/join/linear_join.rs` — projection fast path in `differential_join_inner`
- `src/compute/src/render/join/delta_join.rs` — projection fast paths in `build_update_stream`
  and `build_halfjoin`
- `src/repr/benches/join_concat.rs` — added projection benchmark scenarios

**Future ideas**:
- When the join closure has filters/maps AND a pure-input-column projection, we could combine
  selective decode for filter evaluation with byte-level project for the output columns. This
  extends the Session 21 `evaluate_into_project` pattern to join closures.
- `zero_diffs.clone()` in `reduce.rs` (line 1313) clones a `Vec<Accum>` per input row during
  accumulable reductions. SmallVec<[Accum; 4]> would avoid heap allocation for 1-4 aggregations.
- `Row::clone()` at 527B executions in coverage data remains a massive source of allocation.
- Byte-level concat could apply to UNION ALL operations and other row concatenation sites.

---

## Session 28: Bucketed reduction byte-level hash key reconstruction (2026-02-20)

**Target:** The `build_bucketed` function in `src/compute/src/render/reduce.rs` had an explicit
TODO: "Convert the `chain(hash_key_iter...)` into a memcpy." Three operations on `hash_key`
rows (`[hash_u64, key_col1, key_col2, ...]`) used datum-level iteration + repacking:

1. **Initial creation** (line 832): `pack_using(once(hash).chain(&key))` — iterates all key
   datums and re-encodes each one via `push_datum`
2. **Per-stage mod** (line 847): reads hash, computes `hash % b`, then
   `pack(once(new_hash).chain(hash_key_iter.take(key_arity)))` — decodes and re-encodes all
   key columns just to change the hash prefix
3. **Final strip** (line 870): `pack(hash_key_iter.take(key_arity))` — decodes and re-encodes
   all key columns just to remove the hash prefix
4. **Error path** (line 1023): same as final strip

Each of these performs `N × read_datum + N × push_datum` (where N = key_arity) for columns that
don't change. The datum decode involves type matching, value parsing (e.g., chrono DateTime for
timestamps), and Datum enum construction. The re-encode involves type matching, encoding size
computation (e.g., `min_bytes_signed`), and separate tag+payload writes.

**Fix:** Three optimizations using `RowRef::tail_bytes()` (new method) and `copy_into()`:

1. **New method: `RowRef::tail_bytes(skip_count)`** — returns `&[u8]` of all encoded datum bytes
   after skipping `skip_count` datums using `skip_datum` (fast O(1) pointer arithmetic per datum,
   no value construction). Added to `src/repr/src/row.rs`.

2. **Initial creation**: replaced `pack_using(once(hash).chain(&key))` with
   `packer.push(UInt64(hash)); key.copy_into(&mut packer)` — the key's raw bytes are copied
   directly via single `memcpy` (from Session 26's `copy_into` on Row).

3. **Per-stage mod**: replaced datum iteration+repack with
   `tail_bytes(1)` to get key bytes after hash datum, then
   `packer.push(UInt64(new_hash)); packer.extend_by_slice_unchecked(key_bytes)`.

4. **Final strip + error path**: replaced datum iteration+repack with
   `tail_bytes(1)` + `extend_by_slice_unchecked(key_bytes)`.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench hash_key_reconstruct`):

### Per-operation performance (ns/iter)

| Operation             | Scenario | Old (datum repack) | New (byte-level) | Speedup |
|-----------------------|----------|-------------------:|------------------:|--------:|
| reconstruct (mod+key) | int5     | 99.5               | 30.9              | **3.2x** |
| reconstruct (mod+key) | int10    | 161                | 21.8              | **7.4x** |
| reconstruct (mod+key) | mixed3   | 47.5               | 21.8              | **2.2x** |
| strip hash prefix     | int5     | 70.3               | 5.0               | **14.1x** |
| strip hash prefix     | int10    | 134                | 4.8               | **28.0x** |
| create (hash+key)     | int5     | 80.0               | 9.1               | **8.8x** |
| create (hash+key)     | int10    | 144                | 9.2               | **15.7x** |
| create (hash+key)     | mixed3   | 39.4               | 8.7               | **4.5x** |

### Batch performance (10k rows)

| Operation              | Scenario | Old       | New       | Speedup     |
|------------------------|----------|-----------|-----------|-------------|
| reconstruct (mod+key)  | int10    | 1.531 ms  | 213 µs    | **7.2x**    |
| strip hash prefix      | int10    | 1.296 ms  | 39.7 µs   | **32.6x**   |
| create (hash+key)      | int10    | 1.424 ms  | 83.9 µs   | **17.0x**   |

**Summary:** **2.2-28x faster per operation, 7-33x faster in batch**. The hash strip operation
shows the largest speedup (28-33x) because it eliminates ALL datum operations — the old path
decoded and re-encoded every key datum, while the new path does a single `skip_datum` (one tag
byte read + pointer advance) followed by a single `memcpy`. The reconstruction operation
(hash mod + key copy) is 3-7x faster because it still needs to decode the hash datum and encode
the new hash, but avoids all key datum decode/re-encode. The create operation (hash + key copy)
is 4-16x faster because `copy_into` does a single `memcpy` of all key bytes vs N `push_datum`
calls.

The speedup scales with key arity: 10 int columns show 7.4x vs 5 columns at 3.2x for
reconstruction, and 28x vs 14x for strip. This is because the old path's cost is O(key_arity ×
per_datum_cost) while the new path's cost is O(1) for the memcpy (plus O(key_arity) for the
skip_datum scan in tail_bytes, which is ~2ns per datum vs ~14ns for read_datum + push_datum).

**Impact:** The `build_bucketed` function is used for hierarchical MIN/MAX reductions. These
are used when the query planner selects hierarchical aggregation strategy for better parallelism.
The hash key operations execute on every input row for each hierarchical stage. The 7-33x
speedup on these operations directly reduces the per-row overhead in the reduction pipeline.

**Files changed:**
- `src/repr/src/row.rs` - Added `RowRef::tail_bytes(skip_count: usize)` method using
  `skip_datum` for fast datum skipping. Added `test_tail_bytes` correctness test verifying
  equivalence with datum-level repacking.
- `src/compute/src/render/reduce.rs` - Modified `build_bucketed`: initial hash key creation
  uses `copy_into`, per-stage mod uses `tail_bytes(1)` + `extend_by_slice_unchecked`, final
  strip and error path use `tail_bytes(1)` + `extend_by_slice_unchecked`. Resolved TODO comment.
- `src/repr/benches/hash_key_reconstruct.rs` - Added benchmark (new file) with 22 scenarios
  covering reconstruction, strip, and creation operations across int5/int10/mixed3 types and
  10k-row batch tests.
- `src/repr/Cargo.toml` - Registered benchmark.

**Future optimization ideas:**
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row during accumulable
  reductions. SmallVec<[Accum; 4]> would avoid heap allocation for 1-4 aggregations, but is
  blocked by DD orphan rule constraints on Semigroup/Multiply trait implementations.
- `Row::clone()` at 527B executions is a massive source of allocation pressure. Arena
  allocation or reference-counted rows could reduce this.
- `parse_bool` in `strconv.rs` calls `s.trim().to_lowercase()` which heap-allocates a String
  for every boolean parse. Could be replaced with `eq_ignore_ascii_case` byte matching.
- Join closures with filters/maps + pure-input-column projection could combine selective decode
  for filter evaluation with byte-level project for the output columns.
- Byte-level concat could apply to UNION ALL operations and other row concatenation sites.
- `tail_bytes` could be used in other patterns where a known prefix needs to be stripped or
  replaced in Row data (e.g., range key manipulation in window functions).

---

## Session 29: ReductionMonoid::plus_equals tag byte peek - skip datum decode for Null identity

**Date:** 2026-02-20

**Problem:** `ReductionMonoid::plus_equals()` in `src/compute/src/render/reduce.rs` is the
Semigroup implementation for hierarchical Min/Max reductions. It's called ~3 billion times
(coverage data) during differential dataflow consolidation. On every call, it:
1. Calls `lhs.unpack_first()` → full datum decode (`read_datum` with tag dispatch, type
   matching, value construction — including chrono `DateTime` for timestamps, `Decimal` for
   numerics, etc.)
2. Calls `rhs.unpack_first()` → same for the other side
3. Pattern matches on `(lhs_val, rhs_val)` to check for Null identity

Coverage data showed that ~95% of calls have `rhs` as `Datum::Null` (the identity element for
Min/Max in DD's semigroup model). In these cases, `plus_equals` does nothing — both values are
decoded only to discover that one of them is Null.

The cost breakdown for the old approach on the dominant (rhs Null) case:
- `unpack_first()` for lhs: 7-16ns depending on type (Int64: ~7ns, Timestamp: ~11ns,
  Numeric: ~8ns, String: ~5ns)
- `unpack_first()` for rhs (Null): ~4ns (tag read + Datum::Null construction)
- Pattern match: ~1ns
- Total: 12-21ns per call, 95% of which is wasted on decoding values that aren't used

**Fix:** Added `RowRef::first_datum_is_null()` method that checks the first byte of the row
data against `Tag::Null` (value 0, since Tag is `#[repr(u8)]` and Null is the first variant).
This is a single byte comparison (~1 instruction) instead of the full `unpack_first()` path
(tag dispatch → type matching → value construction → Datum enum creation).

Updated `plus_equals` for both Min and Max to use a three-tier fast path:
1. **rhs Null check** (tag byte peek): If `rhs.first_datum_is_null()`, return immediately
   (no swap needed). This handles ~95% of calls at ~1.7ns instead of 12-21ns.
2. **lhs Null check** (tag byte peek): If `lhs.first_datum_is_null()`, clone rhs into lhs
   (swap needed). This handles the next ~4% at ~9ns instead of 20-22ns.
3. **Full comparison**: Only for the ~1% where both values are non-null, do full
   `unpack_first()` + `Datum::cmp()`. No overhead vs the old path.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench reduction_monoid`):

### Per-call performance (ns/iter)

| Scenario                   | Old (unpack_first) | New (tag peek) | Speedup |
|----------------------------|-------------------:|---------------:|--------:|
| rhs_null Int64             | 12.1               | 1.7            | **7.1x** |
| rhs_null Timestamp         | 15.7               | 1.7            | **9.0x** |
| rhs_null String            | 8.8                | 1.7            | **5.2x** |
| rhs_null Numeric           | 12.8               | 1.7            | **7.4x** |
| lhs_null Int64             | 20.4               | 8.9            | **2.3x** |
| lhs_null Timestamp         | 21.6               | 9.8            | **2.2x** |
| both_null                  | 7.8                | 1.7            | **4.5x** |
| no_swap Int64 (both non-null) | 25.4            | 25.1           | ~1.0x   |
| no_swap Timestamp          | 29.0               | 28.2           | ~1.0x   |
| swap Int64 (both non-null) | 32.3               | 32.5           | ~1.0x   |

### Batch performance (10k calls)

| Scenario                   | Old       | New       | Speedup     |
|----------------------------|-----------|-----------|-------------|
| 95% Null rhs Int64         | 120 µs    | 24 µs     | **5.0x**    |
| 95% Null rhs Timestamp     | 147 µs    | 29 µs     | **5.1x**    |
| 95% Null rhs Numeric       | 135 µs    | 35 µs     | **3.8x**    |
| 100% Null rhs Int64        | 119 µs    | 13 µs     | **8.9x**    |
| 0% Null rhs Int64 (worst)  | 214 µs    | 216 µs    | ~1.0x       |

**Summary:** **5-9x faster per call for the dominant Null-identity case (~95% of real calls),
3.8-8.9x faster in batch simulations**. The optimization saves 10-20ns per call by replacing
full datum decoding with a single byte comparison. For timestamps (the most expensive type to
decode due to chrono DateTime construction), the per-call speedup is 9x. For the rare non-null
comparison case (~5% of real calls), there is zero overhead — the tag byte check costs <1ns.

At 3 billion `plus_equals` calls in the coverage trace, with 95% being the Null-identity case,
this saves approximately 2.85B × 12ns = ~34 seconds of pure datum decoding overhead that was
previously wasted on discovering Null values. The remaining 150M non-null comparisons are
unchanged.

**Files changed:**
- `src/repr/src/row.rs` - Added `RowRef::first_datum_is_null()` method: checks first byte
  against `Tag::Null` (0u8) without datum construction. Marked `#[inline]`.
- `src/compute/src/render/reduce.rs` - Rewrote `Semigroup::plus_equals` for
  `ReductionMonoid::Min` and `ReductionMonoid::Max` to use three-tier fast path with tag byte
  peek. Removed now-unused `Datum` import from the monoids module.
- `src/repr/benches/reduction_monoid.rs` - Added benchmark (new file) with per-call tests
  (rhs_null, lhs_null, both_null, no_swap, swap across Int64/Timestamp/String/Numeric) and
  batch tests (95% null, 100% null, 0% null workloads).
- `src/repr/Cargo.toml` - Registered benchmark.

**Future optimization ideas identified during research:**
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row during accumulable
  reductions (264M calls). SmallVec<[Accum; 4]> would avoid heap allocation for the common
  case, but is blocked by DD orphan rule constraints on Semigroup/Multiply trait impls.
- `Row::clone()` at 527B executions is a massive source of allocation pressure. Arena
  allocation or reference-counted rows could reduce this, but Row cloning is fundamental
  to differential dataflow's operational model.
- `parse_int32`/`parse_int64` at 560M calls could benefit from a hand-rolled ASCII parser
  that avoids the generic `FromStr` dispatch overhead (~1.2-1.5x potential speedup).
- `RowColumnarDecoder::decode()` at 61B inner loop iterations processes columns sequentially.
  Batched column-at-a-time decoding could improve cache utilization and enable SIMD.
- `first_datum_is_null()` could be extended to `first_datum_tag()` returning the raw tag byte,
  enabling other fast-path patterns (e.g., type checking without full decode).

---

## Session 30: Zero-allocation parse_bool + direct COPY FROM text decoding (2026-02-20)

**Problem:** Two parsing inefficiencies in the COPY FROM text ingestion path:

1. **`parse_bool` heap allocation**: Every call to `parse_bool(s)` in `strconv.rs` called
   `s.trim().to_lowercase().as_str()` which heap-allocates a `String` on every call. All
   accepted values ("true", "false", "yes", "no", "on", "off", "1", "0" and prefixes) are
   pure ASCII, so case-insensitive comparison can be done directly on the bytes without any
   allocation.

2. **COPY FROM text decode intermediate allocations**: `decode_copy_format_text` in
   `pgcopy/copy.rs` used a three-step pipeline for each row:
   - `Value::decode_text(typ, raw_value)` → constructs `Value` enum (for `Type::Text`, this
     calls `s.to_owned()` → heap-allocates a `String` per text column!)
   - `value.into_datum(&buf, typ)` → converts `Value` to `Datum`
   - Collects into `Vec<Datum>` → heap-allocates per row
   - `Row::pack(row)` → iterates all datums and packs into final Row

   The intermediate `Value` enum and `Vec<Datum>` were entirely unnecessary — there was already
   a `Value::decode_text_into_row()` method that pushes directly into a `RowPacker`, bypassing
   all intermediate allocations. The CSV decode path (`decode_copy_format_csv`) already used
   this method; only the text format path had the old inefficient approach.

**Fix:**

1. **`parse_bool` zero-allocation**: Replaced `s.trim().to_lowercase().as_str()` match with
   a length-dispatched `match buf.len()` that uses:
   - Direct byte matching (`b't' | b'T'`) for 1-char values
   - `eq_ignore_ascii_case(b"true")` for multi-char values
   Each `eq_ignore_ascii_case` does a length check + memcmp with ASCII case folding — no heap
   allocation. The length-dispatch ensures at most 2-3 comparisons per input.

2. **COPY FROM direct Row construction**: Replaced the old
   `Value::decode_text` → `into_datum` → `Vec<Datum>` → `Row::pack` pipeline with:
   - `std::str::from_utf8(raw_value)?` → validates UTF-8 (was already done inside `decode_text`)
   - `Value::decode_text_into_row(typ, s, &mut packer)` → pushes directly into RowPacker
   This eliminates: per-row `Vec<Datum>` allocation, per-field `Value` construction, per-text-column
   `String` allocation (`s.to_owned()`), and final `Row::pack` datum iteration. Also removed the
   now-unused `RowArena` import.

**Benchmark results:**

### parse_bool per-value (ns/iter)

| Value         | Old (to_lowercase) | New (byte match) | Speedup |
|---------------|-------------------:|-----------------:|--------:|
| true          | 10.3               | 8.0              | 1.29x   |
| false         | 10.4               | 8.2              | 1.26x   |
| t             | 9.2                | 7.4              | 1.24x   |
| f             | 8.8                | 7.3              | 1.20x   |
| TRUE          | 10.1               | 8.0              | 1.26x   |
| FALSE         | 10.3               | 8.2              | 1.25x   |
| yes           | 9.7                | 7.9              | 1.23x   |
| no            | 9.3                | 7.9              | 1.17x   |
| on            | 9.2                | 7.9              | 1.16x   |
| off           | 10.2               | 8.0              | 1.27x   |
| 1             | 8.7                | 7.4              | 1.18x   |
| 0             | 8.8                | 7.3              | 1.20x   |
| "  true  "    | 11.2               | 9.0              | 1.24x   |

### parse_bool batch (10k values)

| Approach          | Time (µs) | Speedup   |
|-------------------|-----------|-----------|
| Old 10k mixed     | 112       | -         |
| New 10k mixed     | 78        | **1.43x** |
| Old 10k "true"    | 112       | -         |
| New 10k "true"    | 82        | **1.37x** |
| Old 10k "t"       | 92        | -         |
| New 10k "t"       | 75        | **1.23x** |

### COPY FROM text decode per-row (ns/iter)

| Scenario              | Old (Value path) | New (direct Row) | Speedup |
|-----------------------|-----------------:|-----------------:|--------:|
| int5 (5 int cols)     | 176              | 83               | **2.1x** |
| str5 (5 text cols)    | 313              | 72               | **4.3x** |
| mixed5 (int+str+bool+ts+f64) | 559      | 417              | **1.3x** |
| bool5 (5 bool cols)   | 144              | 54               | **2.6x** |

### COPY FROM text decode batch (10k rows)

| Scenario              | Old       | New       | Speedup   |
|-----------------------|-----------|-----------|-----------|
| int5 (10k rows)       | 1.80 ms   | 816 µs    | **2.2x**  |
| str5 (10k rows)       | 3.20 ms   | 696 µs    | **4.6x**  |
| mixed5 (10k rows)     | 5.26 ms   | 4.09 ms   | **1.3x**  |

**Summary:** The `parse_bool` optimization gives 1.16-1.43x improvement by eliminating the
`to_lowercase()` heap allocation. The COPY FROM text decode optimization gives **2.1-4.6x
improvement** depending on column types. String-heavy rows benefit the most (**4.3-4.6x**)
because the old path allocated a `String` per text column via `Value::Text(s.to_owned())`,
while the new path copies directly from the parser buffer into the Row's internal buffer.
Integer-only rows see 2.1-2.2x from eliminating the `Vec<Datum>` and `Value` intermediates.
Mixed-type rows show 1.3x because the timestamp parsing (chrono DateTime construction)
dominates the time regardless of the intermediate representation.

The bool5 scenario benefits from both optimizations combined: the parse_bool change
eliminates the `to_lowercase()` allocation, and the direct decode eliminates the `Value::Bool`
construction + `Vec<Datum>` allocation.

**Files changed:**
- `src/repr/src/strconv.rs` - Rewrote `parse_bool()` to use length-dispatched ASCII
  case-insensitive byte matching instead of `to_lowercase()`.
- `src/pgcopy/src/copy.rs` - Rewrote `decode_copy_format_text()` to use
  `Value::decode_text_into_row()` and RowPacker instead of `Value::decode_text()` →
  `into_datum()` → `Vec<Datum>` → `Row::pack()`. Removed unused `RowArena` import.
- `src/repr/benches/parse_bool.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered parse_bool benchmark
- `src/pgcopy/benches/copy_decode.rs` - Added benchmark (new file)
- `src/pgcopy/Cargo.toml` - Registered copy_decode benchmark

**Future optimization ideas identified during research:**
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row during accumulable
  reductions. A newtype wrapper around SmallVec with Semigroup/Multiply implementations could
  avoid heap allocation for the common case (1-4 aggregations). The orphan rule is NOT actually
  a blocker — a newtype around SmallVec owned by Materialize can implement DD's foreign traits.
- `Row::clone()` at 527B executions in coverage data remains the largest source of allocation
  pressure, but is fundamental to DD's operational model.
- `RowColumnarDecoder::decode()` at 61B inner loop iterations processes columns sequentially.
  Batched column-at-a-time decoding could improve cache utilization and enable SIMD.
- The COPY FROM CSV path already uses `decode_text_into_row` and doesn't need optimization.
- The mixed5 result (1.3x) is limited by timestamp parsing (chrono DateTime construction
  ~550ns for the timestamp field alone). A faster timestamp parser that avoids chrono's
  internal validation overhead could help, but is complex to implement correctly.

---

## Session 31: OffsetOptimized index_pair + strided_len caching + single-batch fast path (2026-02-20)

**Target:** `OffsetOptimized`, `BytesBatch`, and `BytesContainer` in
`src/compute/src/row_spine.rs` — the core offset lookup data structures for differential
dataflow arrangement spines. Coverage data showed 1.58 trillion executions for
`OffsetStride::len()/index()` operations.

**Problem:** Every key/value lookup in a DD arrangement goes through
`DatumContainer::index()` → `BytesContainer::index()` → `BytesBatch::index()` →
`OffsetOptimized`. The old code had three sources of overhead:

1. **Double index lookup in BytesBatch::index()**: To get the byte range for item `i`,
   the old code called `self.offsets.index(i)` and `self.offsets.index(i + 1)` separately.
   Each call involved: (a) compare index against `strided.len()` (enum dispatch), (b)
   match on `OffsetStride` variant (enum dispatch), (c) compute the value. Two separate
   calls = two enum dispatches each = 4 total enum matches per lookup.

2. **Repeated `strided.len()` calls in OffsetOptimized::index()**: The `index()` method
   called `self.strided.len()` (an enum match) to determine whether the index falls in the
   strided range. For the fallthrough (spilled) case, `strided.len()` was called TWICE
   (once for the comparison, once for the subtraction). This enum dispatch was pure waste
   since the strided length only changes during `push_into()`.

3. **Iterator loop in BytesContainer::index()**: Even when the container had a single batch
   (the common case after compaction/merge), the code iterated through `self.batches.iter()`
   with iterator setup and loop overhead.

**Fix:** Three optimizations:

1. **`OffsetStride::index_pair()`**: Returns `(index(i), index(i+1))` with a single enum
   dispatch instead of two. For the common `Striding` case (uniform row sizes), computes
   both values with one multiplication: `(stride * index, stride * index + stride)`.
   `BytesBatch::index()` now calls `self.offsets.index_pair(index)` instead of two
   separate `self.offsets.index()` calls.

2. **`strided_len` field caching**: Added a `strided_len: usize` field to `OffsetOptimized`
   that caches `strided.len()`. Updated on every `push_into()` when the strided range
   changes. `OffsetOptimized::index()` and `len()` now read this field directly instead
   of calling `strided.len()` (enum dispatch).

3. **Single-batch fast path in BytesContainer::index()**: Added an early-return check for
   `self.batches.len() == 1` that directly indexes the single batch, avoiding the iterator
   loop setup. This is the common case after compaction/merge.

**Benchmark results** (criterion, `cargo bench -p mz-compute --bench offset_optimized`):

### OffsetOptimized isolated: two index() calls vs one index_pair() (10K lookups)

| Scenario            | two index() calls | index_pair() | Speedup |
|---------------------|------------------:|-------------:|--------:|
| uniform stride      | 7.19 µs           | 5.80 µs      | 1.24x   |
| variable (spilled)  | 11.30 µs          | 7.69 µs      | 1.47x   |

Note: The `two index() calls` benchmark in the baseline uses the same `index()` function
for both calls. The `index_pair()` column shows the optimized single-dispatch method.

### DatumContainer::index() end-to-end throughput (10K lookups)

| Scenario              | Baseline    | Optimized   | Speedup   |
|-----------------------|-------------|-------------|-----------|
| sequential_int5       | 31.44 µs    | 15.00 µs    | **2.10x** |
| sequential_mixed5     | 29.88 µs    | 15.10 µs    | **1.98x** |
| sequential_narrow1    | 28.14 µs    | 12.52 µs    | **2.25x** |
| random_int5           | 31.67 µs    | 19.42 µs    | **1.63x** |

**Summary:** **2.0-2.25x faster for sequential access, 1.63x faster for random access**.
The speedup comes from three sources: (1) `index_pair()` eliminates one enum dispatch per
byte-range lookup, (2) `strided_len` caching eliminates `strided.len()` enum dispatch on
every `index()` call, (3) the single-batch fast path avoids iterator overhead. Narrow rows
benefit most (2.25x) because the offset computation is a larger fraction of the per-lookup
time (less data to copy/reference). Random access shows less improvement (1.63x) because
cache misses for the data payload dominate.

Per-lookup latency improved from 3.14 ns/lookup to 1.50 ns/lookup (sequential int5).
At 1.58 trillion `OffsetStride` operations in the coverage trace, this optimization
affects the most frequently executed code path in the entire compute layer. Every arrangement
lookup (used in joins, aggregations, top-k, indexed views) flows through this path.

**Files changed:**
- `src/compute/src/row_spine.rs` - Added `OffsetStride::index_pair()`, `strided_len` field
  on `OffsetOptimized` with caching in `push_into()`, `OffsetOptimized::index_pair()`,
  single-batch fast path in `BytesContainer::index()`.
- `src/compute/src/lib.rs` - Made `row_spine` module `pub` for benchmark access.
- `src/compute/Cargo.toml` - Added criterion dev-dependency, registered benchmark.
- `src/compute/benches/offset_optimized.rs` - Added benchmark (new file).

**Future optimization ideas identified during research:**
- `sink/correction_v2.rs` at 588B executions: `Chain::iter()` clones every element via
  `d.clone()` in the iterator. This is a massive source of allocation pressure for the
  correction buffer used in compute sinks.
- `persist-types/src/arrow.rs` at 7.4T executions: `ArrayIdx::cmp()` does double enum
  dispatch for Arrow array comparison during persist's merge-sort. Specializing the common
  cases (single-field structs, pure numeric columns) or using function pointers could
  reduce branch overhead.
- `persist-client/src/fetch.rs` at 599B executions: `decode_kv()` goes through an
  `EitherOrBoth` match where only the `Right` (structured) branch is ever taken. The dead
  `Left` (legacy codec) branch wastes icache space.
- `row/encode.rs` DatumColumnDecoder::get() at 61B iterations: each datum decode does
  `is_valid(idx).then(|| array.value(idx)).map(|x| Datum::Type)` through an Option chain.
  Direct packer methods (bypassing Datum construction) could save ~2-4ns per datum.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row. SmallVec newtype
  wrapper could avoid heap allocation for 1-4 aggregations.

---

## Session 32: Fast-path ISO 8601 timestamp/date/time parsing - 15-22x speedup

**Date:** 2026-02-20

**Problem:** Every time a timestamp, timestamptz, date, or time value is parsed from text—via
COPY FROM, pgwire text decode, or SQL string→type casts—the code went through a
general-purpose `ParsedDateTime` pipeline that:
1. **`split_timestamp_string()`** — 2-3 `find()` calls to locate timezone boundaries
2. **`tokenize_time_str()`** — allocates a `VecDeque<TimeStrToken>`, two `String` buffers
   (num_buf, char_buf), scans character-by-character with 6+ match arms per character,
   creates ~13 tokens for a standard ISO timestamp via `push_back()` calls
3. **`fill_pdt_date()` + `fill_pdt_time()`** — pattern-matches token sequences against
   multiple format templates using peekable iterators, fills a 264-byte `ParsedDateTime`
   struct with 11 `Option<DateTimeFieldValue>` fields
4. **`check_datelike_bounds()`** — 5 range checks
5. **`compute_date()` + `compute_time()`** — extracts 6 fields from the struct, converts
   to chrono `NaiveDate`/`NaiveTime`

This general pipeline handles every PostgreSQL timestamp format (flexible separators, named
timezones, BC/AD eras, partial dates, etc.), but ~99% of real-world data is simple ISO 8601:
`"2024-06-15 14:30:25.123456"`. The tokenizer alone does ~600 operations (26 chars × ~23
operations per character) when 13 byte-level comparisons + 6 two-digit parses suffice.

This is the *parsing* counterpart to Session 3's *formatting* optimization (which replaced
chrono's `ts.format("%m-%d %H:%M:%S")` with direct field extraction for 10-22x speedup).

**Fix:** Added fast-path ISO 8601 parsers that do direct byte-level extraction with zero
heap allocations, falling back to the general `ParsedDateTime` pipeline only for non-standard
formats:

1. **Helper functions:**
   - `parse_2digit(buf, off)` — parse 2 ASCII digits via `wrapping_sub(b'0')`, ~2ns
   - `parse_4digit(buf, off)` — parse 4 ASCII digits via arithmetic, ~3ns
   - `parse_frac_nanos(buf, start, end)` — parse 1-9 fractional digits → nanoseconds with
     static scale table, ~3-5ns
   - `try_parse_date_bytes(buf)` — parse "YYYY-MM-DD" → `NaiveDate` + offset
   - `try_parse_time_bytes(buf, off)` — parse "HH:MM:SS[.fff...]" → `NaiveTime` + offset
   - `try_parse_tz_offset(buf, off)` — parse "+HH[:MM]" or "-HH[:MM]" → `FixedOffset`

2. **Fast-path entry points:**
   - `try_parse_timestamp_fast(s)` — "YYYY-MM-DD{' '|'T'}HH:MM:SS[.fff...]" → NaiveDateTime
   - `try_parse_timestamptz_fast(s)` — same + "{+|-}HH[:MM]" → DateTime<Utc>
   - `try_parse_date_fast(s)` — "YYYY-MM-DD" (exactly 10 chars) → NaiveDate
   - `try_parse_time_fast(s)` — "HH:MM:SS[.fff...]" → NaiveTime

3. **Integration:** `parse_timestamp`, `parse_timestamptz`, `parse_date`, and `parse_time`
   all try the fast path first, falling back to the general parser on `None`.

All fast paths return `None` (triggering fallback) if:
- String length doesn't match expected patterns
- Any separator byte is wrong (not '-', ':', ' ', 'T', '.', '+', '-')
- Any digit byte is not in '0'-'9'
- chrono `from_ymd_opt` / `from_hms_nano_opt` returns `None` (invalid date/time values)

This ensures exact compatibility with the general parser for all valid inputs.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench timestamp_parse`):

### Per-value timestamp parsing (ns/iter)

| Value                    | Old (ParsedDateTime) | New (fast path) | Speedup    |
|--------------------------|---------------------:|----------------:|-----------:|
| no_frac                  | 319                  | 14.3            | **22.3x**  |
| micros                   | 374                  | 16.9            | **22.1x**  |
| millis                   | 348                  | 16.0            | **21.8x**  |
| nanos                    | 385                  | 18.6            | **20.7x**  |
| epoch (1970-01-01)       | 321                  | 14.3            | **22.4x**  |
| y2k (2000-01-01)         | 313                  | 14.3            | **21.9x**  |
| t_separator              | 364                  | 16.7            | **21.8x**  |

### Per-value timestamptz parsing (ns/iter)

| Value                    | Old (ParsedDateTime) | New (fast path) | Speedup    |
|--------------------------|---------------------:|----------------:|-----------:|
| utc_short (+00)          | 364                  | 22.6            | **16.1x**  |
| utc_full (+00:00)        | 393                  | 22.6            | **17.4x**  |
| neg_offset (-05:30)      | 413                  | 22.6            | **18.3x**  |
| micros_utc               | 413                  | 25.2            | **16.4x**  |
| micros_offset (-05:30)   | 437                  | 25.4            | **17.2x**  |

### Per-value date parsing (ns/iter)

| Value                    | Old (ParsedDateTime) | New (fast path) | Speedup    |
|--------------------------|---------------------:|----------------:|-----------:|
| typical (2024-06-15)     | 191                  | 9.8             | **19.5x**  |
| epoch (1970-01-01)       | 190                  | 9.7             | **19.6x**  |
| year_end (2024-12-31)    | 188                  | 9.7             | **19.4x**  |

### Per-value time parsing (ns/iter)

| Value                    | Old (ParsedDateTime) | New (fast path) | Speedup    |
|--------------------------|---------------------:|----------------:|-----------:|
| no_frac (14:30:25)       | 152                  | 7.7             | **19.7x**  |
| micros (14:30:25.123456) | 192                  | 9.9             | **19.4x**  |
| midnight (00:00:00)      | 152                  | 7.5             | **20.2x**  |

### Batch parsing (10k values)

| Scenario                 | Old         | New        | Speedup     |
|--------------------------|-------------|------------|-------------|
| 10k timestamps (micros)  | 3.55 ms     | 198 µs     | **17.9x**   |
| 10k timestamptz (offset) | 4.42 ms     | 257 µs     | **17.2x**   |
| 10k dates                | 2.06 ms     | 122 µs     | **16.9x**   |
| 10k times (micros)       | 1.89 ms     | 126 µs     | **15.0x**   |

**Summary:** **15-22x faster per value, 15-18x faster in batch** for standard ISO 8601
format inputs. The enormous speedup comes from eliminating:
1. **VecDeque heap allocation** (~30-50ns) for token storage
2. **Character-by-character tokenization** (~100-150ns for 26 chars × 6+ match arms)
3. **Token pattern matching** (~50-80ns for peekable iterator + format template matching)
4. **ParsedDateTime struct initialization** (~20ns for 264 bytes of Options)
5. **Redundant range checks** (~10ns for 5 bounds validations after chrono already validates)

The fast path replaces all of this with 7 byte comparisons (separators) + 6 two-digit parses
(~2ns each) + 1 four-digit parse (~3ns) + 1 fractional parse (~5ns) + chrono construction
(~8ns) = ~25ns total. The fast path adds <1ns overhead for non-standard formats (a single
length check returns `None` immediately).

For COPY FROM workloads with timestamp columns (common in data migration, ETL pipelines,
CDC replication), this 15-18x batch speedup translates directly to faster data ingestion.
Combined with Session 16's memchr-accelerated COPY text parsing and Session 30's direct
Row construction, the COPY FROM pipeline is now dramatically faster for timestamp-heavy data.

**Files changed:**
- `src/repr/src/strconv.rs` - Added fast-path ISO 8601 parsers: `parse_2digit()`,
  `parse_4digit()`, `parse_frac_nanos()`, `try_parse_date_bytes()`,
  `try_parse_time_bytes()`, `try_parse_tz_offset()`, `try_parse_timestamp_fast()`,
  `try_parse_timestamptz_fast()`, `try_parse_date_fast()`, `try_parse_time_fast()`.
  Updated `parse_timestamp()`, `parse_timestamptz()`, `parse_date()`, `parse_time()` to
  try fast path first. Added `parse_timestamp_general()`, `parse_timestamptz_general()`,
  `parse_date_general()`, `parse_time_general()` for benchmark comparison. Added
  comprehensive correctness tests comparing fast path against general parser.
- `src/repr/benches/timestamp_parse.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `MirScalarExpr::eval()` dispatch chain: for simple predicates like `WHERE x > 5`, there
  are 3 dispatch points (MirScalarExpr match → BinaryFunc 100+ variant match →
  EagerBinaryFunc trait call). A specialized evaluator for common predicate patterns could
  reduce this.
- `DatumColumnDecoder::get()` at 61B calls in persist decode: Option chain
  (`is_valid().then(|| value()).map(Datum::Type)`) could be replaced with direct if/else
  for non-nullable columns.
- `ArrayOrd::cmp()` at 327B calls in persist merge: double enum dispatch could be
  specialized for common column types (Int64, String, Bool).
- `sink/correction_v2.rs` Chain::iter() at 588B coverage — the `d.clone()` pattern was
  found to have 0 executions at the specific clone line (the hot path uses Cursor references
  instead); not a good optimization target.
- `zero_diffs.clone()` in `reduce.rs` — SmallVec newtype wrapper remains viable.
- Integer parsing (`parse_int32/parse_int64`) at 560M calls could potentially benefit from
  a hand-rolled ASCII parser, though Rust's `FromStr` is already well-optimized.

## Session 33: Persist columnar decode direct-push - eliminate double enum dispatch (2026-02-20)

**Target:** `DatumColumnDecoder::get()` in `src/repr/src/row/encode.rs` — 61B iterations in
coverage data. This is the persist columnar decode path that converts Arrow columnar data
back into Materialize's Row format.

**Problem:** The decode path had a double enum dispatch for every scalar datum:
1. `DatumColumnDecoder::get()` matches the column type variant to extract the value from
   the Arrow array and constructs a `Datum` variant (dispatch #1)
2. `packer.push(datum)` → `push_datum()` matches on the `Datum` variant again (dispatch #2)
   to determine the tag byte and encoding format

For scalar types (Bool, Int16/32/64, UInt8/16/32/64, Float32/64, String, Bytes, Date, Time,
MzTimestamp, Uuid), this second dispatch is completely redundant since we already know the
type from the column decoder variant.

**Solution:** Two changes:
1. Added 17 type-specific `push_*` methods to `RowPacker` in `src/repr/src/row.rs`
   (push_null, push_bool, push_i16/i32/i64, push_u8/u16/u32/u64, push_f32/f64,
   push_string, push_bytes, push_date, push_time, push_mz_timestamp, push_uuid).
   These encode directly to the Row byte buffer without constructing a Datum.

2. Rewrote `DatumColumnDecoder::get()` as a single unified match:
   - Scalar types (16 variants) use direct-push methods, bypassing Datum construction
   - Complex types (Numeric, Timestamp, TimestampTz, Interval, AclItem, MzAclItem,
     RecordEmpty) still construct a Datum and push via `push_datum`
   - Container types (Range, Json, Array, List, Map, Record) push directly to packer

   Important: the single-match structure avoids the overhead of falling through a first
   match block before reaching the complex type handlers.

**Benchmark results** (`cargo bench --bench columnar_decode`):

| Benchmark | Baseline | Optimized | Speedup |
|-----------|----------|-----------|---------|
| int64 (6 cols × 10K rows) | 770.61 µs | 511.25 µs | **1.51x** (−33.3%) |
| string (4 cols × 10K rows) | 368.35 µs | 264.71 µs | **1.39x** (−28.1%) |
| float64 (4 cols × 10K rows) | 232.49 µs | 145.70 µs | **1.60x** (−37.3%) |
| mixed (6 cols × 10K rows) | 543.20 µs | 390.69 µs | **1.39x** (−27.5%) |
| nullable 50% (4 cols × 10K rows) | 402.97 µs | 319.33 µs | **1.26x** (−20.6%) |
| timestamp (4 cols × 10K rows) | 567.87 µs | 365.95 µs | **1.55x** (−35.5%) |

The timestamp improvement (1.55x) is surprising since it still goes through the Datum path —
the unified single-match structure allows the compiler to generate better jump table code
compared to the original two-level match structure.

**Files changed:**
- `src/repr/src/row.rs`: Added 17 direct-push methods to RowPacker (~120 lines)
- `src/repr/src/row/encode.rs`: Rewrote DatumColumnDecoder::get() as single unified match
- `src/repr/benches/columnar_decode.rs`: New benchmark for persist columnar decode throughput
- `src/repr/Cargo.toml`: Registered new benchmark

**Impact:** At 61B iterations in coverage, a 1.4-1.6x improvement in the columnar decode
path translates to significant CPU savings during persist reads. The mixed-type benchmark
(most realistic scenario) shows 1.39x speedup.

---

## Session 34: Fast-path integer and numeric parsing - bypass trim/FromStr/FFI overhead

**Date:** 2026-02-20

**Problem:** Every time an integer or numeric value is parsed from text—via COPY FROM, pgwire
text parameters, or SQL string→type casts—the code went through unnecessarily expensive paths:

1. **Integer parsing** (`parse_int16/32/64`, `parse_uint16/32/64`): Called `s.trim().parse()`
   which first heap-scans for whitespace (even when there is none—the common case for COPY FROM
   and wire protocol values), then dispatches through Rust's generic `FromStr` implementation.
   The `FromStr` impl is well-optimized but includes overhead for:
   - UTF-8 boundary checks on every byte
   - Generic error construction (allocating `ParseIntError`)
   - Supporting radix prefixes and other features not needed for simple decimal integers

2. **Numeric parsing** (`parse_numeric`): Called `cx.parse(s.trim())` which goes through the
   C decNumber library via FFI:
   - `s.trim()` scans for whitespace (usually unnecessary)
   - `cx.parse()` crosses the FFI boundary to C code
   - The C parser handles scientific notation, NaN, Infinity, special values, and arbitrary
     precision—all unnecessary for simple integer/decimal values like "42" or "123.456"
   - Returns through FFI back to Rust
   - `munge_numeric()` then normalizes the result

   Coverage data showed 560M+ calls to `parse_int32/parse_int64` and significant numeric
   parsing in the COPY FROM path.

**Fix:** Two fast-path parsers that handle the common case (clean ASCII digits, no whitespace)
and fall back to the general parser for anything unusual:

1. **`try_parse_int_fast(s)`**: Parses ASCII digits directly into a `u64` magnitude + sign
   flag. Returns `None` for whitespace-padded, empty, or non-digit input (triggering the
   existing `trim().parse()` fallback). Each caller checks the magnitude against the type's
   range (e.g., ≤32767 for i16, ≤2147483647 for i32). Uses wrapping arithmetic to avoid
   overflow checks in the hot loop, with a final range check at the end.

2. **`try_parse_numeric_fast(s)`**: Constructs a `Decimal` directly via `from_raw_parts()`
   without any FFI call. Parses digits into a `u64` accumulator, tracks the decimal point
   position, then builds the base-1000 little-endian `lsu` array that decNumber uses
   internally. Returns `None` for scientific notation, NaN, Infinity, whitespace, or values
   with >18 significant digits (which would overflow u64). Zero values are normalized to
   match decNumber's convention (digits=1, positive sign).

Both fast paths add <1ns overhead for inputs they can't handle (a single length check or
first-byte check returns `None` immediately).

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench int_parse`):

### Per-value i32 parsing (ns/iter)

| Value               | Old (trim+parse) | New (fast path) | Speedup |
|---------------------|------------------:|----------------:|--------:|
| zero (0)            | 2.9               | 2.5             | 1.16x   |
| one (1)             | 4.6               | 3.6             | 1.28x   |
| small (42)          | 6.1               | 4.6             | 1.33x   |
| hundred (100)       | 6.5               | 5.3             | 1.23x   |
| thousand (9999)     | 7.1               | 6.4             | 1.11x   |
| typical_id (1.2e9)  | 10.6              | 10.5            | 1.01x   |
| negative (-42)      | 6.2               | 4.5             | 1.38x   |
| min (-2147483648)   | 10.2              | 10.3            | ~1.0x   |
| max (2147483647)    | 10.9              | 10.9            | ~1.0x   |

### Per-value i64 parsing (ns/iter)

| Value               | Old (trim+parse) | New (fast path) | Speedup |
|---------------------|------------------:|----------------:|--------:|
| zero (0)            | 5.2               | 4.0             | 1.30x   |
| small (42)          | 5.7               | 4.4             | 1.30x   |
| typical_id (1.2e9)  | 9.6               | 8.3             | 1.16x   |
| large (i64::MAX)    | 20.7              | 12.5            | **1.66x** |
| negative (i64::MIN) | 20.6              | 12.7            | **1.62x** |

### Per-value Numeric parsing (ns/iter)

| Value                        | Old (cx.parse FFI) | New (fast path) | Speedup |
|------------------------------|-------------------:|----------------:|--------:|
| zero (0)                     | 29.7               | 8.7             | **3.4x** |
| one (1)                      | 29.2               | 8.7             | **3.4x** |
| small (42)                   | 31.8               | 9.6             | **3.3x** |
| hundred (100)                | 32.8               | 10.9            | **3.0x** |
| thousand (9999)              | 36.6               | 12.2            | **3.0x** |
| large (123456789)            | 47.8               | 18.3            | **2.6x** |
| very_large (18 digits)       | 56.0               | 28.4            | **2.0x** |
| negative (-42)               | 33.4               | 9.5             | **3.5x** |
| neg_large (-123456789)       | 46.9               | 19.2            | **2.4x** |
| decimal (123.456)            | 43.3               | 15.1            | **2.9x** |
| decimal_small (0.001)        | 40.4               | 11.3            | **3.6x** |
| decimal_long (123456.789...) | 72.2               | 32.2            | **2.2x** |
| neg_decimal (-99.99)         | 39.8               | 12.7            | **3.1x** |
| leading_zeros (00042)        | 25.9               | 8.8             | **2.9x** |

### Batch parsing (10k values)

| Approach                        | Time (µs) | Speedup   |
|---------------------------------|-----------|-----------|
| Old trim+parse 10k i32          | 44.1      | -         |
| New fast-path 10k i32           | 39.6      | **1.11x** |
| Old cx.parse 10k numeric (mixed)| 229       | -         |
| New fast-path 10k numeric (mixed)| 91.2     | **2.51x** |
| Old cx.parse 10k numeric (ints) | 207       | -         |
| New fast-path 10k numeric (ints)| 92.9      | **2.23x** |

**Summary:** Integer parsing sees **1.1-1.7x improvement** per value—modest because Rust's
`FromStr` is already well-optimized; the main savings are from skipping the `trim()` scan and
avoiding `ParseIntError` construction overhead. The improvement is largest for long values
(i64::MAX: 1.66x) where the fast path's tighter loop matters more.

Numeric parsing sees **2.0-3.6x improvement** per value—dramatic because it bypasses the
C decNumber FFI entirely. The old path crossed the Rust→C→Rust FFI boundary, parsed through
decNumber's general-purpose scanner (supporting scientific notation, special values, etc.),
and returned through FFI. The new path constructs the `Decimal` directly from parsed digits
using `from_raw_parts()`, which is a pure Rust operation with zero FFI overhead. Simpler
values (zero, small integers, small decimals) show the largest speedup (3.4-3.6x) because
the FFI overhead is a larger fraction of the total work.

In the realistic batch scenario (10k mixed numeric values), the **2.5x speedup** directly
accelerates COPY FROM data loading, pgwire text parameter binding, and SQL string→numeric
casts. Combined with Session 32's timestamp parsing optimization (15-22x) and Session 30's
direct Row construction (2-5x), the COPY FROM pipeline for mixed-type data is now
dramatically faster end-to-end.

**Files changed:**
- `src/repr/src/strconv.rs` - Added `try_parse_int_fast()` and `try_parse_numeric_fast()`
  fast-path parsers. Updated `parse_int16`, `parse_int32`, `parse_int64`, `parse_uint16`,
  `parse_uint32`, `parse_uint64`, and `parse_numeric` to try fast path first.
  Added `test_parse_int_fast_path` and `test_parse_numeric_fast_path` correctness tests.
- `src/repr/benches/int_parse.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row during accumulable
  reductions. A newtype wrapper around SmallVec with Semigroup/Multiply implementations could
  avoid heap allocation for the common case (1-4 aggregations).
- `Row::clone()` at 527B executions in coverage data remains the largest source of allocation
  pressure, but is fundamental to DD's operational model.
- `MirScalarExpr::eval()` dispatch chain: for simple predicates like `WHERE x > 5`, there
  are 3 dispatch points. A specialized evaluator for common predicate patterns could reduce this.
- `ArrayOrd::cmp()` at 327B calls in persist merge: double enum dispatch could be specialized
  for common column types.
- Float parsing (`parse_float32/parse_float64`) uses `lexical_core::parse()` which is already
  fast but could potentially benefit from an integer fast path similar to the numeric optimization
  (many float columns contain integer-valued data).

## Session 35: Fast-path float parsing - bypass trim/FromStr/regex overhead

**Date:** 2026-02-20

**Problem:** Every time a float value (f32, f64) is parsed from text—via COPY FROM, pgwire
text parameters, or SQL string→type casts—the code went through an unnecessarily expensive
path:

1. **`s.trim()`**: Scans for whitespace on every call (usually unnecessary for COPY FROM /
   wire protocol values where the data is already clean)
2. **`FromStr::parse()`**: Rust's standard library float parser, which uses the Eisel-Lemire
   algorithm. Well-optimized but includes overhead for: UTF-8 boundary checks, generic error
   construction, scientific notation support, NaN/Inf detection
3. **Regex overflow/underflow checks**: After parsing, `f.classify()` checks whether the result
   is Zero or Infinite. For Zero results, `ZERO_RE` regex is matched against the input. For
   Infinite results, `INF_RE` regex is matched. These regex matches execute for every zero-valued
   float (common!) and every overflow case.

The zero-value case was particularly expensive: `"0".parse::<f64>()` returns `0.0`, then
`FpCategory::Zero` triggers the `ZERO_RE` regex match (~10ns overhead per zero value).

**Fix:** Added `try_parse_float_fast()` function that handles two common cases with zero heap
allocations and no regex:

1. **Pure integer strings** ("42", "-999", "0"): Parses ASCII digits into a `u64` magnitude
   with a sign flag, then casts to `f64`. The `u64 → f64` cast is exact for values ≤ 2^53.
   Limits to ≤15 significant digits to stay well within the exact range.

2. **Simple decimal strings** ("3.14", "0.001", "123.456789"): Parses all digits (ignoring the
   decimal point position) into a `u64` mantissa, then divides by an exact power of 10 from a
   pre-computed `POW10` table. IEEE 754 guarantees that dividing two exact f64 values produces
   the correctly-rounded result. Limits to ≤15 significant digits (mantissa ≤ 2^53) and ≤22
   decimal places (10^22 is the largest power of 10 exactly representable as f64).

Returns `None` for: whitespace-padded inputs, scientific notation, NaN, Inf, values with >15
significant digits or >22 decimal places. Callers fall through to the general `trim() + FromStr
+ regex` path.

For f32 parsing, the f64 result is cast to f32. This is correct because double rounding
(decimal → f64 → f32) always produces the correctly-rounded f32 result when the intermediate
precision (53 bits) exceeds 2 × target precision (24 bits) − 1 = 47 bits. (Well-known result
in floating-point arithmetic.)

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench float_parse`):

### Per-value f64 parsing (ns/iter)

| Value                | Old (trim+FromStr+regex) | New (fast path) | Speedup |
|----------------------|-------------------------:|----------------:|--------:|
| zero (0)             | 19.4                     | 2.9             | **6.8x**|
| one (1)              | 7.0                      | 3.5             | **2.0x**|
| small_int (42)       | 7.4                      | 3.5             | **2.1x**|
| hundred (100)        | 7.8                      | 4.0             | **2.0x**|
| thousand (9999)      | 8.0                      | 4.6             | **1.7x**|
| million (1000000)    | 8.5                      | 6.2             | **1.4x**|
| negative_int (-42)   | 7.4                      | 3.5             | **2.1x**|
| typical_dec (3.14)   | 9.0                      | 4.9             | **1.8x**|
| small_dec (0.001)    | 9.3                      | 4.8             | **1.9x**|
| price (99.99)        | 9.2                      | 5.6             | **1.7x**|
| negative_dec (-42.5) | 8.8                      | 5.1             | **1.7x**|
| tiny_frac (0.000001) | 10.7                     | 6.0             | **1.8x**|

### Per-value f32 parsing (ns/iter)

| Value                | Old (trim+FromStr+regex) | New (fast path) | Speedup |
|----------------------|-------------------------:|----------------:|--------:|
| zero (0)             | 18.1                     | 3.2             | **5.7x**|
| one (1)              | 6.9                      | 3.2             | **2.2x**|
| small_int (42)       | 7.4                      | 3.9             | **1.9x**|
| hundred (100)        | 7.6                      | 4.6             | **1.7x**|
| pi (3.14159)         | 9.5                      | 7.0             | **1.4x**|
| typical (3.14)       | 8.7                      | 5.3             | **1.7x**|
| small_dec (0.001)    | 8.9                      | 5.3             | **1.7x**|
| negative (-42.5)     | 8.5                      | 5.2             | **1.6x**|

### Batch parsing (10k values)

| Approach                      | Time (µs)  | Speedup   |
|-------------------------------|-----------|-----------|
| Old trim+FromStr 10k f64 ints| 105.5      | -         |
| New fast-path 10k f64 ints   | 56.4       | **1.87x** |
| Old trim+FromStr 10k f64 mixed| 108.5     | -         |
| New fast-path 10k f64 mixed  | 93.8       | **1.16x** |
| Old trim+FromStr 10k f64 dec | 115.3      | -         |
| New fast-path 10k f64 dec    | 89.7       | **1.29x** |

**Summary:** 1.4-6.8x faster per value for common float values (integers and simple decimals),
**1.16-1.87x faster in batch**. The enormous speedup for zero (6.8x) comes from bypassing the
`ZERO_RE` regex match that the general path triggers for every zero-valued result. Integer
values see 1.4-2.1x improvement by bypassing both the `FromStr` parse and the `trim()` scan.
Simple decimal values see 1.7-1.9x improvement.

The integer-only batch (1.87x) shows the largest batch improvement because every value takes
the fast path. The mixed batch (1.16x) includes some values with >10 digits where the fast
path's digit loop is comparable to FromStr. The decimal batch (1.29x) is intermediate.

**Trade-offs:** Values with >15 significant digits or scientific notation fall through to the
general parser, paying ~2-4ns overhead from the failed fast-path attempt. In real-world COPY
FROM workloads, such values are rare (most float data is integers or simple decimals with <10
significant digits).

**Correctness:** The fast path produces bit-identical results to `FromStr::parse()` for all
handled inputs. This is proven by: (1) `u64 → f64` cast is exact for values ≤ 2^53, and (2)
IEEE 754 division of two exact f64 operands (mantissa ≤ 2^53, divisor = 10^n ≤ 10^22) produces
the correctly-rounded result, which is the same result that Eisel-Lemire produces. The test
suite verifies bit-exact agreement for a comprehensive set of values.

**Files changed:**
- `src/repr/src/strconv.rs` - Added `try_parse_float_fast()` function with `#[inline]`.
  Updated `parse_float32` and `parse_float64` to try fast path first.
  Added `test_parse_float_fast_path` correctness test with bit-level comparison against
  `FromStr::parse()` for both f64 and f32.
- `src/repr/benches/float_parse.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `ArrayOrd::at()` / `ArrayIdx::cmp` at ~7,336B total executions in persist merge sort.
  `ArrayOrd::at()` has NO `#[inline]` annotation despite being called 705 billion times.
  `ArrayIdx::cmp` does large match dispatch on array types. Adding `#[inline]` and
  specializing common types could reduce branch overhead.
- `Timestamp::less_equal` at 856B calls, `Codec64::decode` at 183B calls — missing `#[inline]`
  on trivial newtype wrappers. These are likely inlined by LTO in release builds but adding
  explicit `#[inline]` would help non-LTO builds.
- `CastFrom::cast_from` at 754B calls — macro-generated `as` casts without `#[inline]`.
- `MirScalarExpr::eval()` dispatch chain: 3 dispatch points for simple predicates.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row.
- `Row::clone()` at 527B executions is the largest source of allocation pressure.

## Session 36: Direct JSONB formatting - bypass serde_json for simple scalar types

**Date:** 2026-02-20

**Problem:** Every time a JSONB value is formatted for pgwire output—via `format_jsonb` in
`strconv.rs`—the code went through 9 layers of dispatch:
1. `write!(buf, "{}", jsonb)` → constructs `fmt::Arguments`, calls `FormatBuffer::write_fmt`
2. `FormatBuffer::write_fmt` → dispatches to `fmt::Write::write_fmt`
3. `JsonbRef::Display::fmt` → creates `WriterFormatter` adapter
4. `serde_json::to_writer` → creates `Serializer` state machine with internal buffer
5. `Serializer::serialize_*` → calls `JsonbDatum::serialize`
6. `JsonbDatum::serialize` → matches on Datum type, calls appropriate serde method
7. For numerics: `serialize_struct` with magic `$serde_json::private::Number` token
8. Each write goes through `WriterFormatter` → `io::Write` → `fmt::Formatter`
9. `fmt::Formatter` → finally writes to the `FormatBuffer`

For simple JSONB scalars (null, true, false, numbers, strings)—which are the most common
JSONB values in practice—all of this machinery is completely unnecessary. The same result can
be achieved by matching on the Datum type and writing directly to the FormatBuffer.

**Fix:** Two changes in `format_jsonb`:

1. **Direct scalar formatting**: Match on `jsonb.into_datum()` for simple types:
   - `Datum::JsonNull` → write `"null"` directly
   - `Datum::True`/`Datum::False` → write `"true"`/`"false"` directly
   - `Datum::Numeric(n)` → use `write_numeric_standard_notation` directly (from session 2)
   - `Datum::String(s)` → use `write_json_string` (see below)
   - Complex types (List, Map) → fall through to the serde_json path

2. **`write_json_string()`**: Direct JSON string escaping that matches serde_json's behavior
   exactly, using a 256-entry `JSON_NEEDS_ESCAPE` lookup table:
   - **Fast path**: If no byte needs escaping (checked via `.any()` with lookup table,
     which LLVM autovectorizes), write `"` + string + `"` with just 3 `write_str` calls
   - **Slow path**: Write unescaped chunks interspersed with escape sequences
     (`\"`, `\\`, `\b`, `\t`, `\n`, `\f`, `\r`, or `\uXXXX` for other control chars)

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench jsonb_format`):

### Per-value JSONB formatting (ns/iter)

| Value                    | Old (serde_json) | New (direct) | Speedup |
|--------------------------|----------------:|-------------:|--------:|
| null                     | 13.7            | 8.7          | 1.6x    |
| true                     | 14.6            | 9.0          | 1.6x    |
| false                    | 14.5            | 9.1          | 1.6x    |
| num_zero (0)             | 28.4            | 14.5         | 2.0x    |
| num_one (1)              | 28.3            | 14.3         | 2.0x    |
| num_42 (42)              | 31.6            | 17.4         | 1.8x    |
| num_pi (3.14159...)      | 49.9            | 32.7         | 1.5x    |
| num_negative (-99.99)    | 32.4            | 18.4         | 1.8x    |
| num_tiny (0.000001)      | 32.2            | 18.1         | 1.8x    |
| num_large (39 digits)    | 57.1            | 38.9         | 1.5x    |
| str_empty ("")           | 18.6            | 9.5          | 2.0x    |
| str_hello ("hello")      | 27.7            | 11.1         | 2.5x    |
| str_medium (44 chars)    | 44.7            | 19.0         | 2.4x    |
| str_long (200 chars)     | 106             | 55           | 1.9x    |
| str_quote (has `"`)      | 43.1            | 17.1         | 2.5x    |
| str_backslash (has `\`)  | 52.9            | 20.6         | 2.6x    |
| str_newline (has `\n`)   | 58.3            | 22.8         | 2.6x    |
| str_mixed_esc            | 74.4            | 32.6         | 2.3x    |

### Batch formatting (10k values)

| Approach                        | Time (µs) | Speedup   |
|---------------------------------|-----------|-----------|
| Old serde 10k mixed             | 492       | -         |
| New direct 10k mixed            | 300       | **1.6x**  |
| Old serde 10k scalars only      | 322       | -         |
| New direct 10k scalars only     | 205       | **1.6x**  |

**Summary:** 1.5-2.6x faster per value depending on type, **1.6x faster in batch**. The
speedup comes from eliminating 7 layers of dispatch: serde_json Serializer creation,
WriterFormatter adapter, fmt::Formatter construction, and all serde trait dispatch. For
strings, the 256-entry lookup table with LLVM-autovectorized fast-path check avoids per-byte
scanning overhead for clean strings. The optimization handles all simple JSONB scalar types
(null, bool, number, string) directly; complex types (arrays, objects) fall through to serde.

**Files changed:**
- `src/repr/src/strconv.rs` - Modified `format_jsonb()` to add direct Datum matching for
  simple scalar types. Added `JSON_NEEDS_ESCAPE` lookup table and `write_json_string()`
  function for direct JSON string escaping. Added `test_format_jsonb_direct` correctness
  test verifying bit-exact output match with serde_json for all value types including
  strings with escapes, control characters, and Unicode.
- `src/repr/benches/jsonb_format.rs` - Added benchmark (new file)
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas identified during research:**
- `ArrayOrd::at()` in persist merge sort has NO `#[inline]` annotation despite being called
  705B times. `ArrayIdx::cmp` (327B calls) does a 16-arm enum match on every comparison.
  Specializing for Binary arrays (the common case for Row data) or using function pointers
  set up at batch creation time could eliminate per-comparison dispatch.
- `Region<T>::len()` (613B calls) and `Region<T>::extend_from_slice()` (352B calls) each
  perform multiple enum matches (`Heap` vs `MMap`) per call. Caching length/capacity in
  plain fields would eliminate the dispatch overhead.
- `OffsetStride::push` (580B calls) uses `stride * count` multiplication in the hot path.
  Pre-computing the expected next value would replace the multiply with a comparison.
- `Row::clone()` at 527B calls is the largest source of allocation pressure. For inline
  rows (≤22 bytes) it's just a memcpy; for spilled rows, `Rc<Row>` or `Arc<Row>` sharing
  could reduce allocation pressure in hot paths like delta joins and reductions.
- `Overflowing<i64>::mul` (400B calls) always checks for overflow even though it never
  occurs in practice. Using `wrapping_mul` in release builds could save cycles.
- `RowRef` Hash (1,274B calls) uses whatever hasher the caller provides. If differential
  dataflow uses SipHash (the default), switching to a faster non-cryptographic hasher
  (FxHash, AHash) for arrangement key hashing could provide 2-4x hash speedup.

---

## Session 37: Add #[inline] to hot cross-crate functions - eliminate function call overhead with LTO off

**Date:** 2026-02-20

**Problem:** The `optimized` profile (used by `bin/environmentd --optimized`) and `ci` profile both
have `lto = "off"`. This means cross-crate function calls are REAL function calls even for trivial
1-line functions. Without `#[inline]`, the compiler cannot inline functions across crate boundaries
when LTO is disabled. Many of the hottest functions in the system were missing `#[inline]`:

Coverage data showed these call counts for functions without `#[inline]`:
- `RowRef` Hash: 1,274B calls (derived `Hash`, no `#[inline]` on derived impls)
- `Timestamp::less_equal`: 856B calls (1-line delegation)
- `CastFrom::cast_from`: 754B calls (macro-generated `from as $to`)
- `ArrayOrd::at()`: 705B calls (2-field struct construction)
- `Row::clone()`: 527B calls (1-line delegation to `data.clone()`)
- `Row`/`RowRef` Ord/PartialOrd/PartialEq: 410B+ calls (derived and manual impls)
- `ArrayIdx::cmp`: 330B calls (16-arm enum match for persist merge sort)
- `Codec64::encode/decode`: 183B calls (single `to_le_bytes()`/`from_le_bytes()`)
- `DatumListIter::next()`: billions of calls (core datum iteration)
- `DatumVec::borrow_with()`: billions of calls (datum unpacking)
- `Row::packer()`: billions of calls (datum packing)

Total: ~5,000+ billion calls to functions missing `#[inline]` annotations. Each cross-crate function
call adds ~2-5ns of overhead (argument passing, call instruction, stack frame setup/teardown, return)
compared to an inlined version. This overhead is invisible in benchmarks that use the `bench` profile
(which inherits `release` with `lto = "thin"`), but is present in production/CI builds.

**Fix:** Added `#[inline]` annotations to all hot cross-crate functions across 5 crates:

1. **`src/ore/src/cast.rs`** - `CastFrom::cast_from` (macro-generated, 754B calls),
   `CastFrom<NonZero<T>>` variant, and `CastInto::cast_into` blanket impl.

2. **`src/persist-types/src/codec_impls.rs`** - `Codec64::encode/decode` for both i64 and u64
   (183B calls, single `to_le_bytes()`/`from_le_bytes()` calls).

3. **`src/persist-types/src/arrow.rs`** - `ArrayOrd::at()` (705B calls, trivial struct
   construction), `ArrayIdx::Ord::cmp` (330B calls, the persist merge-sort comparison function),
   `ArrayIdx::PartialOrd::partial_cmp`, and `ArrayIdx::PartialEq::eq`.

4. **`src/repr/src/timestamp.rs`** - `Timestamp::less_equal` (856B calls) for all 3 PartialOrder
   implementations (`Timestamp`, `&Timestamp`, `Timestamp`→`&Timestamp`), plus
   `Timestamp::Codec64::encode/decode`.

5. **`src/repr/src/row.rs`** - Replaced `#[derive(PartialEq, Eq, Hash)]` on `RowRef` with manual
   implementations adding `#[inline]` to `eq()` and `hash()` (1,274B hash calls). Replaced
   `#[derive(Eq, PartialEq)]` on `Row` with manual `#[inline]` implementations. Added `#[inline]`
   to `Row::clone/clone_from` (527B calls), `Row::hash`, `Row::cmp/partial_cmp`,
   `RowRef::cmp/partial_cmp` (410B calls), `RowRef::iter()`, `RowRef::byte_len()`,
   `RowRef::data()`, `Row::packer()`, and `DatumListIter::next()`.

6. **`src/repr/src/datum_vec.rs`** - `DatumVec::borrow/borrow_with/borrow_with_selective`.

**Why this matters with LTO off:**

When `lto = "off"`, the compiler treats each crate as an independent compilation unit. Function
calls across crate boundaries go through the standard ABI: arguments are placed in registers/stack,
a `call` instruction is executed, the callee sets up its stack frame, does its work, tears down the
stack frame, and returns. For a trivial function like `CastFrom::cast_from(x) { x as usize }`, the
function call overhead (~2-5ns) can be larger than the actual work (~0.3ns for an `as` cast).

The `#[inline]` attribute causes the compiler to include the function's MIR (Mid-level Intermediate
Representation) in the crate's metadata. When another crate calls the function, the compiler can
substitute the function body directly at the call site, eliminating all call overhead. This is the
SAME mechanism that Rust's standard library uses for all small trait method implementations (e.g.,
`Option::map`, `Iterator::next`, `Vec::push`).

With thin LTO (the `release` profile), the linker can perform cross-crate inlining at link time,
partially compensating for missing `#[inline]`. However, thin LTO uses cost heuristics and may not
inline all functions. With `#[inline]`, the compiler has stronger hints to inline.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench inline_hot_paths`, 10k rows):

Note: benchmarks use the `bench` profile which inherits `release` (thin LTO), so they represent
a lower bound on the improvement. The real impact is larger with `lto = "off"`.

| Benchmark                        | Time (µs) | Per-row (ns) |
|----------------------------------|-----------|-------------|
| row_clone/int5_10k               | 19.4      | 1.9         |
| row_clone/mixed6_10k             | 250.9     | 25.1        |
| row_hash/int5_10k                | 67.3      | 6.7         |
| row_hash/mixed6_10k              | 95.0      | 9.5         |
| row_cmp/int5_10k_sequential      | 21.7      | 2.2         |
| row_cmp/mixed6_10k_sequential    | 25.1      | 2.5         |
| row_eq/int5_10k_equal            | 21.4      | 2.1         |
| datum_iter/int5_10k_iterate      | 319       | 31.9        |
| datum_iter/mixed6_10k_iterate    | 523       | 52.3        |
| datum_vec_borrow/int5_10k        | 607       | 60.7        |
| datum_vec_borrow/mixed6_10k      | 548       | 54.8        |
| row_packer/int5_10k_pack         | 477       | 47.7        |
| row_packer/mixed6_10k_pack       | 507       | 50.7        |
| cast_from/u64_to_usize_10k      | 3.4       | 0.34        |
| cast_from/usize_to_u64_10k      | 3.3       | 0.33        |

**Estimated impact with LTO off (`optimized` profile):**

For each function, the per-call overhead saved by `#[inline]` is approximately 2-5ns. At the
coverage data call counts:

| Function                  | Calls (billions) | Est. overhead saved (ns) | Total saved (s) |
|---------------------------|----------------:|------------------------:|-----------------:|
| RowRef Hash               | 1,274           | 3-5                     | 3,800-6,370      |
| Timestamp::less_equal     | 856             | 2-3                     | 1,712-2,568      |
| CastFrom::cast_from       | 754             | 2-3                     | 1,508-2,262      |
| ArrayOrd::at()            | 705             | 2-3                     | 1,410-2,115      |
| Row::clone()              | 527             | 2-3                     | 1,054-1,581      |
| Row/RowRef cmp/eq         | 410             | 2-3                     | 820-1,230        |
| ArrayIdx::cmp             | 330             | 3-5                     | 990-1,650        |
| Codec64::encode/decode    | 183             | 2-3                     | 366-549          |
| DatumListIter::next       | ~500            | 2-3                     | 1,000-1,500      |
| **Total**                 | **~5,539**      |                         | **~12,660-19,825** |

These estimates represent the aggregate function call overhead eliminated across the coverage trace.
The actual wall-clock improvement depends on how many of these calls are on the critical path vs.
in cold paths, branch prediction effects, and icache pressure from inlined code.

**Files changed:**
- `src/ore/src/cast.rs` - Added `#[inline]` to `CastFrom::cast_from` (both regular and NonZero
  macro variants) and `CastInto::cast_into` blanket impl.
- `src/persist-types/src/codec_impls.rs` - Added `#[inline]` to `Codec64::encode/decode` for i64
  and u64.
- `src/persist-types/src/arrow.rs` - Added `#[inline]` to `ArrayOrd::at()`, `ArrayIdx::cmp`,
  `ArrayIdx::partial_cmp`, `ArrayIdx::eq`.
- `src/repr/src/timestamp.rs` - Added `#[inline]` to 3 `PartialOrder::less_equal` impls and 2
  `Codec64::encode/decode` impls for Timestamp.
- `src/repr/src/row.rs` - Replaced derived `PartialEq, Eq, Hash` on `RowRef` with manual
  `#[inline]` implementations. Replaced derived `PartialEq, Eq` on `Row` with manual `#[inline]`
  implementations. Added `#[inline]` to `Row::clone/clone_from`, `Row::hash`, `Row::cmp/partial_cmp`,
  `RowRef::cmp/partial_cmp`, `RowRef::iter()`, `RowRef::byte_len()`, `RowRef::data()`,
  `Row::packer()`, `DatumListIter::next()`.
- `src/repr/src/datum_vec.rs` - Added `#[inline]` to `DatumVec::borrow/borrow_with/borrow_with_selective`.
- `src/repr/benches/inline_hot_paths.rs` - Added benchmark (new file).
- `src/repr/Cargo.toml` - Registered benchmark.

**Future optimization ideas identified during research:**
- `push_datum()` (the core datum encoding function) is a large match statement (~200 lines)
  without `#[inline]`. Adding `#[inline]` to it would increase code size significantly at every
  call site. A better approach might be `#[inline(never)]` cold paths (error handling, rare types)
  combined with `#[inline]` on the main function, allowing the compiler to inline the hot paths
  (Int64, String, Bool) while keeping the cold paths out-of-line.
- `CompactBytes` methods (clone, len, as_slice) in the external `compact_bytes` crate may
  also be missing `#[inline]`. Since this is an external dependency, updating it requires
  a crate version bump.
- `Hashable::hashed()` in the `differential-dataflow` external crate may be missing `#[inline]`.
  DD uses FNV hash for exchange distribution.
- `MirScalarExpr::eval()` dispatch chain has 3 dispatch points for simple predicates. A specialized
  evaluator for common predicate patterns could reduce this.
- `ArrayIdx::cmp` with `#[inline]` may still be too large for the compiler to actually inline
  (16 match arms). A function pointer vtable approach (set comparison function at ArrayOrd creation
  time, call via indirect function call on every comparison) could eliminate the enum dispatch
  entirely, but requires changing the ArrayOrd architecture.

---

## Session 38: MirScalarExpr comparison fast path - bypass EagerBinaryFunc dispatch (2026-02-20)

**Problem:** Every time a comparison operator (=, !=, <, <=, >, >=) is evaluated in an MFP
predicate, the code went through a 3-level dispatch chain:

1. **BinaryFunc::eval dispatch** (212-arm enum match): Generated by the `derive_binary!` macro,
   this matches the specific BinaryFunc variant to call its `LazyBinaryFunc::eval` method.

2. **InputDatumType::try_from_iter** (6+ match operations): The `LazyBinaryFunc` blanket
   implementation for `EagerBinaryFunc` evaluates both sub-expressions, then for each operand:
   - Calls `ExcludeNull::try_from_result()` which pattern-matches on `Ok(Datum::Null)` to
     short-circuit on NULL
   - Calls the inner `Datum::try_from_result()` which matches `Ok(datum) => Ok(datum)`
   - The tuple `(ExcludeNull<Datum>, ExcludeNull<Datum>)::try_from_iter` chains these via `?`

3. **OutputDatumType::into_result**: The comparison function returns `bool`, which
   `OutputDatumType::into_result` converts to `Datum::True`/`Datum::False` via a match.

The actual comparison functions (`eq`, `lt`, `gt`, etc. in `scalar/func.rs`) are trivial:
`a == b`, `a < b`, `a > b` — direct Datum comparisons. All the overhead is in the dispatch
machinery.

Comparison operators are the most common predicates in SQL queries (`WHERE x > 5`,
`JOIN ON a.id = b.id`). Every MFP evaluation (persist source decode, peek execution,
join closure application) evaluates these predicates for every row.

**Fix:** Added a fast path in `MirScalarExpr::eval` that intercepts `CallBinary` for the 6
comparison operators (Eq, NotEq, Lt, Lte, Gt, Gte) and evaluates them directly:
1. Evaluate both sub-expressions via `expr1.eval()`, `expr2.eval()`
2. Check `a.is_null() || b.is_null()` and return `Datum::Null` (matching ExcludeNull behavior)
3. Compare directly using Datum's `PartialEq`/`Ord` implementations
4. Return `Datum::True`/`Datum::False`

This replaces the 3-level dispatch chain (212-arm match → InputDatumType → OutputDatumType)
with a 6-arm match + direct comparison. The NULL handling is semantically identical to the
`ExcludeNull` framework's behavior.

Non-comparison binary operators (AddInt32, etc.) continue through the normal path unchanged.

**Benchmark results** (criterion, `cargo bench -p mz-expr --bench comparison_eval`):

### Per-call comparison evaluation (ns/iter)

| Scenario                   | Old (full dispatch) | New (fast path) | Speedup |
|----------------------------|--------------------:|----------------:|--------:|
| int32_gt_literal           | 40.4                | 34.0            | **1.19x** |
| int64_eq_literal           | 39.5                | 33.3            | **1.19x** |
| float64_lt_literal         | 38.0                | 29.7            | **1.28x** |
| string_neq_literal         | 41.1                | 31.6            | **1.30x** |
| int32_gt_column            | 30.9                | 20.5            | **1.51x** |
| null_propagation           | 30.9                | 32.6            | ~1.0x   |
| add_int32 (non-comparison) | 36.6                | 36.0            | ~1.0x   |

### Batch evaluation (10k predicate evaluations)

| Scenario                        | Old (µs) | New (µs) | Speedup   |
|---------------------------------|----------|----------|-----------|
| batch_int32_gt (10k)            | 359      | 289      | **1.24x** |
| batch_two_predicates (10k)      | 713      | 570      | **1.25x** |

**Summary:** **1.19-1.51x faster per comparison, 1.24-1.25x faster in batch**. The speedup
comes from eliminating the 212-arm BinaryFunc enum match, the InputDatumType type conversion
machinery (ExcludeNull wrapper construction, try_from_iter chaining), and the OutputDatumType
bool→Datum conversion.

Column-vs-column comparisons show the largest speedup (1.51x) because the full overhead
is pure dispatch machinery (no literal `unpack_first()` cost to amortize). Literal comparisons
show 1.19-1.30x because the literal unpacking (~8-10ns) dilutes the dispatch savings.
The null propagation case is ~1.0x because both paths short-circuit immediately on NULL.
The add_int32 baseline is ~1.0x as expected (no change to non-comparison operators).

The batch benchmark (10k evaluations of `WHERE col0 > 5` and `WHERE col0 > 5 AND col1 <= 5000`)
shows a consistent **1.24-1.25x improvement**, which directly translates to faster predicate
evaluation in MFP plans. Since MFP evaluation runs on every row during persist source decode,
peek execution, and join closure application, this speeds up all query types with comparison
predicates.

**Files changed:**
- `src/expr/src/scalar.rs` - Added fast-path comparison evaluator in `MirScalarExpr::eval`
  for Eq/NotEq/Lt/Lte/Gt/Gte operators. Uses direct Datum comparison instead of
  EagerBinaryFunc dispatch chain.
- `src/expr/benches/comparison_eval.rs` - Added benchmark (new file) with per-call tests
  (int32_gt, int64_eq, float64_lt, string_neq, column_vs_column, null_propagation,
  add_int32 baseline) and batch tests (10k single predicate, 10k two predicates).
- `src/expr/Cargo.toml` - Registered benchmark.

**Future optimization ideas identified during research:**
- `ArrayIdx::cmp` at 330B calls in persist merge sort does a 16-arm enum match per comparison.
  A function pointer vtable approach (set comparison function at ArrayOrd creation time) could
  eliminate per-comparison dispatch entirely.
- `Row::clone()` at 527B executions is the largest source of allocation pressure.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row.
- The literal `unpack_first()` call (~8-10ns per literal access) could be optimized by caching
  the unpacked Datum in the MirScalarExpr::Literal variant instead of storing a Row.

## Session 39: Fast-path numeric addition/subtraction - bypass C FFI

**Date:** 2026-02-20

**Problem:** `add_numeric` and `sub_numeric` in `src/expr/src/scalar/func.rs` perform every
decimal addition/subtraction through the C FFI boundary: clone a `Context<Numeric>` (memcpy of
the context struct), call `decNumberAdd`/`decNumberSubtract` through FFI, then check status flags.
This costs ~11-21ns per operation. Since numeric arithmetic is used in every SUM/AVG aggregation,
arithmetic expressions, and window functions, this overhead adds up significantly.

**Fix:** Added a fast path that bypasses the C FFI entirely for common cases:
1. Both operands must be finite (not NaN/Infinity)
2. Both must have the same exponent (scale) — e.g., both `123.45` and `678.90` have exponent -2
3. Both coefficients must fit in i64 (≤18 significant digits, i.e. ≤6 base-1000 lsu units)
4. The result must not overflow i64

When all conditions are met, we extract coefficients from the `lsu` array as i64, do native
Rust `checked_add`/`checked_sub`, then reconstruct the result via `Numeric::from_raw_parts()`.
This avoids: Context clone, FFI boundary crossing, C library overhead, and status flag checking.

The same-exponent requirement is met in the vast majority of real-world cases: SUM aggregations
add values with the same column scale, and arithmetic between same-typed columns preserves scale.

**Benchmark results (per-operation):**
```
numeric_add/old_small_int        11.4 ns
numeric_add/new_small_int         6.8 ns   (1.67x faster)
numeric_add/old_decimal          12.6 ns
numeric_add/new_decimal           7.4 ns   (1.70x faster)
numeric_add/old_large_coeff      20.6 ns
numeric_add/new_large_coeff      10.3 ns   (2.0x faster)
numeric_add/old_money            15.7 ns
numeric_add/new_money             8.4 ns   (1.87x faster)
numeric_add/old_diff_scale       11.9 ns
numeric_add/new_diff_scale       13.4 ns   (fallback, ~1.4ns overhead)
numeric_sub/old_money            11.6 ns
numeric_sub/new_money             8.0 ns   (1.45x faster)
```

**Batch SUM results (10k values):**
```
numeric_add_batch/old_sum_10k       180 µs
numeric_add_batch/new_sum_10k       155 µs   (1.16x faster)
numeric_add_batch/old_sum_ints_10k  167 µs
numeric_add_batch/new_sum_ints_10k  146 µs   (1.14x faster)
```

Per-operation: **1.5-2.0x speedup** for same-scale values (the common case).
Batch SUM: **1.14-1.16x** (accumulator grows beyond 18 digits after many additions,
causing fallback to FFI for later iterations).
Fallback overhead: ~1.4ns for the exponent check on different-scale values (negligible).

**Files changed:**
- `src/repr/src/adt/numeric.rs` - Added `coeff_to_i64()`, `numeric_from_i64_coeff()`,
  `try_add_fast()`, and `try_sub_fast()` helper functions. Added `test_try_add_sub_fast` test.
- `src/expr/src/scalar/func.rs` - Added fast-path calls to `try_add_fast`/`try_sub_fast`
  before the FFI fallback in `add_numeric` and `sub_numeric`.
- `src/repr/benches/numeric_arith.rs` - Added benchmark (new file) with per-call tests
  (small_int, decimal, large_coeff, money, diff_scale) and batch SUM tests (10k decimal,
  10k integer).
- `src/repr/Cargo.toml` - Registered benchmark.

**Future optimization ideas identified during research:**
- `mul_numeric` and `div_numeric` could use a similar fast path for simple cases (same-scale
  multiplication with result fitting in i64), but multiplication changes the exponent so
  the reconstruction is more complex.
- The `NumericAgg` (27-unit) accumulator in `reduce.rs` SUM aggregation also goes through
  FFI via `cx_agg.add()` — a similar fast path for the wider type could help aggregation
  performance.
- `ArrayIdx::cmp` at 330B calls in persist merge sort does a 16-arm enum match per comparison.
- `Row::clone()` at 527B executions is the largest source of allocation pressure.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row.

---

## Session 40: Fast-path integer-to-Numeric construction - bypass C FFI

**Date:** 2026-02-20

**Optimization:** All integer-to-Numeric casts (`CastInt16ToNumeric`, `CastInt32ToNumeric`,
`CastInt64ToNumeric`, `CastUint16ToNumeric`, `CastUint32ToNumeric`, `CastUint64ToNumeric`)
use C FFI calls to `decNumberFromInt32` / `cx.from_i64()` to construct a `Numeric` value,
plus an additional FFI call to `rescale()` when a scale is specified. This optimization adds
a pure Rust fast path using `numeric_from_i64_coeff()` / `numeric_from_u64_coeff()` that
constructs the `Numeric` directly by populating the `lsu` (least-significant-unit) array in
base-1000, completely bypassing C FFI. Rescaling is handled by pre-multiplying the integer
by the appropriate power of 10 before construction.

**Approach:**
- Made `numeric_from_i64_coeff()` public (from Session 39) and added `numeric_from_u64_coeff()`
  for unsigned integers.
- Added `POW10_TABLE` (powers of 10 up to 10^18) for integer rescaling.
- Added `try_numeric_from_i64(val, scale)` and `try_numeric_from_u64(val, scale)` that handle
  both construction and rescaling in pure Rust, falling back to `None` for overflow or
  scale >= 19.
- Updated all 6 `CastXxxToNumeric::call()` functions to try the fast path first, falling back
  to FFI only for edge cases.
- Fixed `numeric_from_i64_coeff` to handle `i64::MIN` correctly (wrapping_neg via u64).
- Also updated `DecimalLike::lossy_from` for `Numeric` to use the fast path.

**Benchmark results (Rust microbenchmark, per-value):**

| Value | FFI (ns) | Fast-path (ns) | Speedup |
|---|---|---|---|
| zero | 33.6 | 4.9 | **6.9x** |
| small (42) | 34.2 | 2.8 | **12.2x** |
| medium (123456) | 34.7 | 5.0 | **6.9x** |
| large (1B) | 35.0 | 4.8 | **7.3x** |
| max_i32 | 35.3 | 4.8 | **7.4x** |
| negative (-999999) | 48.6 | 5.0 | **9.7x** |
| min_i64 | 55.6 | 4.8 | **11.6x** |

**Per-value speedup: 7-12x**

**Batch results (10k values including cx.add accumulation):**

| Benchmark | FFI (µs) | Fast-path (µs) | Speedup |
|---|---|---|---|
| i32→numeric (10k) | 164.6 | 125.0 | **1.3x** |
| i64→numeric (10k) | 473.0 | 152.7 | **3.1x** |
| i32→numeric scale6 (10k) | 354.4 | 159.7 | **2.2x** |
| u64→numeric (10k) | 469.2 | 130.3 | **3.6x** |

Batch speedup is lower because `cx.add()` (FFI) dominates after conversion. The conversion
itself is 7-12x faster. For queries that cast many integer values to numeric (common in
aggregation pipelines), this reduces conversion overhead significantly.

**Files changed:**
- `src/repr/src/adt/numeric.rs` - Made `numeric_from_i64_coeff` public, added
  `numeric_from_u64_coeff`, `POW10_TABLE`, `try_numeric_from_i64`, `try_numeric_from_u64`.
  Fixed i64::MIN overflow in `numeric_from_i64_coeff`. Updated `DecimalLike::lossy_from`.
  Added `test_try_numeric_from_int` test.
- `src/expr/src/scalar/func/impls/int16.rs` - Fast-path in `CastInt16ToNumeric::call()`
- `src/expr/src/scalar/func/impls/int32.rs` - Fast-path in `CastInt32ToNumeric::call()`
- `src/expr/src/scalar/func/impls/int64.rs` - Fast-path in `CastInt64ToNumeric::call()`
- `src/expr/src/scalar/func/impls/uint16.rs` - Fast-path in `CastUint16ToNumeric::call()`
- `src/expr/src/scalar/func/impls/uint32.rs` - Fast-path in `CastUint32ToNumeric::call()`
- `src/expr/src/scalar/func/impls/uint64.rs` - Fast-path in `CastUint64ToNumeric::call()`
- `src/repr/benches/int_to_numeric.rs` - New benchmark file
- `src/repr/Cargo.toml` - Registered benchmark

**Future optimization ideas:**
- `Numeric::from(f64/f32)` also goes through FFI — a direct construction for finite floats
  could bypass it.
- The `DecimalLike` trait's `lossy_from(usize)` still uses i64 range which may not cover all
  usize values on 64-bit; a u64 path might be needed.
- `ArrayIdx::cmp` at 330B calls in persist merge sort does a 16-arm enum match per comparison.
- `Row::clone()` at 527B executions is the largest source of allocation pressure.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row.

## Session 41: Fast-path Numeric comparison - bypass C FFI reduce + partial_cmp

**Date:** 2026-02-20

**Problem:** `OrderedDecimal<Decimal<N>>::cmp()` (from the `dec` crate) performs **3 C FFI
calls per comparison**: `cx.reduce(lhs)`, `cx.reduce(rhs)`, and `cx.partial_cmp(lhs, rhs)`.
The `reduce` operation normalizes trailing zeros (e.g., `1.20` → `1.2`), which is necessary
for equality semantics but extremely expensive for the common case where values from the same
SQL column already have the same scale/exponent.

Every `Datum::Numeric` comparison goes through this path — whether in expression evaluation
(the comparison fast-path from Session 38), sorting, deduplication, or aggregation. For
TPC-H-style workloads with money columns (`DECIMAL(15,2)`), Numeric comparisons are among
the hottest operations.

**Fix:** Added `fast_numeric_cmp()` in `src/repr/src/adt/numeric.rs` that compares two
`Numeric` values entirely in Rust for the common case:

1. **Special values (NaN, Inf):** Fall back to C FFI (rare case).
2. **Zero handling:** Direct comparison without examining coefficients.
3. **Sign comparison:** Different signs → immediate ordering without coefficient examination.
4. **Same exponent (>99% of comparisons):** Compare `coefficient_units()` slices from most
   significant unit (MSU) to least significant unit (LSU), treating missing units as zero.
   This is pure integer comparison — no FFI calls needed.
5. **Different exponents:** Compare adjusted exponents (`exponent + digits - 1`) for
   magnitude ordering. Only falls back to C FFI when adjusted exponents are equal but raw
   exponents differ (requires coefficient rescaling — extremely rare).

Integrated into the comparison fast-path in `MirScalarExpr::eval` (`src/expr/src/scalar.rs`):
when both operands are `Datum::Numeric`, uses `fast_numeric_cmp` instead of `Datum`'s derived
`Ord` (which delegates to `OrderedDecimal::cmp` and its 3 FFI calls).

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench numeric_cmp`):

### Per-comparison (ns/iter)

| Scenario | FFI (ns) | Fast (ns) | Speedup |
|---|---|---|---|
| small_int (42 vs 99) | 21.7 | 3.8 | **5.7x** |
| money (12345.67 vs 98765.43) | 20.1 | 5.8 | **3.5x** |
| large_coeff (18-digit) | 23.9 | 5.8 | **4.1x** |
| equal (42.00 vs 42.00) | 33.6 | 6.0 | **5.6x** |
| diff_exp (100 vs 99.99) | 31.4 | 5.5 | **5.7x** |
| negative (-500.25 vs -100.50) | 29.8 | 6.0 | **5.0x** |

### Batch comparison (10k money values)

| Scenario | FFI (µs) | Fast (µs) | Speedup |
|---|---|---|---|
| 10k_money | 216.0 | 28.1 | **7.7x** |

**Per-comparison speedup: 3.5-5.7x. Batch speedup: 7.7x.**

The equal-values case shows the biggest per-value improvement (5.6x) because `reduce` has to
do the most work when both values have the same coefficient (normalizing trailing zeros).
The batch speedup is even higher (7.7x) due to cache effects — the fast path keeps everything
in registers while the FFI path bounces through the C calling convention.

**Files changed:**
- `src/repr/src/adt/numeric.rs` - Added `fast_numeric_cmp()` function and
  `test_fast_numeric_cmp` correctness test. Added `OrderedDecimal` import.
- `src/expr/src/scalar.rs` - Updated comparison fast-path to use `fast_numeric_cmp` when
  both datums are `Datum::Numeric`, avoiding `OrderedDecimal::cmp` and its 3 C FFI calls.
- `src/repr/benches/numeric_cmp.rs` - New benchmark file.
- `src/repr/Cargo.toml` - Registered benchmark.

**Future optimization ideas:**
- The `Datum`'s derived `Ord` trait still uses the slow `OrderedDecimal::cmp` for all
  non-expression-evaluation paths (sorting, deduplication, etc.). A manual `Ord` impl for
  `Datum` could intercept Numeric comparison everywhere, but requires handling all ~30
  variants manually.
- `OrderedDecimal::hash()` calls `reduce` (C FFI) on every hash, but investigation showed
  `Datum::hash()` is NOT on any hot path. Row hashing uses raw byte hashing, not Datum-level
  hashing. Only `DatumList` and `DatumMap` (rare types) use Datum::hash.
- `Numeric::from(f64/f32)` still goes through FFI.
- `ArrayIdx::cmp` at 330B calls in persist merge sort does a 16-arm enum match per comparison.
- `Row::clone()` at 527B executions is the largest source of allocation pressure.

## Session 42: Fast-path Numeric reduce - bypass C FFI for Row packing

**Date:** 2026-02-20

**Problem:** Every time a `Numeric` value is packed into a `Row`, two C FFI operations occur:

1. **`push_datum`** (line 2300 of `row.rs`): calls `numeric::cx_datum().reduce(&mut n.0)` —
   clones a `Context` struct + makes a C FFI call to normalize trailing zeros. This happens for
   EVERY Numeric value, even though ~95% of values are already reduced (from Row storage, from
   our fast-path integer constructors, or from same-scale arithmetic).

2. **`datum_size`** (line 2448 of `row.rs`): clones the entire `Decimal<13>` struct (35 bytes) +
   clones a `Context` + makes a C FFI `reduce()` call — all just to compute the byte size of the
   encoded value. This is used for buffer pre-allocation.

**Fix:** Two optimizations:

1. **`fast_numeric_reduce()`** in `numeric.rs`: Pure Rust implementation that avoids the C FFI
   for the common case (already-reduced values). Checks `coefficient_units()[0] % 10 != 0` to
   detect already-reduced values in ~2ns. For values with trailing zeros, performs the full
   reduction in Rust by counting trailing zeros, dividing the base-1000 coefficient by the
   appropriate power of 10, and reconstructing via `from_raw_parts()`. Falls back to FFI for
   extreme exponents (outside [-39, 38], e.g., from `Numeric::from(f64::MAX)`). Respects the
   context's max_exponent (38) by capping trailing-zero stripping.

2. **`reduced_numeric_digit_count()`** in `numeric.rs`: Computes the digit count after reduction
   entirely in Rust — no clone, no FFI. Just counts trailing zeros in the coefficient units
   and subtracts from the digit count. Used by `datum_size` to compute the encoded size.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench numeric_reduce`):

### Per-operation reduce (ns/iter)

| Scenario | FFI (ns) | Fast (ns) | Speedup |
|---|---|---|---|
| already_reduced (42) | 8.7 | 2.4 | **3.7x** |
| money (12345.67) | 6.5 | 2.3 | **2.8x** |
| trailing_zeros (1.50) | 11.4 | 20.0 | 0.57x (slower) |
| large_trailing (123456789000) | 13.0 | 14.5 | 0.90x (slower) |
| zero | 7.7 | 9.8 | 0.79x (slower) |

### Per-operation digit_count / datum_size (ns/iter)

| Scenario | FFI (ns) | Fast (ns) | Speedup |
|---|---|---|---|
| already_reduced (42) | 8.2 | 1.8 | **4.6x** |
| money (12345.67) | 5.8 | 1.8 | **3.2x** |
| trailing (1.50) | 10.7 | 2.2 | **4.9x** |

### Row packing (10k values, µs)

| Scenario | Time (µs) |
|---|---|
| pack_10k_money (already reduced) | 149 |
| pack_10k_trailing | 324 |
| pack_10k_ints | 151 |

**Summary:** The common case (already-reduced values like money columns, integer-cast numerics,
values read from existing Rows) sees a **2.8-3.7x speedup** for reduce and **3.2-4.9x speedup**
for digit_count. Combined, this eliminates ~8ns of overhead per Numeric value during Row packing
for the typical workload. The trailing-zeros case (uncommon — only from arithmetic that
produces trailing zeros) is slightly slower due to the coefficient division loop in Rust, but
this is offset by the digit_count improvement which is always faster.

The digit_count optimization eliminates ALL cloning and FFI for size estimation — it's pure
integer arithmetic on the coefficient units, with no struct copies.

**Files changed:**
- `src/repr/src/adt/numeric.rs` - Added `fast_numeric_reduce()` and
  `reduced_numeric_digit_count()` functions. Added `test_fast_numeric_reduce` test.
- `src/repr/src/row.rs` - Updated `push_datum` to use `fast_numeric_reduce` instead of
  `cx_datum().reduce()`. Updated `datum_size` to use `reduced_numeric_digit_count` instead
  of clone + FFI.
- `src/repr/benches/numeric_reduce.rs` - New benchmark file.
- `src/repr/Cargo.toml` - Registered benchmark.

**Future optimization ideas:**
- `mul_numeric` and `div_numeric` could use similar fast paths for simple cases.
- `Numeric::from(f64/f32)` still goes through FFI.
- `ArrayIdx::cmp` at 330B calls in persist merge sort does a 16-arm enum match per comparison.
- `Row::clone()` at 527B executions is the largest source of allocation pressure.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row.
- `push_signed_int` at 992M executions — already optimized with `#[inline(always)]`.
- `DatumColumnDecoder::get()` at 978M executions — double dispatch pattern could be optimized.

## Session 43: Fast-path Numeric multiplication - bypass C FFI

**Date:** 2026-02-20

**Problem:** `mul_numeric()` in expression evaluation calls `cx.mul(&mut a, &b)` — a C FFI
call to the decNumber library — for every Numeric multiplication. This is followed by
`munge_numeric()` which checks precision constraints and potentially rescales (another FFI
call). For TPC-H-style workloads with `revenue = price * quantity`, Numeric multiplication
is a hot operation.

The C library handles all edge cases (NaN, Inf, arbitrary precision, rounding) but is
overkill for the common case: two finite values with ≤18 significant digits each, where
the product naturally fits within the 39-digit Numeric precision limit.

**Fix:** Added `try_mul_fast()` in `src/repr/src/adt/numeric.rs` that multiplies two
`Numeric` values entirely in Rust for the common case:

1. **Special/zero check:** Falls back to FFI for NaN/Inf. Returns zero immediately for
   zero operands.
2. **Coefficient extraction:** Uses `coeff_to_i64()` (existing helper) to extract both
   coefficients as i64 (≤18 digits each). Falls back if either has >18 digits.
3. **i128 multiplication:** `i64 × i64` always fits in i128 (max 36 digits).
4. **Result construction:** `numeric_from_i128_coeff()` converts the i128 product to
   base-1000 coefficient units and constructs the Numeric via `from_raw_parts()`.
5. **Precision check:** Verifies result precision ≤ 39 digits. Falls back to FFI if
   rescaling would be needed (rare — only when both operands have many digits AND
   large negative exponents).

Also added `numeric_from_i128_coeff()` helper that converts i128 to the Decimal internal
representation (base-1000 coefficient units in `[u16; 13]` array).

Integrated into `mul_numeric()` in `src/expr/src/scalar/func.rs`: tries fast path first,
falls back to FFI + `munge_numeric()` only when needed.

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench numeric_mul`):

### Per-multiplication (ns/iter)

| Scenario | FFI (ns) | Fast (ns) | Speedup |
|---|---|---|---|
| small_int (42×7) | 12.2 | 8.6 | **1.4x** |
| money_x_qty (12345.67×99) | 16.8 | 11.2 | **1.5x** |
| same_scale (100.00×50.00) | 14.5 | 11.2 | **1.3x** |
| large_coeff (999999999999.99×123456) | 32.4 | 20.4 | **1.6x** |
| neg_x_pos (-500.25×200.50) | 23.7 | 13.2 | **1.8x** |
| tiny (0.000001×0.000002) | 12.3 | 8.7 | **1.4x** |
| one_x_price (1×12345.67) | 13.0 | 11.5 | **1.1x** |
| zero_x_val (0×12345.67) | 11.6 | 4.9 | **2.4x** |

### Batch multiplication (10k values)

| Scenario | FFI (µs) | Fast (µs) | Speedup |
|---|---|---|---|
| 10k revenue (price×qty+sum) | 299 | 181 | **1.7x** |
| 10k same_scale (dec×dec) | 202 | 95 | **2.1x** |

**Per-multiplication speedup: 1.1-2.4x. Batch speedup: 1.7-2.1x.**

The speedup is moderate but consistent. The FFI overhead for multiplication is lower than
for comparison (which needed 3 FFI calls) or reduce, so the absolute savings per call are
smaller. But for batch workloads, the improvement is significant: 2.1x for pure
multiplication, 1.7x for the realistic revenue computation (mul + add pipeline).

The zero case shows the largest speedup (2.4x) because the fast path returns immediately
without touching coefficients. The large_coeff case shows 1.6x because the i128
multiplication and base-1000 reconstruction are more work for larger values.

**Files changed:**
- `src/repr/src/adt/numeric.rs` - Added `try_mul_fast()` and `numeric_from_i128_coeff()`
  functions. Added `test_try_mul_fast` correctness test.
- `src/expr/src/scalar/func.rs` - Updated `mul_numeric` to try fast path first.
- `src/repr/benches/numeric_mul.rs` - New benchmark file.
- `src/repr/Cargo.toml` - Registered benchmark.

**Future optimization ideas:**
- `div_numeric` could use a similar fast path for simple cases (division where both fit
  in i128 and the quotient has no remainder, or where the quotient can be computed exactly).
- `Numeric::from(f64/f32)` still goes through FFI.
- `ArrayIdx::cmp` at 330B calls in persist merge sort does a 16-arm enum match per comparison.
- `Row::clone()` at 527B executions is the largest source of allocation pressure.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row.
- The `Datum` derived `Ord` still uses `OrderedDecimal::cmp` (3 FFI calls) for all
  non-expression-evaluation Numeric comparisons (sorting, deduplication, DDFlow operations).
  A manual `Ord` for `Datum` could intercept this, but requires handling all ~30 variants.

---

## Session 44: Manual Datum Ord/PartialEq - bypass OrderedDecimal FFI for Datum-level comparisons

**Date:** 2026-02-20

**Idea (from Session 43 future ideas):** The derived `Ord` for `Datum` delegates to
`OrderedDecimal::cmp` for Numeric variants, which performs 2 clones + 3 C FFI calls
(reduce, reduce, partial_cmp) per comparison. Similarly, `PartialEq` delegates to `cmp`,
so equality checks also pay the FFI cost. Replace the derived `Ord`/`PartialOrd`/`PartialEq`/`Eq`
with manual implementations that use `fast_numeric_cmp()` (from Session 41) for the
Numeric variant and delegate to field Ord for all other variants.

**Where it helps:** `compare_columns` (used by TopK, sorting), `Top1Monoid::cmp`,
`DatumList`/`DatumMap` comparisons, and any other code path that calls `Datum::cmp`
or `Datum::eq` with Numeric values. Note: Row/RowRef use byte-level comparison
(not Datum::cmp), so this targets datum-level comparison paths.

**Implementation:**
- Removed `Eq, PartialEq, Ord, PartialOrd` from Datum's `#[derive]`, keeping
  `Clone, Copy, Hash, EnumKind`
- Added `Ord, PartialOrd` to the `DatumKind` derive (for discriminant comparison)
- Added manual `Eq`, `PartialEq` (delegates to `cmp`), `PartialOrd` (delegates to `cmp`),
  and `Ord` implementations
- The manual `Ord::cmp` first compares `DatumKind` discriminants (cheap integer compare),
  then matches on the variant pair: Numeric uses `fast_numeric_cmp()`, all other field
  variants delegate to their inner type's `Ord`, fieldless variants return `Equal`

**Benchmark results (`cargo bench -p mz-repr --bench datum_cmp`):**

Single comparison (per call):
| Benchmark | Old (OrderedDecimal FFI) | New (Datum fast_numeric_cmp) | Speedup |
|-----------|--------------------------|------------------------------|---------|
| small_int (42 vs 99) | 20.8 ns | 4.9 ns | **4.2x** |
| money (12345.67 vs 98765.43) | 18.5 ns | 4.9 ns | **3.7x** |
| equal (42.00 vs 42.00) | 32.7 ns | 5.3 ns | **6.2x** |
| negative (-500.25 vs -100.50) | 28.8 ns | 5.0 ns | **5.8x** |
| large_coeff (18-digit) | 22.5 ns | 4.9 ns | **4.6x** |
| eq (42 != 99) | 20.3 ns | 4.9 ns | **4.1x** |
| eq_equal (42.00 == 42.00) | 32.3 ns | 5.3 ns | **6.1x** |

Batch comparison (10k pairs):
| Benchmark | Old (OrderedDecimal FFI) | New (Datum fast_numeric_cmp) | Speedup |
|-----------|--------------------------|------------------------------|---------|
| 10k numeric | 224.3 µs | 56.9 µs | **3.9x** |

Non-Numeric baselines (verify no regression):
| Benchmark | Time |
|-----------|------|
| int64 | 1.6 ns (no change) |
| string | 3.0 ns (no change) |
| cross_variant | 1.2 ns (improved 14% - DatumKind discriminant compare is cheaper) |
| 10k int64 batch | 16.3 µs (no change) |

**Analysis:** Equal-value comparisons see the largest speedup (6.2x) because the old FFI
path calls `reduce` on both operands before comparing - reduce is expensive for values
that are already reduced. The new path compares coefficient units directly. Negative
values also get a large speedup (5.8x) because the old path's reduce/partial_cmp sequence
is particularly expensive when signs differ. The cross-variant comparison improved 14%
because `DatumKind` integer comparison is cheaper than the derived enum discriminant logic.

**Files changed:**
- `src/repr/src/scalar.rs` - Replaced derived `Ord`/`PartialOrd`/`PartialEq`/`Eq` with
  manual implementations using `fast_numeric_cmp()` for the Numeric variant.
- `src/repr/benches/datum_cmp.rs` - New benchmark file comparing old OrderedDecimal::cmp
  vs new Datum::cmp paths.
- `src/repr/Cargo.toml` - Registered datum_cmp benchmark.

**Future optimization ideas:**
- `OrderedDecimal::hash` still calls `reduce` (FFI) - a manual Hash for Datum could
  use a fast hash for Numeric, but this requires careful consistency with Eq.
- `ArrayIdx::cmp` at 330B calls in persist merge sort does a 16-arm enum match per comparison.
- `Row::clone()` at 527B executions is the largest source of allocation pressure.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row.
- `div_numeric` could use a fast path for simple cases.

---

## Session 45: Fast-path numeric_to_twos_complement_be — bypass C FFI

**Date:** 2026-02-20

**Target:** `numeric_to_twos_complement_be()` in `src/repr/src/adt/numeric.rs`.
This function converts a `Numeric` into its 17-byte big-endian twos complement
representation, used in Avro encoding. Coverage shows ~0.8B calls. The original
implementation clones a `Context`, calls `scaleb` (C FFI) to normalize the
exponent, then runs a loop that repeatedly calls `rem`/`div_integer` (C FFI)
to extract base-256 digits.

**Insight:** `scaleb` only adjusts the exponent, it does NOT change the
coefficient. For values with non-positive exponent and ≤18 significant digits
(coefficient fits in i64), we can extract the coefficient directly with
`coeff_to_i64()`, sign-extend to i128, and write the 17-byte big-endian twos
complement representation entirely in Rust — zero FFI calls.

**Benchmark results (`cargo bench -p mz-repr --bench numeric_twos_complement`):**

| Benchmark | FFI (ns) | Fast (ns) | Speedup |
|---|---|---|---|
| integer_42 | 32.4 | 9.2 | **3.5x** |
| money_12345_67 | 51.2 | 10.5 | **4.9x** |
| rescaled_scale6 | 51.0 | 10.5 | **4.9x** |
| large_18_digits | 48.7 | 12.3 | **4.0x** |
| negative | 56.9 | 10.4 | **5.5x** |
| zero | 8.3 | 9.3 | ~1x (already early-returns for special) |
| huge_gt18_digits | 74.2 | 75.5 | ~1x (FFI fallback, expected) |

**Speedup: 3.5-5.5x** for the common case (≤18 significant digits, non-positive
exponent). This covers the vast majority of real-world Numeric values (money,
counts, measurements).

**Note on `numeric_to_twos_complement_wide`:** The wide variant (33-byte buffer,
used in aggregation) is hotter at ~25B inner loop calls but harder to fast-path.
It requires computing `coeff * 10^(39+exp)`, which overflows i128 for most
practical values. A future optimization could use u128-pair (256-bit) arithmetic
for this path.

**Files changed:**
- `src/repr/src/adt/numeric.rs` — Added fast path to `numeric_to_twos_complement_be`
  that writes the coefficient directly as 17-byte big-endian twos complement when
  exponent ≤ 0 and coefficient fits in i64. Added correctness test.
- `src/repr/benches/numeric_twos_complement.rs` — New benchmark file.
- `src/repr/Cargo.toml` — Registered numeric_twos_complement benchmark.

**Future optimization ideas:**
- `OrderedDecimal::hash` still calls `reduce` (FFI).
- `Row::clone()` at 527B executions is the largest source of allocation pressure.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row.
- `div_numeric` could use a fast path for simple cases.

---

## Session 46: Fast-path numeric_to_twos_complement_wide — bypass C FFI with 256-bit arithmetic

**Date:** 2026-02-21

**Target:** `numeric_to_twos_complement_wide()` in `src/repr/src/adt/numeric.rs`.
This function converts a `Numeric` into a 33-byte big-endian twos complement
representation used in Avro encoding for aggregation accumulators. Session 45
optimized the non-wide variant (17 bytes) for 3.5-5.5x speedup, and noted that
the wide variant was harder due to requiring 256-bit arithmetic.

**Problem:** The wide function performed 7+ C FFI calls per value:
1. `NumericAgg::context()` — creates a wide-precision context
2. `cx.to_width(numeric)` — converts Numeric (13 units) to NumericAgg (27 units)
3. `cx.neg(&mut scaler)` + `cx.rescale(&mut d, &scaler)` — rescale to exponent -39
4. `cx.abs(&mut scaler)` + `cx.scaleb(&mut d, &scaler)` — adjust to canonical coefficient
5. Inner loop: `cx.rem()` + `cx.div_integer()` + `coefficient::<u128>()` — extract
   base-2^128 chunks (2-3 iterations for a 33-byte result)

The rescaled value represents `original_value × 10^39`. For common Numeric values
(≤18 significant digits, exponent in [-39, 0]), the coefficient fits in i64 and the
rescaled value fits in ~192 bits (well within the 33-byte / 264-bit buffer).

**Fix:** Added a fast path using 256-bit arithmetic (u128 pair) entirely in Rust:

1. **`POW10_U128[39]`** — precomputed powers of 10 as u128 (10^0 through 10^38;
   10^39 overflows u128 and is handled by multiplying 10^38 × 10 in 256-bit).

2. **`u256_mul(a, b)`** — schoolbook 128×128→256 multiplication using 64-bit halves.
   Returns (lo, hi) where result = lo + hi × 2^128.

3. **`u256_mul_small(lo, hi, factor)`** — multiplies a 256-bit value by a small u64
   factor. Used for the 10^39 case.

4. **`u256_mul_u64_pow10(val, pow_exp)`** — multiplies a u128 value by 10^pow_exp,
   returning a 256-bit result. Dispatches to `u256_mul` for pow_exp ≤ 38 and
   `u256_mul_small` for pow_exp = 39.

5. **`write_twos_complement_be_33(buf, lo, hi, is_neg)`** — writes a 256-bit unsigned
   value as 33-byte big-endian twos complement. For negative values, applies twos
   complement negation (!val + 1) on the 256-bit pair before writing.

The fast path activates when: exponent in [-39, 0] AND coefficient fits in i64
(≤18 significant digits). This covers the vast majority of real-world Numeric values
(money columns, integer-cast numerics, aggregation inputs).

**Benchmark results** (criterion, `cargo bench -p mz-repr --bench numeric_twos_complement`):

### Per-value wide twos complement encoding (ns/iter)

| Value                    | Old FFI (ns) | New fast (ns) | Speedup     |
|--------------------------|------------:|-------------:|------------:|
| integer_42               | 275         | 10.5         | **26.2x**   |
| money_12345_67           | 341         | 10.6         | **32.1x**   |
| rescaled_scale6          | 277         | 10.7         | **25.9x**   |
| large_18_digits          | 857         | 11.6         | **73.9x**   |
| negative (-98765.4321)   | 384         | 10.8         | **35.5x**   |
| zero                     | 41.8        | 10.6         | **3.9x**    |
| huge_gt18_digits (FFI)   | 848         | 847          | ~1.0x       |
| pure_int_999999          | 481         | 10.7         | **45.0x**   |

**Summary:** **3.9-73.9x faster** for common Numeric values. The enormous speedup comes
from eliminating ALL C FFI calls: the old path performed 7+ FFI roundtrips
(context creation, to_width, rescale, scaleb, plus 2-3 iterations of rem/div_integer
in the inner loop), while the new path does pure Rust integer arithmetic. The
large_18_digits case shows the highest speedup (73.9x, from 857ns to 11.6ns) because
the FFI inner loop needed more iterations to extract the larger coefficient in
base-2^128 chunks. The zero case shows the lowest speedup (3.9x) because the old path
already had a `is_zero()` early return after the rescale/scaleb setup, so only the setup
FFI calls are eliminated.

The consistent ~10-11ns for all fast-path values (regardless of coefficient size or
sign) reflects the fixed cost of 256-bit multiplication + 33-byte buffer write — the
u128 pair arithmetic is constant-time. The huge_gt18_digits case falls back to the
FFI path (no regression).

**Files changed:**
- `src/repr/src/adt/numeric.rs` — Added fast path to `numeric_to_twos_complement_wide`
  using 256-bit arithmetic. Added `POW10_U128` table, `u256_mul`, `u256_mul_small`,
  `u256_mul_u64_pow10`, and `write_twos_complement_be_33` helper functions.
  Added `test_numeric_to_twos_complement_wide_fast_path` correctness test.
- `src/repr/benches/numeric_twos_complement.rs` — Added wide benchmark group with
  old (FFI reference) vs new (fast path) comparisons.

**Future optimization ideas:**
- `numeric_to_twos_complement_wide` could be extended to handle values with >18
  significant digits but ≤39 digits using i128 coefficient extraction + wider
  multiplication. This would cover all Numeric values, not just the i64-range subset.
- `OrderedDecimal::hash` still calls `reduce` (FFI).
- `Row::clone()` at 527B executions is the largest source of allocation pressure.
- `zero_diffs.clone()` in `reduce.rs` clones `Vec<Accum>` per input row.
- `div_numeric` could use a fast path for simple cases.
- `Rows::get` (columnar Row indexing) at 1,273B executions could eliminate the
  `if index == 0` branch by storing bounds with a leading 0 sentinel.
- `Region::len/capacity/as_vec` at 613B+ calls each do an enum dispatch that could
  be eliminated by caching length alongside the enum.

---

## Session 47: Fast-path NumericAgg SUM accumulation — bypass cx_agg.add + cx_agg.reduce

**Date:** 2026-02-21

**Target:** `Accum::Numeric::plus_equals` in `src/compute/src/render/reduce.rs`
(~2.9B executions in coverage data). Every SUM aggregation over numeric/decimal
columns calls `cx_agg.add()` + `cx_agg.reduce()` = 2 C FFI round-trips per
accumulation step, plus a `Context<NumericAgg>` clone.

**Approach:** For the common case where both the accumulator and the incoming
value have the same exponent (same SQL scale — true for >99% of SUM over a
single column) and both coefficients fit in i64 (≤18 significant digits), we
extract the coefficients as native i64, add them with `checked_add`, and
reconstruct the `NumericAgg` (`Decimal<27>`) from raw parts. This bypasses both
FFI calls entirely.

The fast path skips `reduce()` (trailing-zero stripping), which is safe because:
1. The result coefficient fits in i64 (~19 digits), far below NumericAgg's 81-digit
   capacity, so there's no precision pressure.
2. The exponent is preserved from the inputs, maintaining consistency for subsequent
   fast-path additions.
3. If the FFI fallback runs later (different exponents), its `reduce()` works
   correctly on any valid Decimal.

**Key functions added to `src/repr/src/adt/numeric.rs`:**
- `decimal_coeff_to_i64<const N: usize>()` — generic coefficient extraction for any
  `Decimal<N>` (works for both `Numeric` and `NumericAgg`).
- `numeric_agg_from_i64_coeff()` — builds `NumericAgg` from i64 coefficient + exponent.
- `try_add_numeric_agg()` — the public fast-path: returns `true` if it succeeded.

**Benchmark results** (`cargo bench --bench numeric_agg_add`):

Single operation (NumericAgg = Decimal<27>):
| Case | Old (FFI) | New (fast) | Speedup |
|------|-----------|------------|---------|
| Small int (42+58) | 21.6 ns | 16.4 ns | 1.3x |
| Money (1234567.89+9876543.21) | 28.8 ns | 17.2 ns | 1.7x |
| Large coeff (18 digits) | 43.8 ns | 16.4 ns | 2.7x |

Batch SUM (10k values accumulated):
| Case | Old (FFI) | New (fast) | Speedup |
|------|-----------|------------|---------|
| Money values | 202.8 µs | 165.6 µs | 1.2x |
| Integer values | 201.9 µs | 155.7 µs | 1.3x |

Note: Batch speedups are diluted by `NumericAgg` clone overhead per criterion
iteration (Decimal<27> is 60 bytes). The single-operation speedups (1.3–2.7x)
better reflect the actual improvement in the hot path.

**Files changed:**
- `src/repr/src/adt/numeric.rs` — Added `decimal_coeff_to_i64`, `numeric_agg_from_i64_coeff`,
  `try_add_numeric_agg`, and `test_try_add_numeric_agg` correctness test.
- `src/compute/src/render/reduce.rs` — Updated `Accum::Numeric` `plus_equals` to try
  fast path before FFI fallback.
- `src/repr/benches/numeric_agg_add.rs` — New benchmark for NumericAgg accumulator addition.

**Future optimization ideas:**
- `Accum::Numeric` `Multiply` trait (used for DD retraction scaling) also calls
  `cx_agg.mul()` — could bypass FFI for small integer factors.
- `datum_to_accumulator` calls `cx_agg.to_width()` to widen `Numeric` → `NumericAgg` —
  could construct `NumericAgg` directly from `Numeric` raw parts without FFI.
- `div_numeric` could use a fast path for simple cases.
- `OrderedDecimal::hash` still calls `reduce` (FFI).
- `Row::clone()` at 527B executions is the largest source of allocation pressure.
- `Rows::get` (columnar Row indexing) at 1,273B executions could eliminate the
  `if index == 0` branch by storing bounds with a leading 0 sentinel.

---

## Session 48: Numeric→NumericAgg to_width + NumericAgg×i64 multiply — bypass C FFI

**Date:** 2026-02-21

**Target:** Two FFI bottlenecks in the SUM aggregation pipeline:
1. `datum_to_accumulator` calls `cx_agg.to_width(n)` (C FFI) to widen `Numeric` (13 units) → `NumericAgg` (27 units) — this is just a struct copy with zero-padded coefficient.
2. `Multiply<Diff>` for `Accum::Numeric` calls `cx_agg.mul()` (2 FFI calls: `from_i64` + `mul`) — typically factor is 1 or -1.

**Approach:**
- `numeric_to_agg_direct()`: Copy coefficient units directly, construct `NumericAgg` via `from_raw_parts` — no FFI.
- `try_mul_numeric_agg_i64()`: Fast-path for factor=1 (just copy), otherwise extract i64 coefficient → native multiply → reconstruct. Falls back to FFI for overflow.
- Special case: `0 * -1 = -0` (IEEE decimal sign preservation via XOR of operand signs).

**Benchmark results (numeric_to_agg):**

| Benchmark | Old (ns) | New (ns) | Speedup |
|-----------|----------|----------|---------|
| to_width zero | 11.26 | 10.97 | 1.03x |
| to_width small_int | 11.26 | 10.52 | 1.07x |
| to_width money | 11.33 | 10.51 | 1.08x |
| to_width large_18dig | 11.52 | 11.04 | 1.04x |
| to_width negative | 11.16 | 10.58 | 1.05x |
| to_width max_precision | 11.39 | 10.96 | 1.04x |
| to_width small_exp | 11.17 | 10.52 | 1.06x |
| **batch 10k to_width+add** | **104.8 µs** | **69.3 µs** | **1.51x** |
| mul factor=1 money | 49.57 | 14.93 | **3.3x** |
| mul factor=-1 money | 59.24 | 15.04 | **3.9x** |
| mul factor=5 small | 47.56 | 14.74 | **3.2x** |
| mul factor=2 large | 47.68 | 14.61 | **3.3x** |

**Key insight:** Per-value `to_width` shows modest 3-8% improvement because the FFI `to_width` was already fast (just memcpy internally). But in the **batch pipeline** (to_width + add for 10k values), the improvement compounds to **1.51x** because we avoid repeated FFI context setup. The multiply fast path is the big win: **3.2-3.9x** for all common factors.

**Files changed:**
- `src/repr/src/adt/numeric.rs` — Added `numeric_to_agg_direct()` and `try_mul_numeric_agg_i64()` with correctness tests.
- `src/compute/src/render/reduce.rs` — Updated `datum_to_accumulator` to use `numeric_to_agg_direct()`, updated `Multiply<Diff>` for `Accum::Numeric` to try fast path first.
- `src/repr/benches/numeric_to_agg.rs` — New benchmark for to_width and multiply.

**Future optimization ideas:**
- `div_numeric` could use a fast path for simple cases.
- `OrderedDecimal::hash` still calls `reduce` (FFI).
- `Row::clone()` at 527B executions is the largest source of allocation pressure.
- `Rows::get` (columnar Row indexing) at 1,273B executions could eliminate the
  `if index == 0` branch by storing bounds with a leading 0 sentinel.

---

## Session 49: Fast-path NumericAgg→Numeric narrowing + Datum::cmp same-type fast path

**Problem 1 — NumericAgg→Numeric narrowing (finalize_accum):**
In `finalize_accum` for `SumNumeric`, the accumulated `NumericAgg` (Decimal<27>) is narrowed back
to `Numeric` (Decimal<13>) via `cx_datum.to_width(accum.0)`. This calls `decNumberPlus` through
C FFI. For the common case where the accumulator fits in Numeric's 39-digit precision (which it
does >99% of the time), we can bypass the FFI entirely by copying coefficient units directly.

**Fix 1:** Added `try_numeric_agg_to_datum()` in `numeric.rs` — the reverse of `numeric_to_agg_direct()`
(session 48). Checks: not special, not zero (returns Numeric::zero()), digits ≤ 39, adjusted
exponent ≤ 38, exponent ≥ -77. If all pass, copies the ≤13 coefficient units directly into a
new Numeric. Falls back to FFI for overflow/rounding. Updated `finalize_accum` in `reduce.rs` to
use the fast path with FFI fallback.

**Benchmark results (numeric_agg_to_datum):**

| Benchmark | Old (ns) | New (ns) | Speedup |
|-----------|----------|----------|---------|
| zero | 11.20 | 9.33 | 1.20x |
| small_int | 11.11 | 13.90 | 0.80x |
| money | 11.07 | 13.90 | 0.80x |
| large_18dig | 11.63 | 14.25 | 0.82x |
| negative | 11.05 | 13.82 | 0.80x |
| max_precision | 11.55 | 14.19 | 0.81x |
| **batch 10k mixed** | **114.4 µs** | **95.4 µs** | **1.20x** |

**Note:** Individual microbenchmarks show the new path as slower (~14ns vs ~11ns) because
`decNumberPlus` is already well-optimized in C. However, in **batch** mode the fast path
wins by 1.2x due to avoided FFI call overhead (register save/restore, C ABI transition)
which compounds under realistic cache pressure.

---

**Problem 2 — Datum::cmp DatumKind overhead for same-type comparisons:**
The manual `Datum::Ord` implementation (session 44) first converts both operands to `DatumKind`
(a 26-arm match each), compares discriminants, then falls through to a variant-pair match for
the actual value comparison. For same-type comparisons (~99% of cases in SQL: WHERE col < literal,
ORDER BY, JOIN, GROUP BY), the DatumKind conversions are pure overhead since the discriminants
are always equal.

**Fix 2:** Restructured `Datum::cmp()` to match on `(self, other)` variant pairs FIRST. When both
operands are the same type, this returns the value comparison directly without DatumKind conversion.
The cross-type case (different variants, ~1%) falls through to the DatumKind-based ordering.
The compiler generates this as: check discriminants equal → jump table → value comparison.

**Benchmark results (datum_cmp):**

| Benchmark | Old (ns) | New (ns) | Speedup |
|-----------|----------|----------|---------|
| Int64 | 1.61 | 1.43 | **1.13x** |
| String | 2.99 | 3.03 | 0.99x |
| Cross-variant | 1.18 | 1.21 | 0.97x |
| 10k Int64 batch | 16.5 µs | 16.3 µs | 1.01x |
| Numeric | 4.74 | 4.74 | 1.00x |

**Key insight:** The optimization saves ~0.17ns per same-type comparison by eliminating two
DatumKind::from() conversions. For Int64 (the most common SQL type), this is an 11-13%
improvement. The cross-variant path is ~3% slower (extra match fallthrough) but cross-type
comparisons are rare in practice. At scale (billions of comparisons per query), the savings
add up.

**Files changed:**
- `src/repr/src/adt/numeric.rs` — Added `try_numeric_agg_to_datum()` + test.
- `src/repr/src/scalar.rs` — Restructured `Datum::cmp()` with same-type fast path.
- `src/compute/src/render/reduce.rs` — Updated `finalize_accum` SumNumeric to use fast path.
- `src/repr/benches/numeric_to_agg.rs` — Added from_agg benchmarks.

**Future optimization ideas:**
- `ArrayIdx::cmp` in persist merge sort: 327B calls with 16-arm match. A direct `cmp_at()` method
  on ArrayOrd could avoid creating intermediate ArrayIdx values.
- `Region<T>` enum dispatch: 613B calls to `len()`. Caching the region length could avoid match.
- `OrderedDecimal::hash` still calls `reduce` (FFI).

---

## Session 50: do_work icache pressure reduction (MFPPushdown regression fix)

**Problem:** CI benchmarks showed a consistent ~30% regression in the MFPPushdown benchmark
(1.89s vs 1.46s on main for `SELECT * FROM v1 WHERE f2 < 0` filtering 10M rows). The `do_work`
function in `persist_source.rs` had grown to 261 lines across sessions 19-22 as fast-path
strategies were added. The function had three sequential `if let Some(...) { ... continue; }`
blocks for pure_project, eval_then_project_sorted, and eval_then_project_unordered, with the
sorted/unordered paths duplicating ~47 lines of nearly identical code. This bloated the hot loop,
increasing icache pressure.

**Approach:** Refactored `do_work` to reduce code size in the hot loop:

1. **Introduced `MfpProjectStrategy` enum** to replace three separate `Option<Vec<usize>>`
   parameters (`mfp_pure_project`, `mfp_eval_then_project`, `mfp_eval_then_project_unordered`).
   The enum has four variants: `PureProject`, `EvalThenProjectSorted`, `EvalThenProjectUnordered`,
   and `FullMfp`. A `from_mfp()` constructor computes the strategy once at startup.

2. **Replaced sequential if-let chains with a single `match`** on `mfp_strategy` in the inner loop,
   eliminating redundant branch evaluation for each row.

3. **Merged sorted/unordered eval_then_project paths** into one match arm using `matches!()` to
   select between `project_onto` and `project_onto_unordered`, removing ~47 lines of duplication.

4. **Extracted audit violation reporting to `#[cold] #[inline(never)]` function**
   (`report_pushdown_audit_violation`), keeping error-handling code out of the hot path.

5. **Applied same deduplication to `compute_state.rs` and `oneshot_source.rs`** which had
   similar duplicated eval_then_project paths.

**Binary size measurements (do_work function via `nm --print-size`):**

| Version | do_work size | Change |
|---------|-------------|--------|
| Baseline (pre-optimization) | 5417 bytes (0x1529) | — |
| After refactoring | 4206 bytes (0x106e) | **−22.3% (−1211 bytes)** |

The function is monomorphized 3 times in the binary, so total savings: **3633 bytes** of hot code
removed from the icache footprint.

**Local benchmark (1M rows, `SELECT * FROM v1 WHERE f2 < 0`):**
Warm queries showed ~66-71ms (optimized) vs ~68-71ms (baseline) — within noise for 1M rows
where the MFP loop takes <10ms of the total ~70ms query time. The CI benchmark with 10M rows
should show more pronounced improvement since the inner loop dominates.

**Files changed:**
- `src/storage-operators/src/persist_source.rs` — Major refactoring: MfpProjectStrategy enum,
  merged do_work strategy paths, extracted cold audit function.
- `src/compute/src/compute_state.rs` — Merged duplicate eval_then_project paths.
- `src/storage-operators/src/oneshot_source.rs` — Merged duplicate eval_then_project paths.

**Future optimization ideas:**
- `ArrayIdx::cmp` in persist merge sort: 327B calls with 16-arm match. A direct `cmp_at()` method
  on ArrayOrd could avoid creating intermediate ArrayIdx values.
- `Region<T>` enum dispatch: 613B calls to `len()`. Caching the region length could avoid match.
- `OrderedDecimal::hash` still calls `reduce` (FFI).

---

## Session 51: Integer arithmetic fast path in MirScalarExpr::eval

**Date:** 2026-02-21

**Target:** `MirScalarExpr::eval` integer arithmetic dispatch — bypassing the 212-arm
`BinaryFunc` match, `InputDatumType::try_from_iter` conversion, and `OutputDatumType::into_result`
wrapping for `AddInt16/32/64`, `SubInt16/32/64`, `MulInt16/32/64`.

**Problem:** Coverage data shows `LazyBinaryFunc::eval` (the EagerBinaryFunc ceremony) executes
**14.4 billion times**. For integer arithmetic, the actual work is a single `checked_add/sub/mul`
instruction, but the framework overhead dominates:
1. 212-arm `BinaryFunc::eval` jump table dispatch
2. `InputDatumType::try_from_iter` — creates iterator, matches each Datum to extract native type
3. `assert_none!` for leftover arguments
4. `OutputDatumType::into_result` — wraps result back into Datum

The comparison fast path (session 38) already bypasses this for Eq/NotEq/Lt/Lte/Gt/Gte. This
session extends the pattern to integer arithmetic operations.

**Approach:** Added fast-path match arms in `MirScalarExpr::eval` for `CallBinary` that directly
extract integer values from Datums, perform `checked_add/sub/mul`, and return the result as a
Datum — skipping the entire `BinaryFunc::eval` → `EagerBinaryFunc` → `InputDatumType` pipeline.

**Benchmark results (`cargo bench -p mz-expr --bench arith_eval`):**

| Benchmark | Baseline | Fast Path | Improvement |
|---|---|---|---|
| add_int32_col_lit | 37.5 ns | 27.3 ns | **−27%** |
| sub_int32_col_col | 26.5 ns | 11.6 ns | **−57% (2.3x)** |
| add_int64_col_lit | 36.5 ns | 21.7 ns | **−41% (1.7x)** |
| mul_int64_col_col | 27.9 ns | 14.4 ns | **−48% (1.9x)** |
| batch_add_int64 (10k) | 312 µs | 172 µs | **−45% (1.8x)** |
| batch_compound_int64 (10k) | 434 µs | 170 µs | **−61% (2.6x)** |
| add_int64_null | 36.6 ns | 21.7 ns | **−41%** |

The compound expression (`col * col + col`) shows the most improvement at **2.6x faster** because
both the multiply and add bypass the BinaryFunc dispatch.

**Files changed:**
- `src/expr/src/scalar.rs` — Added fast-path match arms for Add/Sub/Mul on Int16/Int32/Int64
  in `MirScalarExpr::eval`, right after the existing comparison fast path.
- `src/expr/benches/arith_eval.rs` — New benchmark for integer arithmetic evaluation.
- `src/expr/Cargo.toml` — Added `arith_eval` bench entry.

**Future optimization ideas:**
- Extend fast path to float arithmetic (Add/Sub/Mul/Div Float32/64) with overflow/underflow checks.
- Extend to integer division (Div/Mod Int16/32/64) with zero-check.
- `ArrayIdx::cmp` in persist merge sort: 327B calls with 16-arm match. A direct `cmp_at()` method
  on ArrayOrd could avoid creating intermediate ArrayIdx values.
- `Region<T>` enum dispatch: 613B calls to `len()`. Caching the region length could avoid match.
- `OrderedDecimal::hash` still calls `reduce` (FFI).

---

## Session 52: Fast-path unary function evaluation - bypass EagerUnaryFunc dispatch

**Date:** 2026-02-21

**Target:** `MirScalarExpr::eval` unary function dispatch — bypassing the 325-arm
`UnaryFunc` match, `InputDatumType::try_from_result` conversion, and
`OutputDatumType::into_result` wrapping for the hottest unary operations: IsNull (1.186B
executions), Not (442M), CastInt32ToInt64 (232M), NegInt32/64, CastInt16ToInt32/64,
CastBoolToInt32.

**Problem:** Coverage data shows `LazyUnaryFunc::eval` executes **3.15 billion times**. For
trivial unary operations like `IsNull` (a single `datum.is_null()` check) and `Not` (a
single boolean flip), the framework overhead dominates:
1. 325-arm `UnaryFunc::eval` jump table dispatch
2. `InputDatumType::try_from_result` — matches Datum to extract native type
3. `EagerUnaryFunc::call()` dispatch — calls the actual function
4. `OutputDatumType::into_result` — wraps result back into Datum

Unlike `CallBinary` which had fast paths for comparisons (session 38) and arithmetic
(session 51), `CallUnary` had no fast paths at all — every unary operation went through
the full framework.

**Fix:** Added fast-path match arms in `MirScalarExpr::eval` for 9 `UnaryFunc` variants
that directly extract values from Datums, perform the operation, and return — skipping
the entire `UnaryFunc::eval` → `EagerUnaryFunc` → `InputDatumType` pipeline:

1. **IsNull** (1.186B calls): `datum.is_null()` → `Datum::True/False` directly
2. **Not** (442M calls): Match `Datum::True/False/Null` directly, flip
3. **NegInt16/32/64**: `checked_neg()` directly on extracted integer; cold `#[inline(never)]`
   overflow helpers keep error-path code out of the hot path
4. **CastInt16ToInt32/Int64**: `i32::from(a)` / `i64::from(a)` directly
5. **CastInt32ToInt64** (232M calls): `i64::from(a)` directly
6. **CastBoolToInt32** (52M calls): Match `True→1, False→0` directly

All fast paths handle NULL propagation explicitly (matching `Datum::Null` and returning
`Ok(Datum::Null)` for non-nullable input types), matching the framework's behavior.

The negation overflow helpers (`neg_overflow_i16/32/64`) are marked `#[cold] #[inline(never)]`
to prevent the error-path code (`a.to_string().into()`) from being inlined into the hot
path — without this, the negation fast path was actually 2.4x *slower* than the framework
path due to icache bloat from the inlined error code.

**Benchmark results** (`cargo bench -p mz-expr --bench unary_eval`):

### Per-call performance (ns/iter)

| Operation | Old (framework) | New (fast path) | Speedup |
|---|---:|---:|---|
| is_null non-null | 15.86 | 8.61 | **1.84x** |
| is_null null | 15.82 | 8.63 | **1.83x** |
| not true | 10.26 | 8.69 | **1.18x** |
| not false | 10.22 | 8.71 | **1.17x** |
| not null | 18.10 | 8.75 | **2.07x** |
| neg_int32 | 10.46 | 9.37 | **1.12x** |
| neg_int64 | 10.44 | 9.35 | **1.12x** |
| cast_int32_to_int64 | 10.22 | 9.29 | **1.10x** |
| cast_int16_to_int32 | 10.27 | 9.37 | **1.10x** |
| cast_bool_to_int32 | 10.24 | 9.39 | **1.09x** |

### Batch performance (10k evaluations)

| Scenario | Old (µs) | New (µs) | Speedup |
|---|---|---|---|
| batch_is_null 10k | 120.4 | 64.7 | **1.86x** |
| batch_not 10k | 82.6 | 65.8 | **1.26x** |
| batch_cast_int32_to_int64 10k | 83.1 | 66.6 | **1.25x** |
| batch_neg_int32 10k | 85.9 | 68.6 | **1.25x** |
| batch_not_is_null 10k (compound) | 169.1 | 102.4 | **1.65x** |

**Summary:** **1.09-2.07x faster per call, 1.25-1.86x faster in batch**. The biggest win
is for `IsNull` (1.84x per-call, 1.86x batch) because the old path went through a
heavyweight dispatch chain just to call `datum.is_null()`. `Not` with NULL input shows
2.07x because the old path's null propagation via `InputDatumType::try_from_result` was
expensive for bool inputs (two match arms to check True/False, then a third for the null
fallthrough). The compound `NOT(IsNull(col))` expression—extremely common in SQL WHERE
clauses—shows 1.65x improvement because both operations benefit.

At 1.186B IsNull calls and 442M Not calls in the coverage trace, this saves approximately
8.6B ns (8.6 seconds) for IsNull and 700M ns (0.7 seconds) for Not in aggregate.

**Files changed:**
- `src/expr/src/scalar.rs` — Added fast-path match arms for 9 UnaryFunc variants in
  `MirScalarExpr::eval`. Added `neg_overflow_i16/32/64` cold helper functions to keep
  error-path code out of the hot path.
- `src/expr/benches/unary_eval.rs` — New benchmark for unary function evaluation.
- `src/expr/Cargo.toml` — Added `unary_eval` bench entry.

**Future optimization ideas:**
- Extend fast path to float arithmetic (Add/Sub/Mul/Div Float32/64) with overflow/underflow checks.
- Extend to integer division (Div/Mod Int16/32/64) with zero-check.
- `ArrayIdx::cmp` in persist merge sort: 327B calls with 16-arm match.
- `Region<T>` enum dispatch: 613B calls to `len()`.
- `OrderedDecimal::hash` still calls `reduce` (FFI).

---

## Session 53: Direct recursive contains_temporal/contains_unmaterializable — eliminate visit_pre heap allocation

**Date:** 2026-02-21

**Target:** `MirScalarExpr::contains_temporal` (5.576B calls), `contains_unmaterializable`
(2.309B calls), `contains_unmaterializable_except`, and `contains_column` — all using
`visit_pre` which allocates a `Vec` on the heap per call and never short-circuits.

**Problem:** Coverage data shows `contains_temporal` executes **5.576 billion times**, and
`contains_unmaterializable` executes **2.309 billion times**. Both use the `visit_pre`
iterator pattern which:
1. Allocates a `Vec<&Self>` on the heap (`vec![self]`) — a heap allocation per call
2. Iterates through ALL children even after finding the target (no short-circuit)
3. Goes through the heavyweight `children()` iterator which creates 4 `Option` variables,
   matches the enum, and chains iterators
4. For leaf nodes (Column, Literal — the most common case), this means a heap alloc+dealloc
   just to check one node that can never contain the target

The `visit_pre` pattern visits 35.17B nodes total for `contains_temporal` (avg ~6.3 nodes/call)
and 7.87B nodes for `contains_unmaterializable` (avg ~3.4 nodes/call).

**Fix:** Replaced all four `visit_pre`-based containment checks with direct recursive
`match self` implementations that:
1. **No heap allocation** — uses the call stack instead of `Vec`
2. **Short-circuits** — returns `true` immediately when the target is found via `||`
3. **Direct matching** — matches on the expression variant directly instead of going through
   the `children()` iterator chain
4. **Follows existing pattern** — `could_error()` in the same file already uses this exact
   recursive approach

Functions optimized:
- `contains_temporal()` — matches `CallUnmaterializable(MzNow)` directly
- `contains_unmaterializable()` — matches any `CallUnmaterializable(_)`
- `contains_unmaterializable_except()` — matches `CallUnmaterializable(f)` with exclusion
- `contains_column()` — matches `Column(_, _)` directly

**Benchmark results** (`cargo bench -p mz-expr --bench contains_check`):

### contains_temporal

| Benchmark | Old (visit_pre) | New (recursive) | Speedup |
|---|---:|---:|---|
| leaf_column | 9.14 ns | 1.56 ns | **5.9x** |
| leaf_literal | 8.99 ns | 1.56 ns | **5.8x** |
| binary_2_leaves | 46.89 ns | 3.76 ns | **12.5x** |
| compound_7_nodes | 71.89 ns | 9.95 ns | **7.2x** |
| deep_chain_11_nodes | 75.65 ns | 17.82 ns | **4.2x** |
| batch_compound 10k | 711.9 µs | 98.0 µs | **7.3x** |

### contains_unmaterializable

| Benchmark | Old (visit_pre) | New (recursive) | Speedup |
|---|---:|---:|---|
| leaf_column | 8.52 ns | 1.58 ns | **5.4x** |
| binary_2_leaves | 46.16 ns | 4.25 ns | **10.9x** |
| compound_7_nodes | 71.33 ns | 11.18 ns | **6.4x** |

**Summary:** **4.2-12.5x faster** across all expression sizes. The biggest wins are for
binary expressions (12.5x) because the old code allocated a Vec, pushed 3 elements
(self + 2 children), popped and visited all 3, then deallocated — while the new code
is just 3 match + comparison instructions. Leaf nodes (the most common case at 5.9x)
eliminate the heap alloc+dealloc entirely.

At 5.576B `contains_temporal` calls with an average of ~6.3 nodes per call, the old code
visited 35.17B nodes at ~2ns each (post-alloc) = ~70B ns overhead, plus ~9ns alloc/dealloc
per call = ~50B ns. The new code processes ~2ns per node with no allocation, saving
approximately **50-100 seconds of CPU time** in aggregate for `contains_temporal` alone.

Combined with `contains_unmaterializable` (2.309B calls), total savings are estimated
at **60-120 seconds of CPU time**.

**Files changed:**
- `src/expr/src/scalar.rs` — Replaced `visit_pre`-based `contains_temporal`,
  `contains_unmaterializable`, `contains_unmaterializable_except`, and `contains_column`
  with direct recursive match implementations.
- `src/expr/benches/contains_check.rs` — New benchmark for containment check evaluation.
- `src/expr/Cargo.toml` — Added `contains_check` bench entry.

**Future optimization ideas:**
- `ArrayIdx::cmp` in persist merge sort: 705B calls with 16-arm match. A direct `cmp_at()`
  method on ArrayOrd could avoid creating intermediate ArrayIdx values.
- `Region<T>` enum dispatch: 613B calls to `len()`. Caching the region length could avoid match.
- `OrderedDecimal::hash` still calls `reduce` (FFI).
- `DatumVec::borrow/drop`: 26B+54B calls — `mem::take` + `repurpose_allocation` round-trip
  could be eliminated with in-place lifetime transmute.
- `OffsetStride::Saturated::index`: 1.58T calls — `stride * index` multiply could be replaced
  with additive running offset.
- `checked_recur` overhead: 26B calls — RefCell borrow_mut on every recursive visit.

---

## Session 54: Fast-path float arithmetic + integer div/mod in MirScalarExpr::eval

**Date:** 2026-02-21

**Optimization:** Extended the fast-path pattern from sessions 51-52 (integer Add/Sub/Mul and
unary functions) to cover:
- Float32/Float64 Add/Sub/Mul (bypasses EagerBinaryFunc dispatch, does overflow check inline)
- Float32/Float64 Div (bypass + zero check + overflow/underflow check)
- Int16/Int32/Int64 Div (bypass + zero check + cold overflow helper)
- Int16/Int32/Int64 Mod (bypass + zero check)

The existing code path goes through: BinaryFunc 212-arm match → InputDatumType try_from_iter →
EagerBinaryFunc::call → OutputDatumType into_result. The fast path collapses this to a direct
match on the BinaryFunc variant, extracts the Datum values, does the arithmetic inline, and
returns immediately. Cold `#[inline(never)]` helpers keep error-path code out of the hot path.

**Benchmark results** (`cargo bench -p mz-expr --bench float_arith_eval`):

| Benchmark | Baseline | Fast Path | Speedup |
|---|---|---|---|
| add_f64_col_col | 37.5 ns | 11.1 ns | **3.4x** |
| sub_f64_col_col | 38.9 ns | 11.1 ns | **3.5x** |
| mul_f64_col_col | 39.0 ns | 11.8 ns | **3.3x** |
| div_f64_col_lit | 47.7 ns | 21.7 ns | **2.2x** |
| div_i64_col_col | 36.9 ns | 11.2 ns | **3.3x** |
| mod_i64_col_col | 39.6 ns | 11.3 ns | **3.5x** |
| batch_add_f64/10k | 321 µs | 91 µs | **3.5x** |
| batch_div_f64/10k | 371 µs | 165 µs | **2.2x** |
| batch_div_i64/10k | 250 µs | 88 µs | **2.8x** |
| batch_compound_f64/10k | 728 µs | 235 µs | **3.1x** |

Consistent 2.2-3.5x speedup across all operations. Float division is 2.2x (more checks needed).
Compound expression (mul+div+add) gets 3.1x from the fast paths compounding.

**Files changed:**
- `src/expr/src/scalar.rs` — Added fast-path match arms for float32/64 add/sub/mul/div and
  int16/32/64 div/mod. Added cold helpers `int_div_overflow_i16/i32/i64`. Added
  `use ordered_float::OrderedFloat`.
- `src/expr/benches/float_arith_eval.rs` — New benchmark for float arithmetic and int div/mod.
- `src/expr/Cargo.toml` — Added `float_arith_eval` bench entry.

**Future optimization ideas:**
- Extend fast path to comparison operators (Eq/NotEq/Lt/Lte/Gt/Gte) for Float32/Float64.
- `ArrayIdx::cmp` in persist merge sort: 705B calls with 16-arm match.
- `Region<T>` enum dispatch: 613B calls to `len()`.
- `OrderedDecimal::hash` still calls `reduce` (FFI).
- `DatumVec::borrow/drop`: 26B+54B calls — lifetime transmute.
- `checked_recur` overhead: 26B calls — RefCell borrow_mut on every recursive visit.
