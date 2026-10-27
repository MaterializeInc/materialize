# Row encoding and its limits

`Row` is Materialize's in-memory representation of a tuple of `Datum`s.
It is a `Tag`-based byte sequence defined in `src/repr/src/row.rs`, where each datum is a one-byte tag followed by a tag-specific payload.
This document collects the size limits the encoding and its datum types impose, so that callers know what a `Row` can hold and where overflow is rejected.
The limits live in scattered constants across `src/repr/src`; this is the index to them.

## What the encoding is (and isn't)

The `Tag` encoding has three properties that bound how these limits may change.

* It is **not durably persisted**: persist stores data as Arrow (`ProtoRow`), so the in-memory layout can change without a migration.
* `Row` **sort order is implementation-defined**, so layout changes cannot break ordering correctness.
* `Row` **equality is byte equality**, so any value must encode to identical bytes regardless of how it was built.

These limits are therefore in-memory-`Row` limits, distinct from durable (persist) limits and from transport limits.

## Per-datum limits

| Datum type | Limit | Enforcement | Source |
| --- | --- | --- | --- |
| `String` / `Bytes` / `List` payload | up to `u64::MAX` bytes (tag tiers Tiny `u8`, Short `u16`, Long `u32`, Huge `u64`) | none; bounded by `usize` and memory | `row.rs` `Tag`, `read_lengthed_datum` |
| `char(n)` / `varchar(n)` length | ≤ 10 MiB (`10_485_760`) | error at type construction | `adt/char.rs::MAX_LENGTH`, `adt/varchar.rs::MAX_MAX_LENGTH` |
| `numeric` precision | ≤ 39 significant digits (`13 * 3`) | error / rounding at construction | `adt/numeric.rs::NUMERIC_DATUM_MAX_PRECISION` |
| array dimensions | ≤ 6 | `InvalidArrayError` at construction | `adt/array.rs::MAX_ARRAY_DIMENSIONS` |
| `regex` pattern | ≤ 1 MiB source, ≤ 10 MiB compiled | error at compilation | `adt/regex.rs::MAX_REGEX_SIZE_BEFORE_COMPILATION` / `_AFTER_COMPILATION` |

## Whole-`Row` size

A `Row` itself stores its bytes in a `CompactBytes` (heap-backed like `Vec<u8>` once it spills past 23 inline bytes), so the in-memory value is bounded by `usize` and available memory rather than by a fixed cap.
The containers a `Row` flows through impose tighter bounds:

* **Arrangements** store `Row`s in the columnar `Rows` container (`impl Columnar for Row`), whose offset bounds are `u64` (default `Vec<u64>`). This does *not* cap a row at 32 bits.
* **Persist** encodes columns as Arrow `BinaryArray` / `StringArray`, which use 32-bit (`i32`) offsets. The cumulative offsets within a part's column must fit `i32`, so a single encoded `Row` (and each variable-length column) is bounded to **< 2 GiB** on the durable path. Parts target far less (~128 MiB) in practice.

A `Row` is also bounded earlier by:

* the per-statement result size (`max_result_size` session variable; see `doc/developer/design/20250415_large_select_result_size.md`), and
* the transport message size (gRPC) between `environmentd` and clusters.

## Maintaining this document

When adding a datum type, a length-tiered tag, or a fixed-size header, record the limit here with its enforcing constant and whether overflow is a graceful error, a panic, or unbounded.
Keep the table pointing at the constant, not a copied literal, so it does not drift.
