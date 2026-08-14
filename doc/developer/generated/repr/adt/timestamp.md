---
source: src/repr/src/adt/timestamp.rs
revision: 32c1e1d39
---

# mz-repr::adt::timestamp

Defines `CheckedTimestamp<T>` wrapping `chrono::NaiveDateTime` or `chrono::DateTime<Utc>` with validation against PostgreSQL timestamp bounds (`LOW_DATE`/`HIGH_DATE`), and `TimestampPrecision` for the optional sub-second scale.

The supported date range (4713 BC to 262142-12-31) is the intersection of PostgreSQL, Avro, and chrono's representable ranges. `CheckedTimestamp::from_timestamplike` enforces this range on construction.

`TimeLike` and `DateLike` extension traits provide common extraction methods (epoch, year, century, millennium, ISO week, etc.) for any `chrono::Timelike` or `chrono::Datelike` implementor. `TimestampLike` composes them with truncation and rounding operations and is implemented for both `NaiveDateTime` and `DateTime<Utc>`.

`CheckedTimestamp::round_to_precision` rounds a timestamp to the specified `TimestampPrecision` (0–6), returning `TimestampError::OutOfRange` when rounding up would leave chrono's representable range. `CheckedTimestamp::age` and `CheckedTimestamp::diff_as` compute calendar-aware differences between two timestamps.

`PackedNaiveDateTime` is a 16-byte fixed-size encoding of `NaiveDateTime` that preserves sort order, implementing `FixedSizeCodec<NaiveDateTime>`.

`checked_add_with_leapsecond` and `checked_sub_with_leapsecond` shift a `NaiveDateTime` by a `FixedOffset` without panicking at the edges of chrono's range, and correctly handle leap-second representations that would become invalid after the shift.
