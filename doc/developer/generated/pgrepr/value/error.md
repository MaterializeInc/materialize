---
source: src/pgrepr/src/value/error.rs
revision: f147b1341a
---

# mz-pgrepr::value::error

Defines error types for decoding wire-format values and for fallible conversions from `Value` to `mz_repr::Datum`.

`NulCharacterError` is returned when a decoded text value contains a NUL (0x00) character, which PostgreSQL-compatible text values must never contain. It displays as `invalid byte sequence for encoding "UTF8": 0x00`, matching PostgreSQL's message.

`IntoDatumError` is the error type for fallible conversions from `Value` to `mz_repr::Datum`.
The enum wraps `InvalidRangeError` and `InvalidArrayError` from `mz_repr`, providing `From` implementations so callers can use `?` when constructing ranges or arrays during datum conversion.
