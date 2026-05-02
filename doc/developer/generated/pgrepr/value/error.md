---
source: src/pgrepr/src/value/error.rs
revision: ccd84bf8e8
---

# mz-pgrepr::value::error

Defines `IntoDatumError`, the error type for fallible conversions from `Value` to `mz_repr::Datum`.
The enum wraps `InvalidRangeError` and `InvalidArrayError` from `mz_repr`, providing `From` implementations so callers can use `?` when constructing ranges or arrays during datum conversion.
