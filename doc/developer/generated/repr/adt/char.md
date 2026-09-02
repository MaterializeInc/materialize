---
source: src/repr/src/adt/char.rs
revision: c6be08fe4f
---

# mz-repr::adt::char

Defines `Char` (fixed-length character type) and `CharLength` (its length constraint), mirroring PostgreSQL's `bpchar` type.

`CharLength` is serialized under the name `"CharMaxLength"` to avoid a naming conflict with the `char_length` SQL function's generated struct in the stable LIR schema registry, which requires container names to be unique. Its `RustType<ProtoCharLength>` implementation re-validates domain constraints on deserialization rather than trusting the wire value.

`format_str_trim` and `format_str_pad` are the two public entry points for applying char/varchar semantics to a string: `format_str_trim` strips trailing whitespace and is suitable for storing values in `Datum::String`; `format_str_pad` blank-pads to the declared length and is suitable for returning values to clients. Both delegate to the private `format_char_str` function with the appropriate `CharWhiteSpace` variant.
