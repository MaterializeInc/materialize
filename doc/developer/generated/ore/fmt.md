---
source: src/ore/src/fmt.rs
revision: e757b4d11b
---

# mz-ore::fmt

Defines `FormatBuffer`, a trait for infallible string/byte buffer targets that unifies `String`, `Vec<u8>`, and (under the `network` feature) `bytes::BytesMut` behind a common `write_fmt`/`write_char`/`write_str` interface.

Unlike `std::fmt::Write`, `FormatBuffer` methods do not return `Result`, reflecting the fact that writing to these in-memory types cannot fail.
Functions that need to write formatted output into either a `String` or a byte buffer generically accept `&mut impl FormatBuffer`, avoiding the need to duplicate logic for each concrete type.
