---
source: src/expr/src/scalar/func/encoding.rs
revision: e757b4d11b
---

# mz-expr::scalar::func::encoding

Implements PostgreSQL-compatible binary-to-text encoding and decoding for the SQL `encode`/`decode` functions.
Defines the `Format` trait and three implementations: `Base64Format` (RFC 2045 / MIME-style with 76-char line wrapping), `EscapeFormat` (octal escape sequences for non-printable bytes), and `HexFormat`.
Exports `lookup_format`, which resolves a format name string (case-insensitive) to a `&'static dyn Format`.
