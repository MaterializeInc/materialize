---
source: src/testdrive/src/format/bytes.rs
revision: 5b2cefc829
---

# testdrive::format::bytes

Provides `unescape`, a small function that interprets testdrive byte-string literals.
The only special escape sequence is `\xNN` for a two-digit hex byte; all other backslash-prefixed characters are taken literally.
