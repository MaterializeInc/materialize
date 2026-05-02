---
source: src/ore/src/lex.rs
revision: a92e6fe74f
---

# mz-ore::lex

Provides `LexBuf`, a cursor-based helper for hand-written lexers operating over a string slice.
`LexBuf` implements `Iterator<Item = char>` and exposes methods for peeking (`peek`), stepping back (`prev`), consuming a specific character or string prefix (`consume`, `consume_str`), reading a fixed number of characters (`next_n`), scanning to a delimiter (`take_to_delimiter`), and scanning while a predicate holds (`take_while`).
