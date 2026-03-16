---
source: src/sql-pretty/src/util.rs
revision: 74ebdd68dd
---

# mz-sql-pretty::util

Provides low-level `RcDoc` combinators used by the `doc` module: `nest`, `nest_title`, `comma_separate`, `comma_separated`, `bracket`, and `bracket_doc`.
These wrap the `pretty` crate's primitives to apply the crate-wide `TAB` indent, comma-with-newline separators, and grouping, keeping the doc-building code in `doc.rs` concise.
