# mz_sql_parser/src

Source root of the `mz-sql-parser` crate. Cargo-conventional location;
no module boundary distinct from the crate itself.

See [`../CONTEXT.md`](../CONTEXT.md) for the crate's role (SQL-text → `AST<Raw>`
pure-syntax boundary) and key concepts.

## Subdirs reviewed (≥5K LOC)

- [`ast/`](ast/CONTEXT.md) — AST node definitions and visitor traits (11,303 LOC)
  - [`ast/defs/`](ast/defs/CONTEXT.md) — grammar-region split: statement, query, expr, name, etc. (10,256 LOC)
