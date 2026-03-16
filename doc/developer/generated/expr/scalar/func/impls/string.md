---
source: src/expr/src/scalar/func/impls/string.rs
revision: 703a0c27c8
---

# mz-expr::scalar::func::impls::string

Provides scalar function implementations for `text` datums: case transformation (`upper`, `lower`), `substring`, `length`, `char_length`, `ltrim`/`rtrim`/`btrim`, `lpad`/`rpad`, `replace`, `split_part`, `regexp_match`, `regexp_replace`, `ascii`, `chr`, `repeat`, `concat`, type casts to/from all other supported types, and string formatting.
This is the largest impls file, as text manipulation is central to SQL.
