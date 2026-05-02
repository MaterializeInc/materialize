---
source: src/sql-parser/src/ident.rs
revision: 4267863081
---

# mz-sql-parser::ident

Defines the `ident!` macro for constructing validated `Ident` values from string literals at compile time.
The macro enforces length and character invariants at compile time, making it preferable to `Ident::new` in contexts where the value is a known constant.
