---
source: src/ore/src/sql.rs
revision: 12fbe31d24
---

# mz-ore::sql

Composable, escape-aware SQL fragment building.

`Sql` wraps a `Cow<'static, str>` and provides safe constructors:
- `Sql::new` — trusted static SQL text.
- `Sql::ident` — PostgreSQL double-quote identifier escaping (doubles embedded `"`).
- `Sql::literal` — PostgreSQL single-quote literal escaping (doubles embedded `'`).
- `Sql::param` — positional parameter (`$N`, one-based, range 1–65535).
- `Sql::join` — joins multiple `Sql` fragments with a static separator.
- `Sql::raw_unchecked` — wraps an owned string without escaping; caller asserts safety.
- `Sql::trusted_external_request` — marks the trust boundary for complete SQL received over the external HTTP/WebSocket SQL API or MCP query tools; must not be used elsewhere.

`Sql::format` performs `{}`-placeholder substitution (with `{{`/`}}` brace escaping), returning a `SqlFormatError` on mismatch between placeholder count and argument count. `Sql::format_unchecked` is the panicking variant used only by the `sql!` macro.

The `sql!` macro accepts a static template literal with `{}` placeholders and verifies at compile time (via `sql_template_placeholder_count`) that the placeholder count matches the supplied argument count.

`Sql` implements `serde::Deserialize` via `Sql::trusted_external_request` and `serde::Serialize` as a plain string. `From<T>` is implemented for integer types `i16`, `i32`, `i64`, `isize`, `u16`, `u32`, `u64`, and `usize`.
