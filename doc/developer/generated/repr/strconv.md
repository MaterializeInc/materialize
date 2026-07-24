---
source: src/repr/src/strconv.rs
revision: 512f45eb6f
---

# mz-repr::strconv

Provides `parse_VARIANT` / `format_VARIANT` function pairs for converting each SQL scalar type between its PostgreSQL string representation and the corresponding Rust value type.
Functions deliberately operate on intermediate types (not `Datum` directly) so the logic can be reused by `pgrepr` and other layers that need string conversion without full datum context.
Deviations from PostgreSQL string format are considered bugs.
`parse_oid` accepts the full `u32` range directly (plus negative `i32` values reinterpreted as `u32`).
`parse_oid_legacy` accepts only the `i32` range (the historical behavior) and is used for persisted PostgreSQL source cast expressions that must remain evaluation-stable.
