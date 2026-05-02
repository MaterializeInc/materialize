---
source: src/repr/src/strconv.rs
revision: 4267863081
---

# mz-repr::strconv

Provides `parse_VARIANT` / `format_VARIANT` function pairs for converting each SQL scalar type between its PostgreSQL string representation and the corresponding Rust value type.
Functions deliberately operate on intermediate types (not `Datum` directly) so the logic can be reused by `pgrepr` and other layers that need string conversion without full datum context.
Deviations from PostgreSQL string format are considered bugs.
