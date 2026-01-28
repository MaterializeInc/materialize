---
headless: true
---
To create a replacement materialized view, you must:
- Specify the target materialized view.
- Specify a `SELECT` statement for the replacement view that produces the same
  output schema (including column order and keys) as the target view.
