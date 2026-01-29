---
headless: true
---
You cannot run `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT` command within a
transaction block. Instead, the `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT`
command must be run outside of an explicit transaction.
