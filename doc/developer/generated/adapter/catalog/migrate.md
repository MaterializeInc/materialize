---
source: src/adapter/src/catalog/migrate.rs
revision: b97334f6e8
---

# adapter::catalog::migrate

Implements AST-level catalog migrations that rewrite stored SQL definitions when the SQL syntax changes incompatibly between versions.
`migrate` is called during catalog open and applies an ordered list of per-item rewrites (e.g. updating `CREATE VIEW` or `CREATE SINK` option names) by parsing, transforming, and re-serialising the stored SQL text.
