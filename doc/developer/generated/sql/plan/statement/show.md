---
source: src/sql/src/plan/statement/show.rs
revision: 8be4c49ee6
---

# mz-sql::plan::statement::show

Plans `SHOW` statements that query catalog state: `SHOW CREATE TABLE/VIEW/SOURCE/SINK/INDEX/CONNECTION/MATERIALIZED VIEW/TYPE/CLUSTER`, `SHOW OBJECTS`, `SHOW COLUMNS`, and related.
Most `SHOW` variants are rewritten to internal `SELECT` queries against `mz_catalog` system relations via the `ShowSelect` helper, unifying them with the standard query-planning path.
`plan_show_create_type` uses the fully-qualified type name (schema + item) in the output row rather than just the bare item name, and rejects system types with an error.
