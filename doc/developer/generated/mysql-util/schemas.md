---
source: src/mysql-util/src/schemas.rs
revision: 0224148a26
---

# mysql-util::schemas

Provides `schema_info`, which queries `information_schema` to retrieve table column and unique-key metadata for a `SchemaRequest` (all tables, specific schemas, or specific tables).
Defines `MySqlTableSchema` (raw schema including `InfoSchema` column rows) and its `to_desc` method, which converts it to `MySqlTableDesc` while applying `text_columns` and `exclude_columns` overrides and mapping MySQL data types to Materialize `SqlScalarType`.
Also exports `QualifiedTableRef`, `SchemaRequest`, and the `SYSTEM_SCHEMAS` constant (the four built-in MySQL schemas excluded from `All` queries).
