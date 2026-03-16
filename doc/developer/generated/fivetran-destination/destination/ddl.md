---
source: src/fivetran-destination/src/destination/ddl.rs
revision: 82d92a7fad
---

# mz-fivetran-destination::destination::ddl

Implements DDL operations: `handle_describe_table` queries `mz_tables`/`mz_columns` to reflect a table's schema back as a Fivetran `Table`; `handle_create_table` issues `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE` statements and stores primary-key metadata as column `COMMENT`s (a workaround since Materialize lacks native primary keys); `handle_alter_table` returns `Unsupported` for all alter requests.
