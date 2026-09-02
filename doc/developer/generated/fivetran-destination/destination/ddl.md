---
source: src/fivetran-destination/src/destination/ddl.rs
revision: 849327076c
---

# mz-fivetran-destination::destination::ddl

DDL operations for the Fivetran destination: `describe_table`, `create_table`, and `alter_table`.

`handle_describe_table` connects and returns the Fivetran `Table` definition (columns with names, types, and primary-key flags) for an existing Materialize table, or `None` if the table does not exist.

`handle_create_table` creates a new table using the Fivetran column definitions, including the `_fivetran_synced` system column (timestamp).

`handle_alter_table` applies column additions and data-type changes to an existing table.

A `PRIMARY_KEY_MAGIC_STRING` constant (`"mz_is_primary_key"`) is stored in column comments as a workaround to track primary key status, since Materialize does not expose primary key metadata directly.
