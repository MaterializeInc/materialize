---
title: "System Tables"
description: "System tables store metadata about your Materialize instance."
weight: 14
menu:
  main:
    parent: 'sql'
---

Like PostgreSQL's `pg_catalog`, Materialize provides a handful of system tables
containing up-to-date metadata for a given Materialize instance.

## Details

Materialize stores system tables in the `mz_catalog` schema. This schema does not belong
to a particular database, but is available from all databases in a Materialize instance.

## Tables

### mz_databases

`mz_catalog.mz_databases` contains a unique row for each database in a Materialize
instance.

Field     | Meaning
----------|----------
global_id | The unique ID of the database.
database  | The name of the database.

### mz_schemas

`mz_catalog.mz_schemas` contains a unique row for each schema.

Field     | Meaning
----------|----------
schema_id | The unique ID of the schema.
database_id  | The `global_id` of the database containing the schema.
name      | The name of the schema.
type      | Either `"SYSTEM"` or `"USER"`. `"SYSTEM"` schemas are created and maintained by the Materialize system, and cannot be updated or deleted. `"USER"` schemas were created by a user of the system, and can be updated or deleted.

### mz_columns

`mz_catalog.mz_columns` contains a unique row for each column in every table, source, and view
in a Materialized instance.

Field     | Meaning
----------|----------
qualified_name | The fully qualified name of the table, source, and view containing the column. E.g., `materialize.public.table`.
global_id | The unique id of the table, source, and view containing the column.
field_number | The index of the column in the table, source, and view.
field | The name of the column, or `?column?` if unknown.
nullable | Boolean value indicating whether or not the given column can contain a null value.
type | The data type of the column.

### mz_indexes

`mz_catalog.mz_indexes` contains a unique row for each index in a Materialize instance.

Field     | Meaning
----------|----------
global_id | The unique id of the index.
on_global_id | The unique id of the table, source, or view the index is on.
field_number | If not `NULL`, the index of the column in the table, source, and view the index is on.
expression | If not `NULL`, the expression that is evaluated to generate the index value.
nullable | Boolean value indicating whether or not the given index can contain a null value.
seq_in_index | The index of the index within the list of indexes for a table, source, or view.
