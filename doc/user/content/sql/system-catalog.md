---
title: "System Catalog"
description: "The system catalog stores metadata about your Materialize instance."
aliases:
  - /sql/system-tables
weight: 14
menu:
  main:
    parent: 'sql'
---

{{< version-added v0.5.0 >}}

Materialize exposes a system catalog that contains metadata about the running
Materialize instance.

{{< warning >}}
The system tables are not part of Materialize's stable interface.
Backwards-incompatible changes to these tables may be made at any time.
{{< /warning >}}

## Details

The system catalog consists of two schemas that are implicitly available in
all databases:

  * [`mz_catalog`](#mz-catalog), which exposes metadata in Materialize's
    native format.

  * [`pg_catalog`](#pg-catalog), which presents the data in `mz_catalog` in
    the format used by PostgreSQL.

These schemas contain sources, tables, and views that expose metadata like:

  * Descriptions of each database, schema, source, table, view, sink, and
    index in the system.

  * Descriptions of all running dataflows.

  * Metrics about dataflow execution.

Whenever possible, applications should prefer to query `mz_catalog` over
`pg_catalog`. The mapping between Materialize concepts and PostgreSQL concepts
is not one-to-one, and so the data in `pg_catalog` cannot accurately represent
the particulars of Materialize. For example, PostgreSQL has no notion of
[sinks](/sql/create-sink), and therefore `pg_catalog` does not display
information about the sinks available in a Materialize.

## `mz_catalog`

The following sections describe the available objects in the `mz_catalog`
schema.

### `mz_databases`

The `mz_databases` table contains a row for each database in the system.

Field  | Meaning
-------|----------
`id`   | Materialize's unique ID for the database.
`oid`  | A [PostgreSQL-compatible OID][oid] for the database.
`name` | The name of the database.

### `mz_schemas`

The `mz_schemas` table contains a row for each schema in the system.

Field         | Meaning
--------------|----------
`id`          | Materialize's unique ID for the schema.
`oid`         | A [PostgreSQL-compatible oid][oid] for the schema.
`database_id` | The ID of the database containing the schema.
`name`        | The name of the schema.
`type`        | Either `"SYSTEM"` or `"USER"`. `"SYSTEM"` schemas are created and maintained by the Materialize system, and cannot be updated or deleted. `"USER"` schemas were created by a user of the system, and can be updated or deleted.

### `mz_columns`

The `mz_columns` contains a row for each column in each table, source, and view
in the system.

Field            | Meaning
-----------------|----------
`qualified_name` | The fully qualified name of the table, source, and view containing the column. E.g., `materialize.public.table`.
`global_id`      | The unique id of the table, source, and view containing the column.
`field_number`   | The index of the column in the table, source, and view.
`field`          | The name of the column, or `?column?` if unknown.
`nullable`       | Boolean value indicating whether or not the given column can contain a null value.
`type`           | The data type of the column.

### `mz_indexes`

The `mz_indexes` table contains a row for each index in the system.

Field          | Meaning
---------------|----------
`global_id`    | The unique id of the index.
`on_global_id` | The unique id of the table, source, or view the index is on.
`field_number` | If not `NULL`, the index of the column in the table, source, and view the index `is` on.
`expression`   | If not `NULL`, the expression that is evaluated to generate the index value.
`nullable`     | Boolean value indicating whether or not the given index can contain a null value.
`seq_in_index` | The index of the index within the list of indexes for a table, source, or view.

## `pg_catalog`

Materialize has compatibility shims for the following tables from [PostgreSQL's
system catalog](https://www.postgresql.org/docs/current/catalogs.html):

  * [`pg_attribute`](https://www.postgresql.org/docs/current/catalog-pg-attribute.html)
  * [`pg_class`](https://www.postgresql.org/docs/current/catalog-pg-class.html)
  * [`pg_database`](https://www.postgresql.org/docs/current/catalog-pg-database.html)
  * [`pg_description`](https://www.postgresql.org/docs/current/catalog-pg-description.html)
  * [`pg_index`](https://www.postgresql.org/docs/current/catalog-pg-index.html)
  * [`pg_namespace`](https://www.postgresql.org/docs/current/catalog-pg-namespace.html)

These compatibility shims are largely incomplete. Most are lacking some columns
that are present in PostgreSQL, or if they do include the column the result set
its value may always be `NULL`. The precise nature of the incompleteness is
intentionally undocumented. New tools developed against Materialize should use
the documented [`mz_catalog`](#mz-catalog) API instead.

If you are having trouble making a PostgreSQL tool work with Materialize, please
[file a GitHub issue][gh-issue]. Many PostgreSQL tools can be made to work with
Materialize with minor changes to the `pg_catalog` compatibility shim.

[gh-issue]: (https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md)
[oid]: /sql/types/oid
