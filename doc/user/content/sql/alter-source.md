---
title: "ALTER SOURCE"
description: "`ALTER SOURCE` changes certain characteristics of a source."
menu:
  main:
    parent: 'commands'
---

`ALTER SOURCE` changes certain characteristics of a source.

## Syntax

{{< diagram "alter-source.svg" >}}

#### alter_source_add_clause

{{< diagram "alter-source-add-clause.svg" >}}

#### alter_source_set_retain_history_clause

{{< diagram "alter-source-set-retain-history-clause.svg" >}}

#### alter_source_reset_retain_history_clause

{{< diagram "alter-source-reset-retain-history-clause.svg" >}}

#### with_options

{{< diagram "with-options.svg" >}}

Field   | Use
--------|-----
_name_  | The identifier of the source you want to alter.
**ADD SUBSOURCE** ... | Add the identified tables from the upstream database (`table_name`) to the named PostgreSQL or MySQL source, with the option of choosing the name for the subsource in Materialize (`subsrc_name`). Supports [additional options](#add-subsource-with_options).
_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

### **ADD SUBSOURCE** `with_options`

Field                                | Value           | Description
-------------------------------------|-----------------|-------------------------------------
`TEXT COLUMNS`                       | A list of names | Decode data as `text` for specific columns that contain PostgreSQL types that are unsupported in Materialize.

## Context

### Adding subsources to a PostgreSQL or MySQL source

Note that using a combination of dropping and adding subsources lets you change
the schema of the PostgreSQL or MySQL tables that are ingested.

### Dropping subsources from a PostgreSQL or MySQL source

Dropping a subsource prevents Materialize from ingesting any data from it, in
addition to dropping any state that Materialize previously had for the table
(such as its contents).

If a subsource encounters a deterministic error, such as an incompatible schema
change (e.g. dropping an ingested column), you can drop the subsource. If you
want to ingest it with its new schema, you can then add it as a new subsource.

You cannot drop the "progress subsource".

## Examples

### Adding subsources

```mzsql
ALTER SOURCE pg_src ADD SUBSOURCE tbl_a, tbl_b AS b WITH (TEXT COLUMNS [tbl_a.col]);
```

### Dropping subsources

To drop a subsource, use the [`DROP SOURCE`](/sql/drop-source/) command:

```mzsql
DROP SOURCE tbl_a, b CASCADE;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the source being altered.

## See also

- [`CREATE SOURCE`](/sql/create-source/)
- [`DROP SOURCE`](/sql/drop-source/)
- [`SHOW SOURCES`](/sql/show-sources)
