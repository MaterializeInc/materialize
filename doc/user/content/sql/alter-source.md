---
title: "ALTER SOURCE"
description: "`ALTER SOURCE` changes certain characteristics of a source, such as its size."
menu:
  main:
    parent: 'commands'
---

`ALTER SOURCE` changes certain characteristics of a source, such as its size.

## Syntax

{{< diagram "alter-source.svg" >}}

#### alter_source_add_clause

{{< diagram "alter-source-add-clause.svg" >}}

#### alter_source_drop_clause

{{< diagram "alter-source-drop-clause.svg" >}}

#### alter_source_set_clause

{{< diagram "alter-source-set-clause.svg" >}}

#### with_options

{{< diagram "with-options.svg" >}}

Field   | Use
--------|-----
_name_  | The identifier of the source you want to alter.
**ADD SUBSOURCE** ... | PostgreSQL sources only: Add the identified tables from the upstream database (`table_name`) to the named source, with the option of choosing the name for the subsource in Materialize (`subsrc_name`). Supports [additional options](#add-subsource-with_options).
**DROP SUBSOURCE** ... | PostgreSQL sources only: Drop the identified subsources from the source. Specifying **CASCADE** also drops all objects that depend on the subsource. **RESTRICT** (default) will not drop the subsource if it has any dependencies.
_value_ | The new value for the source [size](/sql/create-source/#sizing-a-source). Accepts values: `3xsmall`, `2xsmall`, `xsmall`, `small`, `medium`, `large`, `xlarge`.

### **ADD SUBSOURCE** `with_options`

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`TEXT COLUMNS`                       | A list of names | Decode data as `text` for specific columns that contain PostgreSQL types that are unsupported in Materialize.

## Context

### Adding PostgreSQL subsources/tables

When adding subsources to a PostgreSQL source, Materialize opens a temporary
replication slot to snapshot the new subsources' current states. After
completing the snapshot, the table will be kept up-to-date, just as all other
tables in the publication.

Note that using a combination of dropping and adding subsources lets you change
the schema the PostgreSQL sources ingest.

### Dropping PostgreSQL subsources/tables

Dropping a subsource prevents Materialize from ingesting any data from it, in
addition to dropping any state that Materialize previously had for the table
(such as its contents).

If a subsource encounters a deterministic error, such as an incompatible schema
change (e.g. dropping an ingested column), you can drop the subsource. If you
want to ingest it with its new schema, you can then add it as a new subsource.

You cannot drop the "progress subsource".

## Examples

### Adding subsources

```sql
ALTER SOURCE pg_src ADD SUBSOURCE tbl_a, tbl_b AS b WITH (TEXT COLUMNS [tbl_a.col]);
```

### Dropping subsources

```sql
ALTER SOURCE pg_src DROP SUBSOURCE tbl_a, b CASCADE;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the source being altered.

## See also

- [`CREATE SOURCE`](/sql/create-source/)
- [`SHOW SOURCES`](/sql/show-sources)
