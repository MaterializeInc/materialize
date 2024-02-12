---
title: "SHOW SUBSOURCES"
description: "`SHOW SUBSOURCES` returns the subsources in the current schema."
menu:
  main:
    parent: commands
---

`SHOW SUBSOURCES` returns the subsources in the current schema.

## Syntax

{{< diagram "show-subsources.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show subsources from. Defaults to first resolvable schema in the search path if `on_name` is not shown.
_on&lowbar;name_     | The name of the source whose associated subsources you want to show. If omitted, all subsources in the schema are shown.

## Details

A subsource is a relation associated with a source. There are two types of
subsources:

  * Subsources of type `progress` describe Materialize's ingestion progress for
    the parent source. Every source has exactly one subsource of type
    `progress`.

  * Subsources of type `subsource` are used by sources that need to ingest data
    into multiple tables, like PostgreSQL sources. For each upstream table that
    is selected for ingestion, Materialize creates a subsource of type
    `subsource`.

### Output format for `SHOW SUBSOURCES`

`SHOW SUBSOURCES`'s output is a table, with this structure:

```nofmt
 name  | type
-------+-----
 ...   | ...
```

Field    | Meaning
---------|--------
**name** | The name of the subsource.
**type** | The type of the subsource: `subsource` or `progress`.

## Examples

```sql
SHOW SOURCES;
```
```nofmt
    name
----------
 postgres
 kafka
```
```sql
SHOW SUBSOURCES ON pg;
```
```nofmt
        name        | type
--------------------+-----------
 postgres_progress  | progress
 table1_in_postgres | subsource
 table2_in_postgres | subsource
```
```sql
SHOW SUBSOURCES ON kafka;
```
```nofmt
            name    | typef
--------------------+----------
 kafka_progress     | progress
```

## Related pages

- [`SHOW CREATE SOURCE`](../show-create-source)
- [`CREATE SOURCE`](../create-source)
