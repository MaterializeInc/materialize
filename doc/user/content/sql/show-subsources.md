---
title: "SHOW SUBSOURCES"
description: "`SHOW SUBSOURCES` returns the subsources in the current schema."
menu:
  main:
    parent: commands
---

`SHOW SUBSOURCES` returns the subsources in the current schema.

## Syntax

```mzsql
SHOW SUBSOURCES [ FROM <schema_name> | ON <source_name> ];
```

Syntax element         | Description
-----------------------|------------
**FROM** <schema_name> | If specified, only show subsources from the specified schema. Defaults to first resolvable schema in the search path.
**ON** <source_name>   | If specified, only show subsources on the specified source.

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

```mzsql
SHOW SOURCES;
```
```nofmt
    name
----------
 postgres
 kafka
```
```mzsql
SHOW SUBSOURCES ON pg;
```
```nofmt
        name        | type
--------------------+-----------
 postgres_progress  | progress
 table1_in_postgres | subsource
 table2_in_postgres | subsource
```
```mzsql
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
