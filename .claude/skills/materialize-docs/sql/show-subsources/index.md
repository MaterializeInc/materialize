---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-subsources/
complexity: intermediate
description: '`SHOW SUBSOURCES` returns the subsources in the current schema.'
doc_type: reference
keywords:
- 'ON'
- FROM
- SHOW SUBSOURCES
- name
- type
product_area: Sources
status: stable
title: SHOW SUBSOURCES
---

# SHOW SUBSOURCES

## Purpose
`SHOW SUBSOURCES` returns the subsources in the current schema.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW SUBSOURCES` returns the subsources in the current schema.



`SHOW SUBSOURCES` returns the subsources in the current schema.

## Syntax

This section covers syntax.

```mzsql
SHOW SUBSOURCES [ FROM <schema_name> | ON <source_name> ];
```text

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
```text

Field    | Meaning
---------|--------
**name** | The name of the subsource.
**type** | The type of the subsource: `subsource` or `progress`.

## Examples

This section covers examples.

```mzsql
SHOW SOURCES;
```text
```nofmt
    name
----------
 postgres
 kafka
```text
```mzsql
SHOW SUBSOURCES ON pg;
```text
```nofmt
        name        | type
--------------------+-----------
 postgres_progress  | progress
 table1_in_postgres | subsource
 table2_in_postgres | subsource
```text
```mzsql
SHOW SUBSOURCES ON kafka;
```text
```nofmt
            name    | typef
--------------------+----------
 kafka_progress     | progress
```

## Related pages

- [`SHOW CREATE SOURCE`](../show-create-source)
- [`CREATE SOURCE`](../create-source)

