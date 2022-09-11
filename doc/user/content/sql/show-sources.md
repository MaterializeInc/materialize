---
title: "SHOW SOURCES"
description: "`SHOW SOURCES` returns a list of all sources available to your Materialize instances."
menu:
  main:
    parent: commands
---

`SHOW SOURCES` returns a list of all sources available to your Materialize
instances.

## Syntax

{{< diagram "show-sources.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show sources from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

### Output format for `SHOW SOURCES`

`SHOW SOURCES`'s output is a table, with this structure:

```nofmt
 name  | type
-------+------
 ...   | ...
```

Field | Meaning
------|--------
**name** | The name of the source
**type** | The type of the source:  `kafka` or `postgres`

### Internal statistic sources

Materialize comes with a number of sources that contain internal statistics
about the instance's behavior. These are kept in a "hidden" schema called
`mz_catalog`.

To view the internal statistic sources use:

```sql
SHOW SOURCES FROM mz_catalog;
```

To select from these sources, you must specify that you want to read from the
source in the `mz_catalog` schema.

## Examples

### Default behavior

```sql
SHOW SCHEMAS;
```
```nofmt
  name
--------
 public
```
```sql
SHOW SOURCES FROM public;
```
```nofmt
            name    | type
--------------------+---------
 my_kafka_source    | kafka
 my_postgres_source | postgres
```
```sql
SHOW SOURCES;
```
```nofmt
            name    | type
--------------------+---------
 my_kafka_source    | kafka
 my_postgres_source | postgres
```

## Related pages

- [`SHOW CREATE SOURCE`](../show-create-source)
- [`CREATE SOURCE`](../create-source)
