---
title: "SHOW SOURCES"
description: "`SHOW SOURCES` returns a list of all sources available in Materialize."
menu:
  main:
    parent: commands
---

`SHOW SOURCES` returns a list of all sources available in Materialize.

## Syntax

{{< diagram "show-sources.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show sources from. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

### Output format for `SHOW SOURCES`

`SHOW SOURCES`'s output is a table, with this structure:

```nofmt
 name  | type | size
-------+------+-----
 ...   | ...  + ....
```

Field | Meaning
------|--------
**name** | The name of the source.
**type** | The type of the source: `kafka`, `postgres`, `load-generator`, `progress`, or `subsource`.
**size** | The [size](/sql/create-source/#sizing-a-source) of the source. Null if the source is created using the `IN CLUSTER` clause.

## Examples

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
            name    | type     | size
--------------------+----------+-------
 my_kafka_source    | kafka    |
 my_postgres_source | postgres |
```
```sql
SHOW SOURCES;
```
```nofmt
            name    | type     | size
--------------------+----------+-------
 my_kafka_source    | kafka    |
 my_postgres_source | postgres |
```

## Related pages

- [`SHOW CREATE SOURCE`](../show-create-source)
- [`CREATE SOURCE`](../create-source)
