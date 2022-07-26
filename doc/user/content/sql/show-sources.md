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
**FULL** | Return details about your sources.

## Details

### Output format for `SHOW FULL SOURCES`

`SHOW FULL SOURCES`'s output is a table, with this structure:

```nofmt
 name  | type | type
-------+------+------
 ...   | ...  | ...
```

Field | Meaning
------|--------
**name** | The name of the source
**type** | Whether the source was created by the `user` or the `system`.
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
public
```
```sql
SHOW SOURCES FROM public;
```
```nofmt
my_kafka_source
my_postgres_source
```
```sql
SHOW SOURCES;
```
```nofmt
my_kafka_source
my_postgres_source
```

### Show details about sources

```sql
SHOW FULL SOURCES;
```
```nofmt
            name           | type | type
---------------------------+------+------
 my_kafka_source           | user | kafka
```

## Related pages

- [`SHOW CREATE SOURCE`](../show-create-source)
- [`CREATE SOURCE`](../create-source)
