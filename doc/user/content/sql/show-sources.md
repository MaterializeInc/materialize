---
title: "SHOW SOURCES"
description: "`SHOW SOURCES` returns a list of all sources available to your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`SHOW SOURCES` returns a list of all sources available to your Materialize
instances.

## Syntax

{{< diagram "show-sources.html" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show sources from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

### Internal statistic sources

Materialize comes with a number of sources that contain internal statistics
about the instance's behavior. These are kept in a "hidden" schema called
`mz_catalog`.

To view the internal statistic sources using:

```sql
SHOW SOURCES FROM mz_catalog;
```

To select from these sources, you must specify that you want to read from the
source in the `mz_catalog` schema.

## Examples

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
my_stream_source
my_file_source
```

## Related pages

- [`SHOW CREATE SOURCE`](../show-create-source)
- [`CREATE SOURCE`](../create-source)
