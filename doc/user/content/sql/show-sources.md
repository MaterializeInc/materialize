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
**MATERIALIZED** | Only return materialized sources, i.e. those with [indexes](../create-index). Without specifying this option, this command returns all sources, including non-materialized sources.
**FULL** | Return details about your sources.

## Details

### Output format for `SHOW FULL SOURCES`

`SHOW FULL SOURCES`'s output is a table, with this structure:

```nofmt
 name  | type | materialized | volatility | connector_type
-------+------+--------------+------------+---------------
 ...   | ...  | ...          | ...        | ...
```

Field | Meaning
------|--------
**name** | The name of the source
**type** | Whether the source was created by the `user` or the `system`.
**materialized** | Does the source have an in-memory index? For more details, see [`CREATE INDEX`](../create-index).
**volatility** | Whether the source is [volatile](/overview/volatility). Either `volatile`, `nonvolatile`, or `unknown`.
**connector_type** | The type of the source: `file`, `kafka`, `kinesis`, `s3`, `postgres`, or `pubnub`.

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
my_stream_source
my_file_source
```
```sql
SHOW SOURCES;
```
```nofmt
my_stream_source
my_file_source
```

### Only show materialized sources

```sql
SHOW MATERIALIZED SOURCES;
```
```nofmt
        name
----------------------
 my_materialized_source
```

### Show details about sources

```sql
SHOW FULL SOURCES
```
```nofmt
            name           | type | materialized
---------------------------+------+--------------
 my_nonmaterialized_source | user | f
 my_materialized_source    | user | t
```

## Related pages

- [`SHOW CREATE SOURCE`](../show-create-source)
- [`CREATE SOURCE`](../create-source)
