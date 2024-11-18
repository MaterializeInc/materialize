---
title: "SHOW SOURCES"
description: "`SHOW SOURCES` returns a list of all sources available in Materialize."
menu:
  main:
    parent: commands
---

`SHOW SOURCES` returns a list of all sources available in Materialize.

## Syntax

```mzsql
SHOW SOURCES [ FROM <schema_name> ] [ IN CLUSTER <cluster_name> ]
```

Option                        | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show sources from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**IN CLUSTER** <cluster_name> | If specified, only show sources from the specified cluster. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).

## Details

### Output format for `SHOW SOURCES`

`SHOW SOURCES`'s output is a table, with this structure:

```nofmt
name  | type | cluster
------+------+--------
...   | ...  | ...
```

Field | Meaning
------|--------
**name** | The name of the source.
**type** | The type of the source: `kafka`, `postgres`, `load-generator`, `progress`, or `subsource`.
**cluster** | The cluster the source is associated with.

## Examples

```mzsql
SHOW SOURCES;
```
```nofmt
            name    | type     | cluster
--------------------+----------+---------
 my_kafka_source    | kafka    | c1
 my_postgres_source | postgres | c2
```

```mzsql
SHOW SOURCES IN CLUSTER c2;
```
```nofmt
name               | type     | cluster
-------------------+----------+--------
my_postgres_source | postgres | c2
```

## Related pages

- [`CREATE SOURCE`](../create-source)
- [`DROP SOURCE`](../drop-source)
- [`SHOW CREATE SOURCE`](../show-create-source)
