---
title: "SHOW SINKS"
description: "`SHOW SINKS` returns a list of all sinks available in Materialize."
menu:
  main:
    parent: commands
aliases:
    - /sql/show-sink
---

`SHOW SINKS` returns a list of all sinks available in Materialize.

## Syntax

```mzsql
SHOW SINKS [ FROM <schema_name> ] [ IN CLUSTER <cluster_name> ];
```

## Details

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show sinks from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**IN CLUSTER** <cluster_name> | If specified, only show sinks from the specified cluster. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).

### Output format

`SHOW SINKS`'s output is a table, with this structure:

```nofmt
name  | type | cluster
------+------+--------
...   | ...  | ...
```

Field       | Meaning
------------|--------
**name**    | The name of the sink.
**type**    | The type of the sink: currently only `kafka` is supported.
**cluster** | The cluster the sink is associated with.

## Examples

```mzsql
SHOW SINKS;
```
```nofmt
name          | type  | cluster
--------------+-------+--------
my_sink       | kafka | c1
my_other_sink | kafka | c2
```

```mzsql
SHOW SINKS IN CLUSTER c1;
```
```nofmt
name    | type  | cluster
--------+-------+--------
my_sink | kafka | c1
```

## Related pages

- [`CREATE SINK`](../create-sink)
- [`DROP SINK`](../drop-sink)
- [`SHOW CREATE SINK`](../show-create-sink)
