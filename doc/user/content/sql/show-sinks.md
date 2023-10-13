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

{{< diagram "show-sinks.svg" >}}

## Details

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show sinks from. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
_cluster&lowbar;name_ | The cluster to show sinks from. If omitted, sinks from all clusters are shown. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).

### Output format

`SHOW SINKS`'s output is a table, with this structure:

```nofmt
name  | type | size | cluster
------+------+------+--------
...   | ...  | ...  | ...
```

Field       | Meaning
------------|--------
**name**    | The name of the sink.
**type**    | The type of the sink: currently only `kafka` is supported.
**size**    | The size of the sink. Null if the sink is created using the `IN CLUSTER` clause.
**cluster** | The cluster the sink is associated with.

## Examples

```sql
SHOW SINKS;
```
```nofmt
name          | type  | size    | cluster
--------------+-------+---------+--------
my_sink       | kafka |         | c1
my_other_sink | kafka |         | c2
```

```sql
SHOW SINKS IN CLUSTER c1;
```
```nofmt
name    | type  | size    | cluster
--------+-------+---------+--------
my_sink | kafka |         | c1
```

## Related pages

- [`CREATE SINK`](../create-sink)
- [`DROP SINK`](../drop-sink)
- [`SHOW CREATE SINK`](../show-create-sink)
