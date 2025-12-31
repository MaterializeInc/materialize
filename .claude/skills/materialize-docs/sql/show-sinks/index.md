---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-sinks/
complexity: intermediate
description: '`SHOW SINKS` returns a list of all sinks available in Materialize.'
doc_type: reference
keywords:
- FROM
- cluster
- SHOW SINKS
- name
- IN CLUSTER
- type
product_area: Sinks
status: stable
title: SHOW SINKS
---

# SHOW SINKS

## Purpose
`SHOW SINKS` returns a list of all sinks available in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW SINKS` returns a list of all sinks available in Materialize.



`SHOW SINKS` returns a list of all sinks available in Materialize.

## Syntax

This section covers syntax.

```mzsql
SHOW SINKS [ FROM <schema_name> ] [ IN CLUSTER <cluster_name> ];
```bash

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
```text

Field       | Meaning
------------|--------
**name**    | The name of the sink.
**type**    | The type of the sink: currently only `kafka` is supported.
**cluster** | The cluster the sink is associated with.

## Examples

This section covers examples.

```mzsql
SHOW SINKS;
```text
```nofmt
name          | type  | cluster
--------------+-------+--------
my_sink       | kafka | c1
my_other_sink | kafka | c2
```text

```mzsql
SHOW SINKS IN CLUSTER c1;
```text
```nofmt
name    | type  | cluster
--------+-------+--------
my_sink | kafka | c1
```

## Related pages

- [`CREATE SINK`](../create-sink)
- [`DROP SINK`](../drop-sink)
- [`SHOW CREATE SINK`](../show-create-sink)

