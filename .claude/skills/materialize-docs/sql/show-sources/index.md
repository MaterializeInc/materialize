---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-sources/
complexity: intermediate
description: '`SHOW SOURCES` returns a list of all sources available in Materialize.'
doc_type: reference
keywords:
- FROM
- cluster
- SHOW SOURCES
- name
- IN CLUSTER
- type
product_area: Sources
status: stable
title: SHOW SOURCES
---

# SHOW SOURCES

## Purpose
`SHOW SOURCES` returns a list of all sources available in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW SOURCES` returns a list of all sources available in Materialize.



`SHOW SOURCES` returns a list of all sources available in Materialize.

## Syntax

This section covers syntax.

```mzsql
SHOW SOURCES [ FROM <schema_name> ] [ IN CLUSTER <cluster_name> ];
```text

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show sources from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**IN CLUSTER** <cluster_name> | If specified, only show sources from the specified cluster. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).

## Details

This section covers details.

### Output format for `SHOW SOURCES`

`SHOW SOURCES`'s output is a table, with this structure:

```nofmt
name  | type | cluster
------+------+--------
...   | ...  | ...
```text

Field | Meaning
------|--------
**name** | The name of the source.
**type** | The type of the source: `kafka`, `postgres`, `load-generator`, `progress`, or `subsource`.
**cluster** | The cluster the source is associated with.

## Examples

This section covers examples.

```mzsql
SHOW SOURCES;
```text
```nofmt
            name    | type     | cluster
--------------------+----------+---------
 my_kafka_source    | kafka    | c1
 my_postgres_source | postgres | c2
```text

```mzsql
SHOW SOURCES IN CLUSTER c2;
```text
```nofmt
name               | type     | cluster
-------------------+----------+--------
my_postgres_source | postgres | c2
```

## Related pages

- [`CREATE SOURCE`](../create-source)
- [`DROP SOURCE`](../drop-source)
- [`SHOW CREATE SOURCE`](../show-create-source)

