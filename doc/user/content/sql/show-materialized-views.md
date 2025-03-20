---
title: "SHOW MATERIALIZED VIEWS"
description: "`SHOW MATERIALIZED VIEWS` returns a list of materialized views being maintained in Materialize."
menu:
  main:
    parent: commands
---

`SHOW MATERIALIZED VIEWS` returns a list of materialized views being maintained
in Materialize.

## Syntax

```mzsql
SHOW MATERIALIZED VIEWS [ FROM <schema_name> ] [ IN <cluster_name> ]
```

Option                      | Description
----------------------------|------------
**FROM** <schema_name>      | If specified, only show materialized views from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**IN** <cluster_name>       | If specified, only show materialized views from the specified cluster.

## Examples

```mzsql
SHOW MATERIALIZED VIEWS;
```

```nofmt
     name     | cluster
--------------+----------
 winning_bids | quickstart
```

```mzsql
SHOW MATERIALIZED VIEWS LIKE '%bid%';
```

```nofmt
     name     | cluster
--------------+----------
 winning_bids | quickstart
```

## Related pages

- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
