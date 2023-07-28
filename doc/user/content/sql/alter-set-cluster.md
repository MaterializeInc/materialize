---
title: "ALTER SET CLUSTER"
description: "`ALTER SET CLUSTER` changes the association of an object to a cluster"
menu:
  main:
    parent: 'commands'
---

`ALTER SET CLUSTER` changes the association of an object to a cluster.

{{< private-preview />}}

## Syntax

{{< diagram "alter-set-cluster.svg" >}}

## Examples

### Materialized view

Move a materialized view to a specific cluster:

```sql
ALTER MATERIALIZED VIEW mv1 SET CLUSTER c1;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the object.
- `CREATE` privilege in the cluster.

## See also

- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/)
- [`CREATE SOURCE`](/sql/create-source/)
- [`CREATE SINK`](/sql/create-sink/)
