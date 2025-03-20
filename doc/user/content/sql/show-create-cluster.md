---
title: "SHOW CREATE CLUSTER"
description: "`SHOW CREATE CLUSTER` returns the DDL statement used to create the cluster."
menu:
  main:
    parent: commands
---

`SHOW CREATE CLUSTER` returns the DDL statement used to create the cluster.

## Syntax

```sql
SHOW CREATE CLUSTER <cluster_name>
```

For available cluster names, see [`SHOW CLUSTERS`](/sql/show-clusters).

## Examples

```sql
SHOW CREATE CLUSTER c;
```

```nofmt
    name          |    create_sql
------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 c                | CREATE CLUSTER "c" (DISK = false, INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 1, SIZE = '100cc', SCHEDULE = MANUAL)
```

## Privileges

There are no privileges required to execute this statement.

## Related pages

- [`SHOW CLUSTERS`](../show-clusters)
- [`CREATE CLUSTER`](../create-cluster)
