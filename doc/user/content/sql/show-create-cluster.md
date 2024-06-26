---
title: "SHOW CREATE CLUSTER"
description: "`SHOW CREATE CLUSTER` returns the statement used to create the cluster."
menu:
  main:
    parent: commands
---

`SHOW CREATE CLUSTER` returns the DDL statement used to create the cluster.

## Syntax

{{< diagram "show-create-cluster.svg" >}}

 Field                 | Use
-----------------------|-------------------------------------------------------------------------------------------------------------------------
 _cluster&lowbar;name_ | The cluster you want to get the `CREATE` statement for. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).

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
