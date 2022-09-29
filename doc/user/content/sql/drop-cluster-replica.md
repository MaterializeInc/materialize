---
title: "DROP CLUSTER REPLICA"
description: "`DROP CLUSTER REPLICA` removes an existing replica for the specified cluster."
menu:
  main:
    parent: 'commands'

---

`DROP CLUSTER REPLICA` removes an existing replica for the specified cluster. To remove all active replicas for a cluster, use the [`DROP CLUSTER`](/sql/drop-cluster) command.

## Syntax

{{< diagram "drop-cluster-replica.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified cluster replica does not exist.
_cluster_name_ | The cluster you want to remove a replica from. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).
_replica&lowbar;name_ | The cluster replica you want to drop. For available cluster replicas, see [`SHOW CLUSTER REPLICAS`](../show-cluster-replicas).

## Examples

```sql
SHOW CLUSTER REPLICAS WHERE cluster = 'auction_house';
```

```nofmt
    cluster    | replica
---------------+---------
 auction_house | bigger
```

```sql
DROP CLUSTER REPLICA auction_house.bigger;
```

## Related pages

- [`SHOW CLUSTER REPLICAS`](../show-cluster-replicas)
