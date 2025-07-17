---
title: "DROP CLUSTER REPLICA"
description: "`DROP CLUSTER REPLICA` removes an existing replica for the specified cluster."
menu:
  main:
    parent: 'commands'

---

`DROP CLUSTER REPLICA` deprovisions an existing replica of the specified
[unmanaged cluster](/sql/create-cluster/#unmanaged-clusters). To remove
the cluster itself, use the [`DROP CLUSTER`](/sql/drop-cluster) command.

{{< tip >}}
When getting started with Materialize, we recommend starting with managed
clusters.
{{</ tip >}}

## Syntax

{{< diagram "drop-cluster-replica.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified cluster replica does not exist.
_cluster_name_ | The cluster you want to remove a replica from. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).
_replica&lowbar;name_ | The cluster replica you want to drop. For available cluster replicas, see [`SHOW CLUSTER REPLICAS`](../show-cluster-replicas).

## Examples

```mzsql
SHOW CLUSTER REPLICAS WHERE cluster = 'auction_house';
```

```nofmt
    cluster    | replica
---------------+---------
 auction_house | bigger
```

```mzsql
DROP CLUSTER REPLICA auction_house.bigger;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped cluster replica.
- `USAGE` privileges on the containing cluster.

## Related pages

- [`CREATE CLUSTER REPLICA`](../create-cluster-replica)
- [`SHOW CLUSTER REPLICAS`](../show-cluster-replicas)
- [`DROP OWNED`](../drop-owned)
