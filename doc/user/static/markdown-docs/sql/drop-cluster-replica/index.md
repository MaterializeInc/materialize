# DROP CLUSTER REPLICA
`DROP CLUSTER REPLICA` removes an existing replica for the specified cluster.
`DROP CLUSTER REPLICA` deprovisions an existing replica of the specified
[unmanaged cluster](/sql/create-cluster/#unmanaged-clusters). To remove
the cluster itself, use the [`DROP CLUSTER`](/sql/drop-cluster) command.

> **Tip:** When getting started with Materialize, we recommend starting with managed
> clusters.


## Syntax

```mzsql
DROP CLUSTER REPLICA [IF EXISTS] <cluster_name>.<replica_name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified cluster replica does not exist.
`<cluster_name>` | The cluster you want to remove a replica from. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).
`<replica_name>` | The cluster replica you want to drop. For available cluster replicas, see [`SHOW CLUSTER REPLICAS`](../show-cluster-replicas).

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
