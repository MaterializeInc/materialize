---
title: "SHOW CLUSTER REPLICAS"
description: "`SHOW CLUSTER REPLICAS` lists the replicas for each cluster configured in Materialize."
menu:
  main:
    parent: 'commands'

---

`SHOW CLUSTER REPLICAS` lists the [replicas](/sql/create-cluster#replication-factor) for each
cluster configured in Materialize.

## Syntax

```sql
SHOW CLUSTER REPLICAS
[LIKE <pattern> | WHERE <condition(s)>]
;
```

Syntax element                | Description
------------------------------|------------
**LIKE** \<pattern\>          | If specified, only show clusters that match the pattern.
**WHERE** <condition(s)>      | If specified, only show clusters that match the condition(s).

## Output

Column       | Description
-------------|------------
**cluster**  | The name of the cluster.
**replica**  | The name of the replica.
**size**     | The [size](/sql/create-cluster#available-sizes) of the replica.
**ready**    | Whether all objects on the cluster have hydrated. `true` indicates that all indexes, materialized views, and other objects on this replica are caught up with the upstream data and ready to serve queries.
**comment**  | The [comment](/sql/comment-on) associated with the cluster replica, if any.

## Examples

```mzsql
SHOW CLUSTER REPLICAS;
```

```nofmt
    cluster    | replica |  size  | ready | comment
---------------+---------|--------|-------|--------
 auction_house | bigger  | 1600cc | t     |
 quickstart    | r1      | 25cc   | t     |
```

```mzsql
SHOW CLUSTER REPLICAS WHERE cluster = 'quickstart';
```

```nofmt
    cluster    | replica |  size  | ready | comment
---------------+---------|--------|-------|--------
 quickstart    | r1      | 25cc   | t     |
```


## Related pages

- [`CREATE CLUSTER REPLICA`](../create-cluster-replica)
- [`DROP CLUSTER REPLICA`](../drop-cluster-replica)
