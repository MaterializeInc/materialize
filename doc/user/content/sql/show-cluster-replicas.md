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

{{< diagram "show-cluster-replicas.svg" >}}

## Examples

```sql
SHOW CLUSTER REPLICAS;
```

```nofmt
    cluster    | replica |  size  | ready |
---------------+---------|--------|-------|
 auction_house | bigger  | xlarge | t     |
 quickstart    | r1      | xsmall | t     |
```

```sql
SHOW CLUSTER REPLICAS WHERE cluster='quickstart';
```

```nofmt
    cluster    | replica |  size  | ready|
---------------+---------|--------|-------
 quickstart    | r1      | xsmall | t    |
```


## Related pages

- [`CREATE CLUSTER REPLICA`](../create-cluster-replica)
- [`DROP CLUSTER REPLICA`](../drop-cluster-replica)
