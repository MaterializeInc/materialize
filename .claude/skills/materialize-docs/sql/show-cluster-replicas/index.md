---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-cluster-replicas/
complexity: beginner
description: '`SHOW CLUSTER REPLICAS` lists the replicas for each cluster configured
  in Materialize.'
doc_type: reference
keywords:
- SHOW CLUSTER REPLICAS
- WHERE
- SHOW CLUSTER
- SHOW CLUSTERS
- LIKE
product_area: Indexes
status: stable
title: SHOW CLUSTER REPLICAS
---

# SHOW CLUSTER REPLICAS

## Purpose
`SHOW CLUSTER REPLICAS` lists the replicas for each cluster configured in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW CLUSTER REPLICAS` lists the replicas for each cluster configured in Materialize.



`SHOW CLUSTER REPLICAS` lists the [replicas](/sql/create-cluster#replication-factor) for each
cluster configured in Materialize.

## Syntax

This section covers syntax.

```sql
SHOW CLUSTER REPLICAS
[LIKE <pattern> | WHERE <condition(s)>]
;
```text

Syntax element                | Description
------------------------------|------------
**LIKE** \<pattern\>          | If specified, only show clusters that match the pattern.
**WHERE** <condition(s)>      | If specified, only show clusters that match the condition(s).

## Examples

This section covers examples.

```mzsql
SHOW CLUSTER REPLICAS;
```text

```nofmt
    cluster    | replica |  size  | ready |
---------------+---------|--------|-------|
 auction_house | bigger  | 1600cc | t     |
 quickstart    | r1      | 25cc   | t     |
```text

```mzsql
SHOW CLUSTER REPLICAS WHERE cluster = 'quickstart';
```text

```nofmt
    cluster    | replica |  size  | ready|
---------------+---------|--------|-------
 quickstart    | r1      | 25cc   | t    |
```


## Related pages

- [`CREATE CLUSTER REPLICA`](../create-cluster-replica)
- [`DROP CLUSTER REPLICA`](../drop-cluster-replica)

