---
title: "SHOW CLUSTERS"
description: "`SHOW CLUSTERS` lists the clusters configured in Materialize."
menu:
  main:
    parent: 'commands'

---

{{< show-command-note >}}

`SHOW CLUSTERS` lists the [clusters](/overview/key-concepts/#clusters) configured in Materialize.

## Syntax

{{< diagram "show-clusters.svg" >}}

## Pre-installed clusters

Materialize has several pre-installed clusters.

### Default cluster

When you enable a Materialize region, a cluster named `default` with a single
`xsmall` [replica](/overview/key-concepts/#cluster-replicas) named `r1` will be
pre-installed. You can modify or drop this cluster or its replicas at any
time.

{{< note >}} The default value for the `cluster` session parameter is `default`.
If the `default` cluster is dropped, you must run [`SET cluster`](/sql/select/#ad-hoc-queries)
to choose a valid cluster in order to run `SELECT` queries.{{< /note >}}

### `mz_introspection` system cluster

The `mz_introspection` cluster has several indexes installed by default that
speed up common `SHOW` statements. You are not billed for this cluster.
It has the following restrictions:

  * You cannot create indexes or materialized views on this cluster.
  * You cannot drop this cluster.

You are, however, are permitted to run `SELECT` or `SUBSCRIBE` statements on
this cluster.

You are encouraged to use this cluster when running queries against the
system catalog:

```sql
SET cluster = mz_introspection;
SELECT * FROM mz_sources;
SHOW CLUSTERS;
```

### `mz_system` system cluster

The `mz_system` cluster is used for various internal system tasks. You are not
billed for this cluster. You are not permitted to use this cluster.
Specifically:

  * You cannot create indexes or materialized views on this cluster.
  * You cannot drop this cluster.
  * You cannot run `SELECT` or `SUBSCRIBE` on this cluster.

## Examples

```sql
SHOW CLUSTERS;
```

```nofmt
       name
---------------------
 default
 auction_house
```

```sql
SHOW CLUSTERS LIKE 'auction_%';
```

```nofmt
       name
---------------------
 auction_house
```


## Related pages

- [`CREATE CLUSTER`](../create-cluster)
- [`DROP CLUSTER`](../drop-cluster)
