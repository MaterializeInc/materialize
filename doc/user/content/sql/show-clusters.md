---
title: "SHOW CLUSTERS"
description: "`SHOW CLUSTERS` lists the clusters configured in Materialize."
menu:
  main:
    parent: 'commands'

---

`SHOW CLUSTERS` lists the [clusters](/get-started/key-concepts/#clusters) configured in Materialize.

## Syntax

{{< diagram "show-clusters.svg" >}}

## Pre-installed clusters

When you enable a Materialize region, several clusters that are used to improve
the user experience, as well as support system administration tasks, will be
pre-installed.

### `quickstart` cluster

A cluster named `quickstart` with a size of `xsmall` and a replication factor of
`1` will be pre-installed in every environment. You can modify or drop this
cluster at any time.

{{< note >}}
The default value for the `cluster` session parameter is `quickstart`.
If the `quickstart` cluster is dropped, you must run [`SET cluster`](/sql/select/#ad-hoc-queries)
to choose a valid cluster in order to run `SELECT` queries. A _superuser _(i.e. `Organization Admin`)
can also run [`ALTER SYSTEM SET cluster`](/sql/alter-system-set) to change the
default value.
{{< /note >}}

### `mz_introspection` system cluster

A system cluster named `mz_introspection` will be pre-installed in every
environment. This cluster has several indexes installed to speed up common
introspection queries, like `SHOW` commands and queries using the system
catalog. It is recommended to switch to the `mz_introspection` cluster for
improved performance when executing these statements.

The following characteristics apply to the `mz_introspection` cluster:

  * You are **not** billed for this cluster.
  * You cannot create indexes or materialized views on this cluster.
  * You cannot drop this cluster.
  * You can run `SELECT` or `SUBSCRIBE` statements on this cluster.

### `mz_system` system cluster

A system cluster named `mz_system` will be pre-installed in every environment.
This cluster is used for various internal system monitoring tasks.

The following characteristics apply to the `mz_system` cluster:

  * You are **not** billed for this cluster.
  * You cannot create indexes or materialized views on this cluster.
  * You cannot drop this cluster.
  * You cannot run `SELECT` or `SUBSCRIBE` on this cluster.

## Examples

```sql
SET CLUSTER = mz_introspection;

SHOW CLUSTERS;
```

```nofmt
       name                  replicas
--------------------- | ------------------
 default              |  r1 (3xsmall)
 auction_house        |  r1 (3xsmall)
 mz_introspection     |  r1 (2xsmall)
 mz_system            |  r1 (2xsmall)
```

```sql
SHOW CLUSTERS LIKE 'auction_%';
```

```nofmt
      name                  replicas
--------------------- | ------------------
 auction_house        |  r1 (3xsmall)
```


## Related pages

- [`CREATE CLUSTER`](../create-cluster)
- [`DROP CLUSTER`](../drop-cluster)
