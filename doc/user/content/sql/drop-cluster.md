---
title: "DROP CLUSTER"
description: "`DROP CLUSTER` removes an existing cluster from Materialize."
menu:
  main:
    parent: 'commands'

---

`DROP CLUSTER` removes an existing cluster from Materialize. If there are indexes or materialized views depending on the cluster, you must explicitly drop them first, or use the `CASCADE` option.

## Syntax

{{< diagram "drop-cluster.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified cluster does not exist.
_cluster&lowbar;name_ | The cluster you want to drop. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).
**CASCADE** | Remove the cluster and its dependent objects.
**RESTRICT** | Do not drop the cluster if it has dependencies. _(Default)_

## Examples

### Dropping a cluster with no dependencies

To drop an existing cluster, run:

```sql
DROP CLUSTER auction_house;
```

To avoid issuing an error if the specified cluster does not exist, use the `IF EXISTS` option:

```sql
DROP CLUSTER IF EXISTS auction_house;
```

### Dropping a cluster with dependencies

If the cluster has dependencies, Materialize will throw an error similar to:

```sql
DROP CLUSTER auction_house;
```

```nofmt
ERROR:  cannot drop cluster with active indexes or materialized views
```

, and you'll have to explicitly ask to also remove any dependent objects using the `CASCADE` option:

```sql
DROP CLUSTER auction_house CASCADE;
```

## Related pages

- [`SHOW CLUSTERS`](../show-clusters)
