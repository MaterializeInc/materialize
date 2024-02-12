---
title: "ALTER CLUSTER"
description: "`ALTER CLUSTER` changes the configuration of a cluster."
menu:
  main:
    parent: 'commands'
---

`ALTER CLUSTER` changes the configuration of a cluster. To rename a
cluster, use [`ALTER ... RENAME`](/sql/alter-rename/).

## Syntax

{{< diagram "alter-cluster-set.svg" >}}

{{< diagram "alter-cluster-reset.svg" >}}

{{% cluster-options %}}

## Examples

### Replication factor

Alter cluster to two replicas:

```sql
ALTER CLUSTER c1 SET (REPLICATION FACTOR 2);
```

### Size

Alter cluster to size `xsmall`:

```sql
ALTER CLUSTER c1 SET (SIZE 'xsmall');
```

## Converting unmanaged to managed clusters

{{< warning >}}
[Unmanaged clusters](/sql/create-cluster-replica) are a deprecated feature of
Materialize that required manual management of cluster replicas.

We recommend converting any unmanaged clusters to managed clusters
by following the instructions below.
{{< /warning >}}

Alter the `managed` status of a cluster to managed:

```sql
ALTER CLUSTER c1 SET (MANAGED);
```

Materialize permits converting an unmanged cluster to a managed cluster if
the following conditions are met:

* The cluster replica names are `r1`, `r2`, ..., `rN`.
* All replicas have the same size.
* If there are no replicas, `SIZE` needs to be specified.
* If specified, the replication factor must match the number of replicas.

Note that the cluster will not have settings for the availability zones, and
compute-specific settings. If needed, these can be set explicitly.

## Privileges

The privileges required to execute this statement are:

- Ownership of the cluster.

## See also

- [`ALTER ... RENAME`](/sql/alter-rename/)
- [`CREATE CLUSTER`](/sql/create-cluster/)
- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)
