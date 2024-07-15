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

```mzsql
ALTER CLUSTER c1 SET (REPLICATION FACTOR 2);
```

### Size

Alter cluster to size `100cc`:

```mzsql
ALTER CLUSTER c1 SET (SIZE '100cc');
```

### Schedule

{{< private-preview />}}

```sql
ALTER CLUSTER c1 SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'));
```

See the reference documentation for [`CREATE CLUSTER`](../create-cluster/#scheduling)
or [`CREATE MATERIALIZED VIEW`](../create-materialized-view/#refresh-strategies)
for more details on scheduled clusters.

## Converting unmanaged to managed clusters

{{< warning >}}
[Unmanaged clusters](/sql/create-cluster-replica) are a deprecated feature of
Materialize that required manual management of cluster replicas.

We recommend converting any unmanaged clusters to managed clusters
by following the instructions below.
{{< /warning >}}

Alter the `managed` status of a cluster to managed:

```mzsql
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
