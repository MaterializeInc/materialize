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

### `with_options`

{{< private-preview />}}

{{< diagram "with-options-alter-cluster-set.svg" >}}

{{% cluster-options %}}


### `WITH (WAIT UNTIL READY)` options
Field                         | Value                 | Description
------------------------------|-----------------------|-------------------------------------
**TIMEOUT**                   | `duration`            | The maximum duration to wait for the new replicas to be ready.
**ON TIMEOUT**                | [`COMMIT`,`ROLLBACK`] | The action to take on timeout. <br><ul><li>`COMMIT` will cutover to the new replicas regardless of their hydration status, which may lead to downtime.</li><li>`ROLLBACK` will remove any pending replicas and return a timeout error.</li></ul>Default: `COMMIT`.

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

### Graceful reconfiguration

{{< private-preview />}}
Changing the configuration of a cluster using the `ALTER CLUSTER` command
requires the cluster to restart, which incurs **downtime**. For clusters that
don't contain sources or sinks, you can use the `WAIT UNTIL READY` option to
perform a graceful reconfiguration, which spins up an additional cluster
replica under the covers with the desired new configuration, waits for the
replica to be hydrated, and then replaces the original replica. This allows you
to perform operations like cluster resizing with **no downtime**.

```sql
ALTER CLUSTER c1 SET (SIZE '100CC') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'))
````

The `ALTER` statement is blocking and will return only when the new replica
becomes ready. This could take as long as the specified timeout. During this
operation, any other reconfiguration command issued against this cluster will
fail. Additionally, any connection interruption or statement cancelation will
cause a rollback — no configuration changes will take effect in that case.


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
