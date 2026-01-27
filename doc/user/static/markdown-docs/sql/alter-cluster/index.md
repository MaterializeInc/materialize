# ALTER CLUSTER
`ALTER CLUSTER` changes the configuration of a cluster.
Use `ALTER CLUSTER` to:

- Change configuration of a cluster, such as the `SIZE` or
`REPLICATON FACTOR`.
- Rename a cluster.
- Change owner of a cluster.

For completeness, the syntax for `SWAP WITH` operation is provided. However, in
general, you will not need to manually perform this operation.

## Syntax

`ALTER CLUSTER` has the following syntax variations:


**Set a configuration:**

### Set a configuration

To set a cluster configuration:



```mzsql
ALTER CLUSTER <cluster_name>
SET (
    SIZE = <text>
    [, REPLICATION FACTOR = <int>]
    [, MANAGED = <bool>]
    [, SCHEDULE = MANUAL|ON REFRESH(...)]
)
[WITH ( <with_option>[,...])]
;

```

| Syntax element | Description |
| --- | --- |
| `<cluster_name>` | The name of the cluster you want to alter.  |
| `SIZE` | <a name="alter-cluster-size"></a> The size of the resource allocations for the cluster. {{< yaml-list column="Cluster size" data="m1_cluster_sizing" numColumns="3" >}} See [Size](#available-sizes) for details as well as legacy sizes available. {{< warning >}} Changing the size of a cluster may incur downtime. For more information, see [Resizing considerations](#resizing). {{< /warning >}} Not available for `ALTER CLUSTER ... RESET` since there is no default `SIZE` value. |
| `REPLICATION FACTOR` | Optional.The number of replicas to provision for the cluster. Each replica of the cluster provisions a new pool of compute resources to perform exactly the same computations on exactly the same data. For more information, see [Replication factor considerations](#replication-factor).  Default: `1`  |
| `MANAGED` | Optional. Whether to automatically manage the cluster's replicas based on the configured size and replication factor.  If `FALSE`, enables the use of the <em>deprecated</em> [`CREATE CLUSTER REPLICA`](/sql/create-cluster-replica) command.  Default: `TRUE`  |
| `SCHEDULE` | Optional. The [scheduling type](/sql/create-cluster/#scheduling) for the cluster. Valid values are `MANUAL` and `ON REFRESH`.  Default: `MANUAL`  |
| `WITH (<with_option>[,...])` |  The following `<with_option>`s are supported: \| Option  \| Description \| \|--------\|-------------\| \| `WAIT UNTIL READY(...)`    \| ***Private preview.** This option has known performance or stability issues and is under activedevelopment.* {{< include-from-yaml data="examples/alter_cluster" name="wait-until-ready-cmd-option" >}} \| \| `WAIT FOR` \|  ***Private preview.** This option has known performance or stability issues and is under active development.* A fixed duration to wait for the new replicas to be ready. This option can lead to downtime. As such, we recommend using the `WAIT UNTIL READY` option instead.\|  |



**Reset to default:**

### Reset to default

To reset a cluster configuration back to its default value:



```mzsql
ALTER CLUSTER <cluster_name>
RESET (
    REPLICATION FACTOR | MANAGED | SCHEDULE,
    ...
)
;

```

| Syntax element | Description |
| --- | --- |
| `<cluster_name>` | The name of the cluster you want to alter.  |
| `REPLICATION FACTOR` | Optional. The number of replicas to provision for the cluster.  Default: `1`  |
| `MANAGED` | Optional. Whether to automatically manage the cluster's replicas based on the configured size and replication factor.  Default: `TRUE`  |
| `SCHEDULE` | Optional. The [scheduling type](/sql/create-cluster/#scheduling) for the cluster.  Default: `MANUAL`  |



**Rename:**

### Rename

To rename a cluster:



```mzsql
ALTER CLUSTER <cluster_name> RENAME TO <new_cluster_name>;

```

| Syntax element | Description |
| --- | --- |
| `<cluster_name>` | The current name of the cluster.  |
| `<new_cluster_name>` | The new name of the cluster.  |


> **Note:** You cannot rename system clusters, such as `mz_system` and `mz_catalog_server`.



**Change owner:**

### Change owner

To change the owner of a cluster:



```mzsql
ALTER CLUSTER <cluster_name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<cluster_name>` | The name of the cluster you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the cluster.  |
To change the owner, you must have ownership of the cluster and membership in
the `<new_owner_role>`. See also [Required privileges](#required-privileges).



**Swap with:**

### Swap with

> **Important:** Information about the `SWAP WITH` operation is provided for completeness.  The
> `SWAP WITH` operation is used for blue/green deployments. In general, you will
> not need to manually perform this operation.


To swap the name of this cluster with another cluster:



```mzsql
ALTER CLUSTER <cluster1> SWAP WITH <cluster2>;

```

| Syntax element | Description |
| --- | --- |
| `<cluster1>` | The name of the first cluster.  |
| `<cluster2>` | The name of the second cluster.  |





## Considerations

### Resizing

> **Tip:** For help sizing your clusters, navigate to **Materialize Console >**
> [**Monitoring**](/console/monitoring/)>**Environment Overview**. This page
> displays cluster resource utilization and sizing advice.


#### Available sizes


**M.1 Clusters:**

> **Note:** The values set forth in the table are solely for illustrative purposes.
> Materialize reserves the right to change the capacity at any time. As such, you
> acknowledge and agree that those values in this table may change at any time,
> and you should not rely on these values for any capacity planning.




| Cluster size | Compute Credits/Hour | Total Capacity | Notes |
| --- | --- | --- | --- |
| <strong>M.1-nano</strong> | 0.75 | 26 GiB |  |
| <strong>M.1-micro</strong> | 1.5 | 53 GiB |  |
| <strong>M.1-xsmall</strong> | 3 | 106 GiB |  |
| <strong>M.1-small</strong> | 6 | 212 GiB |  |
| <strong>M.1-medium</strong> | 9 | 318 GiB |  |
| <strong>M.1-large</strong> | 12 | 424 GiB |  |
| <strong>M.1-1.5xlarge</strong> | 18 | 636 GiB |  |
| <strong>M.1-2xlarge</strong> | 24 | 849 GiB |  |
| <strong>M.1-3xlarge</strong> | 36 | 1273 GiB |  |
| <strong>M.1-4xlarge</strong> | 48 | 1645 GiB |  |
| <strong>M.1-8xlarge</strong> | 96 | 3290 GiB |  |
| <strong>M.1-16xlarge</strong> | 192 | 6580 GiB | Available upon request |
| <strong>M.1-32xlarge</strong> | 384 | 13160 GiB | Available upon request |
| <strong>M.1-64xlarge</strong> | 768 | 26320 GiB | Available upon request |
| <strong>M.1-128xlarge</strong> | 1536 | 52640 GiB | Available upon request |



**Legacy cc Clusters:**

> **Tip:** In most cases, you **should not** use legacy sizes. [M.1 sizes](#available-sizes)
> offer better performance per credit for nearly all workloads. We recommend using
> M.1 sizes for all new clusters, and recommend migrating existing
> legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
> customers during the transition period as we move to deprecate legacy sizes.
> The legacy size information is provided for completeness.


Valid legacy cc cluster sizes are:

* `25cc`
* `50cc`
* `100cc`
* `200cc`
* `300cc`
* `400cc`
* `600cc`
* `800cc`
* `1200cc`
* `1600cc`
* `3200cc`
* `6400cc`
* `128C`
* `256C`
* `512C`

For clusters using legacy cc sizes, resource allocations are proportional to the
number in the size name. For example, a cluster of size `600cc` has 2x as much
CPU, memory, and disk as a cluster of size `300cc`, and 1.5x as much CPU,
memory, and disk as a cluster of size `400cc`.

Clusters of larger sizes can process data faster and handle larger data volumes.



See also:

- [M.1 to cc size mapping](/sql/m1-cc-mapping/).

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog:Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).

#### Resource allocation

To determine the specific resource allocation for a given cluster size, query
the [`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table.

> **Warning:** The values in the `mz_cluster_replica_sizes` table may change at any
> time. You should not rely on them for any kind of capacity planning.


#### Downtime

Resizing operation can incur downtime unless used with WAIT UNTIL READY option.
See [zero-downtime cluster resizing](#zero-downtime-cluster-resizing) for
details.

#### Zero-downtime cluster resizing



You can use the `WAIT UNTIL READY` option to perform a zero-downtime resizing,
which incurs **no downtime**. Instead of restarting the cluster, this approach
spins up an additional cluster replica under the covers with the desired new
size, waits for the replica to be hydrated, and then replaces the original
replica.

```sql
ALTER CLUSTER c1
SET (SIZE 'M.1-xsmall') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```

The `ALTER` statement is blocking and will return only when the new replica
becomes ready. This could take as long as the specified timeout. During this
operation, any other reconfiguration command issued against this cluster will
fail. Additionally, any connection interruption or statement cancelation will
cause a rollback — no size change will take effect in that case.

> **Note:** Using `WAIT UNTIL READY` requires that the session remain open: you need to
> make sure the Console tab remains open or that your `psql` connection remains
> stable.
> Any interruption will cause a cancellation, no cluster changes will take
> effect.

### Replication factor

The `REPLICATION FACTOR` option determines the number of replicas provisioned
for the cluster. Each replica of the cluster provisions a new pool of compute
resources to perform exactly the same computations on exactly the same data.
Each replica incurs cost, calculated as `cluster size * replication factor` per
second. See [Usage & billing](/administration/billing/) for more details.

#### Replication factor and fault tolerance

Provisioning more than one replica provides **fault tolerance**. Clusters with
multiple replicas can tolerate failures of the underlying hardware that cause a
replica to become unreachable. As long as one replica of the cluster remains
available, the cluster can continue to maintain dataflows and serve queries.

> **Note:** - Each replica incurs cost, calculated as `cluster size *
>   replication factor` per second. See [Usage &
>   billing](/administration/billing/) for more details.
> - Increasing the replication factor does **not** increase the cluster's work
>   capacity. Replicas are exact copies of one another: each replica must do
>   exactly the same work (i.e., maintain the same dataflows and process the same
>   queries) as all the other replicas of the cluster.
>   To increase the capacity of a cluster, you must increase its
>   [size](#resizing).


Materialize automatically assigns names to replicas (e.g., `r1`, `r2`). You can
view information about individual replicas in the Materialize console and the system
catalog.

#### Availability guarantees

When provisioning replicas,

- For clusters sized **under `3200cc`**, Materialize guarantees that all
  provisioned replicas in a cluster are spread across the underlying cloud
  provider's availability zones.

- For clusters sized at **`3200cc` and above**, even distribution of replicas
  across availability zones **cannot** be guaranteed.


## Required privileges

To execute the `ALTER CLUSTER` command, you need:

- Ownership of the cluster.

- To rename a cluster, you must also have membership in the `<new_owner_role>`.

- To swap names with another cluster, you must also have ownership of the other
  cluster.

See also:

- [Access control (Materialize Cloud)](/security/cloud/access-control/)
- [Access control (Materialize
  Self-Managed)](/security/self-managed/access-control/)

### Rename restrictions

You cannot rename system clusters, such as `mz_system` and `mz_catalog_server`.


## Examples

### Replication factor

The following example uses `ALTER CLUSTER` to update the `REPLICATION
FACTOR` of cluster `c1` to ``2``:

```mzsql
ALTER CLUSTER c1 SET (REPLICATION FACTOR 2);
```

Increasing the `REPLICATION FACTOR` increases the cluster's [fault
tolerance](#replication-factor-and-fault-tolerance), not its work capacity.


### Resizing

You can alter the cluster size with **no downtime** (i.e., [zero-downtime
cluster resizing](#zero-downtime-cluster-resizing)) by running the `ALTER
CLUSTER` command with the `WAIT UNTIL READY` [option](#syntax):

```mzsql
ALTER CLUSTER c1
SET (SIZE 'M.1-xsmall') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```

> **Note:** Using `WAIT UNTIL READY` requires that the session remain open: you need to
> make sure the Console tab remains open or that your `psql` connection remains
> stable.
> Any interruption will cause a cancellation, no cluster changes will take
> effect.

Alternatively, you can alter the cluster size immediately, without waiting, by
running the `ALTER CLUSTER` command:

```mzsql
ALTER CLUSTER c1 SET (SIZE 'M.1-xsmall');
```

This will incur downtime when the cluster contains objects that need
re-hydration before they are ready. This includes indexes, materialized views,
and some types of sources.

### Schedule



For use cases that require using [scheduled clusters](/sql/create-cluster/#scheduling),
you can set or change the originally configured schedule and related options
using the `ALTER CLUSTER` command.
```sql
ALTER CLUSTER c1 SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'));
```

See the reference documentation for [`CREATE
CLUSTER`](../create-cluster/#scheduling) or [`CREATE MATERIALIZED
VIEW`](../create-materialized-view/#refresh-strategies) for more details on
scheduled clusters.

### Converting unmanaged to managed clusters

> **Note:** When getting started with Materialize, we recommend using managed clusters. You
> can convert any unmanaged clusters to managed clusters by following the
> instructions below.


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

## See also

- [`CREATE CLUSTER`](/sql/create-cluster/)
- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)
