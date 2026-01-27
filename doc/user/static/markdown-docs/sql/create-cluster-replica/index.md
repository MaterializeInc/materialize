# CREATE CLUSTER REPLICA
`CREATE CLUSTER REPLICA` provisions a new replica of a cluster.

`CREATE CLUSTER REPLICA` provisions a new replica for an [**unmanaged**
cluster](/sql/create-cluster/#unmanaged-clusters).

> **Tip:** When getting started with Materialize, we recommend starting with managed
> clusters.


## Syntax



```mzsql
CREATE CLUSTER REPLICA <cluster_name>.<replica_name> (
    SIZE = <text>
);

```

| Syntax element | Description |
| --- | --- |
| `<cluster_name>` | The cluster you want to attach a replica to.  |
| `<replica_name>` | A name for this replica.  |
| `SIZE` | The size of the resource allocations for the cluster.  {{< yaml-list column="Cluster size" data="m1_cluster_sizing" numColumns="3" >}}  See [Size](#size) for details as well as legacy sizes available.  |


## Details

### Size

The `SIZE` option for replicas is identical to the [`SIZE` option for
clusters](/sql/create-cluster/#size) option, except that the size applies only
to the new replica.


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

Materialize offers the following legacy cc cluster sizes:

> **Tip:** In most cases, you **should not** use legacy sizes. [M.1 sizes](#size)
> offer better performance per credit for nearly all workloads. We recommend using
> M.1 sizes for all new clusters, and recommend migrating existing
> legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
> customers during the transition period as we move to deprecate legacy sizes.
> The legacy size information is provided for completeness.


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

The resource allocations are proportional to the number in the size name. For
example, a cluster of size `600cc` has 2x as much CPU, memory, and disk as a
cluster of size `300cc`, and 1.5x as much CPU, memory, and disk as a cluster of
size `400cc`. To determine the specific resource allocations for a size,
query the [`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes) table.

> **Warning:** The values in the `mz_cluster_replica_sizes` table may change at any
> time. You should not rely on them for any kind of capacity planning.


Clusters of larger sizes can process data faster and handle larger data volumes.



See also:

- [M.1 to cc size mapping](/sql/m1-cc-mapping/).

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog:Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).


### Homogeneous vs. heterogeneous hardware provisioning

Because Materialize uses active replication, all replicas will be instructed to
do the same work, irrespective of their resource allocation.

For the most stable performance, we recommend using the same size and disk
configuration for all replicas.

However, it is possible to use different replica configurations in the same
cluster. In these cases, the replicas with less resources will likely be
continually burdened with a backlog of work. If all of the faster replicas
become unreachable, the system might experience delays in replying to requests
while the slower replicas catch up to the last known time that the faster
machines had computed.

## Example

```mzsql
CREATE CLUSTER REPLICA c1.r1 (SIZE = 'M.1-large');
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the cluster.

## See also

- [`DROP CLUSTER REPLICA`]

[AWS availability zone ID]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
[`DROP CLUSTER REPLICA`]: /sql/drop-cluster-replica
