---
title: "CREATE CLUSTER REPLICA"
description: "`CREATE CLUSTER REPLICA` provisions a new replica of a cluster."
pagerank: 50
menu:
  main:
    parent: commands
---


`CREATE CLUSTER REPLICA` provisions a new replica for an [**unmanaged**
cluster](/sql/create-cluster/#unmanaged-clusters).

{{< tip >}}
When getting started with Materialize, we recommend starting with managed
clusters.
{{</ tip >}}

## Conceptual framework

A cluster consists of zero or more replicas. Each replica of a cluster is a pool
of compute resources that performs exactly the same computations on exactly the
same data.

Using multiple replicas of a cluster facilitates **fault tolerance**. Clusters
with multiple replicas can tolerate failures of the underlying hardware or
network. As long as one replica remains reachable, the cluster as a whole
remains available.

## Syntax

{{< diagram "create-cluster-replica.svg" >}}

Field | Use
------|-----
_cluster_name_ | The cluster you want to attach a replica to.
_replica_name_ | A name for this replica.

### Options

{{% replica-options %}}

## Details

### Size

The `SIZE` option for replicas is identical to the [`SIZE` option for
clusters](/sql/create-cluster/#size) option, except that the size applies only
to the new replica.

{{< tabs >}}
{{< tab "M.1 Clusters" >}}
{{< yaml-table data="m1_cluster_sizing" >}}

{{< /tab >}}

{{< tab "Legacy cc Clusters" >}}

Materialize offers the following legacy cc cluster sizes:

{{< tip >}}
In most cases, you **should not** use legacy sizes. [M.1 sizes](#size)
offer better performance per credit for nearly all workloads. We recommend using
M.1 sizes for all new clusters, and recommend migrating existing
legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
customers during the transition period as we move to deprecate legacy sizes.

The legacy size information is provided for completeness.
{{< /tip >}}

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

{{< warning >}}
The values in the `mz_cluster_replica_sizes` table may change at any
time. You should not rely on them for any kind of capacity planning.
{{< /warning >}}

Clusters of larger sizes can process data faster and handle larger data volumes.
{{< /tab >}}
{{< /tabs >}}

See also:

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
CREATE CLUSTER REPLICA c1.r1 (SIZE = '400cc');
```

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/create-cluster-replica.md" >}}

## See also

- [`DROP CLUSTER REPLICA`]

[AWS availability zone ID]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
[`DROP CLUSTER REPLICA`]: /sql/drop-cluster-replica
