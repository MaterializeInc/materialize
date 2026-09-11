---
title: "Cluster sizes"
description: "Reference for Materialize Cloud cluster sizes and their resource allocations."
menu:
  main:
    name: "Cluster sizes"
    parent: "materialize-cloud"
    weight: 35
aliases:
  - /clusters/m1-cc-mapping/
  - /sql/m1-cc-mapping/
  - /reference/m1-cc-mapping/
---

Materialize Cloud clusters come in two families of sizes: `cc` sizes and `M.1`
sizes. We recommend `cc` sizes for most workloads; `M.1` sizes provide access
to additional disk capacity, which can be beneficial for disk-intensive
workloads. See [`CREATE CLUSTER`](/sql/create-cluster/) for details on
choosing between them.

## cc cluster sizes

{{% include-headless "/headless/cluster-size-disclaimer" %}}

{{% materialize-cloud/cluster-sizes-cc %}}

To confirm the current resource allocations for a size, query the
[`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table.

## M.1 cluster sizes

{{% include-headless "/headless/cluster-size-disclaimer" %}}

`M.1` sizes report a single **Total Capacity** value instead of separate
memory and disk figures: the value is the combined memory-and-disk pool
available to the cluster, not two independent limits. For example, an
`M.1-xsmall` cluster has 106 GiB of combined capacity to split between memory
and disk, not 106 GiB of each.

{{< yaml-table data="m1_cluster_sizing" >}}

See also:

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog: Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).
