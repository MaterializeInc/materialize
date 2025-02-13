---
title: "Appendix: Cluster sizes"
description: "Reference page on self-managed cluster sizes"
menu:
  main:
    parent: "reference"
    weight: 900
---

{{% self-managed/materialize-cluster-sizes %}}

{{< note >}}

If you have modified the default cluster size configurations, you can query the
[`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table for the specific resource allocations.

{{< /note >}}
