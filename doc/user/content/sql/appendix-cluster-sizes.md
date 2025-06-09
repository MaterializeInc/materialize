---
title: "Appendix: Cluster sizes"
description: "Reference page on self-managed cluster sizes"
menu:
  main:
    parent: "reference"
    weight: 900
---

## Default Cluster Sizes

{{% self-managed/materialize-cluster-sizes %}}

## Custom Cluster Sizes

When installing the Materialize Helm chart, you can override the [default
cluster sizes and resource allocations](/sql/appendix-cluster-sizes/). These
cluster sizes are used for both internal clusters, such as the `system_cluster`,
as well as user clusters.

{{< tip >}}

In general, you should not have to override the defaults. At minimum, we
recommend that you keep the 25-200cc cluster sizes.

{{</ tip >}}

```yaml
operator:
  clusters:
    sizes:
      <size>:
        workers: <int>
        scale: 1                  # Generally, should be set to 1.
        cpu_exclusive: <bool>
        cpu_limit: <float>         # e.g., 6
        credits_per_hour: "0.0"    # N/A for self-managed.
        disk_limit: <string>       # e.g., "93150MiB"
        memory_limit: <string>     # e.g., "46575MiB"
```

{{< yaml-table data="best_practices/sizing_recommendation" >}}

{{< note >}}

If you have modified the default cluster size configurations, you can query the
[`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table for the specific resource allocations.

{{< /note >}}
