---
audience: developer
canonical_url: https://materialize.com/docs/installation/appendix-cluster-sizes/
complexity: advanced
description: Reference page on self-managed cluster sizes
doc_type: reference
keywords:
- 'Note:'
- 'Tip:'
- 'Appendix: Cluster sizes'
product_area: Deployment
status: stable
title: 'Appendix: Cluster sizes'
---

# Appendix: Cluster sizes

## Purpose
Reference page on self-managed cluster sizes

If you need to understand the syntax and options for this command, you're in the right place.


Reference page on self-managed cluster sizes


## Default Cluster Sizes

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

## Custom Cluster Sizes

When installing the Materialize Helm chart, you can override the [default
cluster sizes and resource allocations](#default-cluster-sizes). These
cluster sizes are used for both internal clusters, such as the `system_cluster`,
as well as user clusters.

> **Tip:** 

In general, you should not have to override the defaults. At minimum, we
recommend that you keep the 25-200cc cluster sizes.


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
        selectors: <map>           # k8s label selectors
        # ex: kubernetes.io/arch: amd64
```

<!-- Dynamic table: best_practices/sizing_recommendation - see original docs -->

> **Note:** 

If you have modified the default cluster size configurations, you can query the
[`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table for the specific resource allocations.