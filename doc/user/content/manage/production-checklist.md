---
title: "Production checklist"
description: "Production checklist for Self-Managed Materialize."
menu:
  main:
    parent: "operational-guidelines"
    weight: 5
---

## Manage infrastructure as code

Use infrastructure-as-code (e.g., Terraform, dbt, etc) and store in version
control (e.g., GitHub).

## Specify the Environment ID

For [disaster recovery](/manage/disaster-recovery/) purposes, set the
`environmentId` in your Materialize CR (Custom Resource). Otherwise, Materialize
generates a new value each time the environment is recreated, resulting in:

- A new persist namespace in your blob storage
- Inaccessibility of existing persisted data
- Full data rehydration from sources

## Use a container scheduler

Use a container scheduler such as AWS EKS, GCP GKE, Azure AKS. With this setup,
the basic configuration provides [intra-region disaster
recovery](/manage/disaster-recovery/#level-1-basic-configuration-intra-region-recovery)
as long as:

- The scheduler is able to launch a new pod (or equivalent workload) within the
  region, and

- The underlying object storage service remains available.

## Use multi-replica clusters distributed across Availability Zones

Multi-replica **compute clusters** and multi-replica **serving clusters**
(excluding sink clusters) with replicas distributed across Availability Zones
(AZs) provide resilience against: machine-level failures; rack and
building-level outages; and AZ level failures for those clusters.

If your region has multiple AZs, you can enable distribution of replicas across
the AZs in your region. To do so, specify `cluster_topology_spread_min_domains`
value greater than 1 (up to the number of AZs in your region). For example, if
your region has 3 AZs available, you can set
`cluster_topology_spread_min_domains` to 3:

```yaml
materialize_instances = [
  {
    ...
    environmentd_extra_args = [
      "--system-parameter-default=cluster_topology_spread_min_domains=3"
    ]
  }
]
```

## Isolate production workloads

Use production cluster(s) for production workloads only. That is, avoid using
production cluster(s) to run development workloads or non-production tasks.

## Use a three-tier cluster architecture

In production, use a [three-tier
architecture](/manage/operational-guidelines/#three-tier-architecture), if
feasible.

## Set up monitoring and alerts

[Monitor and set up alerts](/manage/monitor/) for pending pods and unhealthy
nodes.

## Scale nodes appropriately

Set the maximum node count to be **at least 2x steady-state node count**.

## Avoid node consolidation on GCP

- For nodes hosting `environmentd` or `clusterd`, do <red>**not**</red> use node
  consolidation.
