---
title: "Production guide"
description: "Production guide/checklist for Self-Managed Materialize."
draft: true

---

## Specify the Environment ID

For [disaster recovery](/manage/disaster-recovery/) purposes, set the
`environmentId` in your Materialize CR (Custom Resource). Otherwise, Materialize
generates a new value each time the environment is recreated, resulting in:

- A new persist namespace in your blob storage
- Inaccessibility of existing persisted data
- Full data rehydration from sources

## Use a container scheduler

Use a container scheduler (such as AWS EKS, GCP GKE, Azure AKS). With this
setup, the basic configuration provides intra-region disaster recovery as long
as:

- The scheduler is able to launch a new pod (or equivalent workload) within the
  region, and

- The underlying object storage service remains available.

## Use multi-replica clusters distsributed across Availability Zones

Multi-replica **compute clusters** and multi-replica **serving clusters**
(excluding sink clusters) with replicas distributed across AZs provide DR
resilience against: machine-level failures; rack and building-level outages; and
AZ level failures for those clusters.

To enable distribution of replicas across Availability Zones (AZs), specify
`cluster_topology_spread_min_domains` value greater than 1.

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
