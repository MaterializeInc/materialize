---
title: "Disaster recovery"
description: "Learn about various disaster recovery (DR) strategies for Materialize."
disable_list: true
menu:
  main:
    parent: "manage"
    weight: 45
    identifier: "disaster-recovery"
---

The following outlines various disaster recovery (DR) strategies for
Materialize.

## Stable Environment ID

For disaster recovery, you must explicitly set the
`environmentId` in your Materialize CR (Custom Resource). Otherwise, Materialize
generates a new value each time the environment is recreated, resulting in:

- A new persist namespace in your blob storage
- Inaccessibility of existing persisted data
- Full data rehydration from sources

## Level 1: Basic configuration (Intra-Region Recovery)

Because Materialize is deterministic, we recommend running on a container
scheduler (such as AWS EKS, GCP GKE, Azure AKS). With this setup, the basic
configuration provides intra-region disaster recovery as long as:

- The scheduler is able to launch a new pod (or equivalent workload) within the
  region;

- The underlying object storage service remains available; and

- The metadata database is available.

In such cases, your mean time to recovery is the **same as your compute
cluster's rehydration time**.

{{< annotation type="💡 Recommendation" >}}

When running with the basic configuration, we recommend that you track
your rehydration time to ensure that it is within an acceptable range for your
business' risk tolerance.
{{</ annotation >}}

## Level 2:  Multi-replica clusters (High availability across AZs)

Materialize supports multi-replica clusters. If your region has multiple
Availability Zones (AZs), you can enable distribution of replicas across the AZs
in your region. To do so, specify `cluster_topology_spread_min_domains` value
greater than 1 (up to the number of AZs in your region). For example, if your
region has 3 AZs available, you can set `cluster_topology_spread_min_domains` to
3:

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

{{< include-md file="shared-content/multi-replica-az.md" >}}

Multi-replica **compute clusters** and multi-replica **serving clusters**
(excluding sink clusters) with replicas distributed across AZs provide
resilience against: machine-level failures; rack and building-level outages; and
AZ level failures for those clusters:

- With multi-replica **compute clusters**, each replica performs the same work.

- With multi-replica **serving clusters** (excluding sink clusters), each
  replica processes the same queries.

As such, your compute and serving clusters will continue to serve up-to-date
data uninterrupted in the case of a replica failure.

{{< annotation type="💡 Work capacity" >}}

Replicas are exact copies of one another: each replica must do exactly the same work as all the other replicas of the cluster(i.e., maintain the same dataflows and process the same queries). To increase the capacity of a cluster, you must increase its size.

{{</ annotation >}}

If you require resilience beyond a single region, consider the Level 3 strategy.

## Level 3: A duplicate Materialize environment (Inter-region resilience)

{{< note >}}

{{< include-md file="/shared-content/regional-dr-infrastructure-as-code.md" >}}

{{</ note >}}

For region-level fault tolerance, you can choose to have a second Materialize
environment in another region. With this strategy:

- You avoid complicated cross-regional communication.

- You avoid state dependency checks and verifications.

- And, because Materialize is deterministic, as long as your upstream sources
can also be accessed from the second region, the two Materialize environments
can guarantee the same results.

{{< annotation type="💡 No strict transactional consistency between environments" >}}

This approach does <red>**not**</red> offer strict transactional consistency
across regions. However, as long as both regions are caught up, the results
should be within about a second of each other.

{{</ annotation >}}

The duplicate Materialize environment setup can be adapted into a more
cost-effective setup if your deployment uses a [three-tier or a two-tier
architecture](/manage/operational-guidelines/). For details, see the [hybrid
variation](#hybrid-variation).

### Hybrid variation

{{< note >}}

- The hybrid strategy is available if your deployment uses a [three-tier or a
two-tier architecture](/manage/operational-guidelines/).

- {{< include-md file="/shared-content/regional-dr-infrastructure-as-code.md" >}}
{{</ note >}}

For a more cost-effective variation to the duplicate Materialize environment in
another region, you can choose a hybrid strategy where:

- Only the sources clusters are running in the second Materialize environment.

- The compute clusters are  provisioned **only** in the event of an incident.

When combined with a [multi-replica
approach](#level-2--multi-replica-clusters-high-availability-across-azs), you
have:

- Immediate failover during an AZ failure.

- Downtime equal to hydration time during intra-region failover.

## See also

- [Materialize DR
  characteristics](/manage/disaster-recovery/recovery-characteristics)
