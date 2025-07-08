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

## Level 1: Basic configuration (Intra-Region Recovery)

Because Materialize is deterministic and its infrastructure runs on a container
scheduler (AWS MSK), basic Materialize configuration provides intra-region
disaster recovery **as long as**:

- Materialize can spin up a new pod somewhere in the region, and

- S3 is available.

In such cases, your mean time to recovery is the **same as your compute
cluster's rehydration time**.

{{< annotation type="💡 Recommendation" >}}

When running with the basic configuration, we recommend that you track
your rehydration time to ensure that it is within an acceptable range for your
business' risk tolerance.
{{</ annotation >}}

## Level 2:  Multi-replica clusters (High availability across AZs)

Materialize supports multi-replica clusters, which enable compute objects to run
across multiple availability zones (AZs):

{{< include-md file="shared-content/multi-replica-az.md" >}}

Multi-replica clusters provides DR resilience against:

- Machine-level failures,
- Rack and building-level outages, and
- AZ level failures.

That is, your clusters will continue to serve data uninterrupted in the case of
a replica failure.

{{< annotation type="💡 Cost and work capacity" >}}

{{< include-md file="shared-content/cluster-replica-cost-capacity-notes.md" >}}

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
