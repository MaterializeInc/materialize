---
audience: developer
canonical_url: https://materialize.com/docs/manage/disaster-recovery/
complexity: advanced
description: Learn about various disaster recovery (DR) strategies for Materialize.
doc_type: reference
keywords:
- Disaster recovery (Cloud)
- 'same as your compute

  cluster''s rehydration time'
- up to and including `3200cc`
- '💡 Recommendation:'
- as long as
- 'Note:'
product_area: Operations
status: stable
title: Disaster recovery (Cloud)
---

# Disaster recovery (Cloud)

## Purpose
Learn about various disaster recovery (DR) strategies for Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


Learn about various disaster recovery (DR) strategies for Materialize.



The following outlines various disaster recovery (DR) strategies for
Materialize.

## Level 1: Basic configuration (Intra-Region Recovery)

Because Materialize is deterministic and its infrastructure runs on a container
scheduler (AWS EKS), basic Materialize configuration provides intra-region
disaster recovery **as long as**:

- Materialize can spin up a new pod somewhere in the region, and

- S3 is available.

In such cases, your mean time to recovery is the **same as your compute
cluster's rehydration time**.

> **💡 Recommendation:** When running with the basic configuration, we recommend that you track
your rehydration time to ensure that it is within an acceptable range for your
business' risk tolerance.

## Level 2:  Multi-replica clusters (High availability across AZs)

> **Note:** 
The hybrid strategy is available if your deployment uses a [three-tier or a
two-tier architecture](/manage/operational-guidelines/).


Materialize supports multi-replica clusters, allowing for distribution across
Availability Zones (AZs):

- For clusters sized **up to and including `3200cc`**, Materialize guarantees
  that all provisioned replicas in a cluster are distributed across the
  underlying cloud provider's availability zones.

- For clusters sized **above `3200cc`**, even distribution of replicas
  across availability zones **cannot** be guaranteed.


Multi-replica **compute clusters** and multi-replica **serving clusters**
(excluding sink clusters) with replicas distributed across AZs provide DR
resilience against: machine-level failures; rack and building-level outages; and
AZ level failures for those clusters:

- With multi-replica **compute clusters**, each replica performs the same work.

- With multi-replica **serving clusters** (excluding sink clusters), each
  replica processes the same queries.

As such, your compute and serving clusters will continue to serve up-to-date
data uninterrupted in the case of a replica failure.

> **💡 Cost and work capacity:** - Each replica incurs cost, calculated as `cluster size *
  replication factor` per second. See [Usage &
  billing](/administration/billing/) for more details.

- Increasing the replication factor does **not** increase the cluster's work
  capacity. Replicas are exact copies of one another: each replica must do
  exactly the same work as all the other replicas of the cluster(i.e., maintain
  the same dataflows and process the same queries). To increase the capacity of
  a cluster, you must increase its size.

If you require resilience beyond a single region, consider the Level 3 strategy.

## Level 3: A duplicate Materialize environment (Inter-region resilience)

> **Note:** 

<!-- Include not found: /shared-content/regional-dr-infrastructure-as-code.md -->



For region-level fault tolerance, you can choose to have a second Materialize
environment in another region. With this strategy:

- You avoid complicated cross-regional communication.

- You avoid state dependency checks and verifications.

- And, because Materialize is deterministic, as long as your upstream sources
can also be accessed from the second region, the two Materialize environments
can guarantee the same results.

> **💡 No strict transactional consistency between environments:** This approach does <red>**not**</red> offer strict transactional consistency
across regions. However, as long as both regions are caught up, the results
should be within about a second of each other.

The duplicate Materialize environment setup can be adapted into a more
cost-effective setup if your deployment uses a [three-tier or a two-tier
architecture](/manage/operational-guidelines/). For details, see the [hybrid
variation](#hybrid-variation).

### Hybrid variation

> **Note:** 

- The hybrid strategy is available if your deployment uses a [three-tier or a
two-tier architecture](/manage/operational-guidelines/).

- <!-- Include not found: /shared-content/regional-dr-infrastructure-as-code.md -->


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




---

## Materialize Cloud DR characteristics


The following provides  various failure mode impact and recovery
characteristics for Materialize.

## System Environment

Materialize system components provide core features like database consensus,
durable storage, network access and security, and compute provisioning. These
components are deployed with high available configurations or with automated
recovery mechanisms. However, they can still experience outages due to failures
in underlying providers.

<!-- Dynamic table: disaster_recovery/system_environment - see original docs -->

> **Recommendation(s):** - Use privatelink when possible and configure to use multiple AZs.

- If you are concerned about multi-AZ outages, consider [duplicate Materialize
  environment in second region strategy](/manage/disaster-recovery/#level-3-a-duplicate-materialize-environment-inter-region-resilience)

## Database environment

This section covers database environment.

### `environmentd`

The `environmentd` runs on a single node in a single AZ. `environmentd`
has no data; as such, the RPO is `N/A`.

The component has the following failure characteristics:

<!-- Dynamic table: disaster_recovery/environmentd - see original docs -->
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

> **Key point(s):** - If `environmentd` becomes unavailable, RTO is non-zero.

- If `environmentd` becomes unavailable, its RTO affects the RTO of the clusters
  as you cannot access data while `environmentd` is unavailable.

### Clusters

<!-- Dynamic table: disaster_recovery/cluster - see original docs -->
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

> **Key point(s):** - Cluster RTO can be affected if the environmentd is down (seconds to minutes).

- For regional failover strategy, you can use a [duplicate Materialize
  environment
  strategy](/manage/disaster-recovery/#level-3-a-duplicate-materialize-environment-inter-region-resilience).

## Materialize data corruption/operations error

<!-- Dynamic table: disaster_recovery/mz_operations - see original docs -->
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

## End-user error

<!-- Dynamic table: disaster_recovery/human - see original docs -->
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

> **Key point(s):** - You can use [RBAC](/security/access-control/) to reduce the risk of
  accidentally dropping sources (and other objects) in Materialize.

## See also

- [Disaster recovery (DR) strategies](/manage/disaster-recovery/)



