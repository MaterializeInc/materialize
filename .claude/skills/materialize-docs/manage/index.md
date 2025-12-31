---
audience: developer
canonical_url: https://materialize.com/docs/manage/
complexity: advanced
description: This section contains various resources for managing Materialize.
doc_type: reference
keywords:
- '💡 Recommendation:'
- Usage & billing
- as long as
- 'same as your compute

  cluster''s rehydration time'
- Manage Materialize
- non
product_area: Operations
status: stable
title: Manage Materialize
---

# Manage Materialize

## Purpose
This section contains various resources for managing Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


This section contains various resources for managing Materialize.

## Operational guides

This section covers operational guides.

| Guide | Description |
|-------|-------------|
| [Operational guidelines](/manage/operational-guidelines/) | General operational guidelines |
| [Monitoring and alerting](/manage/monitor/) | Guides to set up monitoring and alerting |
| [Disaster Recovery](/manage/disaster-recovery/) | Disaster recovery strategies for Materialize Cloud |


## Manage via dbt/Terraform

This section covers manage via dbt/terraform.

| Guide | Description |
|-------|-------------|
| [Using dbt to manage Materialize](/manage/dbt/) | Guides for using dbt to manage Materialize |
| [Using Terraform to manage Materialize](/manage/terraform/) | Guides for using Terraform to manage Materialize |

## Usage and billing

This section covers usage and billing.

| Guide | Description |
|-------|-------------|
| [**Usage & billing**](/administration/billing/) | Understand the billing model of Materialize |


---

## Appendix: Alternative cluster architectures


If the [recommended three-tier
architecture](/manage/operational-guidelines/#three-tier-architecture)
is infeasible or unnecessary due to low volume or a **non**-production setup, a
two-tier or a one-tier architecture may suffice.

## Two-tier architecture

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See best practices documentation --> --> -->

## One-tier architecture

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See best practices documentation --> --> -->


---

## Disaster recovery (Cloud)


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

## Monitoring and alerting


This section covers monitoring and alerting.

## Cloud

This section covers cloud.

### Monitoring

You can monitor the performance and overall health of your Materialize region.
To help you get started, the following guides are available:

- [Datadog](/manage/monitor/cloud/datadog/)

- [Grafana](/manage/monitor/cloud/grafana/)

### Alerting

After setting up a monitoring tool, you can configure alert rules. Alert rules
send a notification when a metric surpasses a threshold. This will help you
prevent operational incidents. For alert rules guidelines, see
[Alerting](/manage/monitor/cloud/alerting/).

## Self-Managed

This section covers self-managed.

### Monitoring

You can monitor the performance and overall health of your Self-Manaed
Materialize.

To help you get started, the following guides are available:

- [Grafana using Prometheus](/manage/monitor/self-managed/prometheus/)

- [Datadog using Prometheus SQL Exporter](/manage/monitor/self-managed/datadog/)


### Alerting

After setting up a monitoring tool, you can configure alert rules. Alert rules
send a notification when a metric surpasses a threshold. This will help you
prevent operational incidents. For alert rules guidelines, see
[Alerting](/manage/monitor/self-managed/alerting/).


---

## Operational guidelines


The following provides some general guidelines for production.

## Clusters

This section covers clusters.

### Production clusters for production workloads only

Use production cluster(s) for production workloads only. That is, avoid using
production cluster(s) to run development workloads or non-production tasks.

### Three-tier architecture

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See best practices documentation --> --> -->

#### Alternatives

If a three-tier architecture is infeasible or unnecessary due to low volume or a
non-production setup, a two cluster or a single cluster architecture may
suffice.

See [Appendix: Alternative cluster
architectures](/manage/appendix-alternative-cluster-architectures/) for details.

## Sources

This section covers sources.

### Scheduling

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See best practices documentation --> --> -->

### Separate cluster(s) for sources

In production, if possible, use a dedicated cluster for
[sources](/concepts/sources/); i.e., avoid putting sources on the same cluster
that hosts compute objects, sinks, and/or serves queries.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See best practices documentation --> --> -->

See also [Production cluster architecture](#three-tier-architecture).

## Sinks

This section covers sinks.

### Separate sinks from sources

To allow for [blue/green deployment](/manage/dbt/blue-green-deployments/), avoid
putting sinks on the same cluster that hosts sources .

See also [Cluster architecture](#three-tier-architecture).

## Snapshotting and hydration considerations

- For upsert sources, snapshotting is a resource-intensive operation that can
  require a significant amount of CPU and memory.

- During hydration (both initial and subsequent rehydrations), materialized
  views require memory proportional to both the input and output. When
  estimating required resources, consider both the hydration cost and the
  steady-state cost.

- During sink creation (initial hydration), sinks need to load an entire
  snapshot of the data in memory.

## Role-based access control (RBAC)

This section covers role-based access control (rbac).

#### Cloud

### Cloud

<!-- Unresolved shortcode: {{% yaml-sections data="rbac/recommendations-cloud... -->

#### Self-Managed

### Self-Managed

<!-- Unresolved shortcode: {{% yaml-sections data="rbac/recommendations-sm"
h... -->


---

## Use dbt to manage Materialize


[dbt](https://docs.getdbt.com/docs/introduction) has become the standard for
data transformation ("the T in ELT"). It combines the accessibility of SQL with
software engineering best practices, allowing you to not only build reliable
data pipelines, but also document, test and version-control them.

Setting up a dbt project with Materialize is similar to setting it up with any
other database that requires a non-native adapter.

> **Note:** 
The `dbt-materialize` adapter can only be used with **dbt Core**. Making the
adapter available in dbt Cloud depends on prioritization by dbt Labs. If you
require dbt Cloud support, please [reach out to the dbt Labs team](https://www.getdbt.com/community/join-the-community/).


## Available guides


**To get started**
[Get started with dbt and Materialize](./get-started/)
**Development guidelines**
[Development guidelines](./development-workflows)
**Deployment**
- [Blue-green deployment guide](/manage/dbt/blue-green-deployments/)

- [Slim deployment guide](/manage/dbt/slim-deployments/)


## See also

As a tool primarily meant to manage your data model, the `dbt-materialize`
adapter does not expose all Materialize objects types. If there is a **clear
separation** between data modeling and **infrastructure management ownership**
in your team, and you want to manage objects like
[clusters](/concepts/clusters/), [connections](/sql/create-connection/), or
[secrets](/sql/create-secret/) as code, we recommend using the [Materialize
Terraform provider](/manage/terraform/) as a complementary deployment tool.


---

## Use Terraform to manage Materialize


[Terraform](https://www.terraform.io/) is an infrastructure-as-code tool that
allows you to manage your resources in a declarative configuration language.
Materialize maintains a [Terraform provider](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs)
to help you safely and predictably provision and manage connections, sources,
and other database objects.

Materialize also maintains [several
modules](/manage/terraform/manage-cloud-modules) that make it easier to manage
other cloud resources that Materialize depends on. Modules allow you to bypass
manually configuring cloud resources and are an efficient way of deploying
infrastructure with a single `terraform apply` command.

## Available guides


**To get started**
[Get started with Terraform and Materialize](./get-started/)

**Manage resources**
[Manage Materialize resources](./manage-resources)

[Manage cloud resources](./manage-cloud-modules/)
**Additional modules**
- [Appendix: Secret stores](./appendix-secret-stores/)


## Contributing

If you want to help develop the Materialize provider, check out the [contribution guidelines](https://github.com/MaterializeInc/terraform-provider-materialize/blob/main/CONTRIBUTING.md).