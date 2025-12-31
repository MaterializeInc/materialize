# Manage Materialize



This section contains various resources for managing Materialize.

## Operational guides

| Guide | Description |
|-------|-------------|
| [Operational guidelines](/manage/operational-guidelines/) | General operational guidelines |
| [Monitoring and alerting](/manage/monitor/) | Guides to set up monitoring and alerting |
| [Disaster Recovery](/manage/disaster-recovery/) | Disaster recovery strategies for Materialize Cloud |


## Manage via dbt/Terraform

| Guide | Description |
|-------|-------------|
| [Using dbt to manage Materialize](/manage/dbt/) | Guides for using dbt to manage Materialize |
| [Using Terraform to manage Materialize](/manage/terraform/) | Guides for using Terraform to manage Materialize |

## Usage and billing

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

{{% best-practices/architecture/two-tier %}}

## One-tier architecture

{{% best-practices/architecture/one-tier %}}




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

{{< annotation type="💡 Recommendation" >}}

When running with the basic configuration, we recommend that you track
your rehydration time to ensure that it is within an acceptable range for your
business' risk tolerance.
{{</ annotation >}}

## Level 2:  Multi-replica clusters (High availability across AZs)

{{< note >}}
The hybrid strategy is available if your deployment uses a [three-tier or a
two-tier architecture](/manage/operational-guidelines/).
{{</ note >}}

Materialize supports multi-replica clusters, allowing for distribution across
Availability Zones (AZs):

{{< include-md file="shared-content/multi-replica-az.md" >}}

Multi-replica **compute clusters** and multi-replica **serving clusters**
(excluding sink clusters) with replicas distributed across AZs provide DR
resilience against: machine-level failures; rack and building-level outages; and
AZ level failures for those clusters:

- With multi-replica **compute clusters**, each replica performs the same work.

- With multi-replica **serving clusters** (excluding sink clusters), each
  replica processes the same queries.

As such, your compute and serving clusters will continue to serve up-to-date
data uninterrupted in the case of a replica failure.

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

## See also

- [Materialize DR
  characteristics](/manage/disaster-recovery/recovery-characteristics)




---

## Monitoring and alerting


## Cloud

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

### Production clusters for production workloads only

Use production cluster(s) for production workloads only. That is, avoid using
production cluster(s) to run development workloads or non-production tasks.

### Three-tier architecture

{{% best-practices/architecture/three-tier %}}

#### Alternatives

If a three-tier architecture is infeasible or unnecessary due to low volume or a
non-production setup, a two cluster or a single cluster architecture may
suffice.

See [Appendix: Alternative cluster
architectures](/manage/appendix-alternative-cluster-architectures/) for details.

## Sources

### Scheduling

{{% best-practices/ingest-data/scheduling %}}

### Separate cluster(s) for sources

In production, if possible, use a dedicated cluster for
[sources](/concepts/sources/); i.e., avoid putting sources on the same cluster
that hosts compute objects, sinks, and/or serves queries.

{{% best-practices/architecture/upsert-source %}}

See also [Production cluster architecture](#three-tier-architecture).

## Sinks

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

{{< tabs >}}

{{< tab "Cloud" >}}

### Cloud

{{% yaml-sections data="rbac/recommendations-cloud"
heading-field="recommendation" heading-level=5 %}}

{{< /tab >}}

{{< tab "Self-Managed" >}}

### Self-Managed

{{% yaml-sections data="rbac/recommendations-sm"
heading-field="recommendation" heading-level=5 %}}
{{< /tab >}}

{{< /tabs >}}




---

## Use dbt to manage Materialize


[dbt](https://docs.getdbt.com/docs/introduction) has become the standard for
data transformation ("the T in ELT"). It combines the accessibility of SQL with
software engineering best practices, allowing you to not only build reliable
data pipelines, but also document, test and version-control them.

Setting up a dbt project with Materialize is similar to setting it up with any
other database that requires a non-native adapter.

{{< note >}}
The `dbt-materialize` adapter can only be used with **dbt Core**. Making the
adapter available in dbt Cloud depends on prioritization by dbt Labs. If you
require dbt Cloud support, please [reach out to the dbt Labs team](https://www.getdbt.com/community/join-the-community/).
{{</ note >}}


## Available guides

{{< multilinkbox >}}
{{< linkbox title="To get started" >}}
[Get started with dbt and Materialize](./get-started/)
{{</ linkbox >}}
{{< linkbox title="Development guidelines" >}}
[Development guidelines](./development-workflows)
{{</ linkbox >}}
{{< linkbox title="Deployment" >}}
- [Blue-green deployment guide](/manage/dbt/blue-green-deployments/)

- [Slim deployment guide](/manage/dbt/slim-deployments/)

{{</ linkbox >}}
{{</ multilinkbox >}}


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

{{< multilinkbox >}}
{{< linkbox title="To get started" >}}
[Get started with Terraform and Materialize](./get-started/)
{{</ linkbox >}}

{{< linkbox title="Manage resources" >}}

[Manage Materialize resources](./manage-resources)

[Manage cloud resources](./manage-cloud-modules/)

{{</ linkbox >}}
{{< linkbox title="Additional modules" >}}

- [Appendix: Secret stores](./appendix-secret-stores/)

{{</ linkbox >}}
{{</ multilinkbox >}}


## Contributing

If you want to help develop the Materialize provider, check out the [contribution guidelines](https://github.com/MaterializeInc/terraform-provider-materialize/blob/main/CONTRIBUTING.md).



