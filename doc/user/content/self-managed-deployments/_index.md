---
title: "Self-Managed Deployments"
description: "Learn about the key components and architecture of self-managed Materialize deployments."
disable_list: true
aliases:
  - /self-hosted/concepts/
  - /self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/
menu:
  main:
    identifier: "sm-deployments"
    weight: 5
---

## Overview

Self-Managed Materialize deployments on Kubernetes consist of several layers of
components that work together to provide a fully functional database
environment. Understanding these components and how they interact is essential
for deploying, managing, and troubleshooting your Self-Managed Materialize.

This page provides an overview of the core architectural components in a Self-Managed deployment, from the infrastructure level (Helm chart) down to the application level (clusters and replicas).

## Architecture layers

A Self-Managed Materialize deployment is organized into the following layers:

Layer | Component | Description
------|-----------|------------
**Infrastructure** | [Helm Chart](#helm-chart) | Package manager component that bootstraps the Kubernetes deployment
**Orchestration** | [Materialize Operator](#materialize-operator) | Kubernetes operator that manages Materialize instances
**Database** | [Materialize Instance](#materialize-instance) | The Materialize database instance itself
**Compute** | [Clusters and Replicas](#clusters-and-replicas) | Isolated compute resources for workloads

## Helm chart

The Helm chart is the entry point for deploying Materialize in a self-managed Kubernetes environment. It serves as a package manager component that defines and deploys the Materialize Operator.

### Working with the Helm chart

You interact with the Helm chart through standard Helm commands. For example:

- To add the Materialize Helm chart repository:

  ```bash
  helm repo add materialize https://materializeinc.github.io/materialize
  ```

- To update the repository index:

  ```bash
  helm repo update materialize
  ```

- To install the Materialize Helm chart and deploy the Materialize Operator and
  other resources:

  ```bash
  helm install materialize materialize/materialize-operator
  ```

- To upgrade the the Materialize Helm chart (and the Materialize Operator and
  other resources):

  ```bash
  helm upgrade materialize materialize/materialize-operator
  ```

- To uninstall the Helm chart (and the Materialize Operator and other
  resources):

  ```bash
  helm uninstall materialize
  ```

### What gets installed

```bash
helm install materialize materialize/materialize-operator
```

When you install the the Materialize Helm Chart, it:

- Deploys the **Materialize Operator** as a Kubernetes deployment.
- Creates necessary cluster-wide resources (CRDs, RBAC roles, service accounts).
- Configures operator settings and permissions.

Once installed, the **Materialize Operator** handles the deployment and
management of Materialize instances.

## Materialize Operator

The Materialize Operator (implemented as `orchestratord`) is a Kubernetes operator that automates the deployment and lifecycle management of Materialize instances. It implements the [Kubernetes operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) to extend Kubernetes with domain-specific knowledge about Materialize.

### Managed resources

The operator watches for Materialize custom resources and creates/manages all the Kubernetes resources required to run a Materialize instance, including:

- **Namespaces**: Isolated Kubernetes namespaces for each instance
- **Services**: Network services for connecting to Materialize
- **Network Policies**: Network isolation and security rules
- **Certificates**: TLS certificates for secure connections
- **ConfigMaps and Secrets**: Configuration and sensitive data
- **Deployments**: These support the `balancerd` and `console` pod used as the ingress layer for Materialize.
- **StatefulSets**: `environmentd` and `clusterd` which are the database control plane and compute resources respectively.

### Configuration

For configuration options for the Materialize Operator, see
the [Materialize Operator Configuration
page](/self-managed-deployments/appendix/configuration/).

## Materialize Instance

A Materialize instance is the actual database that you connect to and interact with. Each instance is an isolated Materialize deployment with its own data, configuration, and compute resources.

### Components

When you create a Materialize instance, the operator deploys three core components as Kubernetes resources:

- **`environmentd`**: The main database control plane, deployed as a
  StatefulSet.

  **`environmentd`** runs as a Kubernetes pod and is the primary component of a
  Materialize instance. It houses the control plane and contains:

  - **Adapter**: The SQL interface that handles client connections, query parsing, and planning
  - **Storage Controller**: Maintains durable metadata for storage
  - **Compute Controller**: Orchestrates compute resources and manages system state

  On startup, `environmentd` will create several built-in clusters.

  When you connect to Materialize with a SQL client (e.g., `psql`), you're connecting to `environmentd`.

- **balancerd**: A pgwire and http proxy used to connect to environmentd,
  deployed as a Deployment.
- **console**: Web-based administration interface, deployed as a Deployment.

### Instance responsibilities

A Materialize instance manages:

- **SQL objects**: Sources, views, materialized views, indexes, sinks
- **Schemas and databases**: Logical organization of objects
- **User connections**: SQL client connections and authentication
- **Catalog metadata**: System information about all objects and configuration
- **Compute orchestration**: Coordination of work across clusters and replicas


### Deploying with the operator

To deploy Materialize instances with the operator, create and apply Materialize
custom resources definitions(CRDs). For a full list of fields available for the
Materialize CR, see [Materialize CRD Field
Descriptions](/self-managed-deployments/appendix/materialize-crd-field-descriptions/).

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:{{< self-managed/versions/get-latest-version >}}
# ... additional fields omitted for brevity
```

When you first apply the Materialize custom resource, the operator automatically
creates all required Kubernetes resources.

### Modifying the custom resource

To modify a custom resource, update the CRD with your changes, including the
`requestRollout` field with a new UUID value. When you apply the CRD, the
operator will roll out the changes.

{{< note >}} If you do not specify  a new `requestRollout` UUID, the operator
watches for updates but does not roll out the changes.
{{< /note >}}

For a full list of fields available for the Materialize CR, see [Materialize CRD
Field
Descriptions](/self-managed-deployments/appendix/materialize-crd-field-descriptions/).

See also:

- [Upgrade Overview](/self-managed-deployments/upgrading/)

### Connecting to an instance

Once deployed, you interact with a Materialize instance through standard
PostgreSQL-compatible tools and drivers:

```bash
# Connect with psql
psql "postgres://materialize@<host>:6875/materialize"
```

Once connected, you can issue SQL commands to create sources, define views, run queries, and manage the database:

```sql
-- Create a source
CREATE SOURCE my_source FROM KAFKA ...;

-- Create a materialized view
CREATE MATERIALIZED VIEW my_view AS
  SELECT ... FROM my_source ...;

-- Query the view
SELECT * FROM my_view;
```

## Clusters and Replicas

Clusters are isolated pools of compute resources that execute workloads in Materialize. They provide resource isolation and fault tolerance for your data processing pipelines.

For a comprehensive overview of clusters in Materialize, see the [Clusters concept page](/concepts/clusters/).

### Cluster architecture

- **Clusters**: Logical groupings of compute resources dedicated to specific workloads (sources, sinks, indexes, materialized views, queries)
- **Replicas**: Physical instantiations of a cluster's compute resources, deployed as Kubernetes StatefulSets

Each replica contains identical compute resources and processes the same data independently, providing fault tolerance and high availability.

### Kubernetes resources

When you create a cluster with one or more replicas in Materialize, the instance coordinates with the operator to create:

- One or more **StatefulSet** resources (one per replica)
- **Pods** within each StatefulSet that execute the actual compute workload
- **Persistent volumes** (if configured) for scratch disk space

For example:

```sql
-- Create a cluster with 2 replicas
CREATE CLUSTER my_cluster SIZE = '100cc', REPLICATION FACTOR = 2;
```

This creates two separate StatefulSets in Kubernetes, each running compute processes.

### Managing clusters

You interact with clusters primarily through SQL:

```sql
-- Create a cluster
CREATE CLUSTER ingest_cluster SIZE = '50cc', REPLICATION FACTOR = 1;

-- Use the previous cluster for a source
CREATE SOURCE my_source
  IN CLUSTER ingest_cluster
  FROM KAFKA ...;

-- Create a cluster for materialized views
CREATE CLUSTER compute_cluster SIZE = '100cc', REPLICATION FACTOR = 2;

-- Use the previous cluster for a materialized view
CREATE MATERIALIZED VIEW my_view
  IN CLUSTER compute_cluster AS
  SELECT ... FROM my_source ...;

-- Resize a cluster
ALTER CLUSTER compute_cluster SET (SIZE = '200cc');

```

Materialize handles the underlying Kubernetes resource creation and management automatically.

## Workflow

The following outlines the workflow process, summarizing how the various
components work together:

1. **Install the Helm chart**: This deploys the Materialize Operator to your
   Kubernetes cluster.

1. **Create a Materialize instance**: Apply a Materialize custom resource. The
   operator detects this and creates all necessary Kubernetes resources,
   including the `environmentd`, `balancerd`, and `console` pods.

1. **Connect to the instance**: Use a SQL client to connect to the
   `environmentd` service endpoint.

1. **Create clusters**: Issue SQL commands to create clusters. Materialize
   coordinates with the operator to provision StatefulSets for replicas.

1. **Run your workloads**: Create sources, materialized views, indexes, and
   sinks on your clusters.

## Terraform Modules

To help you get started, Materialize provides Terraform modules.

{{< important >}}
These modules are intended for evaluation/demonstration purposes and for serving
as a template when building your own production deployment. The modules should
not be directly relied upon for production deployments: **future releases of the
modules will contain breaking changes.** Instead, to use as a starting point for
your own production deployment, either:

- Fork the repo and pin to a specific version; or

- Use the code as a reference when developing your own deployment.

{{</ important >}}

{{< tabs >}}
{{< tab "Unified Terraform Modules  (New!)" >}}
### Unified Terraform Modules

Materialize provides a [**unified Terraform
module**](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main?tab=readme-ov-file#materialize-self-managed-terraform-modules),
which provides concrete examples and an opinionated model for deploying Materialize.

{{< yaml-table data="self_managed/terraform_list" >}}

{{< /tab >}}
{{< tab "Legacy Terraform Modules" >}}
### Legacy Terraform Modules

{{< yaml-table data="self_managed/terraform_list_legacy" >}}
{{< /tab >}}
{{< /tabs >}}

## Relationship to Materialize concepts

Self-managed deployments implement the same core Materialize concepts as the Cloud offering:

- [**Clusters**](/concepts/clusters/): Identical behavior, but backed by Kubernetes StatefulSets
- [**Sources**](/concepts/sources/): Same functionality for ingesting data
- [**Views**](/concepts/views/): Same query semantics and incremental maintenance
- [**Indexes**](/concepts/indexes/): Same in-memory query acceleration
- [**Sinks**](/concepts/sinks/): Same data egress capabilities

The Self-Managed deployment model adds the Kubernetes infrastructure layer (Helm
chart and operator) but does not change how you interact with Materialize at the
SQL level.

## Related pages

- [Installation guides](/self-managed-deployments/installation/)
- [Materialize Operator Configuration](/self-managed-deployments/appendix/configuration/)
- [Materialize CRD Field Descriptions](/self-managed-deployments/appendix/materialize-crd-field-descriptions/)
- [Operational guidelines](/self-managed-deployments/deployment-guidelines/)
- [Clusters concept page](/concepts/clusters/)
- [Materialize architecture overview](/concepts/)
