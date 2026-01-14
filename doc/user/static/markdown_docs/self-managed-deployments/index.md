<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

# Self-Managed Deployments

## Overview

Whereas Materialize Cloud gives you a fully managed service for
Materialize, Self-Managed Materialize allows you to deploy Materialize
in your own infrastructure.

Self-Managed Materialize deployments on Kubernetes consist of several
layers of components that work together to provide a fully functional
database environment. Understanding these components and how they
interact is essential for deploying, managing, and troubleshooting your
Self-Managed Materialize.

This page provides an overview of the core architectural components in a
Self-Managed deployment, from the infrastructure level (Helm chart) down
to the application level (clusters and replicas).

## Architecture layers

A Self-Managed Materialize deployment is organized into the following
layers:

| Layer | Component | Description |
|----|----|----|
| **Infrastructure** | [Helm Chart](#helm-chart) | Package manager component that bootstraps the Kubernetes deployment |
| **Orchestration** | [Materialize Operator](#materialize-operator) | Kubernetes operator that manages Materialize instances |
| **Database** | [Materialize Instance](#materialize-instance) | The Materialize database instance itself |
| **Compute** | [Clusters and Replicas](#clusters-and-replicas) | Isolated compute resources for workloads |

## Helm chart

The Helm chart is the entry point for deploying Materialize in a
self-managed Kubernetes environment. It serves as a package manager
component that defines and deploys the Materialize Operator.

### Working with the Helm chart

You interact with the Helm chart through standard Helm commands. For
example:

- To add the Materialize Helm chart repository:

  <div class="highlight">

  ``` chroma
  helm repo add materialize https://materializeinc.github.io/materialize
  ```

  </div>

- To update the repository index:

  <div class="highlight">

  ``` chroma
  helm repo update materialize
  ```

  </div>

- To install the Materialize Helm chart and deploy the Materialize
  Operator and other resources:

  <div class="highlight">

  ``` chroma
  helm install materialize materialize/materialize-operator
  ```

  </div>

- To upgrade the the Materialize Helm chart (and the Materialize
  Operator and other resources):

  <div class="highlight">

  ``` chroma
  helm upgrade materialize materialize/materialize-operator
  ```

  </div>

- To uninstall the Helm chart (and the Materialize Operator and other
  resources):

  <div class="highlight">

  ``` chroma
  helm uninstall materialize
  ```

  </div>

### What gets installed

<div class="highlight">

``` chroma
helm install materialize materialize/materialize-operator
```

</div>

When you install the the Materialize Helm Chart, it:

- Deploys the **Materialize Operator** as a Kubernetes deployment.
- Creates necessary cluster-wide resources (CRDs, RBAC roles, service
  accounts).
- Configures operator settings and permissions.

Once installed, the **Materialize Operator** handles the deployment and
management of Materialize instances.

## Materialize Operator

The Materialize Operator (implemented as `orchestratord`) is a
Kubernetes operator that automates the deployment and lifecycle
management of Materialize instances. It implements the [Kubernetes
operator
pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)
to extend Kubernetes with domain-specific knowledge about Materialize.

### Managed resources

The operator watches for Materialize custom resources and
creates/manages all the Kubernetes resources required to run a
Materialize instance, including:

- **Namespaces**: Isolated Kubernetes namespaces for each instance
- **Services**: Network services for connecting to Materialize
- **Network Policies**: Network isolation and security rules
- **Certificates**: TLS certificates for secure connections
- **ConfigMaps and Secrets**: Configuration and sensitive data
- **Deployments**: These support the `balancerd` and `console` pod used
  as the ingress layer for Materialize.
- **StatefulSets**: `environmentd` and `clusterd` which are the database
  control plane and compute resources respectively.

### Configuration

For configuration options for the Materialize Operator, see the
[Materialize Operator Configuration
page](/docs/self-managed-deployments/operator-configuration/).

## Materialize Instance

A Materialize instance is the actual database that you connect to and
interact with. Each instance is an isolated Materialize deployment
(deployed via a Kubernetes Custom Resource) with its own data,
configuration, and compute resources.

### Components

When you create a Materialize instance, the operator deploys three core
components as Kubernetes resources:

- **`environmentd`**: The main database control plane, deployed as a
  StatefulSet.

  **`environmentd`** runs as a Kubernetes pod and is the primary
  component of a Materialize instance. It houses the control plane and
  contains:

  - **Adapter**: The SQL interface that handles client connections,
    query parsing, and planning
  - **Storage Controller**: Maintains durable metadata for storage
  - **Compute Controller**: Orchestrates compute resources and manages
    system state

  On startup, `environmentd` will create several built-in clusters.

  When you connect to Materialize with a SQL client (e.g., `psql`),
  you’re connecting to `environmentd`.

- **balancerd**: A pgwire and http proxy used to connect to
  environmentd, deployed as a Deployment.

- **console**: Web-based administration interface, deployed as a
  Deployment.

### Instance responsibilities

A Materialize instance manages:

- **SQL objects**: Sources, views, materialized views, indexes, sinks
- **Schemas and databases**: Logical organization of objects
- **User connections**: SQL client connections and authentication
- **Catalog metadata**: System information about all objects and
  configuration
- **Compute orchestration**: Coordination of work across clusters and
  replicas

### Deploying with the operator

To deploy Materialize instances with the operator, create and apply
Materialize custom resources definitions(CRDs). For a full list of
fields available for the Materialize CR, see [Materialize CRD Field
Descriptions](/docs/self-managed-deployments/materialize-crd-field-descriptions/).

<div class="highlight">

``` chroma
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.6.0
# ... additional fields omitted for brevity
```

</div>

When you first apply the Materialize custom resource, the operator
automatically creates all required Kubernetes resources.

### Modifying the custom resource

To modify a custom resource, update the CRD with your changes, including
the `requestRollout` field with a new UUID value. When you apply the
CRD, the operator will roll out the changes.

<div class="note">

**NOTE:** If you do not specify a new `requestRollout` UUID, the
operator watches for updates but does not roll out the changes.

</div>

For a full list of fields available for the Materialize CR, see
[Materialize CRD Field
Descriptions](/docs/self-managed-deployments/materialize-crd-field-descriptions/).

See also:

- [Upgrade Overview](/docs/self-managed-deployments/upgrading/)

### Connecting to an instance

Once deployed, you interact with a Materialize instance through the
Materialize Console or standard PostgreSQL-compatible tools and drivers:

<div class="highlight">

``` chroma
# Connect with psql
psql "postgres://materialize@<host>:6875/materialize"
```

</div>

Once connected, you can issue SQL commands to create sources, define
views, run queries, and manage the database:

<div class="highlight">

``` chroma
-- Create a source
CREATE SOURCE my_source FROM KAFKA ...;

-- Create a materialized view
CREATE MATERIALIZED VIEW my_view AS
  SELECT ... FROM my_source ...;

-- Query the view
SELECT * FROM my_view;
```

</div>

## Clusters and Replicas

Clusters are isolated pools of compute resources that execute workloads
in Materialize. They provide resource isolation and fault tolerance for
your data processing pipelines.

For a comprehensive overview of clusters in Materialize, see the
[Clusters concept page](/docs/concepts/clusters/).

### Cluster architecture

- **Clusters**: Logical groupings of compute resources dedicated to
  specific workloads (sources, sinks, indexes, materialized views,
  queries)
- **Replicas**: Physical instantiations of a cluster’s compute
  resources, deployed as Kubernetes StatefulSets

Each replica contains identical compute resources and processes the same
data independently, providing fault tolerance and high availability.

### Kubernetes resources

When you create a cluster with one or more replicas in Materialize, the
instance coordinates with the operator to create:

- One or more **StatefulSet** resources (one per replica)
- **Pods** within each StatefulSet that execute the actual compute
  workload
- **Persistent volumes** (if configured) for scratch disk space

For example:

<div class="highlight">

``` chroma
-- Create a cluster with 2 replicas
CREATE CLUSTER my_cluster SIZE = '100cc', REPLICATION FACTOR = 2;
```

</div>

This creates two separate StatefulSets in Kubernetes, each running
compute processes.

### Managing clusters

You interact with clusters primarily through SQL:

<div class="highlight">

``` chroma
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

</div>

Materialize handles the underlying Kubernetes resource creation and
management automatically.

## Workflow

The following outlines the workflow process, summarizing how the various
components work together:

1.  **Install the Helm chart**: This deploys the Materialize Operator to
    your Kubernetes cluster.

2.  **Create a Materialize instance**: Apply a Materialize custom
    resource. The operator detects this and creates all necessary
    Kubernetes resources, including the `environmentd`, `balancerd`, and
    `console` pods.

3.  **Connect to the instance**: Use the Materialize Console on port
    8080 to connecto to the `console` service endpoint or SQL client on
    port 6875 to connect to the `balancerd` service endpoint.

    If authentication is enabled, you must first connect to the
    Materialize Console and set up users.

4.  **Create clusters**: Issue SQL commands to create clusters.
    Materialize coordinates with the operator to provision StatefulSets
    for replicas.

5.  **Run your workloads**: Create sources, materialized views, indexes,
    and sinks on your clusters.

## Available Terraform Modules

To help you get started, Materialize provides Terraform modules.

<div class="important">

**! Important:**

These modules are intended for evaluation/demonstration purposes and for
serving as a template when building your own production deployment. The
modules should not be directly relied upon for production deployments:
**future releases of the modules will contain breaking changes.**
Instead, to use as a starting point for your own production deployment,
either:

- Fork the repo and pin to a specific version; or

- Use the code as a reference when developing your own deployment.

</div>

<div class="code-tabs">

<div class="tab-content">

<div id="tab-terraform-modules-new" class="tab-pane"
title="Terraform Modules (New!)">

### Terraform Modules

Materialize provides [**Terraform
modules**](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main?tab=readme-ov-file#materialize-self-managed-terraform-modules),
which provides concrete examples and an opinionated model for deploying
Materialize.

| Module | Description |
|----|----|
| [Amazon Web Services (AWS)](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws) | An example Terraform module for deploying Materialize on AWS. See [Install on AWS](/docs/self-managed-deployments/installation/install-on-aws/) for detailed instructions usage. |
| [Azure](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure) | An example Terraform module for deploying Materialize on Azure. See [Install on Azure](/docs/self-managed-deployments/installation/install-on-azure/) for detailed instructions usage. |
| [Google Cloud Platform (GCP)](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/gcp) | An example Terraform module for deploying Materialize on GCP. See [Install on GCP](/docs/self-managed-deployments/installation/install-on-gcp/) for detailed instructions usage. |

</div>

<div id="tab-legacy-terraform-modules" class="tab-pane"
title="Legacy Terraform Modules">

### Legacy Terraform Modules

| Sample Module | Description |
|----|----|
| [terraform-helm-materialize (Legacy)](https://github.com/MaterializeInc/terraform-helm-materialize) | A sample Terraform module for installing the Materialize Helm chart into a Kubernetes cluster. |
| [Materialize on AWS (Legacy)](https://github.com/MaterializeInc/terraform-aws-materialize) | A sample Terraform module for deploying Materialize on AWS Cloud Platform with all required infrastructure components. See [Install on AWS (Legacy)](/docs/self-managed-deployments/installation/legacy/install-on-aws-legacy/) for an example usage. |
| [Materialize on Azure (Legacy)](https://github.com/MaterializeInc/terraform-azurerm-materialize) | A sample Terraform module for deploying Materialize on Azure with all required infrastructure components. See [Install on Azure](/docs/self-managed-deployments/installation/legacy/install-on-azure-legacy/) for an example usage. |
| [Materialize on GCP (Legacy)](https://github.com/MaterializeInc/terraform-google-materialize) | A sample Terraform module for deploying Materialize on Google Cloud Platform (GCP) with all required infrastructure components. See [Install on GCP](/docs/self-managed-deployments/installation/legacy/install-on-gcp-legacy/) for an example usage. |

</div>

</div>

</div>

## Related pages

- [Installation guides](/docs/self-managed-deployments/installation/)
- [Materialize Operator
  Configuration](/docs/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/docs/self-managed-deployments/materialize-crd-field-descriptions/)
- [Operational
  guidelines](/docs/self-managed-deployments/deployment-guidelines/)
- [Clusters concept page](/docs/concepts/clusters/)
- [Materialize architecture overview](/docs/concepts/)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/self-managed-deployments/_index.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2026 Materialize Inc.

</div>
