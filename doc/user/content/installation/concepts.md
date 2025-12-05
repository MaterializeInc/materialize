---
title: "Self-Managed Deployment Concepts"
description: "Learn about the key components and architecture of self-managed Materialize deployments."
aliases:
  - /self-hosted/concepts/
menu:
  main:
    parent: 'installation'
    weight: 90
---

## Overview

Self-managed Materialize deployments on Kubernetes consist of several layers of components that work together to provide a fully functional database environment. Understanding these components and how they interact is essential for deploying, managing, and troubleshooting your self-managed installation.

This page provides an overview of the core architectural components in a self-managed deployment, from the infrastructure level (Helm chart) down to the application level (clusters and replicas).

## Architecture layers

A self-managed Materialize deployment is organized into the following layers:

Layer | Component | Description
------|-----------|------------
**Infrastructure** | [Helm Chart](#helm-chart) | Package manager component that bootstraps the Kubernetes deployment
**Orchestration** | [Materialize Operator](#materialize-operator) | Kubernetes operator that manages Materialize instances
**Database** | [Materialize Instance](#materialize-instance) | The Materialize database instance itself
**Compute** | [Clusters and Replicas](#clusters-and-replicas) | Isolated compute resources for workloads

## Helm chart

The Helm chart is the entry point for deploying Materialize in a self-managed Kubernetes environment. It serves as a package manager component that defines and deploys the Materialize Operator.

For configuration options for the the help chart and operator, consulte the [Materialize Operator Configuration page](/installation/configuration/).

### What gets installed

The Helm chart itself is relatively simple and focused. When you install the Materialize Helm chart, it:

- Deploys the **Materialize Operator** as a Kubernetes deployment
- Creates necessary cluster-wide resources (CRDs, RBAC roles, service accounts)
- Configures operator settings and permissions

### Working with the Helm chart

You interact with the Helm chart through standard Helm commands:

```bash
# Install the Materialize operator
helm install materialize materialize/materialize-operator

# Upgrade the operator
helm upgrade materialize materialize/materialize-operator

# Uninstall the operator
helm uninstall materialize
```

Once installed, the operator handles the deployment and management of Materialize instances.

## Materialize Operator

The Materialize Operator (implemented as `orchestratord`) is a Kubernetes operator that automates the deployment and lifecycle management of Materialize instances. It implements the [Kubernetes operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) to extend Kubernetes with domain-specific knowledge about Materialize.

### Managed resources

The operator watches for Materialize custom resources and creates/manages all the Kubernetes resources required to run a Materialize instance, including:

- **Namespaces**: Isolated Kubernetes namespaces for each instance
- **Services**: Network services for connecting to Materialize
- **Network Policies**: Network isolation and security rules
- **Certificates**: TLS certificates for secure connections
- **ConfigMaps and Secrets**: Configuration and sensitive data
- **Deployments**: These support the `Balancerd` and `Consoled` pod used as the ingress layer for materialize.
- **StatefulSets**: `Environmentd` and `Clusterd` which are the database control plane and compute resources respectively.

### Working with the operator

To deploy Materialize instances with the operator you must create Materialize custom resources.

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: my-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.0.0
  ...
```

When you create this resource the operator detects the change and automatically creates or updates all necessary Kubernetes resources.

Updates to the resource are watched, but only rolled out when a new `requestRollout` UUID is provided.

For detailed configuration options, see:
- [Operator Configuration](/installation/configuration/)
- [Materialize CRD Field Descriptions](/installation/appendix-materialize-crd-field-descriptions/)
- [Upgrade Overview](/installation/upgrading/)

## Materialize Instance

A Materialize instance is the actual database that you connect to and interact with. Each instance is an isolated Materialize deployment with its own data, configuration, and compute resources.

### Components

When you create a Materialize instance, the operator deploys three core components as Kubernetes resources:

- **environmentd**: The main database control plane, deployed as a StatefulSet
- **balancerd**: A pgwire and http proxy used to connect to environmentd, deployed as a Deployment
- **console**: Web-based administration interface, deployed as a Deployment

### The environmentd component

The primary component of a Materialize instance is **environmentd**, which runs as a Kubernetes pod. `Environmentd` houses the control plane and contains:

- **Adapter**: The SQL interface that handles client connections, query parsing, and planning
- **Storage Controller**: Maintains durable metadata for storage
- **Compute Controller**: Orchestrates compute resources and manages system state

When you connect to Materialize with a SQL client (e.g., `psql`), you're connecting to `environmentd`.

On startup Environmentd will create several built-in clusters.

### Instance responsibilities

A Materialize instance manages:

- **SQL objects**: Sources, views, materialized views, indexes, sinks
- **Schemas and databases**: Logical organization of objects
- **User connections**: SQL client connections and authentication
- **Catalog metadata**: System information about all objects and configuration
- **Compute orchestration**: Coordination of work across clusters and replicas

### Connecting to an instance

You interact with a Materialize instance through standard PostgreSQL-compatible tools and drivers:

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

-- Use a cluster for a source
CREATE SOURCE my_source
  IN CLUSTER ingest_cluster
  FROM KAFKA ...;

-- Create a cluster for materialized views
CREATE CLUSTER compute_cluster SIZE = '100cc', REPLICATION FACTOR = 2;

-- Use a cluster for a materialized view
CREATE MATERIALIZED VIEW my_view
  IN CLUSTER compute_cluster AS
  SELECT ... FROM my_source ...;

-- Resize a cluster
ALTER CLUSTER compute_cluster SET (SIZE = '200cc');

-- Drop a cluster
DROP CLUSTER ingest_cluster;
```

Materialize handles the underlying Kubernetes resource creation and management automatically.

## Deployment workflow

Understanding how these components work together helps clarify the deployment process:

1. **Install the Helm chart**: This deploys the Materialize Operator to your Kubernetes cluster.

2. **Create a Materialize instance**: Apply a Materialize custom resource. The operator detects this and creates all necessary Kubernetes resources, including the `environmentd`, `balancerd`, and `console` pods.

3. **Connect to the instance**: Use a SQL client to connect to the `environmentd` service endpoint.

4. **Create clusters**: Issue SQL commands to create clusters. Materialize coordinates with the operator to provision StatefulSets for replicas.

5. **Deploy workloads**: Create sources, materialized views, indexes, and sinks on your clusters.

## Relationship to Materialize concepts

Self-managed deployments implement the same core Materialize concepts as the cloud offering:

- [**Clusters**](/concepts/clusters/): Identical behavior, but backed by Kubernetes StatefulSets
- [**Sources**](/concepts/sources/): Same functionality for ingesting data
- [**Views**](/concepts/views/): Same query semantics and incremental maintenance
- [**Indexes**](/concepts/indexes/): Same in-memory query acceleration
- [**Sinks**](/concepts/sinks/): Same data egress capabilities

The self-managed deployment model adds the Kubernetes infrastructure layer (Helm chart and operator) but doesn't change how you interact with Materialize at the SQL level.

## Related pages

- [Installation guides](/installation/)
- [Operator Configuration](/installation/configuration/)
- [Materialize CRD Field Descriptions](/installation/appendix-materialize-crd-field-descriptions/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Clusters concept page](/concepts/clusters/)
- [Materialize architecture overview](/concepts/)
