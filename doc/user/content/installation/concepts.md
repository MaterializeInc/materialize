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
**Orchestration** | [Materialize Operator](#materialize-operator) | Kubernetes operator that manages Materialize environments
**Database** | [Materialize Environment](#materialize-environment) | The Materialize database instance itself
**Compute** | [Clusters and Replicas](#clusters-and-replicas) | Isolated compute resources for workloads

## Helm chart

The Helm chart is the entry point for deploying Materialize in a self-managed Kubernetes environment. It serves as a package manager component that defines and deploys the Materialize Operator.

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

The Helm chart installation is typically a one-time operation per Kubernetes cluster. Once installed, the operator handles the deployment and management of Materialize environments.

## Materialize Operator

The Materialize Operator is a Kubernetes operator that automates the deployment and lifecycle management of Materialize environments. It implements the [Kubernetes operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) to extend Kubernetes with domain-specific knowledge about Materialize.

### Managed resources

The operator watches for Materialize custom resources and creates/manages all the Kubernetes resources required to run a Materialize environment, including:

- **Namespaces**: Isolated Kubernetes namespaces for each environment
- **Services**: Network services for connecting to Materialize
- **Network Policies**: Network isolation and security rules
- **Certificates**: TLS certificates for secure connections
- **ConfigMaps and Secrets**: Configuration and sensitive data
- **Deployments**: The `environmentd` pod and other components
- **StatefulSets**: Cluster replicas for compute workloads

The operator continuously reconciles the actual state of these resources with the desired state defined in the Materialize custom resource.

### Working with the operator

You interact with the operator indirectly by creating and modifying Materialize custom resources:

```yaml
apiVersion: v1alpha1
kind: Materialize
metadata:
  name: my-environment
spec:
  environmentdImageRef: materialize/environmentd:v0.84.2
  ...
```

When you apply this resource with `kubectl apply`, the operator detects the change and automatically creates or updates all necessary Kubernetes resources.

## Materialize Environment

A Materialize environment (sometimes called a Materialize instance) is the actual database that you connect to and interact with. Each environment is an isolated Materialize deployment with its own data, configuration, and compute resources.

### The environmentd component

The primary component of a Materialize environment is **environmentd**, which runs as a Kubernetes pod. `Environmentd` houses two critical subsystems:

- **Adapter**: The SQL interface that handles client connections, query parsing, and planning
- **Control Plane**: Orchestrates compute resources and manages the overall system state

When you connect to Materialize with a SQL client (e.g., `psql`), you're connecting to `environmentd`.

### Environment responsibilities

A Materialize environment manages:

- **SQL objects**: Sources, views, materialized views, indexes, sinks
- **Schemas and databases**: Logical organization of objects
- **User connections**: SQL client connections and authentication
- **Catalog metadata**: System information about all objects and configuration
- **Compute orchestration**: Coordination of work across clusters and replicas

### Connecting to an environment

You interact with a Materialize environment through standard PostgreSQL-compatible tools and drivers:

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

When you create a cluster with one or more replicas in Materialize, the environment coordinates with the operator to create:

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

2. **Create a Materialize environment**: Apply a Materialize custom resource. The operator detects this and creates all necessary Kubernetes resources, including the `environmentd` pod.

3. **Connect to the environment**: Use a SQL client to connect to the `environmentd` service endpoint.

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

- [Installation guides](/self-managed/v25.2/installation/)
- [Configuration](/self-managed/v25.2/installation/configuration/)
- [Operational guidelines](/self-managed/v25.2/installation/operational-guidelines/)
- [Clusters concept page](/concepts/clusters/)
- [Materialize architecture overview](/concepts/)
