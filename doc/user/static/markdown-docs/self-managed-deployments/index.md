# Self-Managed Deployments

Learn about the key components and architecture of self-managed Materialize deployments.



## Overview

Whereas Materialize Cloud gives you a fully managed service for Materialize,
Self-Managed Materialize allows you to deploy Materialize in your own
infrastructure.

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
page](/self-managed-deployments/operator-configuration/).

## Materialize Instance

A Materialize instance is the actual database that you connect to and interact
with. Each instance is an isolated Materialize deployment (deployed via a
Kubernetes Custom Resource) with its own data, configuration, and compute
resources.

### Components

When you create a Materialize instance, the operator deploys three core
components as Kubernetes resources:

- **balancerd**: A pgwire and HTTP proxy that routes all Materialize client
  connections to `environmentd` for handling. `balancerd` is deployed as a
  Kubernetes Deployment.

- **environmentd**: The main database control plane, deployed as a
  StatefulSet.

  **`environmentd`** runs as a Kubernetes pod and is the primary component of a
  Materialize instance. It houses the control plane and contains:

  - **Adapter**: The SQL interface that handles client connections, query parsing, and planning.
  - **Storage Controller**: Maintains durable metadata for storage.
  - **Compute Controller**: Orchestrates compute resources and manages system state.

  On startup, `environmentd` will create several built-in clusters.

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
Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/).

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.8.0
# ... additional fields omitted for brevity
```

When you first apply the Materialize custom resource, the operator automatically
creates all required Kubernetes resources.

### Modifying the custom resource

To modify a custom resource, update the CRD with your changes, including the
`requestRollout` field with a new UUID value. When you apply the CRD, the
operator will roll out the changes.

> **Note:** If you do not specify  a new `requestRollout` UUID, the operator
> watches for updates but does not roll out the changes.


For a full list of fields available for the Materialize CR, see [Materialize CRD
Field
Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/).

See also:

- [Upgrade Overview](/self-managed-deployments/upgrading/)

### Connecting to an instance

Once deployed, you interact with a Materialize instance through the Materialize
Console or standard PostgreSQL-compatible tools and drivers:

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

1. **Connect to the instance**: Use the Materialize Console on port 8080 to
   connecto to the `console` service endpoint or SQL client on port 6875 to
   connect to the `balancerd` service endpoint.

   If authentication is enabled, you must first connect to the Materialize
   Console and set up users.

1. **Create clusters**: Issue SQL commands to create clusters. Materialize
   coordinates with the operator to provision StatefulSets for replicas.

1. **Run your workloads**: Create sources, materialized views, indexes, and
   sinks on your clusters.

## Available Terraform Modules

To help you get started, Materialize provides Terraform modules.

> **Important:** These modules are intended for evaluation/demonstration purposes and for serving
> as a template when building your own production deployment. The modules should
> not be directly relied upon for production deployments: **future releases of the
> modules will contain breaking changes.** Instead, to use as a starting point for
> your own production deployment, either:
> - Fork the repo and pin to a specific version; or
> - Use the code as a reference when developing your own deployment.



**Terraform Modules (New!):**
### Terraform Modules

Materialize provides [**Terraform
modules**](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main?tab=readme-ov-file#materialize-self-managed-terraform-modules),
which provides concrete examples and an opinionated model for deploying Materialize.


| Module | Description |
| --- | --- |
| <a href="https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws" >Amazon Web Services (AWS)</a> | An example Terraform module for deploying Materialize on AWS. See <a href="/self-managed-deployments/installation/install-on-aws/" >Install on AWS</a> for detailed instructions usage. |
| <a href="https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure" >Azure</a> | An example Terraform module for deploying Materialize on Azure. See <a href="/self-managed-deployments/installation/install-on-azure/" >Install on Azure</a> for detailed instructions usage. |
| <a href="https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/gcp" >Google Cloud Platform (GCP)</a> | An example Terraform module for deploying Materialize on GCP. See <a href="/self-managed-deployments/installation/install-on-gcp/" >Install on GCP</a> for detailed instructions usage. |



**Legacy Terraform Modules:**
### Legacy Terraform Modules


| Sample Module | Description |
| --- | --- |
| <a href="https://github.com/MaterializeInc/terraform-helm-materialize" >terraform-helm-materialize (Legacy)</a> | A sample Terraform module for installing the Materialize Helm chart into a Kubernetes cluster. |
| <a href="https://github.com/MaterializeInc/terraform-aws-materialize" >Materialize on AWS (Legacy)</a> | A sample Terraform module for deploying Materialize on AWS Cloud Platform with all required infrastructure components. See <a href="/self-managed-deployments/installation/legacy/install-on-aws-legacy/" >Install on AWS (Legacy)</a> for an example usage. |
| <a href="https://github.com/MaterializeInc/terraform-azurerm-materialize" >Materialize on Azure (Legacy)</a> | A sample Terraform module for deploying Materialize on Azure with all required infrastructure components. See <a href="/self-managed-deployments/installation/legacy/install-on-azure-legacy/" >Install on Azure</a> for an example usage. |
| <a href="https://github.com/MaterializeInc/terraform-google-materialize" >Materialize on GCP (Legacy)</a> | A sample Terraform module for deploying Materialize on Google Cloud Platform (GCP) with all required infrastructure components. See <a href="/self-managed-deployments/installation/legacy/install-on-gcp-legacy/" >Install on GCP</a> for an example usage. |




## Related pages

- [Installation guides](/self-managed-deployments/installation/)
- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)
- [Operational guidelines](/self-managed-deployments/deployment-guidelines/)
- [Clusters concept page](/concepts/clusters/)
- [Materialize architecture overview](/concepts/)



---

## Appendix


## Table of contents

- [Appendix: Cluster sizes](./appendix-cluster-sizes/)
- [Appendix: Prepare for swap and upgrade to v26.0](./upgrade-to-swap/)


---

## Deployment guidelines


Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; and a license key.


## Available deployment guidelines

The following guides outline recommended configurations for deploying Materialize across different cloud environments.

- [AWS Deployment
  Guidelines](/self-managed-deployments/deployment-guidelines/aws-deployment-guidelines/)
- [Azure Deployment
  Guidelines](/self-managed-deployments/deployment-guidelines/azure-deployment-guidelines/)
- [GCP Deployment
  Guidelines](/self-managed-deployments/deployment-guidelines/gcp-deployment-guidelines/)


---

## FAQ


## How long do license keys last?

Community edition license keys are valid for one year. Enterprise license
keys will vary based on the terms of your contract.

## How do I get a license key?


| License key type | Deployment type | Action |
| --- | --- | --- |
| Community | New deployments | <p>To get a license key:</p> <ul> <li>If you have a Cloud account, visit the <a href="https://console.materialize.com/license/" ><strong>License</strong> page in the Materialize Console</a>.</li> <li>If you do not have a Cloud account, visit <a href="https://materialize.com/self-managed/community-license/" >https://materialize.com/self-managed/community-license/</a>.</li> </ul> |
| Community | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |
| Enterprise | New deployments | Visit <a href="https://materialize.com/self-managed/enterprise-license/" >https://materialize.com/self-managed/enterprise-license/</a> to purchase an Enterprise license. |
| Enterprise | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |


## How do I add a license key to an existing installation?

The license key should be configured in the Kubernetes Secret resource
created during the installation process. To configure a license key in an
existing installation, run:

```bash
kubectl -n materialize-environment patch secret materialize-backend -p '{"stringData":{"license_key":"<your license key goes here>"}}' --type=merge
```

## How can I downgrade Self-Managed Materialize?

Downgrading is not supported.


---

## Installation


<p>You can install Self-Managed Materialize on a Kubernetes cluster running
locally or on a cloud provider. Self-Managed Materialize requires:</p>
<ul>
<li>A Kubernetes (v1.31+) cluster.</li>
<li>PostgreSQL as a metadata database.</li>
<li>Blob storage.</li>
<li>A license key.</li>
</ul>
<h2 id="license-key">License key</h2>
<p>Starting in v26.0, Materialize requires a license key.</p>

| License key type | Deployment type | Action |
| --- | --- | --- |
| Community | New deployments | <p>To get a license key:</p> <ul> <li>If you have a Cloud account, visit the <a href="https://console.materialize.com/license/" ><strong>License</strong> page in the Materialize Console</a>.</li> <li>If you do not have a Cloud account, visit <a href="https://materialize.com/self-managed/community-license/" >https://materialize.com/self-managed/community-license/</a>.</li> </ul> |
| Community | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |
| Enterprise | New deployments | Visit <a href="https://materialize.com/self-managed/enterprise-license/" >https://materialize.com/self-managed/enterprise-license/</a> to purchase an Enterprise license. |
| Enterprise | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |

<h2 id="installation-guides">Installation guides</h2>
<p>The following installation guides are available to help you get started:</p>


<h3 id="install-using-helm-commands">Install using Helm Commands</h3>
<table>
  <thead>
      <tr>
          <th>Guide</th>
          <th>Description</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/self-managed-deployments/installation/install-on-local-kind/" >Install locally on Kind</a></td>
          <td>Uses standard Helm commands to deploy Materialize to a Kind cluster in Docker.</td>
      </tr>
  </tbody>
</table>


<h3 id="install-using-terraform-modules">Install using Terraform Modules</h3>
> **Tip:** The Terraform modules are provided as examples. They are not required for
> installing Materialize.

<table>
  <thead>
      <tr>
          <th>Guide</th>
          <th>Description</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/self-managed-deployments/installation/install-on-aws/" >Install on AWS</a></td>
          <td>Uses Terraform module to deploy Materialize to AWS Elastic Kubernetes Service (EKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/installation/install-on-azure/" >Install on Azure</a></td>
          <td>Uses Terraform module to deploy Materialize to Azure Kubernetes Service (AKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/installation/install-on-gcp/" >Install on GCP</a></td>
          <td>Uses Terraform module to deploy Materialize to Google Kubernetes Engine (GKE).</td>
      </tr>
  </tbody>
</table>


<h3 id="install-using-legacy-terraform-modules">Install using Legacy Terraform Modules</h3>
> **Tip:** The Terraform modules are provided as examples. They are not required for
> installing Materialize.

<table>
  <thead>
      <tr>
          <th>Guide</th>
          <th>Description</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/self-managed-deployments/installation/legacy/install-on-aws-legacy/" >Install on AWS (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to AWS Elastic Kubernetes Service (EKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/installation/legacy/install-on-azure-legacy/" >Install on Azure (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to Azure Kubernetes Service (AKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/installation/legacy/install-on-gcp-legacy/" >Install on GCP (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to Google Kubernetes Engine (GKE).</td>
      </tr>
  </tbody>
</table>



---

## Materialize CRD Field Descriptions































#### MaterializeSpec
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>backendSecretName</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>The name of a secret containing <code>metadata_backend_url</code> and <code>persist_backend_url</code>.
It may also contain <code>external_login_password_mz_system</code>, which will be used as
the password for the <code>mz_system</code> user if <code>authenticatorKind</code> is <code>Password</code>.</p>

</td>
</tr>
<tr>
<td><code>environmentdImageRef</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>The environmentd image to run.</p>

</td>
</tr>
<tr>
<td><code>authenticatorKind</code></td>
<td></td>
<td>
<em><strong>Enum</strong></em>


<p><p>How to authenticate with Materialize.</p>
<p>Valid values:</p>
<ul>
<li><code>Frontegg</code>:<br>  Authenticate users using Frontegg.</li>
<li><code>Password</code>:<br>  Authenticate users using internally stored password hashes.
The backend secret must contain external_login_password_mz_system.</li>
<li><code>Sasl</code>:<br>  Authenticate users using SASL.</li>
<li><code>None</code> (default):<br>  Do not authenticate users. Trust they are who they say they are without verification.</li>
</ul>
</p>

<p><strong>Default:</strong> <code>None</code></p></td>
</tr>
<tr>
<td><code>balancerdExternalCertificateSpec</code></td>
<td></td>
<td>
<em><strong><a href='#materializecertspec'>MaterializeCertSpec</a></strong></em>


<p>The configuration for generating an x509 certificate using cert-manager for balancerd
to present to incoming connections.
The <code>dnsNames</code> and <code>issuerRef</code> fields are required.</p>

</td>
</tr>
<tr>
<td><code>balancerdReplicas</code></td>
<td></td>
<td>
<em><strong>Integer</strong></em>


<p>Number of balancerd pods to create.</p>

</td>
</tr>
<tr>
<td><code>balancerdResourceRequirements</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1resourcerequirements'>io.k8s.api.core.v1.ResourceRequirements</a></strong></em>


<p>Resource requirements for the balancerd pod.</p>

</td>
</tr>
<tr>
<td><code>consoleExternalCertificateSpec</code></td>
<td></td>
<td>
<em><strong><a href='#materializecertspec'>MaterializeCertSpec</a></strong></em>


<p>The configuration for generating an x509 certificate using cert-manager for the console
to present to incoming connections.
The <code>dnsNames</code> and <code>issuerRef</code> fields are required.
Not yet implemented.</p>

</td>
</tr>
<tr>
<td><code>consoleReplicas</code></td>
<td></td>
<td>
<em><strong>Integer</strong></em>


<p>Number of console pods to create.</p>

</td>
</tr>
<tr>
<td><code>consoleResourceRequirements</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1resourcerequirements'>io.k8s.api.core.v1.ResourceRequirements</a></strong></em>


<p>Resource requirements for the console pod.</p>

</td>
</tr>
<tr>
<td><code>enableRbac</code></td>
<td></td>
<td>
<em><strong>Bool</strong></em>


<p>Whether to enable role based access control. Defaults to false.</p>

</td>
</tr>
<tr>
<td><code>environmentId</code></td>
<td></td>
<td>
<em><strong>Uuid</strong></em>


<p>The value used by environmentd (via the &ndash;environment-id flag) to
uniquely identify this instance. Must be globally unique, and
is required if a license key is not provided.
NOTE: This value MUST NOT be changed in an existing instance,
since it affects things like the way data is stored in the persist
backend.</p>

<p><strong>Default:</strong> <code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>environmentdConnectionRoleArn</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>If running in AWS, override the IAM role to use to support
the CREATE CONNECTION feature.</p>

</td>
</tr>
<tr>
<td><code>environmentdExtraArgs</code></td>
<td></td>
<td>
<em><strong>Array&lt;String&gt;</strong></em>


<p>Extra args to pass to the environmentd binary.</p>

</td>
</tr>
<tr>
<td><code>environmentdExtraEnv</code></td>
<td></td>
<td>
<em><strong>Array&lt;<a href='#iok8sapicorev1envvar'>io.k8s.api.core.v1.EnvVar</a>&gt;</strong></em>


<p>Extra environment variables to pass to the environmentd binary.</p>

</td>
</tr>
<tr>
<td><code>environmentdResourceRequirements</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1resourcerequirements'>io.k8s.api.core.v1.ResourceRequirements</a></strong></em>


<p>Resource requirements for the environmentd pod.</p>

</td>
</tr>
<tr>
<td><code>environmentdScratchVolumeStorageRequirement</code></td>
<td></td>
<td>
<em><strong>io.k8s.apimachinery.pkg.api.resource.Quantity</strong></em>


<p>Amount of disk to allocate, if a storage class is provided.</p>

</td>
</tr>
<tr>
<td><code>forcePromote</code></td>
<td></td>
<td>
<em><strong>Uuid</strong></em>


<p>If <code>forcePromote</code> is set to the same value as <code>requestRollout</code>, the
current rollout will skip waiting for clusters in the new
generation to rehydrate before promoting the new environmentd to
leader.</p>

<p><strong>Default:</strong> <code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>forceRollout</code></td>
<td></td>
<td>
<em><strong>Uuid</strong></em>


<p>This value will be written to an annotation in the generated
environmentd statefulset, in order to force the controller to
detect the generated resources as changed even if no other changes
happened. This can be used to force a rollout to a new generation
even without making any meaningful changes, by setting it to the
same value as <code>requestRollout</code>.</p>

<p><strong>Default:</strong> <code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>internalCertificateSpec</code></td>
<td></td>
<td>
<em><strong><a href='#materializecertspec'>MaterializeCertSpec</a></strong></em>


<p>The cert-manager Issuer or ClusterIssuer to use for database internal communication.
The <code>issuerRef</code> field is required.
This currently is only used for environmentd, but will eventually support clusterd.
Not yet implemented.</p>

</td>
</tr>
<tr>
<td><code>podAnnotations</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p>Annotations to apply to the pods.</p>

</td>
</tr>
<tr>
<td><code>podLabels</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p>Labels to apply to the pods.</p>

</td>
</tr>
<tr>
<td><code>requestRollout</code></td>
<td></td>
<td>
<em><strong>Uuid</strong></em>


<p><p>When changes are made to the environmentd resources (either via
modifying fields in the spec here or by deploying a new
orchestratord version which changes how resources are generated),
existing environmentd processes won&rsquo;t be automatically restarted.
In order to trigger a restart, the request_rollout field should be
set to a new (random) value. Once the rollout completes, the value
of <code>status.lastCompletedRolloutRequest</code> will be set to this value
to indicate completion.</p>
<p>Defaults to a random value in order to ensure that the first
generation rollout is automatically triggered.</p>
</p>

<p><strong>Default:</strong> <code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>rolloutStrategy</code></td>
<td></td>
<td>
<em><strong>Enum</strong></em>


<p><p>Rollout strategy to use when upgrading this Materialize instance.</p>
<p>Valid values:</p>
<ul>
<li>
<p><code>WaitUntilReady</code> (default):<br>  Create a new generation of pods, leaving the old generation around until the
new ones are ready to take over.
This minimizes downtime, and is what almost everyone should use.</p>
</li>
<li>
<p><code>ManuallyPromote</code>:<br>  Create a new generation of pods, leaving the old generation as the serving generation
until the user manually promotes the new generation.</p>
<p>When using <code>ManuallyPromote</code>, the new generation can be promoted at any
time, even if it has dataflows that are not fully caught up, by setting
<code>forcePromote</code> to the same value as <code>requestRollout</code> in the Materialize spec.</p>
<p>To minimize downtime, promotion should occur when the new generation
has caught up to the prior generation. To determine if the new
generation has caught up, consult the <code>UpToDate</code> condition in the
status of the Materialize Resource. If the condition&rsquo;s reason is
<code>ReadyToPromote</code> the new generation is ready to promote.</p>
> **Warning:** Do not leave new generations unpromoted indefinitely.
>   The new generation keeps open read holds which prevent compaction. Once promoted or
>   cancelled, those read holds are released. If left unpromoted for an extended time, this
>   data can build up, and can cause extreme deletion load on the metadata backend database
>   when finally promoted or cancelled.

</li>
<li>
<p><code>ImmediatelyPromoteCausingDowntime</code>:<br>  > **Warning:** THIS WILL CAUSE YOUR MATERIALIZE INSTANCE TO BE UNAVAILABLE FOR SOME TIME!!!
>   This strategy should ONLY be used by customers with physical hardware who do not have
>   enough hardware for the `WaitUntilReady` strategy. If you think you want this, please
>   consult with Materialize engineering to discuss your situation.
</p>
<p>Tear down the old generation of pods and promote the new generation of pods immediately,
without waiting for the new generation of pods to be ready.</p>
</li>
</ul>
</p>

<p><strong>Default:</strong> <code>WaitUntilReady</code></p></td>
</tr>
<tr>
<td><code>serviceAccountAnnotations</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p><p>Annotations to apply to the service account.</p>
<p>Annotations on service accounts are commonly used by cloud providers for IAM.
AWS uses &ldquo;eks.amazonaws.com/role-arn&rdquo;.
Azure uses &ldquo;azure.workload.identity/client-id&rdquo;, but
additionally requires &ldquo;azure.workload.identity/use&rdquo;: &ldquo;true&rdquo; on the pods.</p>
</p>

</td>
</tr>
<tr>
<td><code>serviceAccountLabels</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p>Labels to apply to the service account.</p>

</td>
</tr>
<tr>
<td><code>serviceAccountName</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Name of the kubernetes service account to use.
If not set, we will create one with the same name as this Materialize object.</p>

</td>
</tr>
<tr>
<td><code>systemParameterConfigmapName</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p><p>The name of a ConfigMap containing system parameters in JSON format.
The ConfigMap must contain a <code>system-params.json</code> key whose value
is a valid JSON object containing valid system parameters.</p>
<p>Run <code>SHOW ALL</code> in SQL to see a subset of configurable system parameters.</p>
<p>Example ConfigMap:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-yaml" data-lang="yaml"><span class="line"><span class="cl"><span class="nt">data</span><span class="p">:</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">  </span><span class="nt">system-params.json</span><span class="p">:</span><span class="w"> </span><span class="p">|</span><span class="sd">
</span></span></span><span class="line"><span class="cl"><span class="sd">    {
</span></span></span><span class="line"><span class="cl"><span class="sd">      &#34;max_connections&#34;: 1000
</span></span></span><span class="line"><span class="cl"><span class="sd">    }</span><span class="w">
</span></span></span></code></pre></div></p>

</td>
</tr>
</tbody>
</table>

#### MaterializeCertSpec
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>dnsNames</code></td>
<td></td>
<td>
<em><strong>Array&lt;String&gt;</strong></em>


<p>Additional DNS names the certificate will be valid for.</p>

</td>
</tr>
<tr>
<td><code>duration</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Duration the certificate will be requested for.
Value must be in units accepted by Go
<a href="https://golang.org/pkg/time/#ParseDuration" ><code>time.ParseDuration</code></a>.</p>

</td>
</tr>
<tr>
<td><code>issuerRef</code></td>
<td></td>
<td>
<em><strong><a href='#certificateissuerref'>CertificateIssuerRef</a></strong></em>


<p>Reference to an <code>Issuer</code> or <code>ClusterIssuer</code> that will generate the certificate.</p>

</td>
</tr>
<tr>
<td><code>renewBefore</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Duration before expiration the certificate will be renewed.
Value must be in units accepted by Go
<a href="https://golang.org/pkg/time/#ParseDuration" ><code>time.ParseDuration</code></a>.</p>

</td>
</tr>
<tr>
<td><code>secretTemplate</code></td>
<td></td>
<td>
<em><strong><a href='#certificatesecrettemplate'>CertificateSecretTemplate</a></strong></em>


<p>Additional annotations and labels to include in the Certificate object.</p>

</td>
</tr>
</tbody>
</table>

#### CertificateSecretTemplate
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>annotations</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p>Annotations is a key value map to be copied to the target Kubernetes Secret.</p>

</td>
</tr>
<tr>
<td><code>labels</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p>Labels is a key value map to be copied to the target Kubernetes Secret.</p>

</td>
</tr>
</tbody>
</table>

#### CertificateIssuerRef
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>Name of the resource being referred to.</p>

</td>
</tr>
<tr>
<td><code>group</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Group of the resource being referred to.</p>

</td>
</tr>
<tr>
<td><code>kind</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Kind of the resource being referred to.</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ResourceRequirements
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>claims</code></td>
<td></td>
<td>
<em><strong>Array&lt;<a href='#iok8sapicorev1resourceclaim'>io.k8s.api.core.v1.ResourceClaim</a>&gt;</strong></em>


<p><p>Claims lists the names of resources, defined in spec.resourceClaims, that are used by this container.</p>
<p>This is an alpha field and requires enabling the DynamicResourceAllocation feature gate.</p>
<p>This field is immutable. It can only be set for containers.</p>
</p>

</td>
</tr>
<tr>
<td><code>limits</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, io.k8s.apimachinery.pkg.api.resource.Quantity&gt;</strong></em>


<p>Limits describes the maximum amount of compute resources allowed. More info: <a href="https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/" >https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/</a></p>

</td>
</tr>
<tr>
<td><code>requests</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, io.k8s.apimachinery.pkg.api.resource.Quantity&gt;</strong></em>


<p>Requests describes the minimum amount of compute resources required. If Requests is omitted for a container, it defaults to Limits if that is explicitly specified, otherwise to an implementation-defined value. Requests cannot exceed Limits. More info: <a href="https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/" >https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/</a></p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ResourceClaim
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>Name must match the name of one entry in pod.spec.resourceClaims of the Pod where this field is used. It makes that resource available inside a container.</p>

</td>
</tr>
<tr>
<td><code>request</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Request is the name chosen for a request in the referenced claim. If empty, everything from the claim is made available, otherwise only the result of this request.</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.EnvVar
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>Name of the environment variable. Must be a C_IDENTIFIER.</p>

</td>
</tr>
<tr>
<td><code>value</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Variable references $(VAR_NAME) are expanded using the previously defined environment variables in the container and any service environment variables. If a variable cannot be resolved, the reference in the input string will be unchanged. Double $$ are reduced to a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. &ldquo;$$(VAR_NAME)&rdquo; will produce the string literal &ldquo;$(VAR_NAME)&rdquo;. Escaped references will never be expanded, regardless of whether the variable exists or not. Defaults to &ldquo;&rdquo;.</p>

</td>
</tr>
<tr>
<td><code>valueFrom</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1envvarsource'>io.k8s.api.core.v1.EnvVarSource</a></strong></em>


<p>Source for the environment variable&rsquo;s value. Cannot be used if value is not empty.</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.EnvVarSource
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>configMapKeyRef</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1configmapkeyselector'>io.k8s.api.core.v1.ConfigMapKeySelector</a></strong></em>


<p>Selects a key of a ConfigMap.</p>

</td>
</tr>
<tr>
<td><code>fieldRef</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1objectfieldselector'>io.k8s.api.core.v1.ObjectFieldSelector</a></strong></em>


<p>Selects a field of the pod: supports metadata.name, metadata.namespace, <code>metadata.labels['&lt;KEY&gt;']</code>, <code>metadata.annotations['&lt;KEY&gt;']</code>, spec.nodeName, spec.serviceAccountName, status.hostIP, status.podIP, status.podIPs.</p>

</td>
</tr>
<tr>
<td><code>resourceFieldRef</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1resourcefieldselector'>io.k8s.api.core.v1.ResourceFieldSelector</a></strong></em>


<p>Selects a resource of the container: only resources limits and requests (limits.cpu, limits.memory, limits.ephemeral-storage, requests.cpu, requests.memory and requests.ephemeral-storage) are currently supported.</p>

</td>
</tr>
<tr>
<td><code>secretKeyRef</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1secretkeyselector'>io.k8s.api.core.v1.SecretKeySelector</a></strong></em>


<p>Selects a key of a secret in the pod&rsquo;s namespace</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.SecretKeySelector
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>key</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>The key of the secret to select from.  Must be a valid secret key.</p>

</td>
</tr>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: <a href="https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names" >https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names</a></p>

</td>
</tr>
<tr>
<td><code>optional</code></td>
<td></td>
<td>
<em><strong>Bool</strong></em>


<p>Specify whether the Secret or its key must be defined</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ResourceFieldSelector
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>resource</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>Required: resource to select</p>

</td>
</tr>
<tr>
<td><code>containerName</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Container name: required for volumes, optional for env vars</p>

</td>
</tr>
<tr>
<td><code>divisor</code></td>
<td></td>
<td>
<em><strong>io.k8s.apimachinery.pkg.api.resource.Quantity</strong></em>


<p>Specifies the output format of the exposed resources, defaults to &ldquo;1&rdquo;</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ObjectFieldSelector
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>fieldPath</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>Path of the field to select in the specified API version.</p>

</td>
</tr>
<tr>
<td><code>apiVersion</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Version of the schema the FieldPath is written in terms of, defaults to &ldquo;v1&rdquo;.</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ConfigMapKeySelector
<table>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>key</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>The key to select.</p>

</td>
</tr>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: <a href="https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names" >https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names</a></p>

</td>
</tr>
<tr>
<td><code>optional</code></td>
<td></td>
<td>
<em><strong>Bool</strong></em>


<p>Specify whether the ConfigMap or its key must be defined</p>

</td>
</tr>
</tbody>
</table>




---

## Materialize Operator Configuration


## Configure the Materialize operator

To configure the Materialize operator, you can:

- Use a configuration YAML file (e.g., `values.yaml`) that specifies the
  configuration values and then install the chart with the `-f` flag:

  ```shell
  # Assumes you have added the Materialize operator Helm chart repository
  helm install my-materialize-operator materialize/materialize-operator \
     -f /path/to/your/config/values.yaml
  ```

- Specify each parameter using the `--set key=value[,key=value]` argument to
  `helm install`. For example:

  ```shell
  # Assumes you have added the Materialize operator Helm chart repository
  helm install my-materialize-operator materialize/materialize-operator  \
    --set observability.podMetrics.enabled=true
  ```


<table>
<thead>
<tr>
<th>Parameter</th>
<th>Default</th>

</tr>
</thead>
<tbody>

<tr>
<td><a href='#balancerdaffinity'><code>balancerd.affinity</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#balancerddefaultresourceslimits'><code>balancerd.defaultResources.limits</code></a></td>
<td>
<code>{&quot;memory&quot;:&quot;256Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#balancerddefaultresourcesrequests'><code>balancerd.defaultResources.requests</code></a></td>
<td>
<code>{&quot;cpu&quot;:&quot;500m&quot;,&quot;memory&quot;:&quot;256Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#balancerdenabled'><code>balancerd.enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#balancerdnodeselector'><code>balancerd.nodeSelector</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#balancerdtolerations'><code>balancerd.tolerations</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#clusterdaffinity'><code>clusterd.affinity</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#clusterdnodeselector'><code>clusterd.nodeSelector</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#clusterdscratchfsnodeselector'><code>clusterd.scratchfsNodeSelector</code></a></td>
<td>
<code>{&quot;materialize.cloud/scratch-fs&quot;:&quot;true&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#clusterdswapnodeselector'><code>clusterd.swapNodeSelector</code></a></td>
<td>
<code>{&quot;materialize.cloud/swap&quot;:&quot;true&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#clusterdtolerations'><code>clusterd.tolerations</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#consoleaffinity'><code>console.affinity</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#consoledefaultresourceslimits'><code>console.defaultResources.limits</code></a></td>
<td>
<code>{&quot;memory&quot;:&quot;256Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#consoledefaultresourcesrequests'><code>console.defaultResources.requests</code></a></td>
<td>
<code>{&quot;cpu&quot;:&quot;500m&quot;,&quot;memory&quot;:&quot;256Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#consoleenabled'><code>console.enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#consoleimagetagmapoverride'><code>console.imageTagMapOverride</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#consolenodeselector'><code>console.nodeSelector</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#consoletolerations'><code>console.tolerations</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#environmentdaffinity'><code>environmentd.affinity</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#environmentddefaultresourceslimits'><code>environmentd.defaultResources.limits</code></a></td>
<td>
<code>{&quot;memory&quot;:&quot;4Gi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#environmentddefaultresourcesrequests'><code>environmentd.defaultResources.requests</code></a></td>
<td>
<code>{&quot;cpu&quot;:&quot;1&quot;,&quot;memory&quot;:&quot;4095Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#environmentdnodeselector'><code>environmentd.nodeSelector</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#environmentdtolerations'><code>environmentd.tolerations</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesegresscidrs'><code>networkPolicies.egress.cidrs</code></a></td>
<td>
<code>[&quot;0.0.0.0/0&quot;]</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesegressenabled'><code>networkPolicies.egress.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesenabled'><code>networkPolicies.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesingresscidrs'><code>networkPolicies.ingress.cidrs</code></a></td>
<td>
<code>[&quot;0.0.0.0/0&quot;]</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesingressenabled'><code>networkPolicies.ingress.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesinternalenabled'><code>networkPolicies.internal.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#observabilityenabled'><code>observability.enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#observabilitypodmetricsenabled'><code>observability.podMetrics.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#observabilityprometheusscrapeannotationsenabled'><code>observability.prometheus.scrapeAnnotations.enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#operatoradditionalmaterializecrdcolumns'><code>operator.additionalMaterializeCRDColumns</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#operatoraffinity'><code>operator.affinity</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#operatorargsenableinternalstatementlogging'><code>operator.args.enableInternalStatementLogging</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#operatorargsenablelicensekeychecks'><code>operator.args.enableLicenseKeyChecks</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#operatorargsstartuplogfilter'><code>operator.args.startupLogFilter</code></a></td>
<td>
<code>&quot;INFO,mz_orchestratord=TRACE&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderprovidersawsaccountid'><code>operator.cloudProvider.providers.aws.accountID</code></a></td>
<td>
<code>&quot;&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderprovidersawsenabled'><code>operator.cloudProvider.providers.aws.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderprovidersawsiamrolesconnection'><code>operator.cloudProvider.providers.aws.iam.roles.connection</code></a></td>
<td>
<code>&quot;&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderprovidersawsiamrolesenvironment'><code>operator.cloudProvider.providers.aws.iam.roles.environment</code></a></td>
<td>
<code>&quot;&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderprovidersgcp'><code>operator.cloudProvider.providers.gcp</code></a></td>
<td>
<code>{&quot;enabled&quot;:false}</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderregion'><code>operator.cloudProvider.region</code></a></td>
<td>
<code>&quot;kind&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudprovidertype'><code>operator.cloudProvider.type</code></a></td>
<td>
<code>&quot;local&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultreplicationfactoranalytics'><code>operator.clusters.defaultReplicationFactor.analytics</code></a></td>
<td>
<code>0</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultreplicationfactorprobe'><code>operator.clusters.defaultReplicationFactor.probe</code></a></td>
<td>
<code>0</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultreplicationfactorsupport'><code>operator.clusters.defaultReplicationFactor.support</code></a></td>
<td>
<code>0</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultreplicationfactorsystem'><code>operator.clusters.defaultReplicationFactor.system</code></a></td>
<td>
<code>0</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizesanalytics'><code>operator.clusters.defaultSizes.analytics</code></a></td>
<td>
<code>&quot;25cc&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizescatalogserver'><code>operator.clusters.defaultSizes.catalogServer</code></a></td>
<td>
<code>&quot;25cc&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizesdefault'><code>operator.clusters.defaultSizes.default</code></a></td>
<td>
<code>&quot;25cc&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizesprobe'><code>operator.clusters.defaultSizes.probe</code></a></td>
<td>
<code>&quot;mz_probe&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizessupport'><code>operator.clusters.defaultSizes.support</code></a></td>
<td>
<code>&quot;25cc&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizessystem'><code>operator.clusters.defaultSizes.system</code></a></td>
<td>
<code>&quot;25cc&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersswap_enabled'><code>operator.clusters.swap_enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#operatorimagepullpolicy'><code>operator.image.pullPolicy</code></a></td>
<td>
<code>&quot;IfNotPresent&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorimagerepository'><code>operator.image.repository</code></a></td>
<td>
<code>&quot;materialize/orchestratord&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorimagetag'><code>operator.image.tag</code></a></td>
<td>
<code>&quot;v26.8.0&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatornodeselector'><code>operator.nodeSelector</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#operatorresourceslimits'><code>operator.resources.limits</code></a></td>
<td>
<code>{&quot;memory&quot;:&quot;512Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#operatorresourcesrequests'><code>operator.resources.requests</code></a></td>
<td>
<code>{&quot;cpu&quot;:&quot;100m&quot;,&quot;memory&quot;:&quot;512Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#operatorsecretscontroller'><code>operator.secretsController</code></a></td>
<td>
<code>&quot;kubernetes&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatortolerations'><code>operator.tolerations</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#rbaccreate'><code>rbac.create</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#schedulername'><code>schedulerName</code></a></td>
<td>
<code>nil</code>
</td>
</tr>

<tr>
<td><a href='#serviceaccountcreate'><code>serviceAccount.create</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#serviceaccountname'><code>serviceAccount.name</code></a></td>
<td>
<code>&quot;orchestratord&quot;</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassallowvolumeexpansion'><code>storage.storageClass.allowVolumeExpansion</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclasscreate'><code>storage.storageClass.create</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassname'><code>storage.storageClass.name</code></a></td>
<td>
<code>&quot;&quot;</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassparameters'><code>storage.storageClass.parameters</code></a></td>
<td>
<code>{&quot;fsType&quot;:&quot;ext4&quot;,&quot;storage&quot;:&quot;lvm&quot;,&quot;volgroup&quot;:&quot;instance-store-vg&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassprovisioner'><code>storage.storageClass.provisioner</code></a></td>
<td>
<code>&quot;&quot;</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassreclaimpolicy'><code>storage.storageClass.reclaimPolicy</code></a></td>
<td>
<code>&quot;Delete&quot;</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassvolumebindingmode'><code>storage.storageClass.volumeBindingMode</code></a></td>
<td>
<code>&quot;WaitForFirstConsumer&quot;</code>
</td>
</tr>

<tr>
<td><a href='#telemetryenabled'><code>telemetry.enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#telemetrysegmentapikey'><code>telemetry.segmentApiKey</code></a></td>
<td>
<code>&quot;hMWi3sZ17KFMjn2sPWo9UJGpOQqiba4A&quot;</code>
</td>
</tr>

<tr>
<td><a href='#telemetrysegmentclientside'><code>telemetry.segmentClientSide</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#tlsdefaultcertificatespecs'><code>tls.defaultCertificateSpecs</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

</tbody>
</table>


## Parameters







### `balancerd` parameters



#### balancerd.affinity

**Default**: <code>{}</code>

Affinity to use for balancerd pods spawned by the operator









#### balancerd.defaultResources.limits

**Default**: <code>{&quot;memory&quot;:&quot;256Mi&quot;}</code>

Default resource limits for balancerd&rsquo;s CPU and memory if not set in the Materialize CR









#### balancerd.defaultResources.requests

**Default**: <code>{&quot;cpu&quot;:&quot;500m&quot;,&quot;memory&quot;:&quot;256Mi&quot;}</code>

Default resources requested for balancerd&rsquo;s CPU and memory if not set in the Materialize CR









#### balancerd.enabled

**Default**: <code>true</code>

Flag to indicate whether to create balancerd pods for the environments









#### balancerd.nodeSelector

**Default**: <code>{}</code>

Node selector to use for balancerd pods spawned by the operator









#### balancerd.tolerations

**Default**: <code>{}</code>

Tolerations to use for balancerd pods spawned by the operator







### `clusterd` parameters



#### clusterd.affinity

**Default**: <code>{}</code>

Affinity to use for clusterd pods spawned by the operator









#### clusterd.nodeSelector

**Default**: <code>{}</code>

Node selector to use for all clusterd pods spawned by the operator









#### clusterd.scratchfsNodeSelector

**Default**: <code>{&quot;materialize.cloud/scratch-fs&quot;:&quot;true&quot;}</code>

Additional node selector to use for clusterd pods when using an LVM scratch disk. This will be merged with the values in <code>nodeSelector</code>.









#### clusterd.swapNodeSelector

**Default**: <code>{&quot;materialize.cloud/swap&quot;:&quot;true&quot;}</code>

Additional node selector to use for clusterd pods when using swap. This will be merged with the values in <code>nodeSelector</code>.









#### clusterd.tolerations

**Default**: <code>{}</code>

Tolerations to use for clusterd pods spawned by the operator







### `console` parameters



#### console.affinity

**Default**: <code>{}</code>

Affinity to use for console pods spawned by the operator









#### console.defaultResources.limits

**Default**: <code>{&quot;memory&quot;:&quot;256Mi&quot;}</code>

Default resource limits for the console&rsquo;s CPU and memory if not set in the Materialize CR









#### console.defaultResources.requests

**Default**: <code>{&quot;cpu&quot;:&quot;500m&quot;,&quot;memory&quot;:&quot;256Mi&quot;}</code>

Default resources requested for the console&rsquo;s CPU and memory if not set in the Materialize CR









#### console.enabled

**Default**: <code>true</code>

Flag to indicate whether to create console pods for the environments









#### console.imageTagMapOverride

**Default**: <code>{}</code>

Override the mapping of environmentd versions to console versions









#### console.nodeSelector

**Default**: <code>{}</code>

Node selector to use for console pods spawned by the operator









#### console.tolerations

**Default**: <code>{}</code>

Tolerations to use for console pods spawned by the operator







### `environmentd` parameters



#### environmentd.affinity

**Default**: <code>{}</code>

Affinity to use for environmentd pods spawned by the operator









#### environmentd.defaultResources.limits

**Default**: <code>{&quot;memory&quot;:&quot;4Gi&quot;}</code>

Default resource limits for environmentd&rsquo;s CPU and memory if not set in the Materialize CR









#### environmentd.defaultResources.requests

**Default**: <code>{&quot;cpu&quot;:&quot;1&quot;,&quot;memory&quot;:&quot;4095Mi&quot;}</code>

Default resources requested for environmentd&rsquo;s CPU and memory if not set in the Materialize CR









#### environmentd.nodeSelector

**Default**: <code>{}</code>

Node selector to use for environmentd pods spawned by the operator









#### environmentd.tolerations

**Default**: <code>{}</code>

Tolerations to use for environmentd pods spawned by the operator







### `networkPolicies` parameters



#### networkPolicies.egress.cidrs

**Default**: <code>[&quot;0.0.0.0/0&quot;]</code>

CIDR blocks to allow egress to









#### networkPolicies.egress.enabled

**Default**: <code>false</code>

Whether to enable egress network policies to sources and sinks









#### networkPolicies.enabled

**Default**: <code>false</code>

Whether to enable network policies for securing communication between pods









#### networkPolicies.ingress.cidrs

**Default**: <code>[&quot;0.0.0.0/0&quot;]</code>

CIDR blocks to allow ingress from









#### networkPolicies.ingress.enabled

**Default**: <code>false</code>

Whether to enable ingress network policies to the SQL and HTTP interfaces on environmentd and balancerd









#### networkPolicies.internal.enabled

**Default**: <code>false</code>

Whether to enable network policies for internal communication between Materialize pods







### `observability` parameters



#### observability.enabled

**Default**: <code>true</code>

Whether to enable observability features









#### observability.podMetrics.enabled

**Default**: <code>false</code>

Whether to enable the pod metrics scraper which populates the Environment Overview Monitoring tab in the web console (requires metrics-server to be installed)









#### observability.prometheus.scrapeAnnotations.enabled

**Default**: <code>true</code>

Whether to annotate pods with common keys used for prometheus scraping.







### `operator` parameters



#### operator.additionalMaterializeCRDColumns

**Default**: <code>{}</code>

Additional columns to display when printing the Materialize CRD in table format.









#### operator.affinity

**Default**: <code>{}</code>

Affinity to use for the operator pod









#### operator.args.enableInternalStatementLogging

**Default**: <code>true</code>











#### operator.args.enableLicenseKeyChecks

**Default**: <code>false</code>











#### operator.args.startupLogFilter

**Default**: <code>&quot;INFO,mz_orchestratord=TRACE&quot;</code>

Log filtering settings for startup logs









#### operator.cloudProvider.providers.aws.accountID

**Default**: <code>&quot;&quot;</code>

When using AWS, accountID is required









#### operator.cloudProvider.providers.aws.enabled

**Default**: <code>false</code>











#### operator.cloudProvider.providers.aws.iam.roles.connection

**Default**: <code>&quot;&quot;</code>

ARN for CREATE CONNECTION feature









#### operator.cloudProvider.providers.aws.iam.roles.environment

**Default**: <code>&quot;&quot;</code>

ARN of the IAM role for environmentd









#### operator.cloudProvider.providers.gcp

**Default**: <code>{&quot;enabled&quot;:false}</code>

GCP Configuration (placeholder for future use)









#### operator.cloudProvider.region

**Default**: <code>&quot;kind&quot;</code>

Common cloud provider settings









#### operator.cloudProvider.type

**Default**: <code>&quot;local&quot;</code>

Specifies cloud provider. Valid values are &lsquo;aws&rsquo;, &lsquo;gcp&rsquo;, &lsquo;azure&rsquo; , &lsquo;generic&rsquo;, or &rsquo;local&rsquo;









#### operator.clusters.defaultReplicationFactor.analytics

**Default**: <code>0</code>











#### operator.clusters.defaultReplicationFactor.probe

**Default**: <code>0</code>











#### operator.clusters.defaultReplicationFactor.support

**Default**: <code>0</code>











#### operator.clusters.defaultReplicationFactor.system

**Default**: <code>0</code>











#### operator.clusters.defaultSizes.analytics

**Default**: <code>&quot;25cc&quot;</code>











#### operator.clusters.defaultSizes.catalogServer

**Default**: <code>&quot;25cc&quot;</code>











#### operator.clusters.defaultSizes.default

**Default**: <code>&quot;25cc&quot;</code>











#### operator.clusters.defaultSizes.probe

**Default**: <code>&quot;mz_probe&quot;</code>











#### operator.clusters.defaultSizes.support

**Default**: <code>&quot;25cc&quot;</code>











#### operator.clusters.defaultSizes.system

**Default**: <code>&quot;25cc&quot;</code>











#### operator.clusters.swap_enabled

**Default**: <code>true</code>

Configure sizes such that the pod QoS class is not Guaranteed, as is required for swap to be enabled. Disk doesn&rsquo;t make much sense with swap, as swap performs better than lgalloc, so it also gets disabled.









#### operator.image.pullPolicy

**Default**: <code>&quot;IfNotPresent&quot;</code>

Policy for pulling the image: &ldquo;IfNotPresent&rdquo; avoids unnecessary re-pulling of images









#### operator.image.repository

**Default**: <code>&quot;materialize/orchestratord&quot;</code>

The Docker repository for the operator image









#### operator.image.tag

**Default**: <code>&quot;v26.8.0&quot;</code>

The tag/version of the operator image to be used









#### operator.nodeSelector

**Default**: <code>{}</code>

Node selector to use for the operator pod









#### operator.resources.limits

**Default**: <code>{&quot;memory&quot;:&quot;512Mi&quot;}</code>

Resource limits for the operator&rsquo;s CPU and memory









#### operator.resources.requests

**Default**: <code>{&quot;cpu&quot;:&quot;100m&quot;,&quot;memory&quot;:&quot;512Mi&quot;}</code>

Resources requested by the operator for CPU and memory









#### operator.secretsController

**Default**: <code>&quot;kubernetes&quot;</code>

Which secrets controller to use for storing secrets. Valid values are &lsquo;kubernetes&rsquo; and &lsquo;aws-secrets-manager&rsquo;. Setting &lsquo;aws-secrets-manager&rsquo; requires a configured AWS cloud provider and IAM role for the environment with Secrets Manager permissions.









#### operator.tolerations

**Default**: <code>{}</code>

Tolerations to use for the operator pod







### `rbac` parameters



#### rbac.create

**Default**: <code>true</code>

Whether to create necessary RBAC roles and bindings







### `schedulerName` parameters



#### schedulerName

**Default**: <code>nil</code>

Optionally use a non-default kubernetes scheduler.







### `serviceAccount` parameters



#### serviceAccount.create

**Default**: <code>true</code>

Whether to create a new service account for the operator









#### serviceAccount.name

**Default**: <code>&quot;orchestratord&quot;</code>

The name of the service account to be created







### `storage` parameters



#### storage.storageClass.allowVolumeExpansion

**Default**: <code>false</code>











#### storage.storageClass.create

**Default**: <code>false</code>

Set to false to use an existing StorageClass instead. Refer to the <a href="https://kubernetes.io/docs/concepts/storage/storage-classes/" >Kubernetes StorageClass documentation</a>









#### storage.storageClass.name

**Default**: <code>&quot;&quot;</code>

Name of the StorageClass to create/use: eg &ldquo;openebs-lvm-instance-store-ext4&rdquo;









#### storage.storageClass.parameters

**Default**: <code>{&quot;fsType&quot;:&quot;ext4&quot;,&quot;storage&quot;:&quot;lvm&quot;,&quot;volgroup&quot;:&quot;instance-store-vg&quot;}</code>

Parameters for the CSI driver









#### storage.storageClass.provisioner

**Default**: <code>&quot;&quot;</code>

CSI driver to use, eg &ldquo;local.csi.openebs.io&rdquo;









#### storage.storageClass.reclaimPolicy

**Default**: <code>&quot;Delete&quot;</code>











#### storage.storageClass.volumeBindingMode

**Default**: <code>&quot;WaitForFirstConsumer&quot;</code>









### `telemetry` parameters



#### telemetry.enabled

**Default**: <code>true</code>











#### telemetry.segmentApiKey

**Default**: <code>&quot;hMWi3sZ17KFMjn2sPWo9UJGpOQqiba4A&quot;</code>











#### telemetry.segmentClientSide

**Default**: <code>true</code>









### `tls` parameters



#### tls.defaultCertificateSpecs

**Default**: <code>{}</code>






## See also

- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)


---

## Self-managed release versions


## V26 releases


| Materialize Operator | orchestratord version | environmentd version | Release date | Notes |
| --- | --- | --- | --- | --- |
| v26.8.0 | v26.8.0 | v26.8.0 | 2026-01-23 | See <a href="/releases/#v2680" >v26.8.0 release notes</a> |
| v26.7.0 | v26.7.0 | v26.7.0 | 2026-01-16 | See <a href="/releases/#v2670" >v26.7.0 release notes</a> |
| v26.6.0 | v26.6.0 | v26.6.0 | 2026-01-12 | See <a href="/releases/#v2660" >v26.6.0 release notes</a> |
| v26.5.1 | v26.5.1 | v26.5.1 | 2025-12-23 | See <a href="/releases/#v2651" >v26.5.1 release notes</a> |
| v26.5.0 | v26.5.0 | v26.5.0 | 2025-12-23 | Do not use. |
| v26.4.0 | v26.4.0 | v26.4.0 | 2025-12-17 | See <a href="/releases/#v2640" >v26.4.0 release notes</a>. |
| v26.3.0 | v26.3.0 | v26.3.0 | 2025-12-12 | See <a href="/releases/#v2630" >v26.3.0 release notes</a>. |
| v26.2.0 | v26.2.0 | v26.2.0 | 2025-12-09 | See <a href="/releases/#v2620" >v26.2.0 release notes</a>. |
| v26.1.0 | v26.1.0 | v26.1.0 | 2025-11-26 | See <a href="/releases/#v2610" >v26.1.0 release notes</a>. |
| v26.0.0 | v26.0.0 | v26.0.0 | 2025-11-18 | See <a href="/releases/#self-managed-v2600" >v26.0.0 release notes</a> |


---

## Troubleshooting


## Troubleshooting Kubernetes

To check the status of the Materialize operator:

```shell
kubectl -n materialize get all
```

If you encounter issues with the Materialize operator,

- Check the operator logs, using the label selector:

  ```shell
  kubectl -n materialize logs -l app.kubernetes.io/name=materialize-operator
  ```

- Check the log of a specific object (pod/deployment/etc) running in
  your namespace:

  ```shell
  kubectl -n materialize logs <type>/<name>
  ```

  In case of a container restart, to get the logs for previous instance, include
  the `--previous` flag.

- Check the events for the operator pod:

  - You can use `kubectl describe`, substituting your pod name for `<pod-name>`:

    ```shell
    kubectl -n materialize describe pod/<pod-name>
    ```

  - You can use `kubectl get events`, substituting your pod name for
    `<pod-name>`:

    ```shell
    kubectl -n materialize get events --sort-by=.metadata.creationTimestamp --field-selector involvedObject.name=<pod-name>
    ```

### Materialize deployment

- To check the status of your Materialize deployment, run:

  ```shell
  kubectl  -n materialize-environment get all
  ```

- To check the log of a specific object (pod/deployment/etc) running in your
  namespace:

  ```shell
  kubectl -n materialize-environment logs <type>/<name>
  ```

  In case of a container restart, to get the logs for previous instance, include
  the `--previous` flag.

- To describe an object, you can use `kubectl describe`:

  ```shell
  kubectl -n materialize-environment describe <type>/<name>
  ```

For additional `kubectl` commands, see [kubectl Quick reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

## Troubleshooting Console unresponsiveness

If you experience long loading screens or unresponsiveness in the Materialize
Console, it may be that the size of the `mz_catalog_server` cluster (where the
majority of the Console's queries are run) is insufficient (default size is
`25cc`).

To increase the cluster's size, you can follow the following steps:

1. Login as the `mz_system` user in order to update `mz_catalog_server`.

   1. To login as `mz_system` you'll need the internal-sql port found in the
      `environmentd` pod (`6877` by default). You can port forward via `kubectl
      port-forward svc/mzXXXXXXXXXX 6877:6877 -n materialize-environment`.

   1. Connect using a pgwire compatible client (e.g., `psql`) and connect using
      the port and user `mz_system`. For example:

       ```
       psql -h localhost -p 6877 --user mz_system
       ```

3. Run the following [ALTER CLUSTER](/sql/alter-cluster/#resizing) statement
   to change the cluster size to `50cc`:

    ```mzsql
    ALTER CLUSTER mz_catalog_server SET (SIZE = '50cc');
    ```

4. Verify your changes via `SHOW CLUSTERS;`

   ```mzsql
   show clusters;
   ```

   The output should include the `mz_catalog_server` cluster with a size of `50cc`:

   ```none
          name        | replicas  | comment
    -------------------+-----------+---------
    mz_analytics      |           |
    mz_catalog_server | r1 (50cc) |
    mz_probe          |           |
    mz_support        |           |
    mz_system         |           |
    quickstart        | r1 (25cc) |
    (6 rows)
    ```


---

## Upgrading


Materialize releases new Self-Managed versions per the schedule outlined in [Release schedule](/releases/schedule/#self-managed-release-schedule).

## General rules for upgrading

<p>When upgrading:</p>
<ul>
<li>
<p><strong>Always</strong> check the <a href="/self-managed-deployments/upgrading/#version-specific-upgrade-notes" >version specific upgrade
notes</a>.</p>
</li>
<li>
<strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
upgrading the Materialize instances.
</li>
</ul>


> **Note:** For major version upgrades, you can <strong>only</strong> upgrade <strong>one</strong> major version
> at a time. For example, upgrades from <strong>v26</strong>.1.0 to <strong>v27</strong>.3.0 is
> permitted but <strong>v26</strong>.1.0 to <strong>v28</strong>.0.0 is not.



## Upgrade guides

The following upgrade guides are available as examples:

<h4 id="upgrade-using-helm-commands">Upgrade using Helm Commands</h4>
<table>
  <thead>
      <tr>
          <th>Guide</th>
          <th>Description</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/upgrade-on-kind/" >Upgrade on Kind</a></td>
          <td>Uses standard Helm commands to upgrade Materialize on a Kind cluster in Docker.</td>
      </tr>
  </tbody>
</table>


<h4 id="upgrade-using-the-new-terraform-modules">Upgrade using the new Terraform Modules</h4>
> **Tip:** The Terraform modules are provided as examples. They are not required for
> upgrading Materialize.

<table>
  <thead>
      <tr>
          <th>Guide</th>
          <th>Description</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/upgrade-on-aws/" >Upgrade on AWS (Terraform)</a></td>
          <td>Uses the new Terraform module to deploy Materialize to AWS Elastic Kubernetes Service (EKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/upgrade-on-azure/" >Upgrade on Azure (Terraform)</a></td>
          <td>Uses the new Terraform module to deploy Materialize to Azure Kubernetes Service (AKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/upgrade-on-gcp/" >Upgrade on GCP (Terraform)</a></td>
          <td>Uses the new Terraform module to deploy Materialize to Google Kubernetes Engine (GKE).</td>
      </tr>
  </tbody>
</table>


<h4 id="upgrade-using-legacy-terraform-modules">Upgrade using Legacy Terraform Modules</h4>
> **Tip:** The Terraform modules are provided as examples. They are not required for
> upgrading Materialize.

<table>
  <thead>
      <tr>
          <th>Guide</th>
          <th>Description</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/legacy/upgrade-on-aws-legacy/" >Upgrade on AWS (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to AWS Elastic Kubernetes Service (EKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/legacy/upgrade-on-azure-legacy/" >Upgrade on Azure (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to Azure Kubernetes Service (AKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/legacy/upgrade-on-gcp-legacy/" >Upgrade on GCP (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to Google Kubernetes Engine (GKE).</td>
      </tr>
  </tbody>
</table>


## Upgrading the Helm Chart and Materialize Operator

> **Important:** When upgrading Materialize, always upgrade the Helm Chart and Materialize
> Operator first.


### Update the Helm Chart repository

To update your Materialize Helm Chart repository:

```shell
helm repo update materialize
```

View the available chart versions:

```shell
helm search repo materialize/materialize-operator --versions
```

### Upgrade your Materialize Operator

The Materialize Kubernetes Operator is deployed via Helm and can be updated
through standard `helm upgrade` command:



```mzsql
helm upgrade -n <namespace> <release-name> materialize/materialize-operator \
  --version <new_version> \
  -f <your-custom-values.yml>

```

| Syntax element | Description |
| --- | --- |
| `<namespace>` | The namespace where the Operator is running. (e.g., `materialize`)  |
| `<release-name>` | The release name. You can use `helm list -n <namespace>` to find your release name.  |
| `<new_version>` | The upgrade version.  |
| `<your-custom-values.yml>` | The name of your customization file, if using. If you are configuring using `\-\-set key=value` options, include them as well.  |


You can use `helm list` to find your release name. For example, if your Operator
is running in the namespace `materialize`, run `helm list`:

```shell
helm list -n materialize
```

Retrieve the name associated with the `materialize-operator` **CHART**; for
example, `my-demo` in the following helm list:

```none
NAME    	  NAMESPACE  	REVISION	UPDATED                             	STATUS  	CHART                                          APP VERSION
my-demo	materialize	1      2025-12-08 11:39:50.185976 -0500 EST	deployed	materialize-operator-v26.1.0    v26.1.0
```

Then, to upgrade:

```shell
helm upgrade -n materialize my-demo materialize/operator \
  -f my-values.yaml \
  --version v26.8.0
```

## Upgrading Materialize Instances

**After** you have upgraded your Materialize Operator, upgrade your Materialize
instance(s) to the **APP Version** of the Operator. To find the version of your
currently deployed Materialize Operator:

```shell
helm list -n materialize
```

You will use the returned **App Version** for the updated `environmentdImageRef`
value. Specifically, for your Materialize instance(s), set
`environmentdImageRef` value to use the new version:

```
spec:
  environmentdImageRef: docker.io/materialize/environmentd:<app_version>
```

To minimize unexpected downtime and avoid connection drops at critical
periods for your application, the upgrade process involves two steps:

- First, stage the changes (update the `environmentdImageRef` with the new
  version) to the Materialize custom resource. The Operator watches for changes
  but does not automatically roll out the changes.

- Second, roll out the changes by specifying a new UUID for `requestRollout`.

### Stage the Materialize instance version change

To stage the Materialize instances version upgrade, update the
`environmentdImageRef` field in the Materialize custom resource spec to the
compatible version of your currently deployed Materialize Operator.

To stage, but **not** rollout, the Materialize instance version upgrade, you can
use the `kubectl patch` command; for example, if the **App Version** is v26.8.0:

```shell
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"docker.io/materialize/environmentd:v26.8.0\"}}"
```

> **Note:** Until you specify a new `requestRollout`, the Operator watches for updates but
> does not roll out the changes.



### Applying the changes via `requestRollout`

To apply chang Materialize instance upgrade, you must update the `requestRollout` field in the Materialize custom resource spec to a new UUID.
Be sure to consult the [Rollout Configurations](#rollout-configuration) to ensure you've selected the correct rollout behavior.
```shell
# Then trigger the rollout with a new UUID
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```

### Staging and applying in a single command

Although separating the staging and rollout of the changes into two steps can
minimize unexpected downtime and avoid connection drops at critical periods, you
can, if preferred, combine both operations in a single command

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"docker.io/materialize/environmentd:v26.8.0\", \"requestRollout\": \"$(uuidgen)\"}}"
```

#### Using YAML Definition

Alternatively, you can update your Materialize custom resource definition directly:

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.8.0 # Update version as needed
  requestRollout: 22222222-2222-2222-2222-222222222222    # Use a new UUID
  forceRollout: 33333333-3333-3333-3333-333333333333      # Optional: for forced rollouts
  inPlaceRollout: false                                   # In Place rollout is deprecated and ignored. Please use rolloutStrategy
  rolloutStrategy: WaitUntilReady                         # The mechanism to use when rolling out the new version. Can be WaitUntilReady or ImmediatelyPromoteCausingDowntime
  backendSecretName: materialize-backend
```

Apply the updated definition:

```shell
kubectl apply -f materialize.yaml
```

## Rollout Configuration

### `requestRollout`

Specify a new `UUID` value for the `requestRollout` to roll out the changes to
the Materialize instance.

> **Note:** `requestRollout` without the `forcedRollout` field only rolls out if changes
> exist to the Materialize instance. To roll out even if there are no changes to
> the instance, use with `forcedRollouts`.


```shell
# Only rolls out if there are changes
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```
#### `requestRollout` with `forcedRollouts`

Specify a new `UUID` value for `forcedRollout` to roll out even when there are
no changes to the instance. Use `forcedRollout` with `requestRollout`.

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\", \"forceRollout\": \"$(uuidgen)\"}}"
```

### Rollout strategies

Rollout strategies control how Materialize transitions from the current generation to a new generation during an upgrade.

The behavior of the new version rollout follows your `rolloutStrategy` setting.

#### *WaitUntilReady* - ***Default***

`WaitUntilReady` creates a new generation of pods and automatically cuts over to them as soon as they catch up to the old generation and become `ReadyToPromote`. This strategy temporarily doubles the required resources to run Materialize.
> **Warning:** `WaitUntilReady` waits up to 72 hours (configurable by the `with_0dt_deployment_max_wait` flag) for the new pods to become ready. If the promotion has not occurred by then, the new pods are automatically promoted.


#### *ImmediatelyPromoteCausingDowntime*
> **Warning:** Using the `ImmediatelyPromoteCausingDowntime` rollout flag will cause downtime.


`ImmediatelyPromoteCausingDowntime` tears down the prior generation, and immediately promotes the new generation without waiting for it to hydrate. This causes downtime until the new generation has hydrated. However, it does not require additional resources.

#### *ManuallyPromote*

`ManuallyPromote` allows you to choose when to promote the new generation. This means you can time the promotion for periods when load is low, minimizing the impact of potential downtime for any clients connected to Materialize. This strategy temporarily doubles the required resources to run Materialize.

To minimize downtime, wait until the new generation has fully hydrated and caught up to the prior generation before promoting. To check hydration status, inspect the `UpToDate` condition in the Materialize resource status. When hydration completes, the condition will be `ReadyToPromote`.

To promote, update the `forcePromote` field to match the `requestRollout` field in the Materialize spec. If you need to promote before hydration completes, you can set `forcePromote` immediately, but clients may experience downtime.

> **Warning:** Leaving a new generation unpromoted for over 6 hours may cause downtime.


**Do not leave new generations unpromoted indefinitely**. They should either be promoted or canceled. New generations open a read hold on the metadata database that prevents compaction. This hold is only released when the generation is promoted or canceled. If left open too long, promoting or canceling can trigger a spike in deletion load on the metadata database, potentially causing downtime. It is not recommended to leave generations unpromoted for over 6 hours.

#### *inPlaceRollout* - ***Deprecated***

The setting is ignored.

## Verifying the Upgrade

After initiating the rollout, you can monitor the status field of the Materialize custom resource to check on the upgrade.

```shell
# Watch the status of your Materialize environment
kubectl get materialize -n materialize-environment -w

# Check the logs of the operator
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

## Version Specific Upgrade Notes

### Upgrading to `v26.1` and later versions

<ul>
<li>To upgrade to <code>v26.1</code> or future versions, you must first upgrade to <code>v26.0</code></li>
</ul>


### Upgrading to `v26.0`

<ul>
<li>
<p>Upgrading to <code>v26.0.0</code> is a major version upgrade. To upgrade to <code>v26.0</code> from
<code>v25.2.X</code> or <code>v25.1</code>, you must first upgrade to <code>v25.2.16</code> and then upgrade to
<code>v26.0.0</code>.</p>
</li>
<li>
<p>For upgrades, the <code>inPlaceRollout</code> setting has been deprecated and will be
ignored. Instead, use the new setting <code>rolloutStrategy</code> to specify either:</p>
<ul>
<li><code>WaitUntilReady</code> (<em>Default</em>)</li>
<li><code>ImmediatelyPromoteCausingDowntime</code></li>
</ul>
<p>For more information, see
<a href="/self-managed-deployments/upgrading/#rollout-strategies" ><code>rolloutStrategy</code></a>.</p>
</li>
<li>
<p>New requirements were introduced for <a href="/releases/#license-key" >license keys</a>.
To upgrade, you will first need to add a license key to the <code>backendSecret</code>
used in the spec for your Materialize resource.</p>
<p>See <a href="/releases/#license-key" >License key</a> for details on getting your license
key.</p>
</li>
<li>
<p>Swap is now enabled by default. Swap reduces the memory required to
operate Materialize and improves cost efficiency. Upgrading to <code>v26.0</code>
requires some preparation to ensure Kubernetes nodes are labeled
and configured correctly. As such:</p>
<ul>
<li>
<p>If you are using the Materialize-provided Terraforms, upgrade to version
<code>v0.6.1</code> of the Terraform.</p>
</li>
<li>
<p>If you are <red><strong>not</strong></red> using a Materialize-provided Terraform, refer
to <a href="/self-managed-deployments/appendix/upgrade-to-swap/" >Prepare for swap and upgrade to v26.0</a>.</p>
</li>
</ul>
</li>
</ul>


### Upgrading between minor versions less than `v26`
 - Prior to `v26`, you must upgrade at most one minor version at a time. For
   example, upgrading from `v25.1.5` to `v25.2.16` is permitted.

## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)

- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)

- [Troubleshooting](/self-managed-deployments/troubleshooting/)
