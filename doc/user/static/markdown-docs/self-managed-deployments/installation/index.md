# Installation

Installation guides for Self-Managed Materialize.



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

## Install Guides (Legacy)



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

## Install locally on kind


Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; and a license key.


The following tutorial uses a local [`kind`](https://kind.sigs.k8s.io/) cluster
and deploys the following components:

- Materialize Operator using Helm into your local `kind` cluster.
- MinIO object storage as the blob storage for your Materialize.
- PostgreSQL database as the metadata database for your Materialize.
- Materialize as a containerized application into your local `kind` cluster.

> **Important:** This tutorial is for local evaluation/testing purposes only.
> - The tutorial uses sample configuration files that are for evaluation/testing
>   purposes only.
> - The tutorial uses a Kubernetes metrics server with TLS disabled. In practice,
>   refer to your organization's official security practices.


## Prerequisites

### kind

Install [`kind`](https://kind.sigs.k8s.io/docs/user/quick-start/).

### Docker

Install [`Docker`](https://docs.docker.com/get-started/get-docker/).

#### Docker resource requirements

For this local deployment, you will need the following Docker resource
requirements:

- 3 CPUs
- 10GB memory


### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, install. For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl`
documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### License key

Starting in v26.0, Self-Managed Materialize requires a license key.


| License key type | Deployment type | Action |
| --- | --- | --- |
| Community | New deployments | <p>To get a license key:</p> <ul> <li>If you have a Cloud account, visit the <a href="https://console.materialize.com/license/" ><strong>License</strong> page in the Materialize Console</a>.</li> <li>If you do not have a Cloud account, visit <a href="https://materialize.com/self-managed/community-license/" >https://materialize.com/self-managed/community-license/</a>.</li> </ul> |
| Community | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |
| Enterprise | New deployments | Visit <a href="https://materialize.com/self-managed/enterprise-license/" >https://materialize.com/self-managed/enterprise-license/</a> to purchase an Enterprise license. |
| Enterprise | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |


## Installation

1. Start Docker if it is not already running.

   For this local deployment, you will need the following Docker resource
   requirements:

   - 3 CPUs
   - 10GB memory


1. Open a Terminal window.

1. Create a working directory and go to the directory.

   ```shell
   mkdir my-local-mz
   cd my-local-mz
   ```

1. Create a `kind` cluster.

   ```shell
   kind create cluster
   ```

1. Add labels `materialize.cloud/disk=true`, `materialize.cloud/swap=true` and
   `workload=materialize-instance` to the `kind` node (in this example, the
   `kind-control-plane` node).

   ```shell
   MYNODE=$(kubectl get nodes --no-headers | awk '{print $1}')
   kubectl label node  $MYNODE materialize.cloud/disk=true
   kubectl label node  $MYNODE materialize.cloud/swap=true
   kubectl label node  $MYNODE workload=materialize-instance
   ```

   Verify that the labels were successfully applied by running the following
   command:

   ```shell
   kubectl get nodes --show-labels
   ```

1. To help you get started for local evaluation/testing, Materialize provides
   some sample configuration files. Download the sample configuration files from
   the Materialize repo:



   ```shell
   mz_version=v26.8.0

   curl -o sample-values.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/operator/values.yaml
   curl -o sample-postgres.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/testing/postgres.yaml
   curl -o sample-minio.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/testing/minio.yaml
   curl -o sample-materialize.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/testing/materialize.yaml
   ```

   - `sample-values.yaml`: Used to configure the Materialize Operator.
   - `sample-postgres.yaml`: Used to configure PostgreSQL as the metadata
     database.
   - `sample-minio.yaml`: Used to configure minIO as the blob storage.
   - `sample-materialize.yaml`: Used to configure Materialize instance.

   These configuration files are for local evaluation/testing purposes only and
   not intended for production use.


1. Add your license key:

   a. To get your license key:


      | License key type | Deployment type | Action |
      | --- | --- | --- |
      | Community | New deployments | <p>To get a license key:</p> <ul> <li>If you have a Cloud account, visit the <a href="https://console.materialize.com/license/" ><strong>License</strong> page in the Materialize Console</a>.</li> <li>If you do not have a Cloud account, visit <a href="https://materialize.com/self-managed/community-license/" >https://materialize.com/self-managed/community-license/</a>.</li> </ul> |
      | Community | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |
      | Enterprise | New deployments | Visit <a href="https://materialize.com/self-managed/enterprise-license/" >https://materialize.com/self-managed/enterprise-license/</a> to purchase an Enterprise license. |
      | Enterprise | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |


   b. Edit `sample-materialize.yaml` to add your license key to the
   `license_key` field in the backend secret.

   ```yaml {hl_lines="10"}
   ---
   apiVersion: v1
   kind: Secret
   metadata:
   name: materialize-backend
   namespace: materialize-environment
   stringData:
   metadata_backend_url: "postgres://materialize_user:materialize_pass@postgres.materialize.svc.cluster.local:5432/materialize_db?sslmode=disable"
   persist_backend_url: "s3://minio:minio123@bucket/12345678-1234-1234-1234-123456789012?endpoint=http%3A%2F%2Fminio.materialize.svc.cluster.local%3A9000&region=minio"
   license_key: "<enter your license key here>"
   ---
   ```

1. Install the Materialize Helm chart.

   1. Add the Materialize Helm chart repository.

      ```shell
      helm repo add materialize https://materializeinc.github.io/materialize
      ```

   1. Update the repository.

      ```shell
      helm repo update materialize
      ```



   1. Install the Materialize Operator. The operator will be installed in the
      `materialize` namespace.

      ```shell
      helm install my-materialize-operator materialize/materialize-operator \
          --namespace=materialize --create-namespace \
          --version v26.8.0 \
          --set observability.podMetrics.enabled=true \
          -f sample-values.yaml
      ```


   1. Verify the installation and check the status:

      ```shell
      kubectl get all -n materialize
      ```

      Wait for the components to be ready and in the `Running` state:

      ```none
      NAME                                           READY   STATUS    RESTARTS   AGE
      pod/my-materialize-operator-6c4c7d6fc9-hbzvr   1/1     Running   0          16s

      NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
      deployment.apps/my-materialize-operator   1/1     1            1           16s

      NAME                                                 DESIRED   CURRENT         READY   AGE
      replicaset.apps/my-materialize-operator-6c4c7d6fc9   1         1               1       16s
      ```

      If you run into an error during deployment, refer to the
      [Troubleshooting](/installation/troubleshooting) guide.

1. Install PostgreSQL and MinIO.

    1. Use the `sample-postgres.yaml` file to install PostgreSQL as the
       metadata database:

        ```shell
        kubectl apply -f sample-postgres.yaml
        ```

    1. Use the `sample-minio.yaml` file to install MinIO as the blob storage:

        ```shell
        kubectl apply -f sample-minio.yaml
        ```

    1. Verify the installation and check the status:

       ```shell
       kubectl get all -n materialize
       ```

       Wait for the components to be ready and in the `Running` state:

       ```none
       NAME                                           READY   STATUS     RESTARTS   AGE
       pod/minio-777db75dd4-zcl89                     1/1     Running    0          84s
       pod/my-materialize-operator-6c4c7d6fc9-hbzvr   1/1     Running    0          107s
       pod/postgres-55fbcd88bf-b4kdv                  1/1     Running    0          86s

       NAME               TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)    AGE
       service/minio      ClusterIP   10.96.51.9     <none>        9000/TCP   84s
       service/postgres   ClusterIP   10.96.19.166   <none>        5432/TCP   86s

       NAME                                      READY   UP-TO-DATE    AVAILABLE   AGE
       deployment.apps/minio                     1/1     1             1           84s
       deployment.apps/my-materialize-operator   1/1     1             1           107s
       deployment.apps/postgres                  1/1     1             1           86s

       NAME                                                 DESIRED    CURRENT         READY   AGE
       replicaset.apps/minio-777db75dd4                     1          1               1       84s
       replicaset.apps/my-materialize-operator-6c4c7d6fc9   1          1               1       107s
       replicaset.apps/postgres-55fbcd88bf                  1          1               1       86s
       ```

1. Install the metrics service to the `kube-system` namespace.

   1. Add the metrics server Helm repository.

      ```shell
      helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
      ```

   1. Update the repository.

      ```shell
      helm repo update metrics-server
      ```

   1. Install the metrics server to the `kube-system` namespace.

      > **Important:** This tutorial is for local evaluation/testing purposes only. For simplicity,
>       the tutorial uses a Kubernetes metrics server with TLS disabled. In practice,
>       refer to your organization's official security practices.


      ```shell
      helm install metrics-server metrics-server/metrics-server \
         --namespace kube-system \
         --set args="{--kubelet-insecure-tls,--kubelet-preferred-address-types=InternalIP\,Hostname\,ExternalIP}"
      ```

      You can verify the installation by running the following command:

      ```bash
      kubectl get pods -n kube-system -l app.kubernetes.io/instance=metrics-server
      ```

      Wait for the `metrics-server` pod to be ready and in the `Running` state:

      ```none
      NAME                             READY   STATUS    RESTARTS   AGE
      metrics-server-89dfdc559-bq59m   1/1     Running   0          2m6s
      ```

1. Install Materialize into a new `materialize-environment` namespace:

   1. Use the `sample-materialize.yaml` file to create the
      `materialize-environment` namespace and install Materialize:

      ```shell
      kubectl apply -f sample-materialize.yaml
      ```

    1. Verify the installation and check the status:

       ```shell
       kubectl get all -n materialize-environment
       ```

       Wait for the components to be ready and in the `Running` state.

       ```none
       NAME                                             READY   STATUS    RESTARTS   AGE
       pod/mz32bsnzerqo-balancerd-756b65959c-6q9db      1/1     Running   0                 12s
       pod/mz32bsnzerqo-cluster-s2-replica-s1-gen-1-0   1/1     Running   0                 14s
       pod/mz32bsnzerqo-cluster-u1-replica-u1-gen-1-0   1/1     Running   0                 14s
       pod/mz32bsnzerqo-console-6b7c975fb9-jkm8l        1/1     Running   0          5s
       pod/mz32bsnzerqo-console-6b7c975fb9-z8g8f        1/1     Running   0          5s
       pod/mz32bsnzerqo-environmentd-1-0                1/1     Running   0                 19s

       NAME                                               TYPE        CLUSTER-IP          EXTERNAL-IP   PORT(S)                                        AGE
       service/mz32bsnzerqo-balancerd                     ClusterIP   None                <none>        6876/TCP,6875/TCP                              12s
       service/mz32bsnzerqo-cluster-s2-replica-s1-gen-1   ClusterIP   None                <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   14s
       service/mz32bsnzerqo-cluster-u1-replica-u1-gen-1   ClusterIP   None                <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   14s
       service/mz32bsnzerqo-console                       ClusterIP   None                <none>        8080/TCP                                       5s
       service/mz32bsnzerqo-environmentd                  ClusterIP   None                <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            12s
       service/mz32bsnzerqo-environmentd-1                ClusterIP   None                <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            19s
       service/mz32bsnzerqo-persist-pubsub-1              ClusterIP   None                <none>        6879/TCP                                       19s

       NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
       deployment.apps/mz32bsnzerqo-balancerd   1/1     1            1           12s
       deployment.apps/mz32bsnzerqo-console     2/2     2            2           5s

       NAME                                                DESIRED   CURRENT   READY          AGE
       replicaset.apps/mz32bsnzerqo-balancerd-756b65959c   1         1         1              12s
       replicaset.apps/mz32bsnzerqo-console-6b7c975fb9     2         2         2              5s

       NAME                                                        READY   AGE
       statefulset.apps/mz32bsnzerqo-cluster-s2-replica-s1-gen-1   1/1     14s
       statefulset.apps/mz32bsnzerqo-cluster-u1-replica-u1-gen-1   1/1     14s
       statefulset.apps/mz32bsnzerqo-environmentd-1                1/1     19s
       ```

       If you run into an error during deployment, refer to the
       [Troubleshooting](/self-hosted/troubleshooting) guide.

1. Open the Materialize Console in your browser:


   1. Find your console service name.

      ```shell
      MZ_SVC_CONSOLE=$(kubectl -n materialize-environment get svc \
        -o custom-columns="NAME:.metadata.name" --no-headers | grep console)
      echo $MZ_SVC_CONSOLE
      ```

   1. Port forward the Materialize Console service to your local machine:[^1]

      ```shell
      (
        while true; do
           kubectl port-forward svc/$MZ_SVC_CONSOLE 8080:8080 -n materialize-environment 2>&1 | tee /dev/stderr |
           grep -q "portforward.go" && echo "Restarting port forwarding due to an error." || break;
        done;
      ) &
      ```

      The command is run in background.
      <br>- To list the background jobs, use `jobs`.
      <br>- To bring back to foreground, use `fg %<job-number>`.
      <br>- To kill the background job, use `kill %<job-number>`.

   1. Open a browser and navigate to
      [http://localhost:8080](http://localhost:8080).

   [^1]: The port forwarding command uses a while loop to handle a [known
   Kubernetes issue 78446](https://github.com/kubernetes/kubernetes/issues/78446),
   where interrupted long-running requests through a standard port-forward cause
   the port forward to hang. The command automatically restarts the port forwarding
   if an error occurs, ensuring a more stable connection. It detects failures by
   monitoring for "portforward.go" error messages.


      > **Tip:** If you experience long loading screens or unresponsiveness in the Materialize
>       Console, we recommend increasing the size of the `mz_catalog_server` cluster.
>       Refer to the [Troubleshooting Console
>       Unresponsiveness](/self-managed-deployments/troubleshooting/#troubleshooting-console-unresponsiveness)
>       guide.


## Next steps


- From the Console, you can get started with the
[Quickstart](/get-started/quickstart/).

- To start ingesting your own data from an external system like Kafka, MySQL or
  PostgreSQL, see [Ingest data](/ingest-data/).


## Clean up

To delete the whole local deployment (including Materialize instances and data):

```bash
kind delete cluster
```

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Installation](/installation/)


---

## Install on AWS


Materialize provides a set of modular [Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
that can be used to deploy all services required for Materialize to run on AWS.
The module is intended to provide a simple set of examples on how to deploy
Materialize. It can be used as is or modules can be taken from the example and
integrated with existing DevOps tooling.

Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; and a license key.
 The example on this page
deploys a complete Materialize environment on AWS using the modular Terraform
setup from this repository.


> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
> - Fork the repo and pin to a specific version; or
> - Use the code as a reference when developing your own deployment.



## What Gets Created

This example provisions the following infrastructure:

### Networking

| Resource | Description |
|----------|-------------|
| VPC | 10.0.0.0/16 with DNS hostnames and support enabled |
| Subnets | 3 private subnets (10.0.1.0/24, 10.0.2.0/24, 10.0.3.0/24) and 3 public subnets (10.0.101.0/24, 10.0.102.0/24, 10.0.103.0/24) across availability zones us-east-1a, us-east-1b, us-east-1c |
| NAT Gateway | Single NAT Gateway for all private subnets |
| Internet Gateway | For public subnet connectivity |

### Compute

| Resource | Description |
|----------|-------------|
| EKS Cluster | Version 1.32 with CloudWatch logging (API, audit) |
| Base Node Group | 2 nodes (t4g.medium) for Karpenter and CoreDNS |
| Karpenter | Auto-scaling controller with two node classes: Generic nodepool (t4g.xlarge instances for general workloads) and Materialize nodepool (r7gd.2xlarge instances with swap enabled and dedicated taints to run materialize instance workloads) |

### Database

| Resource | Description |
|----------|-------------|
| RDS PostgreSQL | Version 15, db.t3.large instance |
| Storage | 50GB allocated, autoscaling up to 100GB |
| Deployment | Single-AZ (non-production configuration) |
| Backups | 7-day retention |
| Security | Dedicated security group with access from EKS cluster and nodes |

### Storage

| Resource | Description |
|----------|-------------|
| S3 Bucket | Dedicated bucket for Materialize persistence |
| Encryption | Disabled (for testing; enable in production) |
| Versioning | Disabled (for testing; enable in production) |
| IAM Role | IRSA role for Kubernetes service account access |

### Kubernetes Add-ons

| Resource | Description |
|----------|-------------|
| AWS Load Balancer Controller | For managing Network Load Balancers |
| cert-manager | Certificate management controller for Kubernetes that automates TLS certificate provisioning and renewal |
| Self-signed ClusterIssuer | Provides self-signed TLS certificates for Materialize instance internal communication (balancerd, console). Used by the Materialize instance for secure inter-component communication. |

### Materialize

| Resource | Description |
|----------|-------------|
| Operator | Materialize Kubernetes operator in the `materialize` namespace |
| Instance | Single Materialize instance in the `materialize-environment` namespace |
| Network Load Balancer | Dedicated NLB for access to Materialize
| Port | Description |
| --- | --- |
| 6875 | For SQL connections to the database |
| 6876 | For HTTP(S) connections to the database |
| 8080 | For HTTP(S) connections to Materialize Console |
 |


## Prerequisites

### AWS Account Requirements

An active AWS account with appropriate permissions to create:
- EKS clusters
- RDS instances
- S3 buckets
- VPCs and networking resources
- IAM roles and policies

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- [kubectl](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)


### License Key


| License key type | Deployment type | Action |
| --- | --- | --- |
| Community | New deployments | <p>To get a license key:</p> <ul> <li>If you have a Cloud account, visit the <a href="https://console.materialize.com/license/" ><strong>License</strong> page in the Materialize Console</a>.</li> <li>If you do not have a Cloud account, visit <a href="https://materialize.com/self-managed/community-license/" >https://materialize.com/self-managed/community-license/</a>.</li> </ul> |
| Community | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |
| Enterprise | New deployments | Visit <a href="https://materialize.com/self-managed/enterprise-license/" >https://materialize.com/self-managed/enterprise-license/</a> to purchase an Enterprise license. |
| Enterprise | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |


## Getting started: Simple example

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
> - Fork the repo and pin to a specific version; or
> - Use the code as a reference when developing your own deployment.


### Step 1: Set Up the Environment

1. Open a terminal window.

1. Clone the Materialize Terraform repository and go to the
   `aws/examples/simple` directory.

   ```bash
   git clone https://github.com/MaterializeInc/materialize-terraform-self-managed.git
   cd materialize-terraform-self-managed/aws/examples/simple
   ```

1. Ensure your AWS CLI is configured with the appropriate profile, substitute
   `<your-aws-profile>` with the profile to use:

   ```bash
   # Set your AWS profile for the session
   export AWS_PROFILE=<your-aws-profile>
   ```

### Step 2: Configure Terraform Variables

1. Create a `terraform.tfvars` file with the following variables:

   - `name_prefix`: Prefix for all resource names (e.g., `simple-demo`)
   - `aws_region`: AWS region for deployment (e.g., `us-east-1`)
   - `aws_profile`: AWS CLI profile to use
   - `license_key`: Materialize license key
   - `tags`: Map of tags to apply to resources

   ```hcl
   name_prefix = "simple-demo"
   aws_region  = "us-east-1"
   aws_profile = "your-aws-profile"
   license_key = "your-materialize-license-key"
   tags = {
     environment = "demo"
   }
   # internal_load_balancer = false   # default = true (internal load balancer). You can set to false = public load balancer.
   # ingress_cidr_blocks = ["x.x.x.x/n", ...]
   # k8s_apiserver_authorized_networks  = ["x.x.x.x/n", ...]
   ```

   <p><strong>Optional variables</strong>:</p>
   <ul>
   <li><code>internal_load_balancer</code>: Flag that determines whether the load balancer
   is internal (default) or public.</li>
   <li><code>ingress_cidr_blocks</code>: List of CIDR blocks allowed to reach the load
   balancer if the load balancer is public (<code>internal_load_balancer: false</code>).
   If unset, defaults to <code>[&quot;0.0.0.0/0&quot;]</code> (i.e., <red><strong>all</strong></red> IPv4
   addresses on the internet). <strong>Only applied when the load balancer is public</strong>.</li>
   <li><code>k8s_apiserver_authorized_networks</code>: List of CIDR
   blocks allowed to access your cluster endpoint. If unset, defaults to
   <code>[&quot;0.0.0.0/0&quot;]</code> (<red><strong>all</strong></red> IPv4 addresses on the internet).</li>
   </ul>
   > **Note:** Refer to your organization's security practices to set these values accordingly.

### Step 3: Apply the Terraform

1. Initialize the Terraform directory to download the required providers
    and modules:

    ```bash
    terraform init
    ```

1. Apply the Terraform configuration to create the infrastructure.

   ```bash
   terraform apply
   ```

   If you are satisfied with the planned changes, type `yes` when prompted to
   proceed.

   > **Tip:** If you previously logged in to Amazon ECR Public, a cached auth token may cause 403 errors even when pulling public images. To remove the token, run:
>    ```bash
>    docker logout public.ecr.aws
>    ```
>    Then, re-apply the Terraform configuration.


1. From the output, you will need the following fields to connect using the
   Materialize Console and PostgreSQL-compatible clients/drivers:
   - `nlb_dns_name`
   - `external_login_password_mz_system`.

   ```bash
   terraform output -raw <field_name>
   ```

   > **Tip:** Your shell may show an ending marker (such as `%`) because the
>    output did not end with a newline. Do not include the marker when using the value.



1. Configure `kubectl` to connect to your cluster, replacing:

   - `<your-eks-cluster-name>` with the your cluster name; i.e., the
     `eks_cluster_name` in the Terraform output. For the
     sample example, your cluster name has the form `{prefix_name}-eks`; e.g.,
     `simple-demo-eks`.

   - `<your-region>` with the region of your cluster. Your region can be
     found in your `terraform.tfvars` file; e.g., `us-east-1`.

   ```bash
   # aws eks update-kubeconfig --name <your-eks-cluster-name> --region <your-region>
   aws eks update-kubeconfig --name $(terraform output -raw eks_cluster_name) --region <your-region>
   ```

### Step 4. Optional. Verify the deployment.

1. Check the status of your deployment:
   **Operator:**
   To check the status of the Materialize operator, which runs in the `materialize` namespace:
   ```bash
   kubectl -n materialize get all
   ```

   **Materialize instance:**
   To check the status of the Materialize instance, which runs in the `materialize-environment` namespace:
   ```bash
   kubectl -n materialize-environment get all
   ```


   <p>If you run into an error during deployment, refer to the
   <a href="/self-managed-deployments/troubleshooting/" >Troubleshooting</a>.</p>

### Step 5: Connect to Materialize

Using the `nlb_dns_name` and `external_login_password_mz_system` from the Terraform
output, you can connect to Materialize via the Materialize Console or
PostgreSQL-compatible tools/drivers using the following ports:


| Port | Description |
| --- | --- |
| 6875 | For SQL connections to the database |
| 6876 | For HTTP(S) connections to the database |
| 8080 | For HTTP(S) connections to Materialize Console |



#### Connect to the Materialize Console

> **Note:** - **If using a public NLB:** Both SQL and Console are available via the
> public NLB. You can connect directly using the NLB's DNS name from anywhere
> on the internet (subject to your `ingress_cidr_blocks` configuration).
> - **If using a private (internal) NLB:** You can connect from inside the same VPC or from networks that are privately connected to it. Alternatively, use Kubernetes port-forwarding for both SQL and Console.



1. To connect to the Materialize Console, open a browser to
    `https://<nlb_dns_name>:8080`, substituting your `<nlb_dns_name>`.

   From the terminal, you can type:

   ```sh
   open "https://$(terraform output -raw  nlb_dns_name):8080/materialize"
   ```

   > **Tip:** The example uses a self-signed ClusterIssuer. As such, you may encounter a
>    warning with regards to the certificate. In production, run with
>    certificates from an official Certificate Authority (CA) rather than
>    self-signed certificates.


1. Log in as `mz_system`, using `external_login_password_mz_system` as the
   password.

1. Create new users and log out.

   In general, other than the initial login to create new users for new
   deployments, avoid using `mz_system` since `mz_system` also used by the
   Materialize Operator for upgrades and maintenance tasks.

   For more information on authentication and authorization for Self-Managed
   Materialize, see:

   - [Authentication](/security/self-managed/authentication/)
   - [Access Control](/security/self-managed/access-control/)

1. Login as one of the created user.

#### Connect using `psql`

> **Note:** - **If using a public NLB:** Both SQL and Console are available via the
> public NLB. You can connect directly using the NLB's DNS name from anywhere
> on the internet (subject to your `ingress_cidr_blocks` configuration).
> - **If using a private (internal) NLB:** You can connect from inside the same VPC or from networks that are privately connected to it. Alternatively, use Kubernetes port-forwarding for both SQL and Console.



1. To connect using `psql`, in the connection string, specify:

   - `mz_system` as the user
   - Your `<nlb_dns_name>` as the host
   - `6875` as the port:

   ```sh
   psql "postgres://mz_system@$(terraform output -raw  nlb_dns_name):6875/materialize"
   ```

   When prompted for the password, enter the
   `external_login_password_mz_system` value.

1. Create new users and log out.

   In general, other than the initial login to create new users for new
   deployments, avoid using `mz_system` since `mz_system` also used by the
   Materialize Operator for upgrades and maintenance tasks.

   For more information on authentication and authorization for Self-Managed
   Materialize, see:

   - [Authentication](/security/self-managed/authentication/)
   - [Access Control](/security/self-managed/access-control/)

1. Login as one of the created user.

## Customizing Your Deployment

> **Tip:** To reduce cost in your demo environment, you can tweak subnet CIDRs
> and instance types in `main.tf`.


You can customize each Terraform module independently.

- For details on the Terraform modules, see both the [top
level](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
and [AWS
specific](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws) READMEs.

- For details on recommended instance sizing and configuration, see the [AWS
deployment
guide](/self-managed-deployments/deployment-guidelines/aws-deployment-guidelines/).

See also:

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)


## Cleanup


To delete the whole sample infrastructure and deployment (including the
Materialize operator and Materialize instances and data), run from the Terraform
directory:

```bash
terraform destroy
```

When prompted to proceed, type `yes` to confirm the deletion.



## See Also


- [Troubleshooting](/self-managed-deployments/troubleshooting/)


---

## Install on Azure


Materialize provides a set of modular [Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
that can be used to deploy all services required for Materialize to run on Azure.
The module is intended to provide a simple set of examples on how to deploy
Materialize. It can be used as is or modules can be taken from the example and
integrated with existing DevOps tooling.

Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; and a license key.
 The example on this page
deploys a complete Materialize environment on Azure using the modular Terraform
setup from this repository.

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
> - Fork the repo and pin to a specific version; or
> - Use the code as a reference when developing your own deployment.


## What Gets Created

This example provisions the following infrastructure:

### Resource Group

| Resource | Description |
|----------|-------------|
| Resource Group | New resource group to contain all resources |

### Networking

| Resource | Description |
|----------|-------------|
| Virtual Network | 20.0.0.0/16 address space |
| AKS Subnet | 20.0.0.0/20 with NAT Gateway association and service endpoints for Storage and SQL |
| PostgreSQL Subnet | 20.0.16.0/24 delegated to PostgreSQL Flexible Server |
| NAT Gateway | Standard SKU with static public IP for outbound connectivity |
| Private DNS Zone | For PostgreSQL private endpoint resolution with VNet link |

### Compute

| Resource | Description |
|----------|-------------|
| AKS Cluster | Version 1.32 with Cilium networking (network plugin: azure, data plane: cilium, policy: cilium) |
| Default Node Pool | Standard_D4pds_v6 VMs, autoscaling 2-5 nodes, labeled for generic workloads |
| Materialize Node Pool | Standard_E4pds_v6 VMs with 100GB disk, autoscaling 2-5 nodes, swap enabled, dedicated taints for Materialize workloads |
| Managed Identities | AKS cluster identity (used by AKS control plane to provision Azure resources like load balancers and network interfaces) and Workload identity (used by Materialize pods for secure, passwordless authentication to Azure Storage) |

### Database

| Resource | Description |
|----------|-------------|
| Azure PostgreSQL Flexible Server | Version 15 |
| SKU | GP_Standard_D2s_v3 (2 vCores, 4GB memory) |
| Storage | 32GB with 7-day backup retention |
| Network Access | Public Network Access is disabled, Private access only (no public endpoint) |
| Database | `materialize` database pre-created |

### Storage

| Resource | Description |
|----------|-------------|
| Storage Account | Premium BlockBlobStorage with LRS replication for Materialize persistence |
| Container | `materialize` blob container |
| Access Control | Workload Identity federation for Kubernetes service account (passwordless authentication via OIDC) |
| Network Access | Currently allows <red>**all traffic**</red>(production deployments should restrict to AKS subnet only traffic) |

### Kubernetes Add-ons

| Resource | Description |
|----------|-------------|
| cert-manager | Certificate management controller for Kubernetes that automates TLS certificate provisioning and renewal |
| Self-signed ClusterIssuer | Provides self-signed TLS certificates for Materialize instance internal communication (balancerd, console). Used by the Materialize instance for secure inter-component communication. |

### Materialize

| Resource | Description |
|----------|-------------|
| Operator | Materialize Kubernetes operator in the `materialize` namespace |
| Instance | Single Materialize instance in the `materialize-environment` namespace |
| Load Balancers | Azure Load Balancers for access to Materialize
| Port | Description |
| --- | --- |
| 6875 | For SQL connections to the database |
| 6876 | For HTTP(S) connections to the database |
| 8080 | For HTTP(S) connections to Materialize Console |
  |

## Prerequisites

### Azure Account Requirements

An active Azure subscription with appropriate permissions to create:
- AKS clusters
- Azure PostgreSQL Flexible Server instances
- Storage accounts
- Virtual networks and networking resources
- Managed identities and role assignments

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)

### License Key


| License key type | Deployment type | Action |
| --- | --- | --- |
| Community | New deployments | <p>To get a license key:</p> <ul> <li>If you have a Cloud account, visit the <a href="https://console.materialize.com/license/" ><strong>License</strong> page in the Materialize Console</a>.</li> <li>If you do not have a Cloud account, visit <a href="https://materialize.com/self-managed/community-license/" >https://materialize.com/self-managed/community-license/</a>.</li> </ul> |
| Community | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |
| Enterprise | New deployments | Visit <a href="https://materialize.com/self-managed/enterprise-license/" >https://materialize.com/self-managed/enterprise-license/</a> to purchase an Enterprise license. |
| Enterprise | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |


## Getting started: Simple example

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
> - Fork the repo and pin to a specific version; or
> - Use the code as a reference when developing your own deployment.


### Step 1: Set Up the Environment

1. Open a terminal window.

1. Clone the Materialize Terraform repository and go to the
   `azure/examples/simple` directory.

   ```bash
   git clone https://github.com/MaterializeInc/materialize-terraform-self-managed.git
   cd materialize-terraform-self-managed/azure/examples/simple
   ```

1. Authenticate with Azure.

    ```bash
    az login
    ```

   The command opens a browser window to sign in to Azure. Sign in.

1. Select the subscription and tenant to use. After you have signed in, back in
   the terminal, your tenant and subscription information is displayed.

    ```none
    Retrieving tenants and subscriptions for the selection...

    [Tenant and subscription selection]

    No     Subscription name    Subscription ID                       Tenant
    -----  -------------------  ------------------------------------  ----------------
    [1]*   ...                  ...                                   ...

   The default is marked with an *; the default tenant is '<Tenant>' and
   subscription is '<Subscription Name>' (<Subscription ID>).
   ```

   Select the subscription and tenant.

### Step 2: Configure Terraform Variables

1. Create a `terraform.tfvars` file with the following variables:

   - `subscription_id`: Azure subscription ID
   - `resource_group_name`: Name for the resource group to create (e.g.
     `mz-demo-rg`)
   - `name_prefix`: Prefix for all resource names (e.g., `simple-demo`)
   - `location`: Azure region for deployment (e.g., `westus2`)
   - `license_key`: Materialize license key
   - `tags`: Map of tags to apply to resources

   ```hcl
   subscription_id     = "your-subscription-id"
   resource_group_name = "mz-demo-rg"
   name_prefix         = "simple-demo"
   location            = "westus2"
   license_key         = "your-materialize-license-key"
   tags = {
     environment = "demo"
   }
   # internal_load_balancer = false   # default = true (internal load balancer). You can set to false = public load balancer.
   # ingress_cidr_blocks = ["x.x.x.x/n", ...]
   # k8s_apiserver_authorized_networks  = ["x.x.x.x/n", ...]
   ```

   <p><strong>Optional variables</strong>:</p>
   <ul>
   <li><code>internal_load_balancer</code>: Flag that determines whether the load balancer
   is internal (default) or public.</li>
   <li><code>ingress_cidr_blocks</code>: List of CIDR blocks allowed to reach the load
   balancer if the load balancer is public (<code>internal_load_balancer: false</code>).
   If unset, defaults to <code>[&quot;0.0.0.0/0&quot;]</code> (i.e., <red><strong>all</strong></red> IPv4
   addresses on the internet). <strong>Only applied when the load balancer is public</strong>.</li>
   <li><code>k8s_apiserver_authorized_networks</code>: List of CIDR
   blocks allowed to access your cluster endpoint. If unset, defaults to
   <code>[&quot;0.0.0.0/0&quot;]</code> (<red><strong>all</strong></red> IPv4 addresses on the internet).</li>
   </ul>
   > **Note:** Refer to your organization's security practices to set these values accordingly.

### Step 3: Apply the Terraform

1. Initialize the Terraform directory to download the required providers
   and modules:

   ```bash
   terraform init
   ```

1. Apply the Terraform configuration to create the infrastructure.

   ```bash
   terraform apply
   ```

   If you are satisfied with the planned changes, type `yes` when prompted
   to proceed.

1. From the output, you will need the following field(s) to connect:
   - `console_load_balancer_ip` for the Materialize Console
   - `balancerd_load_balancer_ip` to connect PostgreSQL-compatible
     clients/drivers.
   - `external_login_password_mz_system`.

   ```bash
   terraform output -raw <field_name>
   ```

   > **Tip:** Your shell may show an ending marker (such as `%`) because the
>    output did not end with a newline. Do not include the marker when using the value.


1. Configure `kubectl` to connect to your cluster, replacing:
   - `<your-resource-group-name>` with your resource group name; i.e., the
     `resource_group_name` in the Terraform output or in the
     `terraform.tfvars` file.

   - `<your-aks-cluster-name>` with your cluster name; i.e., the
     `aks_cluster_name` in the Terraform output. For the sample example,
     your cluster name has the form `{prefix_name}-aks`; e.g., `simple-demo-aks`.

   ```bash
   # az aks get-credentials --resource-group <your-resource-group-name> --name <your-aks-cluster-name>
   az aks get-credentials --resource-group $(terraform output -raw resource_group_name) --name $(terraform output -raw aks_cluster_name)
   ```

### Step 4. Optional. Verify the deployment.

1. Check the status of your deployment:
   **Operator:**
   To check the status of the Materialize operator, which runs in the `materialize` namespace:
   ```bash
   kubectl -n materialize get all
   ```

   **Materialize instance:**
   To check the status of the Materialize instance, which runs in the `materialize-environment` namespace:
   ```bash
   kubectl -n materialize-environment get all
   ```


   <p>If you run into an error during deployment, refer to the
   <a href="/self-managed-deployments/troubleshooting/" >Troubleshooting</a>.</p>

### Step 5: Connect to Materialize

You can connect to Materialize via the Materialize Console or
PostgreSQL-compatible tools/drivers using the following ports:


| Port | Description |
| --- | --- |
| 6875 | For SQL connections to the database |
| 6876 | For HTTP(S) connections to the database |
| 8080 | For HTTP(S) connections to Materialize Console |


#### Connect using the Materialize Console

> **Note:** - **If using a public NLB:** Both SQL and Console are available via the
> public NLB. You can connect directly using the NLB's DNS name from anywhere
> on the internet (subject to your `ingress_cidr_blocks` configuration).
> - **If using a private (internal) NLB:** You can connect from inside the same VPC or from networks that are privately connected to it. Alternatively, use Kubernetes port-forwarding for both SQL and Console.



Using the `console_load_balancer_ip` and `external_login_password_mz_system`
from the Terraform output, you can connect to Materialize via the Materialize
Console.

1. To connect to the Materialize Console, open a browser to
   `https://<console_load_balancer_ip>:8080`, substituting your
   `<console_load_balancer_ip>`.

   From the terminal, you can type:

   ```sh
   open "https://$(terraform output -raw  console_load_balancer_ip):8080/materialize"
   ```

   > **Tip:** The example uses a self-signed ClusterIssuer. As such, you may encounter a
>    warning with regards to the certificate. In production, run with
>    certificates from an official Certificate Authority (CA) rather than
>    self-signed certificates.


1. Log in as `mz_system`, using `external_login_password_mz_system` as the
   password.

1. Create new users and log out.

   In general, other than the initial login to create new users for new
   deployments, avoid using `mz_system` since `mz_system` also used by the
   Materialize Operator for upgrades and maintenance tasks.

   For more information on authentication and authorization for Self-Managed
   Materialize, see:

   - [Authentication](/security/self-managed/authentication/)
   - [Access Control](/security/self-managed/access-control/)

1. Login as one of the created user.

#### Connect using `psql`

> **Note:** - **If using a public NLB:** Both SQL and Console are available via the
> public NLB. You can connect directly using the NLB's DNS name from anywhere
> on the internet (subject to your `ingress_cidr_blocks` configuration).
> - **If using a private (internal) NLB:** You can connect from inside the same VPC or from networks that are privately connected to it. Alternatively, use Kubernetes port-forwarding for both SQL and Console.



Using the `balancerd_load_balancer_ip` and `external_login_password_mz_system`
from the Terraform output, you can connect to Materialize via
PostgreSQL-compatible clients/drivers, such as `psql`.

1. To connect using `psql`, in the connection string, specify:
   - `mz_system` as the user
   - `balancerd_load_balancer_ip` as the host
   - `6875` as the port:

   ```bash
   psql "postgres://mz_system@$(terraform output -raw balancerd_load_balancer_ip):6875/materialize"
   ```

   When prompted for the password, enter the
   `external_login_password_mz_system` value.

1. Create new users and log out.

   In general, other than the initial login to create new users for new
   deployments, avoid using `mz_system` since `mz_system` also used by the
   Materialize Operator for upgrades and maintenance tasks.

   For more information on authentication and authorization for Self-Managed
   Materialize, see:

   - [Authentication](/security/self-managed/authentication/)
   - [Access Control](/security/self-managed/access-control/)

1. Login as one of the created user.


## Customizing Your Deployment

> **Tip:** To reduce cost in your demo environment, you can tweak VM sizes and database tiers in `main.tf`.


You can customize each Terraform module independently.

- For details on the Terraform modules, see both the [top
level](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
and [Azure
specific](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure) modules.

- For details on recommended instance sizing and configuration, see the [Azure
deployment
guide](/self-managed-deployments/deployment-guidelines/azure-deployment-guidelines/).

> **Note:** Autoscaling: Uses Azure's native cluster autoscaler that integrates directly with Azure Virtual Machine Scale Sets for automated node scaling.


See also:
- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)

## Cleanup


To delete the whole sample infrastructure and deployment (including the
Materialize operator and Materialize instances and data), run from the Terraform
directory:

```bash
terraform destroy
```

When prompted to proceed, type `yes` to confirm the deletion.


## See Also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)


---

## Install on GCP


Materialize provides a set of modular [Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
that can be used to deploy all services required for Materialize to run on Google Cloud.
The module is intended to provide a simple set of examples on how to deploy
Materialize. It can be used as is or modules can be taken from the example and
integrated with existing DevOps tooling.

Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; and a license key.
 The example on this page
deploys a complete Materialize environment on GCP using the modular Terraform
setup from this repository.

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
> - Fork the repo and pin to a specific version; or
> - Use the code as a reference when developing your own deployment.


## What Gets Created

This example provisions the following infrastructure:

### Networking

| Resource | Description |
|----------|-------------|
| VPC Network | Custom VPC with auto-create subnets disabled |
| Subnet | 192.168.0.0/20 primary range with private Google access enabled |
| Secondary Ranges | Pods: 192.168.64.0/18, Services: 192.168.128.0/20 |
| Cloud Router | For NAT and routing configuration |
| Cloud NAT | For outbound internet access from private nodes |
| VPC Peering | Service networking connection for Cloud SQL private access |

### Compute

| Resource | Description |
|----------|-------------|
| GKE Cluster | Regional cluster with Workload Identity enabled |
| Generic Node Pool | e2-standard-8 machines, autoscaling 2-5 nodes, 50GB disk, for general workloads |
| Materialize Node Pool | n2-highmem-8 machines, autoscaling 2-5 nodes, 100GB disk, 1 local SSD, swap enabled, dedicated taints for Materialize workloads |
| Service Account | GKE service account with workload identity binding |

### Database

| Resource | Description |
|----------|-------------|
| Cloud SQL PostgreSQL | Private IP only (no public IP) |
| Tier | db-custom-2-4096 (2 vCPUs, 4GB memory) |
| Database | `materialize` database with UTF8 charset |
| User | `materialize` user with auto-generated password |
| Network | Connected via VPC peering for private access |

### Storage

| Resource | Description |
|----------|-------------|
| Cloud Storage Bucket | Regional bucket for Materialize persistence |
| Access | HMAC keys for S3-compatible access (Workload Identity service account with storage permissions is configured but not currently used by Materialize for GCS access, in future we will remove HMAC keys and support access to GCS either via Workload Identity Federation or via Kubernetes ServiceAccounts that impersonate IAM service accounts) |
| Versioning | Disabled (for testing; enable in production) |

### Kubernetes Add-ons

| Resource | Description |
|----------|-------------|
| cert-manager | Certificate management controller for Kubernetes that automates TLS certificate provisioning and renewal |
| Self-signed ClusterIssuer | Provides self-signed TLS certificates for Materialize instance internal communication (balancerd, console). Used by the Materialize instance for secure inter-component communication. |

### Materialize

| Resource | Description |
|----------|-------------|
| Operator | Materialize Kubernetes operator in the `materialize` namespace |
| Instance | Single Materialize instance in the `materialize-environment` namespace |
| Load Balancers | GCP Load Balancers for access to Materialize
| Port | Description |
| --- | --- |
| 6875 | For SQL connections to the database |
| 6876 | For HTTP(S) connections to the database |
| 8080 | For HTTP(S) connections to Materialize Console |
 |

## Prerequisites

### GCP Account Requirements

A Google account with permission to:
- Enable Google Cloud APIs/services on for your project.
- Create:
  - GKE clusters
  - Cloud SQL instances
  - Cloud Storage buckets
  - VPC networks and networking resources
  - Service accounts and IAM bindings

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [gcloud CLI](https://cloud.google.com/sdk/docs/install)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)
- [kubectl gke plugin](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#install_plugin)

### License Key


| License key type | Deployment type | Action |
| --- | --- | --- |
| Community | New deployments | <p>To get a license key:</p> <ul> <li>If you have a Cloud account, visit the <a href="https://console.materialize.com/license/" ><strong>License</strong> page in the Materialize Console</a>.</li> <li>If you do not have a Cloud account, visit <a href="https://materialize.com/self-managed/community-license/" >https://materialize.com/self-managed/community-license/</a>.</li> </ul> |
| Community | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |
| Enterprise | New deployments | Visit <a href="https://materialize.com/self-managed/enterprise-license/" >https://materialize.com/self-managed/enterprise-license/</a> to purchase an Enterprise license. |
| Enterprise | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |


## Getting started: Simple example

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
> - Fork the repo and pin to a specific version; or
> - Use the code as a reference when developing your own deployment.


### Step 1: Set Up the Environment

1. Open a terminal window.

1. Clone the Materialize Terraform repository and go to the
   `gcp/examples/simple` directory.

   ```bash
   git clone https://github.com/MaterializeInc/materialize-terraform-self-managed.git
   cd materialize-terraform-self-managed/gcp/examples/simple
   ```

1. Authenticate to GCP with your user account.

   ```bash
   gcloud auth login
   ```

1. Find the list of GCP projects:

   ```bash
   gcloud projects list
   ```

1. Set your active GCP project, substitute with your `<PROJECT_ID>`.

   ```bash
   gcloud config set project <PROJECT_ID>
   ```

1. Enable the following APIs for your project:

   ```bash
   gcloud services enable container.googleapis.com               # For creating Kubernetes clusters
   gcloud services enable compute.googleapis.com                 # For creating GKE nodes and other compute resources
   gcloud services enable sqladmin.googleapis.com                # For creating databases
   gcloud services enable cloudresourcemanager.googleapis.com    # For managing GCP resources
   gcloud services enable servicenetworking.googleapis.com       # For private network connections
   gcloud services enable iamcredentials.googleapis.com          # For security and authentication
   gcloud services enable iam.googleapis.com                     # For managing IAM service accounts and policies
   gcloud services enable storage.googleapis.com                 # For Cloud Storage buckets
   ```

1. Authenticate application default credentials for Terraform

   ```bash
   gcloud auth application-default login
   ```

### Step 2: Configure Terraform Variables

1. Create a `terraform.tfvars` file and specify the following variables:

   | Variable      | Description                 |
   | -----------   | ----------------------------|
   | `project_id`  | Set to your GCP project ID. |
   | `name_prefix` | Set a prefix for all resource names (e.g., `simple-demo`) as well as your release name for the Operator |
   | `region`      | Set the GCP region for the deployment (e.g., `us-central1`).  |
   | `license_key` | Set to your Materialize license key.     |
   | `labels`      | Set to the labels to apply to resources. |

   ```bash
   project_id  = "my-gcp-project"
   name_prefix = "simple-demo"
   region      = "us-central1"
   license_key = "your-materialize-license-key"
   labels = {
     environment = "demo"
     created_by  = "terraform"
   }
   # internal_load_balancer = false   # default = true (internal load balancer). You can set to false = public load balancer.
   # ingress_cidr_blocks = ["x.x.x.x/n", ...]
   # k8s_apiserver_authorized_networks  = ["x.x.x.x/n", ...]
   ```

   <p><strong>Optional variables</strong>:</p>
   <ul>
   <li><code>internal_load_balancer</code>: Flag that determines whether the load balancer
   is internal (default) or public.</li>
   <li><code>ingress_cidr_blocks</code>: List of CIDR blocks allowed to reach the load
   balancer if the load balancer is public (<code>internal_load_balancer: false</code>).
   If unset, defaults to <code>[&quot;0.0.0.0/0&quot;]</code> (i.e., <red><strong>all</strong></red> IPv4
   addresses on the internet). <strong>Only applied when the load balancer is public</strong>.</li>
   <li><code>k8s_apiserver_authorized_networks</code>: List of CIDR
   blocks allowed to access your cluster endpoint. If unset, defaults to
   <code>[&quot;0.0.0.0/0&quot;]</code> (<red><strong>all</strong></red> IPv4 addresses on the internet).</li>
   </ul>
   > **Note:** Refer to your organization's security practices to set these values accordingly.

### Step 3: Apply the Terraform

1. Initialize the Terraform directory to download the required providers
   and modules:

   ```bash
   terraform init
   ```

1. Apply the Terraform configuration to create the infrastructure.

   ```bash
   terraform apply
   ```

   If you are satisfied with the planned changes, type `yes` when prompted
   to proceed.

1. From the output, you will need the following field(s) to connect:
   - `console_load_balancer_ip` for the Materialize Console
   - `balancerd_load_balancer_ip` to connect PostgreSQL-compatible
     clients/drivers.
   - `external_login_password_mz_system`.

   ```bash
   terraform output -raw <field_name>
   ```

   > **Tip:** Your shell may show an ending marker (such as `%`) because the
>    output did not end with a newline. Do not include the marker when using the value.


1. Configure `kubectl` to connect to your GKE cluster, replacing:

   - `<your-gke-cluster-name>` with your cluster name; i.e., the
     `gke_cluster_name` in the Terraform output. For the sample example, your
     cluster name has the form `<name_prefix>-gke`; e.g., `simple-demo-gke`

   - `<your-region>` with your cluster location; i.e., the
     `gke_cluster_location` in the Terraform output. Your
     region can also be found in your `terraform.tfvars` file.

   - `<your-project-id>` with your GCP project ID.

   ```bash
   # gcloud container clusters get-credentials <your-gke-cluster-name> --region <your-region> --project <your-project-id>
   gcloud container clusters get-credentials $(terraform output -raw gke_cluster_name) \
    --region $(terraform output -raw gke_cluster_location) \
    --project <your-project-id>
   ```

### Step 4. Optional. Verify the status of your deployment

1. Check the status of your deployment:
   **Operator:**
   To check the status of the Materialize operator, which runs in the `materialize` namespace:
   ```bash
   kubectl -n materialize get all
   ```

   **Materialize instance:**
   To check the status of the Materialize instance, which runs in the `materialize-environment` namespace:
   ```bash
   kubectl -n materialize-environment get all
   ```


   <p>If you run into an error during deployment, refer to the
   <a href="/self-managed-deployments/troubleshooting/" >Troubleshooting</a>.</p>

### Step 5: Connect to Materialize

You can connect to Materialize via the Materialize Console or
PostgreSQL-compatible tools/drivers using the following ports:


| Port | Description |
| --- | --- |
| 6875 | For SQL connections to the database |
| 6876 | For HTTP(S) connections to the database |
| 8080 | For HTTP(S) connections to Materialize Console |


#### Connect using the Materialize Console

> **Note:** - **If using a public NLB:** Both SQL and Console are available via the
> public NLB. You can connect directly using the NLB's DNS name from anywhere
> on the internet (subject to your `ingress_cidr_blocks` configuration).
> - **If using a private (internal) NLB:** You can connect from inside the same VPC or from networks that are privately connected to it. Alternatively, use Kubernetes port-forwarding for both SQL and Console.



Using the `console_load_balancer_ip`  and `external_login_password_mz_system`
from the Terraform output, you can connect to Materialize via the Materialize
Console.

1. To connect to the Materialize Console, open a browser to
   `https://<console_load_balancer_ip>:8080`, substituting your
   `<console_load_balancer_ip>`.

   From the terminal, you can type:

   ```sh
   open "https://$(terraform output -raw console_load_balancer_ip):8080/materialize"
   ```

   > **Tip:** The example uses a self-signed ClusterIssuer. As such, you may encounter a
>    warning with regards to the certificate. In production, run with
>    certificates from an official Certificate Authority (CA) rather than
>    self-signed certificates.


1. Log in as `mz_system`, using `external_login_password_mz_system` as the
   password.

1. Create new users and log out.

   In general, other than the initial login to create new users for new
   deployments, avoid using `mz_system` since `mz_system` also used by the
   Materialize Operator for upgrades and maintenance tasks.

   For more information on authentication and authorization for Self-Managed
   Materialize, see:

   - [Authentication](/security/self-managed/authentication/)
   - [Access Control](/security/self-managed/access-control/)

1. Login as one of the created user.

#### Connect using `psql`

> **Note:** - **If using a public NLB:** Both SQL and Console are available via the
> public NLB. You can connect directly using the NLB's DNS name from anywhere
> on the internet (subject to your `ingress_cidr_blocks` configuration).
> - **If using a private (internal) NLB:** You can connect from inside the same VPC or from networks that are privately connected to it. Alternatively, use Kubernetes port-forwarding for both SQL and Console.



Using the `balancerd_load_balancer_ip` and `external_login_password_mz_system`
from the Terraform output, you can connect to Materialize via
PostgreSQL-compatible clients/drivers, such as `psql`:

1. To connect using `psql`, in the connection string, specify:
   - `mz_system` as the user
   - `balancerd_load_balancer_ip` as the host
   - `6875` as the port:

   ```bash
   psql "postgres://mz_system@$(terraform output -raw balancerd_load_balancer_ip):6875/materialize"
   ```

   When prompted for the password, enter the
   `external_login_password_mz_system` value.

1. Create new users and log out.

   In general, other than the initial login to create new users for new
   deployments, avoid using `mz_system` since `mz_system` also used by the
   Materialize Operator for upgrades and maintenance tasks.

   For more information on authentication and authorization for Self-Managed
   Materialize, see:

   - [Authentication](/security/self-managed/authentication/)
   - [Access Control](/security/self-managed/access-control/)

1. Login as one of the created user.

## Customizing Your Deployment

> **Tip:** To reduce cost in your demo environment, you can tweak machine types and database tiers in `main.tf`.


You can customize each module independently.

- For details on the Terraform modules, see both the [top
level](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
and [GCP
specific](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/gcp)
modules.

- For details on recommended instance sizing and configuration, see the [GCP
deployment
guide](/self-managed-deployments/deployment-guidelines/gcp-deployment-guidelines/).

> **Note:** **GCP Storage Authentication Limitation:** Materialize currently only supports HMAC key authentication for GCS access (S3-compatible API). While the modules configure both HMAC keys and Workload Identity, Materialize uses HMAC keys for actual storage access.


See also:
- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)

## Cleanup


To delete the whole sample infrastructure and deployment (including the
Materialize operator and Materialize instances and data), run from the Terraform
directory:

```bash
terraform destroy
```

When prompted to proceed, type `yes` to confirm the deletion.


## See Also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
