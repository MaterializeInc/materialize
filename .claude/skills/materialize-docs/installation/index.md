# Install/Upgrade (Self-Managed)

Installation and upgrade guides for Self-Managed Materialize.



{{< include-md file="shared-content/self-managed/install-landing-page.md" >}}

## Upgrade

{{< include-md file="shared-content/self-managed/general-rules-for-upgrades.md" >}}

### Upgrade guides

The following upgrade guides are available:

|               | Notes  |
| ------------- | -------|
| [Upgrade on kind](/installation/install-on-local-kind/upgrade-on-local-kind/) |
| [Upgrade on AWS](/installation/install-on-aws/upgrade-on-aws/) | Uses Materialize provided Terraform |
| [Upgrade on Azure Kubernetes Service (AKS)](/installation/install-on-azure/upgrade-on-azure/) | Uses Materialize provided Terraform |
| [Upgrade on Google Kubernetes Engine (GKE)](/installation/install-on-gcp/upgrade-on-gcp/) | Uses Materialize provided Terraform |


### General notes for upgrades

The following provides some general notes for upgrades. For specific examples,
see the [Upgrade guides](#upgrade-guides)


#### Upgrading the Helm Chart and Kubernetes Operator

{{< important >}}

When upgrading Materialize, always upgrade the operator first.

{{</ important >}}

The Materialize Kubernetes operator is deployed via Helm and can be updated through standard Helm upgrade commands.

```shell
helm upgrade my-materialize-operator materialize/misc/helm-charts/operator
```

If you have custom values, make sure to include your values file:

```shell
helm upgrade my-materialize-operator materialize/misc/helm-charts/operator -f my-values.yaml
```

#### Upgrading Materialize Instances

In order to minimize unexpected downtime and avoid connection drops at critical
periods for your application, changes are not immediately and automatically
rolled out by the Operator. Instead, the upgrade process involves two steps:
- First, staging spec changes to the Materialize custom resource.
- Second, applying the changes via a `requestRollout`.

When upgrading your Materialize instances, you'll first want to update the `environmentdImageRef` field in the Materialize custom resource spec.

##### Updating the `environmentdImageRef`
To find a compatible version with your currently deployed Materialize operator, check the `appVersion` in the Helm repository.

```shell
helm list -n materialize
```

Using the returned version, we can construct an image ref.
We always recommend using the official Materialize image repository
`docker.io/materialize/environmentd`.

```
environmentdImageRef: docker.io/materialize/environmentd:v26.0.0
```

The following is an example of how to patch the version.
```shell
# For version updates, first update the image reference
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:v26.0.0\"}}"
```

##### Applying the changes via `requestRollout`

To apply changes and kick off the Materialize instance upgrade, you must update the `requestRollout` field in the Materialize custom resource spec to a new UUID.
Be sure to consult the [Rollout Configurations](#rollout-configuration) to ensure you've selected the correct rollout behavior.
```shell
# Then trigger the rollout with a new UUID
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```


It is possible to combine both operations in a single command if preferred:

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:v26.0.0\", \"requestRollout\": \"$(uuidgen)\"}}"
```

##### Using YAML Definition

Alternatively, you can update your Materialize custom resource definition directly:

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.0.0 # Update version as needed
  requestRollout: 22222222-2222-2222-2222-222222222222    # Generate new UUID
  forceRollout: 33333333-3333-3333-3333-333333333333      # Optional: for forced rollouts
  inPlaceRollout: false                                   # In Place rollout is deprecated and ignored. Please use rolloutStrategy
  rolloutStrategy: WaitUntilReady                         # The mechanism to use when rolling out the new version. Can be WaitUntilReady or ImmediatelyPromoteCausingDowntime
  backendSecretName: materialize-backend
```

Apply the updated definition:

```shell
kubectl apply -f materialize.yaml
```

#### Rollout Configuration

##### Forced Rollouts

If you need to force a rollout even when there are no changes to the instance:

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\", \"forceRollout\": \"$(uuidgen)\"}}"
```

##### Rollout Strategies

The behavior of the new version rollout follows your `rolloutStrategy` setting:

`WaitUntilReady` (default):

New instances are created and all dataflows are determined to be ready before cutover and terminating the old version, temporarily requiring twice the resources during the transition.

`ImmediatelyPromoteCausingDowntime`:

Tears down the prior version before creating and promoting the new version. This causes downtime equal to the duration it takes for dataflows to hydrate, but does not require additional resources.

##### In Place Rollout

The `inPlaceRollout` setting has been deprecated and will be ignored.

### Verifying the Upgrade

After initiating the rollout, you can monitor the status field of the Materialize custom resource to check on the upgrade.

```shell
# Watch the status of your Materialize environment
kubectl get materialize -n materialize-environment -w

# Check the logs of the operator
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```
### Version Specific Upgrade Notes

#### Upgrading to `v26.1` and later versions
{{< include-md file="shared-content/self-managed/upgrade-notes/v26.1.md" >}}

#### Upgrading to `v26.0`

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.0.md" >}}

#### Upgrading between minor versions less than `v26`
 - Prior to `v26`, you must upgrade at most one minor version at a time. For
   example, upgrading from `v25.1.5` to `v25.2.16` is permitted.

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [FAQ](/installation/faq/)




---

## Appendix: Cluster sizes


## Default Cluster Sizes

{{% self-managed/materialize-cluster-sizes %}}

## Custom Cluster Sizes

When installing the Materialize Helm chart, you can override the [default
cluster sizes and resource allocations](#default-cluster-sizes). These
cluster sizes are used for both internal clusters, such as the `system_cluster`,
as well as user clusters.

{{< tip >}}

In general, you should not have to override the defaults. At minimum, we
recommend that you keep the 25-200cc cluster sizes.

{{</ tip >}}

```yaml
operator:
  clusters:
    sizes:
      <size>:
        workers: <int>
        scale: 1                  # Generally, should be set to 1.
        cpu_exclusive: <bool>
        cpu_limit: <float>         # e.g., 6
        credits_per_hour: "0.0"    # N/A for self-managed.
        disk_limit: <string>       # e.g., "93150MiB"
        memory_limit: <string>     # e.g., "46575MiB"
        selectors: <map>           # k8s label selectors
        # ex: kubernetes.io/arch: amd64
```

{{< yaml-table data="best_practices/sizing_recommendation" >}}

{{< note >}}

If you have modified the default cluster size configurations, you can query the
[`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table for the specific resource allocations.

{{< /note >}}




---

## Appendix: Materialize CRD Field Descriptions


{{% self-managed/materialize-crd-descriptions %}}




---

## Appendix: Prepare for swap and upgrade to v26.0


{{< annotation type="Disambiguation" >}}

This page outlines the general steps for upgrading from v25.2 to v26.0 if you
are <red>**not**</red> using Materialize provided Terraforms.

If you are using Materialize-provided Terraforms, `v0.6.1` and higher of the
Terraforms handle the preparation for you.  If using Materialize-provided
Terraforms, upgrade your Terraform version to `v0.6.1` or higher and follow the
Upgrade notes:

- {{< include-md
file="shared-content/self-managed/aws-terraform-v0.6.1-upgrade-notes.md" >}}.

- {{< include-md
file="shared-content/self-managed/gcp-terraform-v0.6.1-upgrade-notes.md" >}}.

- {{< include-md
file="shared-content/self-managed/azure-terraform-v0.6.1-upgrade-notes.md" >}}.

See also [General notes for upgrades](/installation/#upgrade).

{{< /annotation >}}

{{< include-md file="shared-content/self-managed/prepare-nodes-and-upgrade.md" >}}




---

## Appendix: Terraforms


To help you get started, Materialize provides some template Terraforms.

{{< important >}}
These modules are intended for evaluation/demonstration purposes and for serving
as a template when building your own production deployment. The modules should
not be directly relied upon for production deployments: **future releases of the
modules will contain breaking changes.** Instead, to use as a starting point for
your own production deployment, either:

- Fork the repo and pin to a specific version; or

- Use the code as a reference when developing your own deployment.

{{</ important >}}

{{< yaml-table data="self_managed/terraform_list" >}}

## Releases

### Materialize on AWS Terraform module

{{< yaml-table data="self_managed/aws_terraform_versions" >}}

{{% self-managed/aws-terraform-upgrade-notes %}}

See also [Upgrade Notes](
https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#upgrade-notes)
for release-specific upgrade notes.


### Materialize on Azure Terraform module

{{< yaml-table data="self_managed/azure_terraform_versions" >}}

{{% self-managed/azure-terraform-upgrade-notes %}}

### Materialize on GCP Terraform module

{{< yaml-table data="self_managed/gcp_terraform_versions" >}}

{{% self-managed/gcp-terraform-upgrade-notes %}}




---

## FAQ: Self-managed installation


## How long do license keys last?

Community edition license keys are valid for one year. Enterprise license
keys will vary based on the terms of your contract.

## How do I get a license key?

{{< yaml-table data="self_managed/license_key" >}}

## How do I add a license key to an existing installation?

The license key should be configured in the Kubernetes Secret resource
created during the installation process. To configure a license key in an
existing installation, run:

```bash
kubectl -n materialize-environment patch secret materialize-backend -p '{"stringData":{"license_key":"<your license key goes here>"}}' --type=merge
```




---

## Install locally on kind (via Helm)


{{% self-managed/materialize-components-sentence %}}

The following tutorial uses a local [`kind`](https://kind.sigs.k8s.io/) cluster
and deploys the following components:

- Materialize Operator using Helm into your local `kind` cluster.
- MinIO object storage as the blob storage for your Materialize.
- PostgreSQL database as the metadata database for your Materialize.
- Materialize as a containerized application into your local `kind` cluster.

{{< important >}}

This tutorial is for local evaluation/testing purposes only.

- The tutorial uses sample configuration files that are for evaluation/testing
  purposes only.
- The tutorial uses a Kubernetes metrics server with TLS disabled. In practice,
  refer to your organization's official security practices.

{{< /important >}}

## Prerequisites

### kind

Install [`kind`](https://kind.sigs.k8s.io/docs/user/quick-start/).

### Docker

Install [`Docker`](https://docs.docker.com/get-started/get-docker/).

#### Docker resource requirements

{{% self-managed/local-resource-requirements %}}

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

{{< yaml-table data="self_managed/license_key" >}}

## Installation

1. Start Docker if it is not already running.

   {{% self-managed/local-resource-requirements %}}

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

   {{% self-managed/versions/curl-sample-files-local-install %}}

1. Add your license key:

   a. To get your license key:

      {{% yaml-table data="self_managed/license_key" %}}

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

   {{% self-managed/versions/step-install-helm-version-local-kind-install %}}

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

      {{< important >}}

      This tutorial is for local evaluation/testing purposes only. For simplicity,
      the tutorial uses a Kubernetes metrics server with TLS disabled. In practice,
      refer to your organization's official security practices.

      {{< /important >}}

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

   {{% self-managed/port-forwarding-handling-local %}}

      {{< tip >}}

      {{% self-managed/troubleshoot-console-mz_catalog_server_blurb %}}

      {{< /tip >}}

## Next steps

{{% self-managed/next-steps %}}

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

## Install on AWS (via Terraform)


{{% self-managed/materialize-components-sentence %}}

The tutorial deploys Materialize to AWS Elastic Kubernetes Service (EKS) with a
PostgreSQL RDS database as the metadata database and AWS S3 for blob storage.
The tutorial uses [Materialize on AWS Terraform
module](https://github.com/MaterializeInc/terraform-aws-materialize) to:

- Set up the AWS Kubernetes environment.
- Call
   [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
   module to deploy Materialize Operator and Materialize instances to that EKS
   cluster.

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

{{% self-managed/aws-recommended-instances %}}

See [Appendix: AWS Deployment
guidelines](/installation/install-on-aws/appendix-deployment-guidelines/) for
more information.

## Prerequisites

### Terraform

If you don't have Terraform installed, [install
Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### AWS CLI

If you do not have the AWS CLI installed, install. For details, see the [AWS
documentation](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).

### kubectl

If you do not have `kubectl`, install. See the [Amazon EKS: install `kubectl`
documentation](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html)
for details.

### Helm 3.2.0+

If you do not have Helm 3.2.0+, install. For details, see the [Helm
documentation](https://helm.sh/docs/intro/install/).

### License key

Starting in v26.0, Self-Managed Materialize requires a license key.

{{< yaml-table data="self_managed/license_key" >}}

## Set up AWS Kubernetes environment and install Materialize

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< self-managed/tutorial-disclaimer >}}

{{< /warning >}}

{{< tabs >}}

{{< tab "Deployed components" >}}

[Materialize on AWS Terraform
module](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/README.md)
deploys a sample infrastructure on AWS (region `us-east-1`) with the following
components:

{{< yaml-table data="self_managed/aws_terraform_deployed_components" >}}

{{< tip >}}
{{% self-managed/aws-terraform-configs %}}
{{< /tip >}}

{{</ tab >}}
{{< tab "Releases" >}}

{{< yaml-table data="self_managed/aws_terraform_versions" >}}

{{</ tab >}}
{{</ tabs >}}

1. Open a Terminal window.

1. Configure AWS CLI with your AWS credentials. For details, see the [AWS
   documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

{{% self-managed/versions/step-clone-aws-terraform-repo %}}

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-aws-materialize/examples/simple
   ```

   {{< tip >}}
   {{< self-managed/aws-terraform-configs >}}
   {{< /tip >}}

1. Create a `terraform.tfvars` file (you can copy from the
   `terraform.tfvars.example` file) and specify the following variables:

   | Variable          | Description |
   |--------------------|-------------|
   | `namespace`       | A namespace (e.g., `my-demo`) that will be used to form part of the prefix for your AWS resources. <br> **Requirements:** <br> - Maximum of 12 characters <br> - Must start with a lowercase letter <br> - Must be lowercase alphanumeric and hyphens only |
   | `environment`     | An environment name (e.g., `dev`, `test`) that will be used to form part of the prefix for your AWS resources. <br> **Requirements:** <br> - Maximum of 8 characters <br> - Must be lowercase alphanumeric only |


   ```bash
   # The namespace and environment variables are used to construct the names of   the resources
   # e.g. ${namespace}-${environment}-storage, ${namespace}-${environment}-db   etc.

   namespace = "enter-namespace"   // maximum 12 characters, start with a   letter, contain lowercase alphanumeric and hyphens only (e.g. my-demo)
   environment = "enter-environment" // maximum 8 characters, lowercase   alphanumeric only (e.g., dev, test)
   ```

   {{< tip >}}
   {{< self-managed/aws-terraform-configs >}}
   {{< /tip >}}

1. Initialize the terraform directory.

    ```bash
    terraform init
    ```

1. Use terraform plan to review the changes to be made.

    ```bash
    terraform plan
    ```

1. If you are satisfied with the changes, apply.

   ```bash
   terraform apply
   ```

   To approve the changes and apply, enter `yes`.

   <a name="terraform-output"></a>

   Upon successful completion, various fields and their values are output:

   ```none
   Apply complete! Resources: 89 added, 0 changed, 0 destroyed.

   Outputs:

   cluster_certificate_authority_data = <sensitive>
   database_endpoint = "my-demo-dev-db.abcdefg8dsto.us-east-1.rds.amazonaws.com:5432"
   eks_cluster_endpoint = "https://0123456789A00BCD000E11BE12345A01.gr7.us-east-1.eks.amazonaws.com"
   eks_cluster_name = "my-demo-dev-eks"
   materialize_s3_role_arn = "arn:aws:iam::000111222333:role/my-demo-dev-mz-role"
   metadata_backend_url = <sensitive>
   nlb_details = []
   oidc_provider_arn = "arn:aws:iam::000111222333:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/7D14BCA3A7AA896A836782D96A24F958"
   persist_backend_url = "s3://my-demo-dev-storage-f2def2a9/dev:serviceaccount:materialize-environment:12345678-1234-1234-1234-12345678912"
   s3_bucket_name = "my-demo-dev-storage-f2def2a9"
   vpc_id = "vpc-0abc000bed1d111bd"
   ```

1. Note your specific values for the following fields:

   - `eks_cluster_name` (Used to configure `kubectl`)

1. Configure `kubectl` to connect to your EKS cluster, replacing:

   - `<your-eks-cluster-name>` with the name of your EKS cluster. Your cluster
       name has the form `{namespace}-{environment}-eks`; e.g.,
       `my-demo-dev-eks`.

   - `<your-region>` with the region of your EKS cluster. The
     simple example uses `us-east-1`.

   ```bash
   aws eks update-kubeconfig --name <your-eks-cluster-name> --region <your-region>
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

1. By default, the example Terraform installs the Materialize Operator and,
   starting in v0.4.0, a `cert-manager`. Verify the installation and check the
   status:

   {{< tabs >}}
   {{< tab "Materialize Operator" >}}

   Verify the installation and check the status:

   ```shell
   kubectl get all -n materialize
   ```

   Wait for the components to be in the `Running` state:

   ```none
   NAME                                                           READY  STATUS    RESTARTS   AGE
   pod/my-demo-dev-materialize-operator-84ff4b4648-brjhl   1/1     Running  0          12s

   NAME                                                      READY  UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/my-demo-dev-materialize-operator   1/1     1           1           12s

   NAME                                                             DESIRED    CURRENT   READY   AGE
   replicaset.apps/my-demo-dev-materialize-operator-84ff4b4648   1        1         1       12s
   ```

   {{</ tab >}}
   {{< tab "cert-manager (Starting in version 0.4.0)" >}}

   Verify the installation and check the status:

   ```shell
   kubectl get all -n cert-manager
   ```
   Wait for the components to be in the `Running` state:
   ```
   NAME                                           READY   STATUS   RESTARTS     AGE
   pod/cert-manager-cainjector-686546c9f7-v9hwp   1/1     Running  0            4m20s
   pod/cert-manager-d6746cf45-cdmb5               1/1     Running  0            4m20s
   pod/cert-manager-webhook-5f79cd6f4b-rcjbq      1/1     Running  0            4m20s
   NAME                              TYPE        CLUSTER-IP      EXTERNAL-IP     PORT(S)            AGE
   service/cert-manager              ClusterIP   172.20.2.136    <none>          9402/TCP           4m20s
   service/cert-manager-cainjector   ClusterIP   172.20.154.137  <none>          9402/TCP           4m20s
   service/cert-manager-webhook      ClusterIP   172.20.63.217   <none>          443/TCP,9402/TCP   4m20s
   NAME                                      READY   UP-TO-DATE  AVAILABLE     AGE
   deployment.apps/cert-manager              1/1     1           1             4m20s
   deployment.apps/cert-manager-cainjector   1/1     1           1             4m20s
   deployment.apps/cert-manager-webhook      1/1     1           1             4m20s
   NAME                                                 DESIRED   CURRENT    READY   AGE
   replicaset.apps/cert-manager-cainjector-686546c9f7   1         1          1       4m20s
   replicaset.apps/cert-manager-d6746cf45               1         1          1       4m20s
   replicaset.apps/cert-manager-webhook-5f79cd6f4b      1         1         1
   4m20s
   ```

   {{</ tab >}}
   {{</ tabs >}}

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting) guide.

1. Once the Materialize operator is deployed and running, you can deploy the
   Materialize instances. To deploy Materialize instances, create  a
   `mz_instances.tfvars` file with the [Materialize instance
   configuration](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances).

   For example, the following specifies the configuration for a `demo`:

   ```bash
   cat <<EOF > mz_instances.tfvars

   materialize_instances = [
       {
         name           = "demo"
         namespace      = "materialize-environment"
         database_name  = "demo_db"
         cpu_request    = "1"
         memory_request = "2Gi"
         memory_limit   = "2Gi"
         license_key    = "<ENTER YOUR LICENSE KEY HERE>"
       }
   ]
   EOF
   ```

   - **Starting in v26.0**, Self-Managed Materialize requires a license key. To
     get your license key:
     {{% yaml-table data="self_managed/license_key" %}}

   - **Starting in v0.3.0**, the Materialize on AWS Terraform module also
   deploys, by default, Network Load Balancers (NLBs) for each Materialize
   instance (i.e., the
   [`create_nlb`](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`).  The NLBs, by default, are configured to be
    internal (i.e., the
    [`internal_nlb`](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`). See [`materialize_instances`](
   https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_materialize_instances)
   for the Materialize instance configuration options.

   - **Starting in v0.4.0**, a self-signed `ClusterIssuer` is deployed by
   default. The `ClusterIssuer` is deployed on subsequent after the
   `cert-manager` is running.

   - **Starting in v0.4.6**, you can specify addition configuration options via
     `environmentd_extra_args`.

   {{< tip >}}
   {{% self-managed/aws-terraform-upgrade-notes %}}

   See [Materialize on AWS releases](/installation/appendix-terraforms/#materialize-on-aws-terraform-module) for notable changes.
   {{</ tip >}}

1. Run `terraform plan` with both `.tfvars` files and review the changes to be
   made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```
   Plan: 17 to add, 1 to change, 0 to destroy.
   ```

1. If you are satisfied with the changes, apply.

   ```bash
   terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   To approve the changes and apply, enter `yes`.

   Upon successful completion, you should see output with a summary similar to
   the following:

   <a name="aws-terrafrom-output"></a>

   ```bash
   Apply complete! Resources: 17 added, 1 changed, 0 destroyed.

   Outputs:

   cluster_certificate_authority_data = <sensitive>
   database_endpoint = "my-demo-dev-db.abcdefg8dsto.us-east-1.rds.amazonaws.com:5432"
   eks_cluster_endpoint = "https://0123456789A00BCD000E11BE12345A01.gr7.us-east-1.eks.amazonaws.com"
   eks_cluster_name = "my-demo-dev-eks"
   materialize_s3_role_arn = "arn:aws:iam::000111222333:role/my-demo-dev-mz-role"
   metadata_backend_url = <sensitive>
   nlb_details = [
     "demo" = {
       "arn" = "arn:aws:elasticloadbalancing:us-east-1:000111222333:loadbalancer/net/my-demo-dev/aeae3d936afebcfe"
       "dns_name" = "my-demo-dev-aeae3d936afebcfe.elb.us-east-1.amazonaws.com"
     }
   ]
   oidc_provider_arn = "arn:aws:iam::000111222333:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/7D14BCA3A7AA896A836782D96A24F958"
   persist_backend_url = "s3://my-demo-dev-storage-f2def2a9/dev:serviceaccount:materialize-environment:12345678-1234-1234-1234-12345678912"
   s3_bucket_name = "my-demo-dev-storage-f2def2a9"
   vpc_id = "vpc-0abc000bed1d111bd"
   ```

   The Network Load Balancer (NLB) details `nlb_details` are available when
   running the Terraform module v0.3.0+.

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be in the `Running` state.

   ```none
   NAME                                             READY   STATUS      RESTARTS      AGE
   pod/create-db-demo-db-6swk7                      0/1     Completed   0             33s
   pod/mzutd2fbabf5-balancerd-6c9755c498-28kcw      1/1     Running     0             11s
   pod/mzutd2fbabf5-cluster-s2-replica-s1-gen-1-0   1/1     Running     0             11s
   pod/mzutd2fbabf5-cluster-u1-replica-u1-gen-1-0   1/1     Running     0             11s
   pod/mzutd2fbabf5-console-57f94b4588-6lg2x        1/1     Running     0             4s
   pod/mzutd2fbabf5-console-57f94b4588-v65lk        1/1     Running     0             4s
   pod/mzutd2fbabf5-environmentd-1-0                1/1     Running     0             16s

   NAME                                               TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                                        AGE
   service/mzutd2fbabf5-balancerd                     ClusterIP   None            <none>        6876/TCP,6875/TCP                              11s
   service/mzutd2fbabf5-cluster-s2-replica-s1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   12s
   service/mzutd2fbabf5-cluster-u1-replica-u1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   12s
   service/mzutd2fbabf5-console                       ClusterIP   None            <none>        8080/TCP                                       4s
   service/mzutd2fbabf5-environmentd                  ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            11s
   service/mzutd2fbabf5-environmentd-1                ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            16s
   service/mzutd2fbabf5-persist-pubsub-1              ClusterIP   None            <none>        6879/TCP                                       16s

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mzutd2fbabf5-balancerd   1/1     1            1           11s
   deployment.apps/mzutd2fbabf5-console     2/2     2            2           4s

   NAME                                                DESIRED   CURRENT   READY      AGE
   replicaset.apps/mzutd2fbabf5-balancerd-6c9755c498   1         1         1          11s
   replicaset.apps/mzutd2fbabf5-console-57f94b4588     2         2         2          4s

   NAME                                                        READY   AGE
   statefulset.apps/mzutd2fbabf5-cluster-s2-replica-s1-gen-1   1/1     12s
   statefulset.apps/mzutd2fbabf5-cluster-u1-replica-u1-gen-1   1/1     11s
   statefulset.apps/mzutd2fbabf5-environmentd-1                1/1     16s

   NAME                          STATUS     COMPLETIONS   DURATION   AGE
   job.batch/create-db-demo-db   Complete   1/1           11s        33s
   ```

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Open the Materialize Console in your browser:

   {{< tabs >}}

   {{< tab  "Via Network Load Balancer" >}}

   Starting in v0.3.0, for each Materialize instance, Materialize on AWS
   Terraform module also deploys AWS Network Load Balancers (by default,
   internal) with the following listeners, including a listener on port 8080 for
   the Materialize Console:

   | Port | Description |
   | ---- | ------------|
   | 6875 | For SQL connections to the database |
   | 6876 | For HTTP(S) connections to the database |
   | **8080** | **For HTTP(S) connections to Materialize Console** |

   The Network Load Balancer (NLB) details are found in the `nlb_details`  in
   the [Terraform output](#aws-terrafrom-output).

   The example uses a self-signed ClusterIssuer. As such, you may encounter a
   warning with regards to the certificate. In production, run with certificates
   from an official Certificate Authority (CA) rather than self-signed
   certificates.

   {{</ tab >}}

   {{< tab "Via port forwarding" >}}

   {{% self-managed/port-forwarding-handling %}}

   {{</ tab>}}
   {{</ tabs >}}

   {{< tip >}}

   {{% self-managed/troubleshoot-console-mz_catalog_server_blurb %}}

   {{< /tip >}}

## Next steps

{{% self-managed/next-steps %}}

## Cleanup

{{% self-managed/cleanup-cloud %}}

  {{< tip >}}

  - To delete your S3 bucket, you may need to empty the S3 bucket first. If the
    `terraform destroy` command is unable to delete the S3 bucket and does not
    progress beyond "Still destroying...", empty the S3 bucket first and rerun
    the `terraform destroy` command.

  - Upon successful destroy, you may receive some informational messages with
    regards to CustomResourceDefinition(CRD). You may safely ignore these
    messages as your whole deployment has been destroyed, including the CRDs.

  {{</ tip >}}

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Appendix: AWS Deployment
guidelines](/installation/install-on-aws/appendix-deployment-guidelines/)
- [Installation](/installation/)




---

## Install on Azure (via Terraform)


{{% self-managed/materialize-components-sentence blobstorage="blob storage; specifically **block** blob storage on Azure" %}}

The tutorial deploys Materialize to Azure Kubernetes Service (AKS) with a
PostgreSQL database as the metadata database and Azure premium block blob
storage for blob storage. The tutorial uses [Materialize on Azure Terraform
modules](https://github.com/MaterializeInc/terraform-azurerm-materialize) to:

- Set up the Azure Kubernetes environment
- Call
   [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
   module to deploy Materialize Operator and Materialize instances to that AKS
   cluster

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< self-managed/tutorial-disclaimer >}}

{{< /warning >}}

## Prerequisites

### Azure subscription

If you do not have an Azure subscription to use for this tutorial, create one.

### Azure CLI

If you don't have Azure CLI installed, [install Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).

### Terraform

If you don't have Terraform installed, [install Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### kubectl

If you do not have `kubectl`, install `kubectl`.

### Python (v3.12+) and pip

If you don't have Python (v3.12 or greater) installed, install it. See
[Python.org](https://www.python.org/downloads/). If `pip` is not included with
your version of Python, install it.

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, install. For details, see to
the [Helm documentation](https://helm.sh/docs/intro/install/).

### jq (Optional)

*Optional*. `jq` is used to parse the AKS cluster name and region from the
Terraform outputs. Alternatively, you can manually specify the name and region.
If you want to use `jq` and do not have `jq` installed, install.

### License key

Starting in v26.0, Self-Managed Materialize requires a license key.

{{< yaml-table data="self_managed/license_key" >}}

## A. Authenticate with Azure

1. Open a Terminal window.

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

1. Set `ARM_SUBSCRIPTION_ID` to the subscription ID.

    ```bash
    export ARM_SUBSCRIPTION_ID=<subscription-id>
    ```

## B. Set up Azure Kubernetes environment and install Materialize

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

{{< tabs >}}

{{< tab "Deployed components" >}}

[Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize) for
deploys a sample infrastructure on Azure with the following components:

{{< yaml-table data="self_managed/azure_terraform_deployed_components" >}}

{{< tip >}}

{{% self-managed/azure-terraform-configs %}}

{{< /tip >}}

{{</ tab >}}
{{< tab "Releases" >}}

{{< yaml-table data="self_managed/azure_terraform_versions" >}}

{{</ tab >}}
{{</ tabs >}}

1. Open a Terminal window.

{{% self-managed/versions/step-clone-azure-terraform-repo %}}

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-azurerm-materialize/examples/simple
   ```

   {{< tip >}}

   {{% self-managed/azure-terraform-configs %}}

   {{< /tip >}}


1. Optional. Create a virtual environment, specifying a path for the new virtual
   environment:

    ```bash
    python3 -m venv <path to the new virtual environment>

    ```

   Activate the virtual environment:
    ```bash
    source <path to the new virtual environment>/bin/activate
    ```

1. Install the required packages.

    ```bash
    pip install -r requirements.txt
    ```

1. Create a `terraform.tfvars` file (you can copy from the
   `terraform.tfvars.example` file) and specify:

   - The prefix for the resources. Prefix has a maximum of 12 characters and
     contains only alphanumeric characters and hyphens; e.g., `mydemo`.

   -  The location for the AKS cluster.

   ```bash
   prefix="enter-prefix"  //  maximum 12 characters, containing only alphanumeric characters and hyphens; e.g. mydemo
   location="eastus2"
   ```

   {{< tip >}}

   {{% self-managed/azure-terraform-configs %}}

   {{< /tip >}}

1. Initialize the terraform directory.

    ```bash
    terraform init
    ```

1. Use terraform plan to review the changes to be made.

    ```bash
    terraform plan
    ```

1. If you are satisfied with the changes, apply.

    ```bash
    terraform apply
    ```

   To approve the changes and apply, enter `yes`.

   Upon successful completion, various fields and their values are output:

   ```bash
   Apply complete! Resources: 33 added, 0 changed, 0 destroyed.

   Outputs:

   aks_cluster = <sensitive>
   connection_strings = <sensitive>
   kube_config = <sensitive>
   load_balancer_details = {}
   resource_group_name = "mydemo-rg"
   ```

1. Configure `kubectl` to connect to your cluster:

   - `<cluster_name>`. Your cluster name has the form `<your prefix>-aks`; e.g.,
     `mz-simple-aks`.

   - `<resource_group_name>`, as specified in the output.

   ```bash
   az aks get-credentials --resource-group <resource_group_name> --name <cluster_name>
   ```

   Alternatively, you can use the following command to get the cluster name and
   resource group name from the Terraform output:

   ```bash
   az aks get-credentials --resource-group $(terraform output -raw resource_group_name) --name $(terraform output -json aks_cluster | jq -r '.name')
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl cluster-info
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

1. By default, the example Terraform installs the Materialize Operator and,
   starting in v0.3.0, a `cert-manager`. Verify the
   installation and check the status:

   {{< tabs >}}
   {{< tab "Materialize Operator" >}}

   Verify the installation and check the status:

   ```shell
   kubectl get all -n materialize
   ```

   Wait for the components to be in the `Running` state:

   ```none
   NAME                                                              READY       STATUS    RESTARTS   AGE
   pod/materialize-mydemo-materialize-operator-74d8f549d6-lkjjf      1/1         Running   0          36m

   NAME                                                         READY       UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/materialize-mydemo-materialize-operator      1/1         1            1           36m

   NAME                                                                        DESIRED   CURRENT   READY   AGE
   replicaset.apps/materialize-mydemo-materialize-operator-74d8f549d6          1         1         1       36m
   ```

   {{</ tab >}}

   {{< tab "cert-manager (Starting in version 0.3.0)" >}}

   Verify the installation and check the status:

   ```shell
   kubectl get all -n cert-manager
   ```
   Wait for the components to be in the `Running` state:
   ```
   NAME                                           READY   STATUS    RESTARTS   AGE
   pod/cert-manager-8576d99cc8-xqxbc              1/1     Running   0          4m22s
   pod/cert-manager-cainjector-664b5878d6-wc4tz   1/1     Running   0          4m22s
   pod/cert-manager-webhook-6ddb7bd6c5-vrm2p      1/1     Running   0          4m22s

   NAME                              TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)            AGE
   service/cert-manager              ClusterIP   10.1.227.230   <none>        9402/TCP           4m22s
   service/cert-manager-cainjector   ClusterIP   10.1.222.156   <none>        9402/TCP           4m22s
   service/cert-manager-webhook      ClusterIP   10.1.84.207    <none>        443/TCP,9402/TCP   4m22s

   NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/cert-manager              1/1     1            1           4m23s
   deployment.apps/cert-manager-cainjector   1/1     1            1           4m23s
   deployment.apps/cert-manager-webhook      1/1     1            1           4m23s

   NAME                                                 DESIRED   CURRENT   READY   AGE
   replicaset.apps/cert-manager-8576d99cc8              1         1         1       4m23s
   replicaset.apps/cert-manager-cainjector-664b5878d6   1         1         1       4m23s
   replicaset.apps/cert-manager-webhook-6ddb7bd6c5      1         1         1       4m23s
   ```

   {{</ tab >}}
   {{</ tabs >}}

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Once the Materialize operator is deployed and running, you can deploy the
   Materialize instances. To deploy Materialize instances, create a
   `mz_instances.tfvars` file with the Materialize instance configuration.

   For example, the following specifies the configuration for a `demo` instance.

   ```bash
   cat <<EOF > mz_instances.tfvars

   materialize_instances = [
       {
         name           = "demo"
         namespace      = "materialize-environment"
         database_name  = "demo_db"
         cpu_request    = "1"
         memory_request = "2Gi"
         memory_limit   = "2Gi"
         license_key    = "<ENTER YOUR LICENSE KEY HERE>"
       }
   ]
   EOF
   ```

   - **Starting in v26.0**, Self-Managed Materialize requires a license key. To
     get your license key:
     {{% yaml-table data="self_managed/license_key" %}}

   - **Starting in v0.3.0**, the Materialize on Azure Terraform module also
     deploys, by default, a self-signed `ClusterIssuer`. The `ClusterIssuer` is
     deployed after the `cert-manager` is deployed and running.

   - **Starting in v0.3.1**, the Materialize on Azure Terraform module also
   deploys, by default, [Load
   balancers](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_materialize_instances)
   for Materialize instances (i.e., the
   [`create_load_balancer`](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`). The load balancers, by default, are configured to
   be internal (i.e., the
   [`internal_load_balancer`](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_materialize_instances)
   flag defaults to `true`).

   - **Starting in v0.4.3**, you can specify addition configuration options via
     `environmentd_extra_args`.

   {{< tip >}}
   {{% self-managed/azure-terraform-upgrade-notes %}}

   See [Materialize on Azure releases](/installation/appendix-terraforms/#materialize-on-azure-terraform-module) for notable changes.
   {{</ tip >}}

1. Run `terraform plan` with both `.tfvars` files and review the changes to be
   made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```
   Plan: 9 to add, 1 to change, 0 to destroy.
   ```

1. If you are satisfied with the changes, apply.

   ```bash
   terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   To approve the changes and apply, enter `yes`.

   <a name="azure-terraform-output"></a>

   Upon successful completion, you should see output with a summary similar to the following:

   ```bash
   Apply complete! Resources: 9 added, 1 changed, 0 destroyed.

   Outputs:

   aks_cluster = <sensitive>
   connection_strings = <sensitive>
   kube_config = <sensitive>
   load_balancer_details = {
      "demo" = {
         "balancerd_load_balancer_ip" = "192.0.2.10"
         "console_load_balancer_ip" = "192.0.2.254"
      }
   }
   resource_group_name = "mydemo-rg"
   ```

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be ready and in the `Running` state.

   ```none
   NAME                                             READY   STATUS      RESTARTS   AGE
   pod/db-demo-db-l6ss8                             0/1     Completed   0          2m21s
   pod/mz62lr3yltj8-balancerd-6d5dd6d4cf-r9nf4      1/1     Running     0          111s
   pod/mz62lr3yltj8-cluster-s2-replica-s1-gen-1-0   1/1     Running     0          114s
   pod/mz62lr3yltj8-cluster-u1-replica-u1-gen-1-0   1/1     Running     0          114s
   pod/mz62lr3yltj8-console-bfc797745-6nlwv         1/1     Running     0          96s
   pod/mz62lr3yltj8-console-bfc797745-tk9vm         1/1     Running     0          96s
   pod/mz62lr3yltj8-environmentd-1-0                1/1     Running     0          2m4s

   NAME                                               TYPE           CLUSTER-IP     EXTERNAL-IP       PORT(S)                                        AGE
   service/mz62lr3yltj8-balancerd                     ClusterIP      None           <none>            6876/TCP,6875/TCP                              111s
   service/mz62lr3yltj8-balancerd-lb                  LoadBalancer   10.1.201.77    192.0.2.10        6875:30890/TCP,6876:31750/TCP                  2m4s
   service/mz62lr3yltj8-cluster-s2-replica-s1-gen-1   ClusterIP      None           <none>            2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   114s
   service/mz62lr3yltj8-cluster-u1-replica-u1-gen-1   ClusterIP      None           <none>            2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   114s
   service/mz62lr3yltj8-console                       ClusterIP      None           <none>            8080/TCP                                       96s
   service/mz62lr3yltj8-console-lb                    LoadBalancer   10.1.130.212   192.0.2.254       8080:30379/TCP                                 2m4s
   service/mz62lr3yltj8-environmentd                  ClusterIP      None           <none>            6875/TCP,6876/TCP,6877/TCP,6878/TCP            111s
   service/mz62lr3yltj8-environmentd-1                ClusterIP      None           <none>            6875/TCP,6876/TCP,6877/TCP,6878/TCP            2m5s
   service/mz62lr3yltj8-persist-pubsub-1              ClusterIP      None           <none>            6879/TCP                                       2m4s

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mz62lr3yltj8-balancerd   1/1     1            1           111s
   deployment.apps/mz62lr3yltj8-console     2/2     2            2           96s

   NAME                                                DESIRED   CURRENT   READY   AGE
   replicaset.apps/mz62lr3yltj8-balancerd-6d5dd6d4cf   1         1         1       111s
   replicaset.apps/mz62lr3yltj8-console-bfc797745      2         2         2       96s

   NAME                                                        READY   AGE
   statefulset.apps/mz62lr3yltj8-cluster-s2-replica-s1-gen-1   1/1     114s
   statefulset.apps/mz62lr3yltj8-cluster-u1-replica-u1-gen-1   1/1     114s
   statefulset.apps/mz62lr3yltj8-environmentd-1                1/1     2m4s

   NAME                   STATUS     COMPLETIONS   DURATION   AGE
   job.batch/db-demo-db   Complete   1/1           10s        2m21s

   ```

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Open the Materialize Console in your browser:


   {{< tabs >}}

   {{< tab  "Via Network Load Balancer" >}}

   Starting in v0.3.1, for each Materialize instance, Materialize on Azure
   Terraform module also deploys load balancers (by default, internal) with the
   following listeners, including a listener on port 8080 for the Materialize
   Console:

   | Port | Description |
   | ---- | ------------|
   | 6875 | For SQL connections to the database |
   | 6876 | For HTTP(S) connections to the database |
   | **8080** | **For HTTP(S) connections to Materialize Console** |

   The load balancer details are found in the `load_balancer_details`  in
   the [Terraform output](#azure-terraform-output).

   The example uses a self-signed ClusterIssuer. As such, you may encounter a
   warning with regards to the certificate. In production, run with certificates
   from an official Certificate Authority (CA) rather than self-signed
   certificates.

   {{</ tab >}}

   {{< tab "Via port forwarding" >}}

   {{% self-managed/port-forwarding-handling %}}

   {{</ tab>}}
   {{</ tabs >}}

   {{< tip >}}

   {{% self-managed/troubleshoot-console-mz_catalog_server_blurb %}}

   {{< /tip >}}

## Next steps

{{% self-managed/next-steps %}}

## Cleanup

{{% self-managed/cleanup-cloud %}}

  {{< tip>}}

  If the `terraform destroy` command is unable to delete the subnet because it
  is in use, you can rerun the `terraform destroy` command.

  {{</ tip >}}

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Appendix: Azure deployment guidelines](/installation/install-on-azure/
  appendix-deployment-guidelines)
- [Installation](/installation/)




---

## Install on GCP (via Terraform)


{{% self-managed/materialize-components-sentence %}}

This tutorial deploys Materialize to GCP Google Kubernetes Engine (GKE) cluster
with a Cloud SQL PostgreSQL database as the metadata database and Cloud Storage
bucket for blob storage. Specifically, the tutorial uses [Materialize on Google
Cloud Provider Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) to:

- Set up the GCP environment.

- Call
   [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
   module to deploy Materialize Operator and Materialize instances to the GKE
   cluster.

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< self-managed/tutorial-disclaimer >}}

{{< /warning >}}

## Prerequisites

### Google cloud provider project

You need a GCP project for which you have a role (such as
`roles/resourcemanager.projectIamAdmin` or `roles/owner`) that includes [
permissions to manage access to the
project](https://cloud.google.com/iam/docs/granting-changing-revoking-access).

### gcloud CLI

If you do not have gcloud CLI, install. For details, see the [Install the gcloud
CLI documentation](https://cloud.google.com/sdk/docs/install).

### Google service account

The tutorial assumes the use of a service account. If you do not have a service
account to use for this tutorial, create a service account. For details, see
[Create service
accounts](https://cloud.google.com/iam/docs/service-accounts-create#creating).

### Terraform

If you do not have Terraform installed, [install
Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### kubectl and plugins

{{< tip >}}

Using `gcloud` to install `kubectl` will also install the needed plugins.
Otherwise, you will need to manually install the `gke-gcloud-auth-plugin` for
`kubectl`.

{{< /tip >}}

- If you do not have `kubectl`, install `kubectl`.  To install, see [Install
  kubectl and configure cluster
  access](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl)
  for details. You will configure `kubectl` to interact with your GKE cluster
  later in the tutorial.

- If you do not have `gke-gcloud-auth-plugin` for `kubectl`, install the
  `gke-gcloud-auth-plugin`. For details, see [Install the
  gke-gcloud-auth-plugin](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#install_plugin).

### Helm 3.2.0+

If you do not have Helm version 3.2.0+ installed, install.  For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### jq (Optional)

*Optional*. `jq` is used to parse the GKE cluster name and region from the
Terraform outputs. Alternatively, you can manually specify the name and region.
If you want to use `jq` and do not have `jq` installed, install.

### License key

Starting in v26.0, Self-Managed Materialize requires a license key.

{{< yaml-table data="self_managed/license_key" >}}

## A. Configure GCP project and service account

1. Open a Terminal window.

1. Initialize the gcloud CLI (`gcloud init`) to specify the GCP project you want
   to use. For details, see the [Initializing the gcloud CLI
   documentation](https://cloud.google.com/sdk/docs/initializing#initialize_the).

   {{< tip >}}
   You do not need to configure a default Compute Region and Zone as you will
   specify the region.
   {{</ tip >}}

1. Enable the following services for your GCP project, if not already enabled:

   ```bash
   gcloud services enable container.googleapis.com        # For creating Kubernetes clusters
   gcloud services enable sqladmin.googleapis.com         # For creating databases
   gcloud services enable cloudresourcemanager.googleapis.com # For managing GCP resources
   gcloud services enable servicenetworking.googleapis.com  # For private network connections
   gcloud services enable iamcredentials.googleapis.com     # For security and authentication
   ```

1. To the service account that will run the Terraform script,
   grant the following IAM roles:

   - `roles/editor`
   - `roles/iam.serviceAccountAdmin`
   - `roles/servicenetworking.networksAdmin`
   - `roles/storage.admin`
   - `roles/container.admin`

   1. Enter your GCP project ID.

      ```bash
      read -s PROJECT_ID
      ```

   1. Find your service account email for your GCP project

      ```bash
      gcloud iam service-accounts list --project $PROJECT_ID
      ```

   1. Enter your service account email.

      ```bash
      read -s SERVICE_ACCOUNT
      ```

   1. Grant the service account the neccessary IAM roles.

      ```bash
      gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT" \
      --role="roles/editor"

      gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT" \
      --role="roles/iam.serviceAccountAdmin"

      gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT" \
      --role="roles/servicenetworking.networksAdmin"

      gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT" \
      --role="roles/storage.admin"

      gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$SERVICE_ACCOUNT" \
      --role="roles/container.admin"
      ```

1. For the service account, authenticate to allow Terraform to
   interact with your GCP project. For details, see [Terraform: Google Cloud
   Provider Configuration
   reference](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#authentication).

   For example, if using [User Application Default
   Credentials](https://cloud.google.com/sdk/gcloud/reference/auth/application-default),
   you can run the following command:

   ```bash
   gcloud auth application-default login
   ```

   {{< tip >}}
   If using `GOOGLE_APPLICATION_CREDENTIALS`, use absolute path to your key file.
   {{</ tip >}}

## B. Set up GCP Kubernetes environment and install Materialize

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

{{< tabs >}}

{{< tab "Deployed components" >}}
[Materialize on GCP Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) deploys
a sample infrastructure on GCP (region `us-central1`) with the following
components:

{{< yaml-table data="self_managed/gcp_terraform_deployed_components" >}}

{{< tip >}}
{{< self-managed/gcp-terraform-configs >}}
{{< /tip >}}
{{</ tab >}}
{{< tab "Releases" >}}

{{< yaml-table data="self_managed/gcp_terraform_versions" >}}

{{</ tab >}}
{{</ tabs >}}

{{% self-managed/versions/step-clone-google-terraform-repo %}}

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-google-materialize/examples/simple
   ```

   {{< tip >}}
   {{< self-managed/gcp-terraform-configs >}}
   {{< /tip >}}

1. Create a `terraform.tfvars` file (you can copy from the
   `terraform.tfvars.example` file) and specify the following variables:

   | **Variable** | **Description** |
   |--------------|-----------------|
   | `project_id` | Your GCP project ID. |
   | `prefix`     | A prefix (e.g., `mz-simple`) for your resources. Prefix has a maximum of 15 characters and contains only alphanumeric characters and dashes. |
   | `region`     | The region for the GKE cluster. |

   ```bash
   project_id = "enter-your-gcp-project-id"
   prefix  = "enter-your-prefix" //  Maximum of 15 characters, contain lowercase alphanumeric and hyphens only (e.g., mz-simple)
   region = "us-central1"
   ```

   {{< tip >}}

   {{< self-managed/gcp-terraform-configs >}}

   {{< /tip >}}

1. Initialize the terraform directory.

    ```bash
    terraform init
    ```

1. Run terraform plan and review the changes to be made.

    ```bash
    terraform plan
    ```

1. If you are satisfied with the changes, apply.

    ```bash
    terraform apply
    ```

   To approve the changes and apply, enter `yes`.

   Upon successful completion, various fields and their values are output:

   ```bash
   Apply complete! Resources: 27 added, 0 changed, 0 destroyed.

   Outputs:

   connection_strings = <sensitive>
   gke_cluster = <sensitive>
   load_balancer_details = {}
   network = {
      "network_id" = "projects/my-project/global/networks/mz-simple-network"
      "network_name" = "mz-simple-network"
      "subnet_name" = "mz-simple-subnet"
   }
   service_accounts = {
      "gke_sa" = "mz-simple-gke-sa@my-project.iam.gserviceaccount.com"
      "materialize_sa" = "mz-simple-materialize-sa@my-project.iam.gserviceaccount.com"
   }
   ```

1. Configure `kubectl` to connect to your GKE cluster, specifying:

   - `<cluster name>`. Your cluster name has the form `<your prefix>-gke`; e.g.,
     `mz-simple-gke`.

   - `<region>`. By default, the example Terraform module uses the `us-central1`
     region.

   - `<project>`. Your GCP project ID.

   ```bash
   gcloud container clusters get-credentials <cluster-name>  \
    --region <region> \
    --project <project>
   ```

   Alternatively, you can use the following command to get the cluster name and
   region from the Terraform output and the project ID from the environment
   variable set earlier.

   ```bash
   gcloud container clusters get-credentials $(terraform output -json gke_cluster | jq -r .name) \
    --region $(terraform output -json gke_cluster | jq -r .location) --project $PROJECT_ID
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl cluster-info
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

1. By default, the example Terraform installs the Materialize Operator and,
   starting in v0.3.0, a `cert-manager`. Verify the
   installation and check the status:

   {{< tabs >}}
   {{< tab "Materialize Operator" >}}

   Verify the installation and check the status:

   ```shell
   kubectl get all -n materialize
   ```

   Wait for the components to be in the `Running` state:

   ```none
   NAME                                                              READY       STATUS    RESTARTS   AGE
   pod/materialize-mz-simple-materialize-operator-74d8f549d6-lkjjf   1/1         Running   0          36m

   NAME                                                         READY       UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/materialize-mz-simple-materialize-operator   1/1         1            1           36m

   NAME                                                                        DESIRED   CURRENT   READY   AGE
   replicaset.apps/materialize-mz-simple-materialize-operator-74d8f549d6       1         1         1       36m
   ```

   {{</ tab >}}
   {{< tab "cert-manager (Starting in version 0.3.0)" >}}

   Verify the installation and check the status:

   ```shell
   kubectl get all -n cert-manager
   ```
   Wait for the components to be in the `Running` state:
   ```
   NAME                                           READY   STATUS    RESTARTS   AGE
   pod/cert-manager-6794b8d569-vt264              1/1     Running   0          22m
   pod/cert-manager-cainjector-7f69cd69f7-7brqw   1/1     Running   0          22m
   pod/cert-manager-webhook-6cc5dccc4b-7tmd4      1/1     Running   0          22m

   NAME                              TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)            AGE
   service/cert-manager              ClusterIP   10.52.3.63     <none>        9402/TCP           22m
   service/cert-manager-cainjector   ClusterIP   10.52.15.171   <none>        9402/TCP           22m
   service/cert-manager-webhook      ClusterIP   10.52.5.148    <none>        443/TCP,9402/TCP   22m

   NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/cert-manager              1/1     1            1           22m
   deployment.apps/cert-manager-cainjector   1/1     1            1           22m
   deployment.apps/cert-manager-webhook      1/1     1            1           22m

   NAME                                                 DESIRED   CURRENT   READY   AGE
   replicaset.apps/cert-manager-6794b8d569              1         1         1       22m
   replicaset.apps/cert-manager-cainjector-7f69cd69f7   1         1         1       22m
   replicaset.apps/cert-manager-webhook-6cc5dccc4b      1         1         1       22m
   ```

   {{</ tab >}}
   {{</ tabs >}}

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Once the Materialize operator is deployed and running, you can deploy the
   Materialize instances. To deploy Materialize instances, create a
   `mz_instances.tfvars` file with the Materialize instance configuration.

   For example, the following specifies the configuration for a `demo` instance.

   ```bash
   cat <<EOF > mz_instances.tfvars

   materialize_instances = [
       {
         name           = "demo"
         namespace      = "materialize-environment"
         database_name  = "demo_db"
         cpu_request    = "1"
         memory_request = "2Gi"
         memory_limit   = "2Gi"
         license_key    = "<ENTER YOUR LICENSE KEY HERE>"
       }
   ]
   EOF
   ```

   - **Starting in v26.0**, Self-Managed Materialize requires a license key. To
     get your license key:
     {{% yaml-table data="self_managed/license_key" %}}

   - **Starting in v0.3.0**, the Materialize on GCP Terraform module also
     deploys, by default:

     - [Load balancers](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) for Materialize instances (i.e., the [`create_load_balancer`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) flag defaults to `true`). The load balancers, by default, are configured  to be internal (i.e., the [`internal_load_balancer`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) flag defaults to `true`).

     - A self-signed `ClusterIssuer`. The `ClusterIssuer` is deployed  after the
     `cert-manager` is deployed and running.

   - **Starting in v0.4.3**, you can specify addition configuration options via
     `environmentd_extra_args`.

   {{< tip >}}
   {{% self-managed/gcp-terraform-upgrade-notes %}}

   See [Materialize on GCP releases](/installation/appendix-terraforms/#materialize-on-gcp-terraform-module) for notable changes.
   {{</ tip >}}

1. Run `terraform plan` with both `.tfvars` files and review the changes to be
   made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```
   Plan: 9 to add, 1 to change, 0 to destroy.
   ```

1. If you are satisfied with the changes, apply.

    ```bash
    terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
    ```

   To approve the changes and apply, enter `yes`.

   <a name="gcp-terraform-output"></a>
   Upon successful completion, you should see output with a summary similar to the following:

   ```bash
   Apply complete! Resources: 9 added, 1 changed, 0 destroyed.

   Outputs:

   connection_strings = <sensitive>
   gke_cluster = <sensitive>
   load_balancer_details = {
      "demo" = {
         "balancerd_load_balancer_ip" = "192.0.2.10"
         "console_load_balancer_ip" = "192.0.2.254"
      }
   }
   network = {
      "network_id" = "projects/my-project/global/networks/mz-simple-network"
      "network_name" = "mz-simple-network"
      "subnet_name" = "mz-simple-subnet"
   }
   service_accounts = {
      "gke_sa" = "mz-simple-gke-sa@my-project.iam.gserviceaccount.com"
      "materialize_sa" = "mz-simple-materialize-sa@my-project.iam.gserviceaccount.com"
   }
   ```

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be in the `Running` state.

   ```none
   NAME                                             READY   STATUS      RESTARTS   AGE
   pod/db-demo-db-wrvhw                             0/1     Completed   0          4m26s
   pod/mzdtwvu4qe4q-balancerd-6989df5c75-mpmqx      1/1     Running     0          3m54s
   pod/mzdtwvu4qe4q-cluster-s2-replica-s1-gen-1-0   1/1     Running     0          3m53s
   pod/mzdtwvu4qe4q-cluster-u1-replica-u1-gen-1-0   1/1     Running     0          3m52s
   pod/mzdtwvu4qe4q-console-7c9bc94bcb-6t7lg        1/1     Running     0          3m41s
   pod/mzdtwvu4qe4q-console-7c9bc94bcb-9x5qq        1/1     Running     0          3m41s
   pod/mzdtwvu4qe4q-environmentd-1-0                1/1     Running     0          4m9s

   NAME                                               TYPE           CLUSTER-IP    EXTERNAL-IP     PORT(S)                                        AGE
   service/mzdtwvu4qe4q-balancerd                     ClusterIP      None          <none>          6876/TCP,6875/TCP                              3m54s
   service/mzdtwvu4qe4q-balancerd-lb                  LoadBalancer   10.52.5.105   192.0.2.10      6875:30844/TCP,6876:32307/TCP                  4m9s
   service/mzdtwvu4qe4q-cluster-s2-replica-s1-gen-1   ClusterIP      None          <none>          2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   3m53s
   service/mzdtwvu4qe4q-cluster-u1-replica-u1-gen-1   ClusterIP      None          <none>          2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   3m52s
   service/mzdtwvu4qe4q-console                       ClusterIP      None          <none>          8080/TCP                                       3m41s
   service/mzdtwvu4qe4q-console-lb                    LoadBalancer   10.52.4.2     192.0.2.254     8080:32193/TCP                                 4m9s
   service/mzdtwvu4qe4q-environmentd                  ClusterIP      None          <none>          6875/TCP,6876/TCP,6877/TCP,6878/TCP            3m54s
   service/mzdtwvu4qe4q-environmentd-1                ClusterIP      None          <none>          6875/TCP,6876/TCP,6877/TCP,6878/TCP            4m9s
   service/mzdtwvu4qe4q-persist-pubsub-1              ClusterIP      None          <none>          6879/TCP                                       4m9s

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mzdtwvu4qe4q-balancerd   1/1     1            1           3m54s
   deployment.apps/mzdtwvu4qe4q-console     2/2     2            2           3m41s

   NAME                                                DESIRED   CURRENT   READY   AGE
   replicaset.apps/mzdtwvu4qe4q-balancerd-6989df5c75   1         1         1       3m54s
   replicaset.apps/mzdtwvu4qe4q-console-7c9bc94bcb     2         2         2       3m41s

   NAME                                                        READY   AGE
   statefulset.apps/mzdtwvu4qe4q-cluster-s2-replica-s1-gen-1   1/1     3m53s
   statefulset.apps/mzdtwvu4qe4q-cluster-u1-replica-u1-gen-1   1/1     3m52s
   statefulset.apps/mzdtwvu4qe4q-environmentd-1                1/1     4m9s

   NAME                   STATUS     COMPLETIONS   DURATION   AGE
   job.batch/db-demo-db   Complete   1/1           12s        4m27s

   ```

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Open the Materialize Console in your browser:

   {{< tabs >}}

   {{< tab  "Via Network Load Balancer" >}}

   Starting in v0.3.0, for each Materialize instance, Materialize on GCP
   Terraform module also deploys load balancers (by default, internal) with the
   following listeners, including a listener on port 8080 for the Materialize
   Console:

   | Port | Description |
   | ---- | ------------|
   | 6875 | For SQL connections to the database |
   | 6876 | For HTTP(S) connections to the database |
   | **8080** | **For HTTP(S) connections to Materialize Console** |

   The load balancer details are found in the `load_balancer_details`  in
   the [Terraform output](#gcp-terraform-output).

   The example uses a self-signed ClusterIssuer. As such, you may encounter a
   warning with regards to the certificate. In production, run with certificates
   from an official Certificate Authority (CA) rather than self-signed
   certificates.

   {{</ tab >}}

   {{< tab "Via port forwarding" >}}

   {{% self-managed/port-forwarding-handling console="console-lb"%}}

   {{</ tab>}}
   {{</ tabs >}}


   {{< tip >}}

   {{% self-managed/troubleshoot-console-mz_catalog_server_blurb %}}

   {{< /tip >}}

## Next steps

{{% self-managed/next-steps %}}

## Cleanup

{{% self-managed/cleanup-cloud %}}

## See also

- [Troubleshooting](/installation/troubleshooting/)
- [Materialize Operator Configuration](/installation/configuration/)
- [Appendix: Google deployment guidelines](/installation/install-on-gcp/appendix-deployment-guidelines/)
- [Installation](/installation/)




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

{{%  self-managed/materialize-operator-chart-parameters-table %}}

## Parameters

{{%  self-managed/materialize-operator-chart-parameters %}}

## See also

- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)




---

## Operational guidelines


## Recommended instance types

- ARM-based CPU
- 1:8 ratio of vCPU to GiB memory (if spill-to-disk is not enabled)
- 1:16 ratio of vCPU to GiB local instance storage (if spill-to-disk is enabled)

See also the specific cloud provider guidance:

- [AWS Deployment
  guidelines](/installation/install-on-aws/appendix-deployment-guidelines/#recommended-instance-types)

- [GCP Deployment
  guidelines](/installation/install-on-gcp/appendix-deployment-guidelines/#recommended-instance-types)

- [Azure Deployment
  guidelines](/installation/install-on-azure/appendix-deployment-guidelines/#recommended-instance-types)

## CPU affinity

It is strongly recommended to enable the Kubernetes `static` [CPU management policy](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy).
This ensures that each worker thread of Materialize is given exclusively access to a vCPU. Our benchmarks have shown this
to substantially improve the performance of compute-bound workloads.

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## Locally-attached NVMe storage

For optimal performance, Materialize requires fast, locally-attached NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. Network-attached storage (like EBS
volumes) can significantly degrade performance and is not supported.

Refer to the specific cloud provider guidelines:

- [AWS Deployment
  guidelines](/installation/install-on-aws/appendix-deployment-guidelines/)

- [GCP Deployment
  guidelines](/installation/install-on-gcp/appendix-deployment-guidelines/)

- [Azure Deployment
  guidelines](/installation/install-on-azure/appendix-deployment-guidelines/)

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)




---

## Self-managed release versions


## V26 releases

{{< yaml-table data="self_managed/self_managed_operator_compatibility" >}}




---

## Troubleshooting


## Troubleshooting Kubernetes

### Materialize operator

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

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)



