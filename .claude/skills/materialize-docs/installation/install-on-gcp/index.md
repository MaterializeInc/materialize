---
audience: developer
canonical_url: https://materialize.com/docs/installation/install-on-gcp/
complexity: advanced
description: '<!-- Unresolved shortcode: {{% self-managed/materialize-components-sentence
  %... -->'
doc_type: reference
keywords:
- 'Warning:'
- CREATE SERVICE
- Install on GCP (via Terraform)
- CREATE A
- 'Tip:'
- 'Note:'
- SHOW THE
product_area: Deployment
status: stable
title: Install on GCP (via Terraform)
---

# Install on GCP (via Terraform)

## Purpose
<!-- Unresolved shortcode: {{% self-managed/materialize-components-sentence %... -->

If you need to understand the syntax and options for this command, you're in the right place.


<!-- Unresolved shortcode: {{% self-managed/materialize-components-sentence %... -->

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

> **Warning:** 

> **Note:** Terraform configurations may vary based on your environment.


## Prerequisites

This section covers prerequisites.

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

> **Tip:** 

Using `gcloud` to install `kubectl` will also install the needed plugins.
Otherwise, you will need to manually install the `gke-gcloud-auth-plugin` for
`kubectl`.


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

<!-- Dynamic table: self_managed/license_key - see original docs -->

## A. Configure GCP project and service account

1. Open a Terminal window.

1. Initialize the gcloud CLI (`gcloud init`) to specify the GCP project you want
   to use. For details, see the [Initializing the gcloud CLI
   documentation](https://cloud.google.com/sdk/docs/initializing#initialize_the).

   > **Tip:** 
   You do not need to configure a default Compute Region and Zone as you will
   specify the region.
   

1. Enable the following services for your GCP project, if not already enabled:

   ```bash
   gcloud services enable container.googleapis.com        # For creating Kubernetes clusters
   gcloud services enable sqladmin.googleapis.com         # For creating databases
   gcloud services enable cloudresourcemanager.googleapis.com # For managing GCP resources
   gcloud services enable servicenetworking.googleapis.com  # For private network connections
   gcloud services enable iamcredentials.googleapis.com     # For security and authentication
   ```text

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
      ```text

   1. Find your service account email for your GCP project

      ```bash
      gcloud iam service-accounts list --project $PROJECT_ID
      ```text

   1. Enter your service account email.

      ```bash
      read -s SERVICE_ACCOUNT
      ```text

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
      ```text

1. For the service account, authenticate to allow Terraform to
   interact with your GCP project. For details, see [Terraform: Google Cloud
   Provider Configuration
   reference](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#authentication).

   For example, if using [User Application Default
   Credentials](https://cloud.google.com/sdk/gcloud/reference/auth/application-default),
   you can run the following command:

   ```bash
   gcloud auth application-default login
   ```text

   > **Tip:** 
   If using `GOOGLE_APPLICATION_CREDENTIALS`, use absolute path to your key file.
   

## B. Set up GCP Kubernetes environment and install Materialize

> **Warning:** 

> **Note:** Terraform configurations may vary based on your environment.


#### Deployed components

[Materialize on GCP Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) deploys
a sample infrastructure on GCP (region `us-central1`) with the following
components:

<!-- Dynamic table: self_managed/gcp_terraform_deployed_components - see original docs -->

> **Tip:** 


#### Releases


<!-- Dynamic table: self_managed/gcp_terraform_versions - see original docs -->


<!-- Unresolved shortcode: {{% self-managed/versions/step-clone-google-terraf... -->

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-google-materialize/examples/simple
   ```text

   > **Tip:** 
   
   

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
   ```text

   > **Tip:** 

   

   

1. Initialize the terraform directory.

    ```bash
    terraform init
    ```text

1. Run terraform plan and review the changes to be made.

    ```bash
    terraform plan
    ```text

1. If you are satisfied with the changes, apply.

    ```bash
    terraform apply
    ```text

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
   ```text

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
   ```text

   Alternatively, you can use the following command to get the cluster name and
   region from the Terraform output and the project ID from the environment
   variable set earlier.

   ```bash
   gcloud container clusters get-credentials $(terraform output -json gke_cluster | jq -r .name) \
    --region $(terraform output -json gke_cluster | jq -r .location) --project $PROJECT_ID
   ```text

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl cluster-info
   ```text

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

1. By default, the example Terraform installs the Materialize Operator and,
   starting in v0.3.0, a `cert-manager`. Verify the
   installation and check the status:

   
   #### Materialize Operator


   Verify the installation and check the status:

   ```shell
   kubectl get all -n materialize
   ```text

   Wait for the components to be in the `Running` state:

   ```none
   NAME                                                              READY       STATUS    RESTARTS   AGE
   pod/materialize-mz-simple-materialize-operator-74d8f549d6-lkjjf   1/1         Running   0          36m

   NAME                                                         READY       UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/materialize-mz-simple-materialize-operator   1/1         1            1           36m

   NAME                                                                        DESIRED   CURRENT   READY   AGE
   replicaset.apps/materialize-mz-simple-materialize-operator-74d8f549d6       1         1         1       36m
   ```json

   
   #### cert-manager (Starting in version 0.3.0)


   Verify the installation and check the status:

   ```shell
   kubectl get all -n cert-manager
   ```text
   Wait for the components to be in the `Running` state:
   ```text
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
   ```json

   
   

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
   ```text

   - **Starting in v26.0**, Self-Managed Materialize requires a license key. To
     get your license key:
     <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- Dynamic table - see original docs --> --> -->

   - **Starting in v0.3.0**, the Materialize on GCP Terraform module also
     deploys, by default:

     - [Load balancers](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) for Materialize instances (i.e., the [`create_load_balancer`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) flag defaults to `true`). The load balancers, by default, are configured  to be internal (i.e., the [`internal_load_balancer`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) flag defaults to `true`).

     - A self-signed `ClusterIssuer`. The `ClusterIssuer` is deployed  after the
     `cert-manager` is deployed and running.

   - **Starting in v0.4.3**, you can specify addition configuration options via
     `environmentd_extra_args`.

   > **Tip:** 
   <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

   See [Materialize on GCP releases](/installation/appendix-terraforms/#materialize-on-gcp-terraform-module) for notable changes.
   

1. Run `terraform plan` with both `.tfvars` files and review the changes to be
   made.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars
   ```text

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```text
   Plan: 9 to add, 1 to change, 0 to destroy.
   ```text

1. If you are satisfied with the changes, apply.

    ```bash
    terraform apply -var-file=terraform.tfvars -var-file=mz_instances.tfvars
    ```text

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
   ```text

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```text

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

   ```text

   If you run into an error during deployment, refer to the
   [Troubleshooting](/installation/troubleshooting/).

1. Open the Materialize Console in your browser:

   

   #### Via Network Load Balancer


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

   

   #### Via port forwarding


   <!-- Unresolved shortcode: {{% self-managed/port-forwarding-handling console=... -->

   
   


   > **Tip:** 

   <!-- Unresolved shortcode: {{% self-managed/troubleshoot-console-mz_catalog_s... -->

   

## Next steps

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

## Cleanup

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

## See also

- [Troubleshooting](/installation/troubleshooting/)
- [Materialize Operator Configuration](/installation/configuration/)
- [Appendix: Google deployment guidelines](/installation/install-on-gcp/appendix-deployment-guidelines/)
- [Installation](/installation/)


---

## Appendix: GCP deployment guidelines


This section covers appendix: gcp deployment guidelines.

## Recommended instance types

As a general guideline, we recommend:

- Processor Type: ARM-based CPU

- Sizing: 2:1 disk-to-RAM ratio with spill-to-disk enabled.

When operating on GCP in production, we recommend the following machine types
that support local SSD attachment:

| Series | Examples   |
| ------ | ---------- |
| [N2 high-memory series] | `n2-highmem-16` or `n2-highmem-32` with local NVMe SSDs |
| [N2D  high-memory series] | `n2d-highmem-16` or `n2d-highmem-32` with local NVMe SSDs |

To maintain the recommended 2:1 disk-to-RAM ratio for your machine type, see
[Number of local SSDs](#number-of-local-ssds) to determine the number of local
SSDs
([`disk_support_config.local_ssd_count`](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/README.md#input_disk_support_config))
to use.

See also [Locally attached NVMe storage](#locally-attached-nvme-storage).

## Number of local SSDs

Each local NVMe SSD in GCP provides 375GB of storage. Use the appropriate number
of local SSDs
([`disk_support_config.local_ssd_count`](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/README.md#input_disk_support_config))
to ensure your total disk space is at least twice the amount of RAM in your
machine type for optimal Materialize performance.

> **Note:** 

Your machine type may only supports predefined number of local SSDs. For instance, `n2d-highmem-32` allows only the following number of local
SSDs: `4`,`8`,`16`, or `24`. To determine the valid number of Local SSDs to attach for your machine type, see the [GCP
documentation](https://cloud.google.com/compute/docs/disks/local-ssd#lssd_disk_options).


For example, the following table provides a minimum local SSD count to ensure
the 2:1 disk-to-RAM ratio. Your actual
count will depend on the [your machine
type](https://cloud.google.com/compute/docs/disks/local-ssd#lssd_disk_options).

| Machine Type    | RAM     | Required Disk | Minimum Local SSD Count | Total SSD Storage |
|-----------------|---------|---------------|-----------------------------|-------------------|
| `n2-highmem-8`  | `64GB`  | `128GB`       | 1                           | `375GB`           |
| `n2-highmem-16` | `128GB` | `256GB`       | 1                           | `375GB`           |
| `n2-highmem-32` | `256GB` | `512GB`       | 2                           | `750GB`           |
| `n2-highmem-64` | `512GB` | `1024GB`      | 3                           | `1125GB`          |
| `n2-highmem-80` | `640GB` | `1280GB`      | 4                           | `1500GB`          |

[N2 high-memory series]: https://cloud.google.com/compute/docs/general-purpose-machines#n2-high-mem

[N2D high-memory series]: https://cloud.google.com/compute/docs/general-purpose-machines#n2d_machine_types

[enables spill-to-disk]: https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#disk-support-for-materialize-on-gcp

## Locally-attached NVMe storage

For optimal performance, Materialize requires fast, locally-attached NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. Network-attached storage (like EBS
volumes) can significantly degrade performance and is not supported.

### Swap support

Starting in v0.6.1 of Materialize on Google Cloud PRovider (GCP) Terraform,
disk support (using swap on NVMe instance storage) may be enabled for
Materialize. With this change, the Terraform:

- Creates a node group for Materialize.
- Configures NVMe instance store volumes as swap using a daemonset.
- Enables swap at the Kubelet.

For swap support, the following configuration options are available:

- [`swap_enabled`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_swap_enabled)

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#v061).

## CPU affinity

It is strongly recommended to enable the Kubernetes `static` [CPU management policy](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy).
This ensures that each worker thread of Materialize is given exclusively access to a vCPU. Our benchmarks have shown this
to substantially improve the performance of compute-bound workloads.

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## Storage bucket versioning

Starting in v0.3.1 of Materialize on GCP Terraform, storage bucket versioning is
disabled (i.e.,
[`storage_bucket_versioning`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_storage_bucket_versioning)
is set to `false` by default) to facilitate cleanup of resources during testing.
When running in production, versioning should be turned on with a sufficient TTL
([`storage_bucket_version_ttl`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_storage_bucket_version_ttl))
to meet any data-recovery requirements.

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)


---

## Appendix: Required configuration


This section covers appendix: required configuration.

## Required variables

The following variables are required when using the [Materialize on Google Cloud
Provider Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize).

<!-- Dynamic table: self_managed/gcp_required_variables - see original docs -->

For a list of all variables, see the
[README.md](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#inputs)
or the [`variables.tf` file](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/variables.tf).

## Required providers and data source declaration

To use [Materialize on Google Cloud Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) v0.2.0+,
you need to declare:

- The following providers:

  ```hcl
  provider "google" {
    project = var.project_id
    region  = var.region
    # Specify additional Google provider configuration as needed
  }

  # Required for GKE authentication
  provider "kubernetes" {
    host                   = "https://${module.gke.cluster_endpoint}"
    token                  = data.google_client_config.current.access_token
    cluster_ca_certificate = base64decode(module.gke.cluster_ca_certificate)
  }

  provider "helm" {
    kubernetes {
      host                   = "https://${module.gke.cluster_endpoint}"
      token                  = data.google_client_config.current.access_token
      cluster_ca_certificate = base64decode(module.gke.cluster_ca_certificate)
    }
  }
  ```text

- The following data source:

  ```hcl
  data "google_client_config" "current" {}
  ```text


---

## Upgrade on GCP (Terraform)


> **Disambiguation:** - To upgrade to `v26.0` using Materialize-provided Terraforms, upgrade your
Terraform version to `v0.6.1` or higher, [GCP Terraform v0.6.1 Upgrade
Notes](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#v061)
.

- To upgrade to `v26.0` if <red>**not**</red> using a Materialize-provided
Terraforms, you must prepare your nodes by adding the required labels. For
detailed instructions, see [Prepare for swap and upgrade to
v26.0](/installation/upgrade-to-swap/).

To upgrade your Materialize instances, first choose a new operator version and upgrade the Materialize operator. Then, upgrade your Materialize instances to the same version. The following tutorial upgrades your
Materialize deployment running on GCP Google Kubernetes Engine (GKE).

The tutorial assumes you have installed Materialize on GCP Google Kubernetes
Engine (GKE) using the instructions on [Install on
GCP](/installation/install-on-gcp/) (either from the examples/simple directory
or the root).

## Version compatibility

> **Important:** 

When performing major version upgrades, you can upgrade only one major version
at a time. For example, upgrades from **v26**.1.0 to **v27**.2.0 is permitted
but **v26**.1.0 to **v28**.0.0 is not. Skipping major versions or downgrading is
not supported. To upgrade from v25.2 to v26.0, you must [upgrade first to v25.2.16+](../self-managed/v25.2/release-notes/#v25216).


#### Materialize on GCP Terraform Releases


<!-- Dynamic table: self_managed/gcp_terraform_versions - see original docs -->


## Prerequisites

> **Important:** 

The following procedure performs a rolling upgrade, where both the old and new
Materialize instances are running before the the old instance are removed.
When performing a rolling upgrade, ensure you have enough resources to support
having both the old and new Materialize instances running.


### Google cloud project

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

> **Tip:** 

Using `gcloud` to install `kubectl` will also install the needed plugins.
Otherwise, you will need to manually install the `gke-gcloud-auth-plugin` for
`kubectl`.


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

*Optional*. `jq` is used to parse the EKS cluster name and region from the
Terraform outputs. Alternatively, you can manually specify the name and region.
If you want to use `jq` and do not have `jq` installed, install.

### License key

Starting in v26.0, Materialize requires a license key. If your existing
deployment does not have a license key configured, contact [Materialize support](../support/).


## Procedure

This section covers procedure.

### A. Setup GCP service account and authenticate

1. Open a Terminal window.

1. Initialize the gcloud CLI (`gcloud init`) to specify the GCP project you want
   to use. For details, see the [Initializing the gcloud CLI
   documentation](https://cloud.google.com/sdk/docs/initializing#initialize_the).

   > **Tip:** 
   You do not need to configure a default Compute Region and Zone as you will
   specify the region.
   

1. To the service account that will be used to perform the upgrade,
   grant the following IAM roles (if the account does not have them already):

   - `roles/editor`
   - `roles/iam.serviceAccountAdmin`
   - `roles/storage.admin`

   1. Enter your GCP project ID.

      ```bash
      read -s PROJECT_ID
      ```text

   1. Find your service account email for your GCP project

      ```bash
      gcloud iam service-accounts list --project $PROJECT_ID
      ```text

   1. Enter your service account email.

      ```bash
      read -s SERVICE_ACCOUNT
      ```text

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
      --role="roles/storage.admin"
      ```text

1. For the service account, authenticate to allow Terraform to interact with
   your GCP project. For details, see [Terraform: Google Cloud Provider
   Configuration
   reference](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#authentication).

   For example, if using [User Application Default
   Credentials](https://cloud.google.com/sdk/gcloud/reference/auth/application-default),
   you can run the following command:

   ```bash
   gcloud auth application-default login
   ```text

   > **Tip:** 
   If using `GOOGLE_APPLICATION_CREDENTIALS`, use absolute path to your key file.
   

### B. Upgrade Materialize operator and instances

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-google-materialize/examples/simple
   ```text

1. Optional. You may need to update your fork of the Terraform module to
   upgrade.

   > **Tip:** 
   <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

   See [Materialize on GCP releases](/installation/appendix-terraforms/#materialize-on-gcp-terraform-module) for notable changes.

   

1. Configure `kubectl` to connect to your EKS cluster, specifying:

   - `<cluster name>`. Your cluster name has the form `<your prefix>-gke`; e.g.,
     `mz-simple-gke`.

   - `<region>`. By default, the example Terraform module uses the `us-central1`
     region.

   - `<project>`. Your GCP project ID.

   ```bash
   gcloud container clusters get-credentials <cluster-name>  \
    --region <region> \
    --project <project>
   ```text

   Alternatively, you can use the following command to get the cluster name and
   region from the Terraform output and the project ID from the environment
   variable set earlier.

   ```bash
   gcloud container clusters get-credentials $(terraform output -json gke_cluster | jq -r .name) \
    --region $(terraform output -json gke_cluster | jq -r .location) --project $PROJECT_ID
   ```text

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl cluster-info
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

<!-- Unresolved shortcode: {{% self-managed/versions/upgrade/upgrade-steps-cl... -->