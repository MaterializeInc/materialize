# Install on GCP (Legacy Terraform)
Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; and a license key.


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

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
> - Fork the repo and pin to a specific version; or
> - Use the code as a reference when developing your own deployment.
> For simplicity, this tutorial stores various secrets in a file as well as prints
> them to the terminal. In practice, refer to your organization's official
> security and Terraform/infrastructure practices.


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

> **Tip:** Using `gcloud` to install `kubectl` will also install the needed plugins.
> Otherwise, you will need to manually install the `gke-gcloud-auth-plugin` for
> `kubectl`.


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



## A. Configure GCP project and service account

1. Open a Terminal window.

1. Initialize the gcloud CLI (`gcloud init`) to specify the GCP project you want
   to use. For details, see the [Initializing the gcloud CLI
   documentation](https://cloud.google.com/sdk/docs/initializing#initialize_the).

   > **Tip:** You do not need to configure a default Compute Region and Zone as you will
>    specify the region.


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

   > **Tip:** If using `GOOGLE_APPLICATION_CREDENTIALS`, use absolute path to your key file.


## B. Set up GCP Kubernetes environment and install Materialize

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
> - Fork the repo and pin to a specific version; or
> - Use the code as a reference when developing your own deployment.




**Deployed components:**
[Materialize on GCP Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) deploys
a sample infrastructure on GCP (region `us-central1`) with the following
components:


| Component | Version |
| --- | --- |
| Google Kubernetes Engine (GKE) cluster | All |
| Cloud Storage bucket for blob storage | All |
| Cloud SQL PostgreSQL database for metadata storage | All |
| Dedicated VPC | All |
| Service accounts with proper IAM permissions | All |
| Materialize Operator | All |
| Materialize instances (Deployed during subsequent runs after the Operator is running) | All |
| Load balancers for each Materialize instance | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-gcp-terraform-module" >v0.3.0+</a> |
| <code>cert-manager</code> and a self-signed <code>ClusterIssuer</code>. <code>ClusterIssuer</code> is deployed on subsequent runs after the <code>cert-manager</code> is running. | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-gcp-terraform-module" >v0.3.0+</a> |
| OpenEBS and NVMe instance storage to enable spill-to-disk | <a href="/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-gcp-terraform-module" >v0.4.0+</a> |


> **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory, which
> requires minimal user input. For details on the  `examples/simple/`
> infrastructure configuration (such as the node instance type, etc.), see the
> [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/examples/simple/main.tf).
> For more configuration options, you can use the `main.tf` file at the [root of
> the repository](https://github.com/MaterializeInc/terraform-google-materialize/)
> instead. When running with the root `main.tf`, see [GCP required
> configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-gcp/).


**Releases:**


| Terraform version | Notable changes |
| --- | --- |
| <a href="https://github.com/MaterializeInc/terraform-google-materialize/releases/tag/v0.6.4" >v0.6.4</a> | <ul> <li>Released as part of v26.0.0.</li> <li>Uses <code>terraform-helm-materialize</code> version <code>v0.1.35</code>.</li> </ul>  |







1. Fork the [Materialize's sample Terraform
   repo](https://github.com/MaterializeInc/terraform-google-materialize).

1. Set `MY_ORGANIZATION` to your github organization name, substituting your
   organization's name for `<enter-your-organization>`:

   ```bash
   MY_ORGANIZATION=<enter-your-organization>
   ```

1. Clone your forked repo and checkout the `v0.8.11` tag. For example,

   - If cloning via SSH:

     ```bash
     git clone --depth 1 -b v0.8.11 git@github.com:${MY_ORGANIZATION}/terraform-google-materialize.git
     ```

   - If cloning via HTTPS:

     ```bash
     git clone --depth 1 -b v0.8.11 https://github.com/${MY_ORGANIZATION}/terraform-google-materialize.git
     ```


1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-google-materialize/examples/simple
   ```

   > **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory, which
>    requires minimal user input. For details on the  `examples/simple/`
>    infrastructure configuration (such as the node instance type, etc.), see the
>    [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/examples/simple/main.tf).
>    For more configuration options, you can use the `main.tf` file at the [root of
>    the repository](https://github.com/MaterializeInc/terraform-google-materialize/)
>    instead. When running with the root `main.tf`, see [GCP required
>    configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-gcp/).


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

   > **Tip:** The tutorial uses the `main.tf` found in the `examples/simple/` directory, which
>    requires minimal user input. For details on the  `examples/simple/`
>    infrastructure configuration (such as the node instance type, etc.), see the
>    [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/examples/simple/main.tf).
>    For more configuration options, you can use the `main.tf` file at the [root of
>    the repository](https://github.com/MaterializeInc/terraform-google-materialize/)
>    instead. When running with the root `main.tf`, see [GCP required
>    configuration](/self-managed-deployments/appendix/legacy/appendix-configuration-legacy-gcp/).


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


   **Materialize Operator:**

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


   **cert-manager (Starting in version 0.3.0):**

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
         license_key    = ""
       }
   ]
   EOF
   ```

   - **Starting in v0.3.0**, the Materialize on GCP Terraform module also
     deploys, by default:

     - [Load balancers](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) for Materialize instances (i.e., the [`create_load_balancer`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) flag defaults to `true`). The load balancers, by default, are configured  to be internal (i.e., the [`internal_load_balancer`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_materialize_instances) flag defaults to `true`).

     - A self-signed `ClusterIssuer`. The `ClusterIssuer` is deployed  after the
     `cert-manager` is deployed and running.

   - **Starting in v0.4.3**, you can specify addition configuration options via
     `environmentd_extra_args`.

   > **Tip:** If upgrading from a deployment that was set up using an earlier version of the
>    Terraform modules, additional considerations may apply when using an updated
>    Terraform modules to your existing deployments.
>    See [Materialize on GCP releases](/self-managed-deployments/appendix/legacy/appendix-legacy-terraform-releases/#materialize-on-gcp-terraform-module) for notable changes.


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



   **Via Network Load Balancer:**

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



   **Via port forwarding:**

   1. Find your console service name.

      ```shell
      MZ_SVC_CONSOLE=$(kubectl -n materialize-environment get svc \
        -o custom-columns="NAME:.metadata.name" --no-headers | grep console-lb)
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
      [https://localhost:8080](https://localhost:8080) (or, if you have not enabled
      TLS, [http://localhost:8080](http://localhost:8080)).

      The example uses a self-signed ClusterIssuer. As such, you may encounter a
      warning with regards to the certificate. In production, run with certificates
      from an official Certificate Authority (CA) rather than self-signed
      certificates.

   [^1]: The port forwarding command uses a while loop to handle a [known
   Kubernetes issue 78446](https://github.com/kubernetes/kubernetes/issues/78446),
   where interrupted long-running requests through a standard port-forward cause
   the port forward to hang. The command automatically restarts the port forwarding
   if an error occurs, ensuring a more stable connection. It detects failures by
   monitoring for "portforward.go" error messages.






   > **Tip:** If you experience long loading screens or unresponsiveness in the Materialize
>    Console, we recommend increasing the size of the `mz_catalog_server` cluster.
>    Refer to the [Troubleshooting Console
>    Unresponsiveness](/self-managed-deployments/troubleshooting/#troubleshooting-console-unresponsiveness)
>    guide.


## Next steps


- From the Console, you can get started with the
[Quickstart](/get-started/quickstart/).

- To start ingesting your own data from an external system like Kafka, MySQL or
  PostgreSQL, see [Ingest data](/ingest-data/).


## Cleanup


To delete the whole sample infrastructure and deployment (including the
Materialize operator and Materialize instances and data), run from the Terraform
directory:

```bash
terraform destroy
```

When prompted to proceed, type `yes` to confirm the deletion.


## See also

- [Troubleshooting](/self-managed-deployments/troubleshooting/)
- [Materialize Operator Configuration](/installation/configuration/)
