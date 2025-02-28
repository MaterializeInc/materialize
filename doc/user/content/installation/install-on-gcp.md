---
title: "Install on GCP"
description: ""
aliases:
  - /self-hosted/install-on-gcp/
menu:
  main:
    parent: "installation"
---

{{% self-managed/materialize-components-sentence %}}

This tutorial deploys Materialize to GCP Google Kubernetes Engine (GKE) cluster
with a Cloud SQL PostgreSQL database as the metadata database and Cloud Storage
bucket for blob storage. Specifically, the tutorial uses [Materialize on Google Cloud Terraform
modules](https://github.com/MaterializeInc/terraform-google-materialize) to:

- Set up the GCP environment.

- Call
   [terraform-helm-materialize](https://github.com/MaterializeInc/terraform-helm-materialize)
   module to deploy Materialize Operator and Materialize instances to the GKE
   cluster.

{{< warning >}}

The Terraform modules used in this tutorial are provided for
demonstration/evaluation purposes only and not intended for production use.
Materialize does not support nor recommend these modules for production use.

{{< /warning >}}

## Prerequisites

### Google cloud project

You need a GCP project for which you have a role (such as
`roles/resourcemanager.projectIamAdmin` or `roles/owner`) that includes
[permissions to manage access to the
project](https://cloud.google.com/iam/docs/granting-changing-revoking-access).

### gcloud CLI (Inititalized)

- If you do not have gcloud CLI, install. For details, see the [Install the
  gcloud CLI documentation](https://cloud.google.com/sdk/docs/install).

- Initialize the gcloud CLI to specify the GCP project you want to use. For
  details, see the [Install the gcloud CLI documentation](https://cloud.google.com/sdk/docs/install).

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

*Optional*. `jq` is used to parse the EKS cluster name and region from the
Terraform outputs. Alternatively, you can manually specify the name and region.
If you want to use `jq` and do not have `jq` installed, install.

## A. Configure GCP project andservice account

1. Open a Terminal window.

1. Enable the following services for your GCP project:

   ```bash
   gcloud services enable container.googleapis.com        # For creating Kubernetes clusters
   gcloud services enable sqladmin.googleapis.com         # For creating databases
   gcloud services enable cloudresourcemanager.googleapis.com # For managing GCP resources
   gcloud services enable servicenetworking.googleapis.com  # For private network connections
   gcloud services enable iamcredentials.googleapis.com     # For security and authentication
   ```

   When finished, you should see output similar to the following:

   ```bash
   Operation "operations/acf.p2-87743450299-3cfd3269-06b9-48da-bbfd-83ce5f979208" finished successfully.
   Operation "operations/acat.p2-87743450299-9456bdf0-486f-41b9-9f04-87bae9d31217" finished successfully.
   Operation "operations/acat.p2-87743450299-83fa25df-e36b-427e-ab98-0067ee6905fe" finished successfully.
   Operation "operations/acat.p2-87743450299-6298c59f-b6fc-4a7f-9d27-b2e7c7d24648" finished successfully.
   ```

1. To the account or the service account that will run the Terraform script,
   grant the following IAM roles:

   - `roles/editor`
   - `roles/iam.serviceAccountAdmin`
   - `roles/servicenetworking.networksAdmin`
   - `roles/storage.admin`

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
      ```

1. For the service account, authenticate to allow Terraform to
   interact with your GCP project. For details, see [Terraform: Google Cloud
   Provider Configuration
   reference](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#authentication).

   For example, to use User Application Default Credentials, you can run the
   following command:

   ```bash
   gcloud auth application-default login
   ```

## B. Set up GCP Kubernetes environment and install Materialize

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

Materialize provides [sample Terraform
modules](https://github.com/MaterializeInc/terraform-google-materialize) for
evaluation purposes only. The modules deploy a sample infrastructure on GCP
(region `us-central1`) with the following components:

- Google Kubernetes Engine (GKE) cluster
- Cloud SQL PostgreSQL database for metadata storage
- Cloud Storage bucket for blob storage
- A dedicated VPC
- Service accounts with proper IAM permissions
- Materialize Operator
- Materialize instances (during subsequent runs after the Operator is running)

{{< tip >}}
The tutorial uses the module found in the `examples/simple/`
directory, which requires minimal user input. For more configuration options,
you can run the modules at the [root of the
repository](https://github.com/MaterializeInc/terraform-google-materialize/)
instead.

For details on the  `examples/simple/` infrastructure configuration (such as the
node instance type, etc.), see the
[examples/simple/main.tf](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/examples/simple/main.tf).
{{< /tip >}}


{{% self-managed/versions/step-clone-google-terraform-repo %}}

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-google-materialize/examples/simple
   ```

   {{< tip >}}
   The tutorial uses the module found in the `examples/simple/` directory, which
   requires minimal user input. For more configuration options, you can run the
   modules at the [root of the
   repository](https://github.com/MaterializeInc/terraform-google-materialize/)
   instead.

   For details on the  `examples/simple/` infrastructure configuration (such as
   the node instance type, etc.), see the [examples/simple/main.tf](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/examples/simple/main.tf).
   {{< /tip >}}

1. Create a `terraform.tfvars` file (you can copy from the
   `terraform.tfvars.example` file) and specify:

   -  Your GCP project ID.

   -  A prefix (e.g., `mz-simple`) for your resources.

   -  The region for the GKE cluster.

   ```bash
   project_id = "enter-your-gcp-project-id"
   prefix  = "enter-your-prefix" //  e.g., mz-simple
   region = "us-central1"
   ```

1. Initialize the terraform directory.

    ```bash
    terraform init
    ```

1. Create a terraform plan and review the changes.

    ```bash
    terraform plan -out my-plan.tfplan
    ```

1. If you are satisfied with the changes, apply the terraform plan.

    ```bash
    terraform apply my-plan.tfplan
    ```

   Upon successful completion, various fields and their values are output:

   ```bash
   Apply complete! Resources: 20 added, 0 changed, 0 destroyed.

   Outputs:

   connection_strings = <sensitive>
   gke_cluster = <sensitive>
   service_accounts = {
   "gke_sa" = "mz-simple-gke-sa@my-project.iam.gserviceaccount.com"
   "materialize_sa" = "mz-simple-materialize-sa@my-project.iam.gserviceaccount.com"
   }
   ```

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

1. By default, the example Terraform installs the Materialize Operator. Verify
   the installation and check the status:

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
       }
   ]
   EOF
   ```

1. Create a terraform plan with both `.tfvars` files and review the changes.

   ```bash
   terraform plan -var-file=terraform.tfvars -var-file=mz_instances.tfvars -out my-plan.tfplan
   ```

   The plan should show the changes to be made, with a summary similar to the
   following:

   ```
   Plan: 4 to add, 0 to change, 0 to destroy.

   Saved the plan to: my-plan.tfplan

   To perform exactly these actions, run the following command to apply:
   terraform apply "my-plan.tfplan"
   ```

1. If you are satisfied with the changes, apply the terraform plan.

    ```bash
    terraform apply my-plan.tfplan
    ```

   Upon successful completion, you should see output with a summary similar to the following:

   ```bash
   Apply complete! Resources: 4 added, 0 changed, 0 destroyed.

   Outputs:

   connection_strings = <sensitive>
   gke_cluster = <sensitive>
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
   NAME                                             READY   STATUS      RESTARTS      AGE
   pod/create-db-demo-db-jcpnn                      0/1     Completed   0             2m11s
   pod/mzpzk74xji8b-balancerd-669988bb94-5vbps      1/1     Running     0             98s
   pod/mzpzk74xji8b-cluster-s2-replica-s1-gen-1-0   1/1     Running     0             96s
   pod/mzpzk74xji8b-cluster-u1-replica-u1-gen-1-0   1/1     Running     0             96s
   pod/mzpzk74xji8b-console-5dc9f87498-hqxdw        1/1     Running     0             91s
   pod/mzpzk74xji8b-console-5dc9f87498-x95qj        1/1     Running     0             91s
   pod/mzpzk74xji8b-environmentd-1-0                1/1     Running     0             113s

   NAME                                               TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                                        AGE
   service/mzpzk74xji8b-balancerd                     ClusterIP   None            <none>        6876/TCP,6875/TCP                              98s
   service/mzpzk74xji8b-cluster-s2-replica-s1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   97s
   service/mzpzk74xji8b-cluster-u1-replica-u1-gen-1   ClusterIP   None            <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   96s
   service/mzpzk74xji8b-console                       ClusterIP   None            <none>        8080/TCP                                       91s
   service/mzpzk74xji8b-environmentd                  ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            99s
   service/mzpzk74xji8b-environmentd-1                ClusterIP   None            <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            113s
   service/mzpzk74xji8b-persist-pubsub-1              ClusterIP   None            <none>        6879/TCP                                       113s

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mzpzk74xji8b-balancerd   1/1     1            1           98s
   deployment.apps/mzpzk74xji8b-console     2/2     2            2           91s

   NAME                                                DESIRED   CURRENT   READY      AGE
   replicaset.apps/mzpzk74xji8b-balancerd-669988bb94   1         1         1          98s
   replicaset.apps/mzpzk74xji8b-console-5dc9f87498     2         2         2          91s

   NAME                                                        READY   AGE
   statefulset.apps/mzpzk74xji8b-cluster-s2-replica-s1-gen-1   1/1     97s
   statefulset.apps/mzpzk74xji8b-cluster-u1-replica-u1-gen-1   1/1     96s
   statefulset.apps/mzpzk74xji8b-environmentd-1                1/1     113s

   NAME                          STATUS     COMPLETIONS   DURATION   AGE
   job.batch/create-db-demo-db   Complete   1/1           13s        2m11s
   ```

1. Open the Materialize Console in your browser:

   1. From the previous `kubectl` output, find the Materialize console service.

      ```none
      NAME                           TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
      service/mzpzk74xji8b-console   ClusterIP   None         <none>        8080/TCP   91s
      ```

   1. Forward the Materialize Console service to your local machine (substitute
      your service name for `mzpzk74xji8b-console`):

      ```shell
      while true;
      do kubectl port-forward svc/mzpzk74xji8b-console 8080:8080 -n materialize-environment 2>&1 |
      grep -q "portforward.go" && echo "Restarting port forwarding due to an error." || break;
      done;
      ```
      {{< note >}}
      Due to a [known Kubernetes issue](https://github.com/kubernetes/kubernetes/issues/78446),
      interrupted long-running requests through a standard port-forward cause the port forward to hang. The command above
      automatically restarts the port forwarding if an error occurs, ensuring a more stable
      connection. It detects failures by monitoring for "portforward.go" error messages.
      {{< /note >}}

   1. Open a browser and navigate to
      [http://localhost:8080](http://localhost:8080). From the Console, you can get started with the Quickstart.

      {{< tip >}}

      {{% self-managed/troubleshoot-console-mz_catalog_server_blurb %}}

      {{< /tip >}}

## Troubleshooting

If you encounter issues:

1. Check operator logs:
```bash
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

2. Check environment logs:
```bash
kubectl logs -l app.kubernetes.io/name=environmentd -n materialize-environment
```

3. Verify the storage configuration:
```bash
kubectl get sc
kubectl get pv
kubectl get pvc -A
```

## Cleanup


To uninstall the Materialize operator:
```bash
helm uninstall materialize-operator -n materialize
```

This will remove the operator but preserve any PVs and data. To completely clean
up:

```bash
kubectl delete namespace materialize
kubectl delete namespace materialize-environment
```

In your Terraform directory, run:

```bash
terraform destroy
```

When prompted to proceed, type `yes` to confirm the deletion.

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Installation](/installation/)
