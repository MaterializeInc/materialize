---
title: "Install on GCP"
description: ""
robots: "noindex, nofollow"
---

Self-managed Materialize requires:

{{% self-managed/requirements-list %}}

The tutorial deploys Materialize to GCP Google Kubernetes Engine (GKE) cluster
with a Cloud SQL PostgreSQL database as the metadata database and Cloud Storage
bucket for blob storage.

{{< important >}}

Assumes the `gke_config.machine_type = "e2-standard-4"` in the sample
examples/simple Terraform module.

{{< /important >}}

## Prerequisites

### Google cloud project

If you do not have a GCP project to use for this tutorial, create one.

### gcloud CLI

If you do not have the gcloud CLI installed,

- Install the gcloud CLI. For details, see the [Install the gcloud CLI
  documentation](https://cloud.google.com/sdk/docs/install).

- Initialize the gcloud CLI to specify the GCP project you want to use. For
  details, see the [Install the gcloud CLI documentation](https://cloud.google.com/sdk/docs/install).

### Terraform

If you don't have Terraform installed, [install
Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### kubectl

If you do not have `kubectl`, install `kubectl`. See [Install kubectl and
configure cluster access](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl)
for details.

{{< tip >}}

Using `gcloud` to install `kubectl` will also install the needed plugins.
Otherwise, install the `gke-gcloud-auth-plugin` for `kubectl`. For
details, see [Install the
gke-gcloud-auth-plugin](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#install_plugin).

{{< /tip >}}

During the [Kubernetes environment setup](#a-set-up-gcp-kubernetes-environment),
you will configure `kubectl` to interact with your GKE cluster.

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, refer to the [Helm
documentation](https://helm.sh/docs/intro/install/).

### GCP Kubernetes environment

{{% self-managed/requirements-list %}}

See [A. Set up GCP Kubernetes environment](#a-set-up-gcp-kubernetes-environment)
for a sample setup.

## A. Set up GCP Kubernetes environment

{{< tabs  >}}

{{< tab "Terraform" >}}

Materialize provides a [sample Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) for
evaluation purposes only. The module deploys a sample infrastructure on GCP
(region `us-central1`) with the following components:

- Google Kubernetes Engine (GKE) cluster
- Cloud SQL PostgreSQL database for metadata storage
- Cloud Storage bucket for blob storage
- A dedicated VPC
- Service accounts with proper IAM permissions

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

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

1. Grant the service account the neccessary IAM roles.

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
      ```

1. Authenticate to allow Terraform to interact with your GCP project. For
   details, see [Terraform: Google Cloud Provider Configuration
   reference](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#authentication).

   For example, to use User Application Default Credentials, you can run the
   following command:

   ```bash
   gcloud auth application-default login
   ```

1. Clone or download the [Materialize's sample Terraform
   repo](https://github.com/MaterializeInc/terraform-google-materialize).

1. Go to the `examples/simple` folder in the Materialize Terraform repo
   directory.

   ```bash
   cd terraform-google-materialize/examples/simple
   ```

1. Create a `terraform.tfvars` file and and specify:

   -  Your GCP project ID and

   -  A secure password for the Cloud SQL PostgreSQL database (to be created).

   ```bash
   project_id = "enter-your-gcp-project-id"
   database_password  = "enter-secure-password"
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

   <a name="terraform-output"></a>
   Upon successful completion, various fields and their values are output:

   ```bash
   Apply complete! Resources: 16 added, 0 changed, 0 destroyed.

   Outputs:

   connection_strings = <sensitive>
   gke_cluster = <sensitive>
   service_accounts = {
      "gke_sa" = "mz-simple-gke-sa@your-project-id.iam.gserviceaccount.com"
      "materialize_sa" = "mz-simple-materialize-sa@your-project-id.iam.gserviceaccount.com"
   }
   ```

1. Note the following output values to be used during [C. Install
     Materialize](#c-install-materialize)

   <a name="terraform-output"></a>

   - `service_accounts.materialize_sa` value before the `@` (e.g.,
     `my-simple-materialize-sa`)

   - `connection_strings`. You can get the connection strings by running the
     following command:

     ```bash
     terraform output -json connection_strings | jq
     ```

1. Configure `kubectl` to connect to your EKS cluster:

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

{{< /tab >}}

{{< /tabs >}}

## B. Install the Materialize Operator

1. Clone/download the [Materialize
   repo](https://github.com/MaterializeInc/materialize).

1. Go to the Materialize repo directory.

   ```bash
   cd materialize
   ```

1. Check out the {{% self-managed/latest_version %}} tag.

1. Create a `my-materialize-operator-values.yaml` configuration file for
   the Materialize operator.  Update with:

   - your GCP region (the sample Terraform module uses a default region of
     `us-central1`).


      ```yaml
      # my-materialize-operator-values.yaml

      operator:
        cloudProvider:
          type: "gcp"
          region: "your-gcp-region"  # e.g., us-central1
          providers:
            gcp:
              enabled: true

      # Adjust network policies as needed
      networkPolicies:
        enabled: true
        egress:
          enabled: true
          cidrs: ["0.0.0.0/0"]
        ingress:
          enabled: true
          cidrs: ["0.0.0.0/0"]
        internal:
          enabled: true
      ```


1. Install the Materialize operator `materialize-operator`, specifying the path
   to your `my-materialize-operator-values.yaml` file:

   ```shell
   helm install materialize-operator misc/helm-charts/operator \
      -f my-materialize-operator-values.yaml  \
      --namespace materialize --create-namespace
   ```

1. Verify the installation and check the status:

    ```shell
    kubectl get all -n materialize
    ```

    Wait for the components to be in the `Running` state:

    ```none
    NAME                                        READY   STATUS    RESTARTS   AGE
    pod/materialize-operator-59cb6768cb-vk87l   1/1     Running   0          14s

    NAME                                   READY   UP-TO-DATE   AVAILABLE   AGE
    deployment.apps/materialize-operator   1/1     1            1           15s

    NAME                                              DESIRED   CURRENT   READY   AGE
    replicaset.apps/materialize-operator-59cb6768cb   1         1         1       15s
    ```


## C. Install Materialize

To deploy Materialize:

1. For your backend configuration, create a file
   `materialize-backend-secret.yaml` for your [Kubernetes
   Secret](https://kubernetes.io/docs/concepts/configuration/secret/).

    ```yaml
    apiVersion: v1
    kind: Secret
    metadata:
      name: materialize-backend
      namespace: materialize-environment
    stringData:
      persist_backend_url: "your-persist-backend-url"
      metadata_backend_url: "your-metadata-backend-url"
    ```

    - For `your-persist-backend-url`, set to the `persist_backend_url` value
      from the [Terraform output](#terraform-output).

    - For `your-metadata-backend-url`, set to the `metadata_backend_url` value
      from the [Terraform output](#terraform-output).

      {{< tip >}}
      You may need to URL encode your database password in the
      `metadata_backend_url`.
      {{< /tip >}}

1. Create a YAML file (e.g., `my-materialize.yaml`) for your Materialize
   configuration.

   Replace `your-service-account-name` with the service account name as
   specified in the [Terraform output](#terraform-output).

   ```yaml
   apiVersion: materialize.cloud/v1alpha1
   kind: Materialize
   metadata:
     name: "your-service-account-name"      # e.g. my-simple-materialize-sa
     namespace: materialize-environment
   spec:
     environmentdImageRef: materialize/environmentd:v0.127.0
     environmentdResourceRequirements:
       limits:
         memory: 16Gi
       requests:
         cpu: "2"
         memory: 16Gi
     balancerdResourceRequirements:
       limits:
         memory: 256Mi
       requests:
         cpu: "100m"
         memory: 256Mi
     backendSecretName: materialize-backend
   ```

1. Create the `materialize-environment` namespace and apply the files to install
   Materialize:

   ```shell
   kubectl create namespace materialize-environment
   kubectl apply -f materialize-backend-secret.yaml
   kubectl apply -f my-materialize.yaml
   ```

1. Verify the installation and check the status:

   ```bash
   kubectl get all -n materialize-environment
   ```

   Wait for the components to be in the `Running` state.

   ```none
   NAME                                             READY   STATUS    RESTARTS   AGE
   pod/mzqqi9d9n2rw-balancerd-5b56d9b5d-q767h       1/1     Running   0          13m
   pod/mzqqi9d9n2rw-cluster-s1-replica-s1-gen-1-0   1/1     Running   0          13m
   pod/mzqqi9d9n2rw-cluster-s2-replica-s2-gen-1-0   1/1     Running   0          13m
   pod/mzqqi9d9n2rw-cluster-s3-replica-s3-gen-1-0   1/1     Running   0          13m
   pod/mzqqi9d9n2rw-cluster-u1-replica-u1-gen-1-0   1/1     Running   0          13m
   pod/mzqqi9d9n2rw-console-585b4cbc87-7cqfr        1/1     Running   0          13m
   pod/mzqqi9d9n2rw-console-585b4cbc87-b482w        1/1     Running   0          13m
   pod/mzqqi9d9n2rw-environmentd-1-0                1/1     Running   0          14m

   NAME                                               TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                                        AGE
   service/mzqqi9d9n2rw-balancerd                     ClusterIP   None         <none>        6876/TCP,6875/TCP                              13m
   service/mzqqi9d9n2rw-cluster-s1-replica-s1-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   13m
   service/mzqqi9d9n2rw-cluster-s2-replica-s2-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   13m
   service/mzqqi9d9n2rw-cluster-s3-replica-s3-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   13m
   service/mzqqi9d9n2rw-cluster-u1-replica-u1-gen-1   ClusterIP   None         <none>        2100/TCP,2103/TCP,2101/TCP,2102/TCP,6878/TCP   13m
   service/mzqqi9d9n2rw-console                       ClusterIP   None         <none>        8080/TCP                                       13m
   service/mzqqi9d9n2rw-environmentd                  ClusterIP   None         <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            13m
   service/mzqqi9d9n2rw-environmentd-1                ClusterIP   None         <none>        6875/TCP,6876/TCP,6877/TCP,6878/TCP            14m
   service/mzqqi9d9n2rw-persist-pubsub-1              ClusterIP   None         <none>        6879/TCP                                       14m

   NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/mzqqi9d9n2rw-balancerd   1/1     1            1           13m
   deployment.apps/mzqqi9d9n2rw-console     2/2     2            2           13m

   NAME                                               DESIRED   CURRENT   READY   AGE
   replicaset.apps/mzqqi9d9n2rw-balancerd-5b56d9b5d   1         1         1       13m
   replicaset.apps/mzqqi9d9n2rw-console-585b4cbc87    2         2         2       13m

   NAME                                                        READY   AGE
   statefulset.apps/mzqqi9d9n2rw-cluster-s1-replica-s1-gen-1   1/1     13m
   statefulset.apps/mzqqi9d9n2rw-cluster-s2-replica-s2-gen-1   1/1     13m
   statefulset.apps/mzqqi9d9n2rw-cluster-s3-replica-s3-gen-1   1/1     13m
   statefulset.apps/mzqqi9d9n2rw-cluster-u1-replica-u1-gen-1   1/1     13m
   statefulset.apps/mzqqi9d9n2rw-environmentd-1                1/1     14m
   ```

1. Open the Materialize console in your browser:

   1. From the previous `kubectl` output, find the Materialize console service.

      ```none
      NAME                           TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
      service/mzqqi9d9n2rw-console   ClusterIP   None         <none>        8080/TCP   13m
      ```

   1. Forward the Materialize console service to your local machine (substitute
      your service name for `mzqqi9d9n2rw-console`):

      ```shell
      while true;
      do kubectl port-forward svc/mzqqi9d9n2rw-console 8080:8080 -n materialize-environment 2>&1 |
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
      [http://localhost:8080](http://localhost:8080).


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

Delete the Materialize environment:
```bash
kubectl delete -f materialize-environment.yaml
```

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

- [Materialize Kubernetes Operator Helm Chart](/self-managed/)
- [Materialize Operator Configuration](/self-managed/configuration/)
- [Troubleshooting](/self-managed/troubleshooting/)
- [Operational guidelines](/self-managed/operational-guidelines/)
- [Installation](/self-managed/installation/)
- [Upgrading](/self-managed/upgrading/)
