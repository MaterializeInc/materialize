# Install on GCP



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
>
> - Fork the repo and pin to a specific version; or
>
> - Use the code as a reference when developing your own deployment.
>
>
>


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
| Community | New deployments | <p>To get a license key:</p> <ul> <li>If you have a Cloud account, visit the <a href="https://console.materialize.com/license/"><strong>License</strong> page in the Materialize Console</a>.</li> <li>If you do not have a Cloud account, visit <a href="https://materialize.com/self-managed/community-license/">https://materialize.com/self-managed/community-license/</a>.</li> </ul> |
| Community | Existing deployments | Contact <a href="https://materialize.com/docs/support/">Materialize support</a>. |
| Enterprise | New deployments | Visit <a href="https://materialize.com/self-managed/enterprise-license/">https://materialize.com/self-managed/enterprise-license/</a> to purchase an Enterprise license. |
| Enterprise | Existing deployments | Contact <a href="https://materialize.com/docs/support/">Materialize support</a>. |


## Getting started: Simple example

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
>
> - Fork the repo and pin to a specific version; or
>
> - Use the code as a reference when developing your own deployment.
>
>
>


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
   >

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
>


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
   <a href="/self-managed-deployments/troubleshooting/">Troubleshooting</a>.</p>

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
>
> - **If using a private (internal) NLB:** You can connect from inside the same VPC or from networks that are privately connected to it. Alternatively, use Kubernetes port-forwarding for both SQL and Console.
>



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
>
>


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
>
> - **If using a private (internal) NLB:** You can connect from inside the same VPC or from networks that are privately connected to it. Alternatively, use Kubernetes port-forwarding for both SQL and Console.
>



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
>


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
>


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
