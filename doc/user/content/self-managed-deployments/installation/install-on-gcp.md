---
title: "Install on GCP"
description: ""
menu:
  main:
    parent: "installation"
    identifier: "install-gcp-terraform"
    weight: 30

---

Materialize provides a set of modular [Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
that can be used to deploy all services required for Materialize to run on Google Cloud.
The module is intended to provide a simple set of examples on how to deploy
Materialize. It can be used as is or modules can be taken from the example and
integrated with existing DevOps tooling.

{{% self-managed/materialize-components-sentence %}} The example on this page
deploys a complete Materialize environment on GCP using the modular Terraform
setup from this repository.

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

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
| Load Balancers | GCP Load Balancers for Materialize access {{< yaml-table data="self_managed/default_ports" >}} |

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

{{< yaml-table data="self_managed/license_key" >}}

## Getting started: Simple example

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

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
   ```

### Step 3: Apply the Terraform

1. Initialize the Terraform directory to download the required providers
   and modules:

   ```bash
   terraform init
   ```

1. Apply the Terraform configuration to create the infrastructure.

   - To deploy with the default **internal NLB** for Materialize access:

   ```bash
   terraform apply
   ```

   - To deploy with <red>**public NLB**</red> for Materialize access:

   ```bash
   terraform apply -var="internal=false"
   ```

   If you are satisfied with the planned changes, type `yes` when prompted
   to proceed.

1. From the output, you will need the following field(s) to connect:
   - `console_load_balancer_ip` for the Materialize Console
   - `balancerd_load_balancer_ip` to connect PostgreSQL-compatible clients/drivers.

1. Configure `kubectl` to connect to your GKE cluster, replacing:

   - `<your-cluster-name>` with the name of your GKE cluster. Your cluster name
     can be found in the Terraform output. For the sample example, the cluster
     name is `<name_prefix>-gke`.

   - `<your-region>` with the region of your GKE cluster. Your region can be
     found in the Terraform output `gke_cluster_location`, corresponds to the
     `region` value in your `terraform.tfvars`.

   - `<your-project-id>` with your GCP project ID.

   ```bash
   gcloud container clusters get-credentials <your-cluster-name>  \
    --region <your-region> \
    --project <your-project-id>
   ```

### Step 4. Optional. Verify the status of your deployment

1. Check the status of your deployment:
   {{% include-from-yaml data="self_managed/installation"
   name="installation-verify-status" %}}

### Step 5: Connect to Materialize


{{< note >}}

If using an **internal Network Load Balancer (NLB)** for your Materialize
access, you can connect from inside the same VPC or from networks that are
privately connected to it.

{{< /note >}}

#### Connect using the Materialize Console

Using the `console_load_balancer_ip` from the Terraform output, you can connect
to Materialize via the Materialize Console.

To connect to the Materialize Console, open a browser to
`https://<console_load_balancer_ip>:8080`, substituting your
`<console_load_balancer_ip>`.

{{< tip >}}

{{% include-from-yaml data="self_managed/installation"
name="install-uses-self-signed-cluster-issuer" %}}

{{< /tip >}}

#### Connect using the `psql`

Using the `balancerd_load_balancer_ip` value from the Terraform output, you can
connect to Materialize via PostgreSQL-compatible clients/drivers, such as
`psql`:

```bash
psql postgres://<balancerd_load_balancer_ip>:6875/materialize
```

## Customizing Your Deployment

For more information on the Terraform modules, see both the [top
level](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
and [GCP
specific](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/gcp)
details.

{{< tip >}}
You can customize each module independently. To reduce cost in your demo environment, you can tweak machine types and database tiers in `main.tf`.
{{< /tip >}}

{{< note >}}
**GCP Storage Authentication Limitation:** Materialize currently only supports HMAC key authentication for GCS access (S3-compatible API).
Current State: The modules configure both HMAC keys and Workload Identity, but Materialize uses HMAC keys for actual storage access.
Future: Native GCS access via Workload Identity Federation or Kubernetes service account impersonation will be supported in a future release, eliminating the need for static credentials.
{{< /note >}}

For details on recommended instance sizing and configuration, see the [GCP
deployment
guide](/self-managed-deployments/deployment-guidelines/gcp-deployment-guidelines/).

## Cleanup

{{% self-managed/cleanup-cloud %}}

## See Also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
