---
title: "Install on Azure"
description: "Install Materialize on Azure using the Unified Terraform module."
menu:
  main:
    parent: "installation"
    identifier: "install-on-azure"
    weight: 20
---

Materialize provides a set of modular [Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
that can be used to deploy all services required for Materialize to run on Azure.
The module is intended to provide a simple set of examples on how to deploy
Materialize. It can be used as is or modules can be taken from the example and
integrated with existing DevOps tooling.

{{% self-managed/materialize-components-sentence %}} The example on this page
deploys a complete Materialize environment on Azure using the modular Terraform
setup from this repository.

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

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
| Load Balancers | Internal Azure Load Balancers for Materialize access {{< yaml-table data="self_managed/default_ports" >}}  |

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

{{< yaml-table data="self_managed/license_key" >}}

## Getting started: Simple example

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

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

   ```bash
   terraform output -raw <field_name>
   ```

   {{< tip >}}
   Your shell may show an ending marker (such as `%`) because the
   output did not end with a newline. Do not include the marker when using the value.
   {{< /tip >}}

1. Configure `kubectl` to connect to your cluster using your:
   - `resource_group_name`. Your
     resource group name can be found in the Terraform output or in the
     `terraform.tfvars` file.

   - `akw_cluster_name`. Your cluster name can be found in the Terraform output.
     For the sample example, your cluster name has the form `{prefix_name}-aks`;
     e.g., simple-demo-aks`.

   ```bash
   # az aks get-credentials --resource-group <resource_group_name> --name <your-aks-cluster-name>
   az aks get-credentials --resource-group $(terraform output -raw resource_group_name) --name $(terraform output -raw aks_cluster_name)
   ```


### Step 4. Optional. Verify the deployment.

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

{{< tip >}}
To reduce cost in your demo environment, you can tweak VM sizes and database tiers in `main.tf`.
{{< /tip >}}

You can customize each Terraform module independently.

- For details on the Terraform modules, see both the [top
level](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
and [Azure
specific](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure) modules.

- For details on recommended instance sizing and configuration, see the [Azure
deployment
guide](/self-managed-deployments/deployment-guidelines/azure-deployment-guidelines/).

{{< note >}}
Autoscaling: Uses Azure's native cluster autoscaler that integrates directly with Azure Virtual Machine Scale Sets for automated node scaling. In future we are planning to enhance this by making use of karpenter-provider-azure.
{{< /note >}}

See also:
- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)

## Cleanup

{{% self-managed/cleanup-cloud %}}

## See Also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
