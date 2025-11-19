---
title: "Terraform Module"
description: ""
menu:
  main:
    parent: "install-on-azure"
    identifier: "install-azure-terraform"
    weight: 5
---

Materialize provides a set of modular Terraform modules that can be used to
deploy all services required for a production ready Materialize database.
The module is intended to provide a simple set of examples on how to deploy
materialize. It can be used as is or modules can be taken from the example and
integrated with existing DevOps tooling.

The repository can be found at:

***[Materialize Terraform Self-Managed Azure](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure)***

Please see the [top level](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main) and [cloud specific](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure) documentation for a full understanding
of the module structure and customizations.

Also check out the [Azure deployment guide](/installation/install-on-azure/appendix-deployment-guidelines/) for details on recommended instance sizing and configuration.

{{< note >}}
{{% self-managed/materialize-components-sentence %}}
{{< /note >}}

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}


## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [Azure Cli ](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)

#### License key

{{< include-md file="shared-content/license-key-required.md" >}}

---

# Example: Simple Materialize Deployment on Azure

This example demonstrates how to deploy a complete Materialize environment on Azure using the modular Terraform setup from this repository.


## Setup
```shell
git clone https://github.com/MaterializeInc/materialize-terraform-self-managed.git
cd materialize-terraform-self-managed/azure/examples/simple
````

## What Gets Created

This example provisions the following infrastructure:

### Resource Group
- **Resource Group**: New resource group to contain all resources

### Networking
- **Virtual Network**: 20.0.0.0/16 address space
- **AKS Subnet**: 20.0.0.0/20 with NAT Gateway association and service endpoints for Storage and SQL
- **PostgreSQL Subnet**: 20.0.16.0/24 delegated to PostgreSQL Flexible Server
- **NAT Gateway**: Standard SKU with static public IP for outbound connectivity
- **Private DNS Zone**: For PostgreSQL private endpoint resolution with VNet link

### Compute
- **AKS Cluster**: Version 1.32 with Cilium networking (network plugin: azure, data plane: cilium, policy: cilium)
- **Default Node Pool**: Standard_D4pds_v6 VMs, autoscaling 2-5 nodes, labeled for generic workloads
- **Materialize Node Pool**: Standard_E4pds_v6 VMs with 100GB disk, autoscaling 2-5 nodes, swap enabled, dedicated taints for Materialize workloads
- **Managed Identities**:
  - AKS cluster identity: Used by AKS control plane to provision Azure resources (creating load balancers when Materialize LoadBalancer services are created, managing network interfaces)
  - Workload identity: Used by Materialize pods for secure, passwordless authentication to Azure Storage (no storage account keys stored in cluster)

### Database
- **Azure PostgreSQL Flexible Server**: Version 15
- **SKU**: GP_Standard_D2s_v3 (2 vCores, 4GB memory)
- **Storage**: 32GB with 7-day backup retention
- **Network Access**: Public Network Access is disabled, Private access only (no public endpoint)
- **Database**: `materialize` database pre-created

### Storage
- **Storage Account**: Premium BlockBlobStorage with LRS replication for Materialize persistence
- **Container**: `materialize` blob container
- **Access Control**: Workload Identity federation for Kubernetes service account (passwordless authentication via OIDC)
- **Network Access**: Currently allows all traffic (production deployments should restrict to AKS subnet only traffic)

### Kubernetes Add-ons
- **cert-manager**: Certificate management controller for Kubernetes that automates TLS certificate provisioning and renewal
- **Self-signed ClusterIssuer**: Provides self-signed TLS certificates for Materialize instance internal communication (balancerd, console). Used by the Materialize instance for secure inter-component communication.

### Materialize
- **Operator**: Materialize Kubernetes operator
- **Instance**: Single Materialize instance in `materialize-environment` namespace
- **Load Balancers**: Internal Azure Load Balancers for Materialize access

---

## Getting Started

### Step 1: Set Required Variables

Before running Terraform, create a `terraform.tfvars` file with the following variables:

```hcl
subscription_id     = "12345678-1234-1234-1234-123456789012"
resource_group_name = "materialize-demo-rg"
name_prefix         = "simple-demo"
location            = "westus2"
license_key         = "your-materialize-license-key"  # Optional: Get from https://materialize.com/self-managed/
tags = {
  environment = "demo"
}
```

**Required Variables:**
- `subscription_id`: Azure subscription ID
- `resource_group_name`: Name for the resource group (will be created)
- `name_prefix`: Prefix for all resource names
- `location`: Azure region for deployment
- `tags`: Map of tags to apply to resources
- `license_key`: Materialize license key

---

### Step 2: Deploy Materialize

Run the usual Terraform workflow:

```bash
terraform init
terraform apply
```

## Notes

*Autoscaling: Uses Azure's native cluster autoscaler that integrates directly with Azure Virtual Machine Scale Sets for automated node scaling. In future we are planning to enhance this by making use of karpenter-provider-azure*

* You can customize each module independently.
* To reduce cost in your demo environment, you can tweak VM sizes and database tiers in `main.tf`.

***Don't forget to destroy resources when finished:***

```bash
terraform destroy
```
