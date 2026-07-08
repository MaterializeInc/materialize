---
title: "Install on Azure"
description: "Deploy the Ory-based enterprise SSO stack on Azure with Materialize."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-azure"
    weight: 30
---

This guide walks through the
[`azure/examples/enterprise`](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure/examples/enterprise)
example in the [Materialize Terraform
repository](https://github.com/MaterializeInc/materialize-terraform-self-managed),
which extends the base [Install on
Azure](/self-managed-deployments/installation/install-on-azure/) walkthrough
with the Ory-based enterprise SSO stack on AKS.

{{% self-managed/materialize-components-sentence %}} This example layers the
Ory stack (Kratos, Hydra, the selfservice UI, and optional Polis) on top so
that the Materialize console authenticates users through OIDC, with SAML and
SCIM available when Polis is enabled.

{{< note >}}

{{< self-managed/terraform-disclaimer >}}

{{< /note >}}

## What Gets Created

This example provisions everything from the base [Install on
Azure](/self-managed-deployments/installation/install-on-azure/) guide, plus
the additions below.

### Networking

| Resource | Description |
|----------|-------------|
| Public Hostnames | Six browser-facing hostnames (Hydra, Kratos, the selfservice UI, optional Polis, the Materialize console, balancerd). DNS records are created by you after `terraform apply`. |
| LoadBalancer Services | One per browser-facing service in the `ory` and `materialize-environment` namespaces. Backed by Azure standard load balancers. |

### Database

| Resource | Description |
|----------|-------------|
| Ory Azure PostgreSQL Flexible Server | Separate instance from the Materialize backend. PostgreSQL 18, `Standard_B1ms` SKU, 32GB storage, private endpoint only. |
| Databases | `kratos`, `hydra`, plus `polis` when `enable_polis = true`. |
| User | `oryadmin` with auto-generated password. |

### Kubernetes Add-ons

| Resource | Description |
|----------|-------------|
| Ory Kratos | Helm release in the `ory` namespace. Identity management: login, registration, recovery, account flows. |
| Ory Hydra | Helm release in the `ory` namespace. OAuth2 / OIDC provider that the Materialize console trusts. Hydra Maester is enabled. |
| Ory Selfservice UI | Helm release in the `ory` namespace. Renders the Kratos login, consent, and recovery pages. |
| Ory Polis (optional) | Helm release in the `ory` namespace when `enable_polis = true`. SAML-to-OIDC bridge plus SCIM endpoint. |
| Polis TLS Proxy (optional) | `polis-tls-proxy` Deployment fronting Polis. Polis itself only speaks plain HTTP, so a pingap-based TLS terminator is deployed alongside it when Polis is enabled. |
| cert-manager `ClusterIssuer` | Defaults to the in-cluster self-signed issuer. Override via `cert_issuer_ref` to plug in a real one (corporate CA, Let's Encrypt, etc.). |

### Materialize

| Resource | Description |
|----------|-------------|
| Materialize Instance | Configured for OIDC sign-in against the Hydra issuer URL. The browser-facing console hostname is registered as the OAuth2 redirect URI. |

## Azure-specific requirements

Cross-cutting requirements (license key with the `ory` entitlement, DNS
hostnames, cert-manager strategy, required tools) are covered on the shared
[Prerequisites](/self-managed-deployments/enterprise-sso/prerequisites/)
page. This section only lists the Azure-specific bits.

An active Azure subscription with permission to create:

- Resource groups
- Virtual networks, subnets, and NAT gateways
- AKS clusters and node pools
- Azure Database for PostgreSQL Flexible Servers
- Storage accounts
- Managed identities and role assignments

## Getting Started: Enterprise SSO Example

{{< note >}}

{{< self-managed/terraform-disclaimer >}}

{{< /note >}}

### Step 1: Set Up the Environment

1. Open a terminal window.

1. Clone the Materialize Terraform repository and go to the
   `azure/examples/enterprise` directory:

   ```bash
   git clone https://github.com/MaterializeInc/materialize-terraform-self-managed.git
   cd materialize-terraform-self-managed/azure/examples/enterprise
   ```

1. Sign in to Azure and select the subscription you'll deploy into:

   ```bash
   az login
   az account set --subscription <your-subscription-id>
   ```

### Step 2: Configure Terraform Variables

1. Create a `terraform.tfvars` file with the required variables:

   - `subscription_id`: Azure subscription ID
   - `resource_group_name`: Name of the resource group to create
   - `name_prefix`: Prefix for all resource names
   - `location`: Azure region
   - `license_key`: Materialize license key JWT with the `ory` entitlement
   - `k8s_apiserver_authorized_networks`: CIDRs allowed to reach the AKS API server (required, no default)
   - `ory_hydra_fqdn`, `ory_ui_fqdn`, `ory_kratos_fqdn`, `materialize_console_fqdn`, `materialize_balancerd_fqdn`: Public hostnames for the browser-facing services
   - `tags`: Map of tags to apply to resources

   ```hcl
   subscription_id     = "12345678-1234-1234-1234-123456789012"
   resource_group_name = "materialize-enterprise-rg"
   name_prefix         = "mz-enterprise"
   location            = "westus2"
   license_key         = "your-materialize-license-key"

   k8s_apiserver_authorized_networks = ["0.0.0.0/0"]   # tighten for production

   ory_hydra_fqdn             = "hydra.example.com"
   ory_ui_fqdn                = "auth.example.com"
   ory_kratos_fqdn            = "kratos.example.com"
   materialize_console_fqdn   = "console.example.com"
   materialize_balancerd_fqdn = "balancerd.example.com"

   tags = {
     environment = "demo"
   }
   ```

1. {{< include-md file="shared-content/self-managed/enterprise-sso/polis-tfvars.md" >}}

1. {{< include-md file="shared-content/self-managed/enterprise-sso/cert-issuer-tfvars.md" >}}

1. {{< include-md file="shared-content/self-managed/enterprise-sso/upstream-oidc-tfvars.md" >}}

### Step 3: Apply the Terraform

1. Initialize the Terraform directory:

   ```bash
   terraform init
   ```

1. Apply the Terraform configuration:

   ```bash
   terraform apply
   ```

   Expect 30 to 45 minutes for the full apply. The slowest parts are AKS
   provisioning, the two Flexible Server instances, and the Materialize
   instance reaching ready.

1. Configure `kubectl` against the new cluster:

   ```bash
   az aks get-credentials \
     --resource-group $(terraform output -raw resource_group_name) \
     --name $(terraform output -raw aks_cluster_name)
   ```

### Step 4: Create DNS Records

{{< include-md file="shared-content/self-managed/enterprise-sso/dns-records.md" >}}

### Step 5: Verify the Deployment

{{< include-md file="shared-content/self-managed/enterprise-sso/verify.md" >}}

If you haven't configured an identity provider yet, see
[Configure identity
providers](/self-managed-deployments/enterprise-sso/identity-providers/).

## Customizing Your Deployment

You can override module inputs independently. For details on the per-cloud
modules, see the [top-level
README](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
and the [Azure-specific
README](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure).

Notes specific to Azure:

- **PostgreSQL version**: The example targets PostgreSQL 18 on Azure
  Database for PostgreSQL Flexible Server. The `azurerm` provider declaration
  in `versions.tf` is pinned at `>= 4.55.0` to support PG 18.
- **AKS API server access**: `k8s_apiserver_authorized_networks` has no
  default. Production deployments should pin a tight allowlist instead of
  `0.0.0.0/0`.
- **VM sizes**: Default node pool uses `Standard_D4ps_v6` (arm64). The
  Materialize node pool uses `Standard_E*` instances. Adjust via
  `materialize_nodepool` and the default pool size variables.

## Cleanup

```bash
terraform destroy
```

{{< include-md file="shared-content/self-managed/enterprise-sso/destroy-finalizer-note.md" >}}

## See Also

- [Configure identity providers](/self-managed-deployments/enterprise-sso/identity-providers/)
- [Operations](/self-managed-deployments/enterprise-sso/operations/)
- [Troubleshooting](/self-managed-deployments/enterprise-sso/troubleshooting/)
- [Install on Azure (base Materialize stack)](/self-managed-deployments/installation/install-on-azure/)
