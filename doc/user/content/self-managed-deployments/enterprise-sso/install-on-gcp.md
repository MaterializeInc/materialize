---
title: "Install on GCP"
description: "Deploy the Ory-based enterprise SSO stack on GCP with Materialize."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-gcp"
    weight: 30
---

This guide walks through the
[`gcp/examples/enterprise`](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/gcp/examples/enterprise)
example in the [Materialize Terraform
repository](https://github.com/MaterializeInc/materialize-terraform-self-managed),
which extends the base [Install on
GCP](/self-managed-deployments/installation/install-on-gcp/) walkthrough with
the Ory-based enterprise SSO stack on GKE.

{{% self-managed/materialize-components-sentence %}} This example layers the
Ory stack (Kratos, Hydra, the selfservice UI, and optional Polis) on top so
that the Materialize console authenticates users through OIDC, with SAML and
SCIM available when Polis is enabled.

{{< note >}}

{{< self-managed/terraform-disclaimer >}}

{{< /note >}}

Before starting, work through the
[Prerequisites](/self-managed-deployments/enterprise-sso/prerequisites/): a
license key carrying the `ory` entitlement, the six browser-facing DNS
hostnames, and a cert-manager strategy.

## What Gets Created

This example provisions everything from the base [Install on
GCP](/self-managed-deployments/installation/install-on-gcp/) guide, plus the
additions below.

### Networking

| Resource | Description |
|----------|-------------|
| Public Hostnames | Six browser-facing hostnames (Hydra, Kratos, the selfservice UI, optional Polis, the Materialize console, balancerd). DNS records are created by you after `terraform apply`. |
| LoadBalancer Services | One per browser-facing service in the `ory` and `materialize-environment` namespaces. Backed by GCP network load balancers. |

### Database

| Resource | Description |
|----------|-------------|
| Ory Cloud SQL for PostgreSQL | Separate instance from the Materialize backend. PostgreSQL 18, `db-f1-micro` tier, private IP only. |
| Databases | `kratos`, `hydra`, plus `polis` when `enable_polis = true`. Cloud SQL supports multiple databases on a single instance, so all three Ory components share this instance. |
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

## Prerequisites

### GCP Account Requirements

A Google account with permission to enable the required APIs on your project and to create:

- GKE clusters and node pools
- Cloud SQL instances and VPC peering
- Cloud Storage buckets
- VPC networks, subnets, Cloud Router, Cloud NAT
- Service accounts and IAM bindings

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [gcloud CLI](https://cloud.google.com/sdk/docs/install)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)
- [GKE auth plugin](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#install_plugin)

### License Key

{{< yaml-table data="self_managed/license_key" >}}

The same license key authenticates pulls from the Materialize-hosted Ory
registry proxy (`ory.registry.cloud.materialize.com`), so it must carry the
`ory` entitlement. See [Prerequisites: License
key](/self-managed-deployments/enterprise-sso/prerequisites/#license-key-with-the-ory-entitlement).

## Getting Started: Enterprise SSO Example

{{< note >}}

{{< self-managed/terraform-disclaimer >}}

{{< /note >}}

### Step 1: Set Up the Environment

1. Open a terminal window.

1. Clone the Materialize Terraform repository and go to the
   `gcp/examples/enterprise` directory:

   ```bash
   git clone https://github.com/MaterializeInc/materialize-terraform-self-managed.git
   cd materialize-terraform-self-managed/gcp/examples/enterprise
   ```

1. Authenticate with Google Cloud and select your project:

   ```bash
   gcloud auth application-default login
   gcloud config set project <your-project-id>
   ```

### Step 2: Configure Terraform Variables

1. Create a `terraform.tfvars` file with the required variables:

   - `project_id`: GCP project ID
   - `region`: GCP region (defaults to `us-central1`)
   - `name_prefix`: Prefix for all resource names
   - `license_key`: Materialize license key JWT with the `ory` entitlement
   - `k8s_apiserver_authorized_networks`: CIDRs allowed to reach the GKE master endpoint (required, no default)
   - `ory_hydra_fqdn`, `ory_ui_fqdn`, `ory_kratos_fqdn`, `materialize_console_fqdn`, `materialize_balancerd_fqdn`: Public hostnames for the browser-facing services
   - `labels`: Map of labels to apply to resources

   ```hcl
   project_id  = "my-gcp-project-id"
   region      = "us-central1"
   name_prefix = "mz-enterprise"
   license_key = "your-materialize-license-key"

   k8s_apiserver_authorized_networks = [
     {
       cidr_block   = "0.0.0.0/0"   # tighten for production
       display_name = "lab"
     },
   ]

   ory_hydra_fqdn             = "hydra.example.com"
   ory_ui_fqdn                = "auth.example.com"
   ory_kratos_fqdn            = "kratos.example.com"
   materialize_console_fqdn   = "console.example.com"
   materialize_balancerd_fqdn = "balancerd.example.com"

   labels = {
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

   Expect 30 to 45 minutes for the full apply. The slowest parts are GKE
   provisioning, the two Cloud SQL instances, and the Materialize instance
   reaching ready.

1. Configure `kubectl` against the new cluster:

   ```bash
   gcloud container clusters get-credentials \
     $(terraform output -raw gke_cluster_name) \
     --region $(terraform output -raw gke_cluster_location)
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
and the [GCP-specific
README](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/gcp).

Notes specific to GCP:

- **Shared Cloud SQL instance for Ory**: Cloud SQL supports multiple databases
  on a single instance, so Kratos, Hydra, and optional Polis share one
  `db-f1-micro` instance with separate databases. (AWS, in contrast,
  provisions one RDS instance per Ory component.)
- **GKE master access**: `k8s_apiserver_authorized_networks` has no default.
  Production deployments should pin a tight allowlist instead of `0.0.0.0/0`.
- **Egress to the OEL proxy**: GKE nodes need outbound access to
  `ory.registry.cloud.materialize.com` and `storage.googleapis.com`. The proxy
  returns HTTP 307 redirects to signed GCS URLs for blob layers, which the
  kubelet follows directly. Adjust your VPC firewall and Cloud NAT egress
  rules accordingly.
- **Node pools**: Default `e2-standard-8` for the generic pool;
  `n2-highmem-8` with one local SSD for the Materialize pool. Override via the
  `generic_nodepool` and `materialize_nodepool` variables.

## Cleanup

```bash
terraform destroy
```

{{< include-md file="shared-content/self-managed/enterprise-sso/destroy-finalizer-note.md" >}}

## See Also

- [Configure identity providers](/self-managed-deployments/enterprise-sso/identity-providers/)
- [Operations](/self-managed-deployments/enterprise-sso/operations/)
- [Troubleshooting](/self-managed-deployments/enterprise-sso/troubleshooting/)
- [Install on GCP (base Materialize stack)](/self-managed-deployments/installation/install-on-gcp/)
