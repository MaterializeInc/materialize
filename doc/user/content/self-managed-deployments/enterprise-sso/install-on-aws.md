---
title: "Install on AWS"
description: "Deploy the Ory-based enterprise SSO stack on AWS with Materialize."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-aws"
    weight: 20
---

This guide walks through the
[`aws/examples/enterprise`](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws/examples/enterprise)
example in the [Materialize Terraform
repository](https://github.com/MaterializeInc/materialize-terraform-self-managed),
which extends the base [Install on
AWS](/self-managed-deployments/installation/install-on-aws/) walkthrough with
the Ory-based enterprise SSO stack on EKS.

{{% self-managed/materialize-components-sentence %}} This example layers the
Ory stack (Kratos, Hydra, the selfservice UI, and optional Polis) on top so
that the Materialize console authenticates users through OIDC, with SAML and
SCIM available when Polis is enabled.

{{< note >}}

{{< self-managed/terraform-disclaimer >}}

{{< /note >}}

## What Gets Created

This example provisions everything from the base [Install on
AWS](/self-managed-deployments/installation/install-on-aws/) guide, plus the
additions below.

### Networking

| Resource | Description |
|----------|-------------|
| Public Hostnames | Six browser-facing hostnames (Hydra, Kratos, the selfservice UI, optional Polis, the Materialize console, balancerd). DNS records are created by you after `terraform apply`. |
| LoadBalancer Services | One per browser-facing service in the `ory` and `materialize-environment` namespaces. Backed by AWS Network Load Balancers via the AWS Load Balancer Controller, target type `ip`. |

### Database

| Resource | Description |
|----------|-------------|
| Ory Kratos RDS | Dedicated RDS instance for the `kratos` database. PostgreSQL 18, `db.t3.small`. |
| Ory Hydra RDS | Dedicated RDS instance for the `hydra` database. PostgreSQL 18, `db.t3.small`. |
| Ory Polis RDS (optional) | Dedicated RDS instance for the `polis` database when `enable_polis = true`. PostgreSQL 18, `db.t3.small`. |
| User | `oryadmin` shared across instances, with auto-generated password. |

AWS RDS is one-database-per-instance, so each Ory component gets its own RDS
instance. (GCP Cloud SQL, in contrast, hosts them as separate databases on a
single shared instance.)

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

## AWS-specific requirements

Cross-cutting requirements (license key with the `ory` entitlement, DNS
hostnames, cert-manager strategy, required tools) are covered on the shared
[Prerequisites](/self-managed-deployments/enterprise-sso/prerequisites/)
page. This section only lists the AWS-specific bits.

An active AWS account with permission to create:

- EKS clusters and Karpenter nodepools
- RDS instances
- S3 buckets
- VPCs and networking resources
- IAM roles and policies

## Getting Started: Enterprise SSO Example

{{< note >}}

{{< self-managed/terraform-disclaimer >}}

{{< /note >}}

{{< tip >}}

* {{% self-managed/terraform-enterprise-example-tip %}}

{{< /tip >}}

### Step 1: Set Up the Environment

1. Open a terminal window.

1. Clone the Materialize Terraform repository and go to the
   `aws/examples/enterprise` directory:

   ```bash
   git clone https://github.com/MaterializeInc/materialize-terraform-self-managed.git
   cd materialize-terraform-self-managed/aws/examples/enterprise
   ```

1. Ensure your AWS CLI is configured with the appropriate profile, substituting
   `<your-aws-profile>` with the profile to use:

   ```bash
   export AWS_PROFILE=<your-aws-profile>
   ```

### Step 2: Configure Terraform Variables

1. Create a `terraform.tfvars` file with the required variables:

   - `aws_region`: AWS region (defaults to `us-east-1`)
   - `aws_profile`: AWS CLI profile to use
   - `name_prefix`: Prefix for all resource names
   - `license_key`: Materialize license key JWT with the `ory` entitlement
   - `k8s_apiserver_authorized_networks`: CIDRs allowed to reach the EKS API server (required, no default)
   - `ory_hydra_fqdn`, `ory_ui_fqdn`, `ory_kratos_fqdn`, `materialize_console_fqdn`, `materialize_balancerd_fqdn`: Public hostnames for the browser-facing services
   - `tags`: Map of tags to apply to resources

   ```hcl
   aws_region  = "us-east-1"
   aws_profile = "default"
   name_prefix = "mz-enterprise"
   license_key = "your-materialize-license-key"

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

1. {{% include-headless "/headless/self-managed-deployments/enterprise-sso/polis-tfvars" %}}

1. {{% include-headless "/headless/self-managed-deployments/enterprise-sso/cert-issuer-tfvars" %}}

1. {{% include-headless "/headless/self-managed-deployments/enterprise-sso/upstream-oidc-tfvars" %}}

### Step 3: Apply the Terraform

1. Initialize the Terraform directory:

   ```bash
   terraform init
   ```

1. Apply the Terraform configuration:

   ```bash
   terraform apply
   ```

   Expect 30 to 45 minutes for the full apply. The slowest parts are EKS
   provisioning, the RDS instances, and the Materialize instance reaching
   ready.

1. Configure `kubectl` against the new cluster:

   ```bash
   aws eks update-kubeconfig \
     --name $(terraform output -raw eks_cluster_name) \
     --region <your-aws-region>
   ```

### Step 4: Create DNS Records

{{% include-headless "/headless/self-managed-deployments/enterprise-sso/dns-records" %}}

### Step 5: Verify the Deployment

{{% include-headless "/headless/self-managed-deployments/enterprise-sso/verify" %}}

If you haven't configured an identity provider yet, see
[Configure identity
providers](/self-managed-deployments/enterprise-sso/identity-providers/).

## Customizing Your Deployment

You can override module inputs independently. For details on the per-cloud
modules, see the [top-level
README](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
and the [AWS-specific
README](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws).

Notes specific to AWS:

- **One RDS per Ory component**: AWS RDS is one-database-per-instance, so
  Kratos, Hydra, and Polis (when enabled) each get their own `db.t3.small`
  RDS instance. GCP Cloud SQL hosts them as separate databases on a single
  shared instance.
- **EKS API server access**: `k8s_apiserver_authorized_networks` has no
  default. Production deployments should pin a tight allowlist instead of
  `0.0.0.0/0`.
- **Karpenter nodepools**: The generic nodepool defaults to `t4g.xlarge`
  (arm64 Graviton); the Materialize nodepool uses `r7gd.2xlarge`. Both use
  Bottlerocket. Override via the `instance_types_*` locals in `main.tf`.
- **NLB target type `ip`**: Ory and console Services are exposed via Network
  Load Balancers with the `ip` target type, so traffic goes directly to pod
  IPs without an intermediate node hop.

## Cleanup

```bash
terraform destroy
```

{{< note >}}
**AWS-specific teardown gotchas:** the AWS Load Balancer Controller and
Karpenter can deadlock each other on destroy. If `terraform destroy` hangs,
you may need to manually delete Karpenter-managed nodes and the LBC-created
NLB target groups before the destroy can finish. See the example
[README](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws/examples/enterprise#destroy)
for the exact cleanup commands.
{{</ note >}}

{{% include-headless "/headless/self-managed-deployments/enterprise-sso/destroy-finalizer-note" %}}

## See Also

- [Configure identity providers](/self-managed-deployments/enterprise-sso/identity-providers/)
- [Operations](/self-managed-deployments/enterprise-sso/operations/)
- [Troubleshooting](/self-managed-deployments/enterprise-sso/troubleshooting/)
- [Install on AWS (base Materialize stack)](/self-managed-deployments/installation/install-on-aws/)
