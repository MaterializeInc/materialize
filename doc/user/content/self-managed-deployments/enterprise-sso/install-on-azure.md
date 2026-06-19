---
title: "Install on Azure"
description: "Deploy the Ory-based enterprise SSO stack on Azure with Materialize."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-azure"
    weight: 20
---

This guide walks through the Azure enterprise example in
[`materialize-terraform-self-managed`](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure/examples/enterprise),
which provisions a full Materialize + Ory deployment on AKS.

{{< note >}}
Before starting, work through the [prerequisites](/self-managed-deployments/enterprise-sso/prerequisites/):
license key with the `ory` entitlement, DNS hostnames, cert-manager
strategy, and (if you're deploying Polis on arm64 nodes) the amd64 node
pool and Polis chart key file.
{{</ note >}}

## What gets created

The Azure enterprise example provisions:

- A resource group
- A virtual network with AKS, Postgres, and API server subnets
- An AKS cluster with two node pools: a default pool for system + Ory
  workloads, and a dedicated `Standard_E*` pool for Materialize
  (tainted so only Materialize pods land there)
- Two Azure Database for PostgreSQL Flexible Server instances (one for
  Materialize, one shared by Kratos + Hydra + optional Polis)
- An Azure Storage account for Materialize's persistence backend
- The Materialize operator and a Materialize instance CR
- The Ory stack (Kratos, Hydra, selfservice UI) and optionally Polis,
  all in the `ory` namespace
- cert-manager, plus an optional Let's Encrypt + Cloudflare DNS-01
  ClusterIssuer if you drop in the `letsencrypt.tf` snippet
- Optional Prometheus and Grafana (`enable_observability = true` by
  default)

## Step 1. Configure required variables

Create `terraform.tfvars` in the example directory:

```hcl
subscription_id     = "12345678-1234-1234-1234-123456789012"
resource_group_name = "materialize-enterprise-rg"
name_prefix         = "mz-enterprise"
location            = "westus2"
license_key         = "<Materialize license key JWT with ory entitlement>"

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

To enable Polis (SAML + SCIM):

```hcl
enable_polis                 = true
ory_polis_fqdn           = "polis.example.com"
ory_polis_oci_chart_key_file = "/path/to/ory-artifacts-sa-key.json"

# Pin Polis to amd64 nodes (skip if your default node pool is amd64)
polis_helm_values = {
  deployment = { nodeSelector = { workload = "polis-amd64" } }
  job        = { nodeSelector = { workload = "polis-amd64" } }
}
```

To bring your own cert-manager issuer:

```hcl
cert_issuer_ref = {
  name = "letsencrypt-prod"
  kind = "ClusterIssuer"
}
```

If you're using Let's Encrypt, drop in a `letsencrypt.tf` next to
`main.tf` with the Cloudflare DNS-01 ClusterIssuer. A starter snippet is
in the example's README.

To federate logins through an OIDC IdP (Okta OIDC, Google Workspace,
Auth0 OIDC, Entra OIDC):

```hcl
upstream_oidc_providers = [
  {
    id            = "okta"
    provider      = "generic"
    client_id     = "<from Okta>"
    client_secret = "<from Okta>"
    issuer_url    = "https://your-org.okta.com/oauth2/default"
    scope         = ["openid", "email", "profile"]
    label         = "Sign in with Okta"
  },
]
```

See [Configure identity providers](/self-managed-deployments/enterprise-sso/identity-providers/) for SAML and
SCIM setup once the stack is up.

## Step 2. Add the amd64 node pool (only when Polis is enabled)

If your default AKS node pool is arm64 (it's recommended for cost on
Azure), Polis needs a small amd64 node pool. Add one with the label that
matches your `polis_helm_values` selector:

```bash
az aks nodepool add \
  --resource-group materialize-enterprise-rg \
  --cluster-name mz-enterprise-aks \
  --name polisamd \
  --node-vm-size Standard_B2s \
  --node-count 1 \
  --labels workload=polis-amd64
```

One small VM is enough. Both the Polis Deployment and its pre-install
migration Job will schedule there.

This step goes away when Ory ships a multi-arch `polis-oel` image.

## Step 3. Apply

```bash
terraform init
terraform apply
```

Expect 30 to 45 minutes for the full apply. The slowest parts are AKS
provisioning, the two Flexible Server instances, and the Materialize
instance reaching ready.

## Step 4. Create DNS records

After apply finishes, grab the LoadBalancer IPs:

```bash
kubectl get svc -A -o jsonpath='{range .items[?(@.spec.type=="LoadBalancer")]}{.metadata.namespace}/{.metadata.name}{"\t"}{.status.loadBalancer.ingress[0].ip}{"\n"}{end}'
```

Create A records (DNS-only, not proxied if you're on Cloudflare):

| Hostname | Backed by |
|---|---|
| `hydra.example.com` | `ory/hydra-public-lb` |
| `kratos.example.com` | `ory/kratos-public-lb` |
| `auth.example.com` | `ory/ory-selfservice-ui-lb` |
| `polis.example.com` | `ory/polis-public-lb` (only when `enable_polis = true`) |
| `console.example.com` | `materialize-environment/main-console-https` |
| `balancerd.example.com` | `materialize-environment/<release-id>-balancerd-lb` |

cert-manager will start issuing TLS certs as soon as the records
resolve. Wait for all Certificates to report `READY=True`:

```bash
kubectl get certificate -A -w
```

Typically 1-3 minutes per cert via Let's Encrypt DNS-01.

## Step 5. Verify

Smoke-test each component:

```bash
# Hydra OIDC discovery (should return JSON with issuer matching ory_hydra_fqdn)
curl -fsSL https://hydra.example.com/.well-known/openid-configuration | jq .issuer

# Kratos health
curl -fsSL https://kratos.example.com/health/ready

# Selfservice UI health
curl -fsSL https://auth.example.com/health/alive

# Polis health (when enabled)
curl -fsSL https://polis.example.com/api/health

# Materialize console
curl -fsSL -o /dev/null -w "%{http_code}\n" https://console.example.com
```

All should return 200 or a valid health response.

Open the Materialize console in a browser:

```
https://console.example.com
```

You should see the Kratos login screen. If you wired an
`upstream_oidc_providers` entry, you'll see a "Sign in with X" button.
If you have Polis enabled and registered a SAML connection, you'll see
a "Sign in via SAML" button too.

If you haven't configured an IdP yet, see
[Configure identity providers](/self-managed-deployments/enterprise-sso/identity-providers/) next.

## Step 6. Tear down

```bash
terraform destroy
```

{{< note >}}
**Known destroy issue:** `terraform destroy` can hang on the `ory`
namespace because the OAuth2Client CRD has a Hydra Maester finalizer
that's not always cleared before Maester itself is torn down. If the
destroy stalls on the namespace, run:

```bash
kubectl patch oauth2client materialize-oauth2-client -n ory \
  --type=json -p='[{"op":"remove","path":"/metadata/finalizers"}]'
```

Then re-run `terraform destroy`. We're tracking a cleaner fix.
{{</ note >}}

If you provisioned the amd64 node pool for Polis out of band (Step 2),
delete it separately:

```bash
az aks nodepool delete \
  --resource-group materialize-enterprise-rg \
  --cluster-name mz-enterprise-aks \
  --name polisamd
```

Or just delete the whole resource group if you're sure nothing else
lives in it.

## Notes specific to Azure

- **PostgreSQL version**: The example targets PostgreSQL 18 on Azure
  Database for PostgreSQL Flexible Server. The `azurerm` provider
  declaration in `versions.tf` is pinned at >= 4.55.0 to support PG 18
  as a valid version.
- **Balancerd hostname**: required input (no default). The Materialize
  console's browser-side JS calls balancerd directly, so it needs both
  a public hostname and a trusted TLS cert. The cert is provisioned
  automatically once the DNS record resolves.
- **AKS API server access**: `k8s_apiserver_authorized_networks` is a
  required input with no default; production deployments should pin a
  tight allowlist instead of `0.0.0.0/0`.
- **VM sizes**: Default node pool uses `Standard_D4ps_v6` (4 vCPU,
  16 GiB, arm64). Materialize node pool uses `Standard_E*` instances.
  Adjust via `materialize_nodepool` and the default pool size variables
  if you need different sizing.

## Next steps

- [Configure identity providers](/self-managed-deployments/enterprise-sso/identity-providers/) -- wire up
  Okta, Entra, Auth0, or another IdP
- [Operations](/self-managed-deployments/enterprise-sso/operations/) -- day-2 tasks: rotating credentials,
  adding OAuth2 clients, managing identities
- [Troubleshooting](/self-managed-deployments/enterprise-sso/troubleshooting/) -- common errors and fixes
