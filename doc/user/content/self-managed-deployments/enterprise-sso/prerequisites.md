---
title: "Prerequisites"
description: "Requirements to deploy the Ory-based enterprise SSO stack."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-prerequisites"
    weight: 10
---

Before running the enterprise example for your cloud, you'll need:

## Materialize license key with the `ory` entitlement

The Ory components ship as private OEL (Ory Enterprise License) images.
Materialize hosts a registry proxy at `ory.registry.cloud.materialize.com`
that pulls these images on behalf of customers. Authentication to the
proxy uses your Materialize license key JWT (passed as the password in a
Kubernetes `imagePullSecret`), so you don't need a separate Ory
credential.

The license key has to carry the `ory` entitlement. Contact Materialize
sales or support to have one issued.

{{< note >}}
The license key is also used by Materialize itself; the same JWT covers
both. There's no separate "Ory key" you have to manage.
{{</ note >}}

## Cluster egress

The Ory pods need network egress to two hosts:

- `ory.registry.cloud.materialize.com` (the registry proxy)
- `storage.googleapis.com` (the proxy returns HTTP 307 redirects to
  signed GCS URLs for blob layers, which the kubelet follows directly)

If your cluster has egress restrictions or a NAT gateway with allowlist
rules, both hosts must be reachable.

## DNS hostnames

You need DNS hostnames you control for each browser-facing service.
With Polis enabled, that's six hostnames:

| Hostname | Purpose |
|---|---|
| `hydra.example.com` | OAuth2 / OIDC issuer that Materialize trusts |
| `kratos.example.com` | Kratos public API; browser-side redirect target |
| `auth.example.com` | Selfservice UI (login, consent, registration pages) |
| `polis.example.com` | Polis (SAML ACS, SCIM endpoint, OIDC token endpoint). Only when Polis is enabled. |
| `console.example.com` | Materialize console |
| `balancerd.example.com` | Materialize's SQL-over-HTTP endpoint. The console JS calls this from the browser, so it needs a public hostname and a trusted TLS cert. |

You'll create A records pointing at the LoadBalancer IPs after the first
`terraform apply`. The example deployment doesn't create the DNS records
for you; the per-cloud install pages show the exact commands to grab the
LB IPs.

## cert-manager and a ClusterIssuer

The example deploys cert-manager into the cluster and uses it to
provision TLS certificates for each browser-facing hostname. You can
choose one of three modes:

### In-cluster self-signed (demos and air-gapped clusters)

Default behaviour if you don't override `cert_issuer_ref`. cert-manager
generates an in-cluster CA and signs all browser-facing certs from it.
Browsers will not trust the certs out of the box.

Suitable for offline demos or proof-of-concept clusters where no public
DNS / ACME path is available. Production deployments should use a real
issuer.

### Bring your own ClusterIssuer

Set `cert_issuer_ref` in tfvars to point at an existing ClusterIssuer
managed outside of the example. Typical sources: a corporate CA, an
ACME issuer (Let's Encrypt) already configured for other workloads, or
a managed cloud issuer.

```hcl
cert_issuer_ref = {
  name = "letsencrypt-prod"
  kind = "ClusterIssuer"
}
```

The browser-facing certs use this issuer. The internal mTLS cert
between Materialize components continues to use the in-cluster
self-signed cluster issuer because it includes `*.cluster.local` SANs
that public ACME issuers cannot sign.

### Let's Encrypt with cert-manager DNS-01

For new deployments where you want browser-trusted certs without a
managed cloud cert service, the example supports a Let's Encrypt
ClusterIssuer backed by cert-manager's DNS-01 solver. Cloudflare,
Route 53, Azure DNS, and Google Cloud DNS are all supported by
cert-manager out of the box.

A starter `letsencrypt.tf` block is documented in the README of each
per-cloud enterprise example. Drop it into your root module, set your
DNS provider API token, and set `cert_issuer_ref` to
`{ name = "letsencrypt-prod", kind = "ClusterIssuer" }`.

## Polis-specific prerequisites (only when `enable_polis = true`)

Polis has two upstream issues that the example works around with extra
configuration. These are temporary and tracked with Ory; the
requirements below will go away when Ory ships the fixes.

### Dedicated amd64 node pool

The Polis OEL image (`polis-oel`) is currently published as `amd64` only.
Kratos, Hydra, and the selfservice UI all ship multi-arch images and
can run on any cluster, but Polis cannot run on `arm64` nodes.

If your cluster's default node pool runs on `arm64` instances (Azure
ARM, AWS Graviton, GCP Tau, Ampere), you need to add a small `amd64`
node pool and pin Polis to it via Helm values:

```hcl
polis_helm_values = {
  deployment = {
    nodeSelector = { workload = "polis-amd64" }
  }
  job = {
    nodeSelector = { workload = "polis-amd64" }
  }
}
```

One `Standard_B2s` (Azure) / `t3.small` (AWS) / `e2-small` (GCP)
instance is enough. Both the Polis Deployment and the chart's
pre-install migration Job need the selector, hence the two blocks.

### GCP service-account JSON key for the chart pull

The Polis Helm chart is published as a private OCI artifact on GCP
Artifact Registry. The OEL registry proxy currently handles image pulls
but doesn't yet serve OCI Helm chart manifests, so the chart needs to
be pulled directly from GCP with a service account key.

Set the path to the key file in tfvars:

```hcl
ory_polis_oci_chart_key_file = "/path/to/ory-artifacts-sa-key.json"
```

Contact Materialize sales or support to be issued a key. This will go
away once the chart is published publicly or the OEL registry proxy
gains OCI chart support.

## Tooling

Local tools you'll need:

- `terraform` (>= 1.8)
- `kubectl`
- `helm` (only if you want to inspect chart values)
- The cloud CLI for the cloud you're deploying to (`az`, `gcloud`, or
  `aws`)
- `jq` (optional, helpful when piping through admin API responses)

## Next steps

Once you have the license key, DNS plan, and cert-manager strategy
sorted, pick your cloud and follow the install guide:

- [Install on Azure](/self-managed-deployments/enterprise-sso/install-on-azure/)
- [Install on GCP](/self-managed-deployments/enterprise-sso/install-on-gcp/)
- [Install on AWS](/self-managed-deployments/enterprise-sso/install-on-aws/)
