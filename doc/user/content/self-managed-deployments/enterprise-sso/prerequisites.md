---
title: "Prerequisites"
description: "Requirements to deploy the Ory-based enterprise SSO stack."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-prerequisites"
    weight: 10
---

Before running the enterprise example for your cloud, gather the items
below.

## License Key with the `ory` Entitlement

{{< yaml-table data="self_managed/license_key" >}}

The Ory components ship as private OEL (Ory Enterprise License) images.
Materialize hosts a registry proxy at `ory.registry.cloud.materialize.com`
that pulls these images on behalf of customers. Authentication to the proxy
uses your Materialize license key JWT (passed as the password in a Kubernetes
`imagePullSecret`), so there is no separate Ory credential to manage.

The enterprise SSO stack requires a Materialize **enterprise** license whose
JWT carries the `ory` entitlement. Community licenses do not include this
entitlement and cannot pull the OEL images. If your existing license was
issued before the `ory` entitlement existed, contact Materialize sales or
support to have a replacement key issued; the old key will continue to work
for Materialize itself but the Ory registry proxy will reject it.

{{< note >}}
The license key is also used by Materialize itself; the same JWT covers both.
There is no separate "Ory key" to manage.
{{</ note >}}

## Cluster Egress

The Ory pods need network egress to two hosts:

| Host | Purpose |
|------|---------|
| `ory.registry.cloud.materialize.com` | The Materialize-hosted Ory registry proxy. |
| `storage.googleapis.com` | The proxy returns HTTP 307 redirects to signed GCS URLs for blob layers, which the kubelet follows directly. |

If your cluster has egress restrictions or a NAT gateway with allowlist
rules, both hosts must be reachable.

## DNS Hostnames

You need DNS hostnames you control for each browser-facing service. With
Polis enabled, that is six hostnames:

| Hostname | Purpose |
|----------|---------|
| `hydra.example.com` | OAuth2 / OIDC issuer that Materialize trusts |
| `kratos.example.com` | Kratos public API; browser-side redirect target |
| `auth.example.com` | Selfservice UI (login, consent, registration pages) |
| `polis.example.com` | Polis (SAML ACS, SCIM endpoint, OIDC token endpoint). Only when Polis is enabled. |
| `console.example.com` | Materialize console |
| `balancerd.example.com` | Materialize's SQL-over-HTTP endpoint. The console's browser-side JS calls this directly, so it needs a public hostname and a trusted TLS cert. |

You will create DNS records pointing at the LoadBalancer IPs (or hostnames,
on AWS) after the first `terraform apply`. The example does not create the
DNS records for you; the per-cloud install pages show the exact commands to
look up each LB.

## cert-manager and a `ClusterIssuer`

The example deploys cert-manager into the cluster and uses it to provision
TLS certificates for each browser-facing hostname. Pick one of three modes:

### In-Cluster Self-Signed (Demos and Air-Gapped Clusters)

Default behaviour when `cert_issuer_ref` is not set. cert-manager generates
an in-cluster CA and signs all browser-facing certs from it. Browsers will
not trust the certs out of the box.

Suitable for offline demos or proof-of-concept clusters where no public DNS
or ACME path is available. Production deployments should use a real issuer.

### Bring Your Own `ClusterIssuer`

Set `cert_issuer_ref` in tfvars to point at an existing `ClusterIssuer`
managed outside of the example. Typical sources: a corporate CA, an ACME
issuer (Let's Encrypt) already configured for other workloads, or a managed
cloud issuer.

```hcl
cert_issuer_ref = {
  name = "letsencrypt-prod"
  kind = "ClusterIssuer"
}
```

The browser-facing certs use this issuer. The internal mTLS cert between
Materialize components continues to use the in-cluster self-signed cluster
issuer because it includes `*.cluster.local` SANs that public ACME issuers
cannot sign.

### Let's Encrypt with cert-manager DNS-01

For new deployments that want browser-trusted certs without a managed cloud
cert service, the example supports a Let's Encrypt `ClusterIssuer` backed by
cert-manager's DNS-01 solver. Cloudflare, Route 53, Azure DNS, and Google
Cloud DNS are all supported by cert-manager out of the box.

A starter `letsencrypt.tf` block is documented in the README of each
per-cloud enterprise example. Drop it into your root module, set your DNS
provider API token, and point `cert_issuer_ref` at it.

## Polis (Optional, SAML and SCIM)

Polis is the SAML-to-OIDC bridge that lets Kratos consume a SAML IdP as an
upstream OIDC provider, and exposes a SCIM endpoint for IdP-driven user
provisioning. It is off by default; opt in by setting `enable_polis = true`
and supplying `ory_polis_fqdn` in the per-cloud install.

The Polis Helm chart and image are pulled through the same OEL registry
proxy as the rest of the Ory stack, authenticated with the same license key
JWT.

## Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform) (>= 1.8)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/) (only required if you want to inspect chart values)
- The cloud CLI for your target cloud:
  [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli),
  [gcloud CLI](https://cloud.google.com/sdk/docs/install), or
  [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- `jq` (optional, helpful when piping through admin API responses)

## Next Steps

Once you have the license key, DNS plan, and cert-manager strategy sorted,
pick your cloud and follow the install guide:

- [Install on Azure](/self-managed-deployments/enterprise-sso/install-on-azure/)
- [Install on GCP](/self-managed-deployments/enterprise-sso/install-on-gcp/)
- [Install on AWS](/self-managed-deployments/enterprise-sso/install-on-aws/)
