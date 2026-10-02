---
title: "Prerequisites"
description: "Requirements to deploy the advanced SSO stack."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-prerequisites"
    weight: 10
---

Before running the enterprise example for your cloud, gather the items
below.

## First, get a license key that includes the advanced SSO entitlements

The advanced SSO stack requires a Materialize enterprise license whose JWT
carries the `ory` entitlement. Community licenses don't include this
entitlement, and licenses issued before the entitlement existed will keep
working for Materialize itself but will be rejected by the Ory registry
proxy. Contact
[Materialize support](/support/) to have an
ory-enabled key issued.

## Allow cluster egress

The Ory pods need network egress to two hosts:

| Host | Purpose |
|------|---------|
| `ory.registry.cloud.materialize.com` | The Materialize-hosted Ory registry proxy. |
| `storage.googleapis.com` | The proxy returns HTTP 307 redirects to signed GCS URLs for blob layers, which the kubelet follows directly. |

If your cluster has egress restrictions or a NAT gateway with allowlist
rules, both hosts must be reachable. For example, to check from inside the
cluster:

```bash
kubectl run egress-check --rm -it --restart=Never --image=curlimages/curl -- \
  sh -c 'curl -sS -o /dev/null -w "%{http_code}\n" https://ory.registry.cloud.materialize.com/v2/; \
         curl -sS -o /dev/null -w "%{http_code}\n" https://storage.googleapis.com/'
```

Any HTTP status code, such as `401` from the registry or `400` from
`storage.googleapis.com`, means the host is reachable. A timeout or connection error means egress is blocked.

## Set up DNS hostnames

You need DNS hostnames you control for each browser-facing service:

| Hostname | Purpose |
|----------|---------|
| `hydra.example.com` | OAuth2 / OIDC issuer that Materialize trusts |
| `kratos.example.com` | Kratos public API; browser-side redirect target |
| `auth.example.com` | Selfservice UI (login, consent, registration pages) |
| `polis.example.com` | Polis (SAML ACS, SCIM endpoint, OIDC token endpoint). Only when Polis is enabled. |
| `console.example.com` | Materialize Console |
| `balancerd.example.com` | Materialize's SQL-over-HTTP endpoint. The console's browser-side JS calls this directly, so it needs a public hostname and a trusted TLS cert. |

You will create DNS records pointing at the LoadBalancer IPs (or hostnames,
on AWS) after the first `terraform apply`. The example does not create the
DNS records for you; the per-cloud install pages show the exact commands to
look up each LB.

## Install cert-manager and set up a `ClusterIssuer`

cert-manager is required to provision TLS certificates for each
browser-facing hostname. The [self-managed Terraform](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/kubernetes/modules/cert-manager)
provides a module to deploy it. cert-manager must be paired with a
`ClusterIssuer`, which you can configure in one of three modes:

### In-cluster self-signed (demos and air-gapped clusters)

The default when `cert_issuer_ref` is not set: cert-manager generates an
in-cluster CA and signs all browser-facing certs from it. Browsers will not
trust the certs out of the box.

Suitable for offline demos or proof-of-concept clusters where no public DNS
or ACME path is available. Production deployments should use a real issuer.

### Bring your own `ClusterIssuer`

Set `cert_issuer_ref` in tfvars to point at an existing `ClusterIssuer` you
manage yourself, outside the Materialize Terraform modules. Typical sources:
a corporate CA, an ACME issuer (Let's Encrypt) already configured for other
workloads, or a managed cloud issuer.

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
cert service, you can configure a Let's Encrypt `ClusterIssuer` backed by
cert-manager's DNS-01 solver. Cloudflare, Route 53, Azure DNS, and Google
Cloud DNS are all supported by cert-manager out of the box.

A starter `letsencrypt.tf` block is documented in the README of each
per-cloud enterprise example. Drop it into your root module, set your DNS
provider API token, and point `cert_issuer_ref` at it.

## Optional: Enable SAML

Polis is the SAML-to-OIDC bridge that acts as the SAML service provider for
your IdP. Kratos consumes it through its SAML sign-in method (`saml_providers`),
not as an upstream OIDC provider. Polis also exposes a SCIM endpoint for IdP-driven user
provisioning. It is off by default; opt in by setting `enable_polis = true`
and supplying `ory_polis_fqdn` in the per-cloud install.

The Polis Helm chart and image are pulled through the same OEL registry
proxy as the rest of the Ory stack, authenticated with the same license key
JWT.

## Required tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform) (>= 1.8)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/) (only required if you want to inspect chart values)
- The cloud CLI for your target cloud:
  [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli),
  [gcloud CLI](https://cloud.google.com/sdk/docs/install), or
  [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- `jq` (optional, helpful when piping through admin API responses)

## Next steps

Once you have the license key, DNS plan, and cert-manager strategy sorted,
add the stack to your existing installation, or pick your cloud and follow
the install guide:

- [Add to an existing installation](/self-managed-deployments/sso/advanced/existing-installation/)
- [Install on Azure](/self-managed-deployments/sso/advanced/install-on-azure/)
- [Install on GCP](/self-managed-deployments/sso/advanced/install-on-gcp/)
- [Install on AWS](/self-managed-deployments/sso/advanced/install-on-aws/)
