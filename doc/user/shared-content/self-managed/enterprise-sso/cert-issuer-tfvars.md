To bring your own cert-manager `ClusterIssuer` for the browser-facing TLS certs (Hydra, Kratos, the selfservice UI, Polis, the Materialize console, and balancerd):

```hcl
cert_issuer_ref = {
  name = "letsencrypt-prod"
  kind = "ClusterIssuer"
}
```

If you want a Let's Encrypt issuer signed via DNS-01, the example's README ships a starter `letsencrypt.tf` snippet for Cloudflare, Route 53, Azure DNS, and Google Cloud DNS. Drop it next to `main.tf`, set your DNS provider API token, and point `cert_issuer_ref` at it.
