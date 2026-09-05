---
headless: true
---
After `terraform apply`, list the LoadBalancer ingress addresses:

```bash
kubectl get svc -A -o jsonpath='{range .items[?(@.spec.type=="LoadBalancer")]}{.metadata.namespace}/{.metadata.name}{"\t"}{.status.loadBalancer.ingress[0].ip}{"\t"}{.status.loadBalancer.ingress[0].hostname}{"\n"}{end}'
```

Create DNS records pointing the six browser-facing hostnames at the corresponding LB IP (Azure, GCP) or hostname (AWS):

| Hostname | Backed by Service |
|----------|-------------------|
| `hydra.example.com` | `ory/hydra-public-lb` |
| `kratos.example.com` | `ory/kratos-public-lb` |
| `auth.example.com` | `ory/ory-selfservice-ui-lb` |
| `polis.example.com` | `ory/polis-public-lb` (only when `enable_polis = true`) |
| `console.example.com` | `materialize-environment/main-console-https` |
| `balancerd.example.com` | `materialize-environment/<release-id>-balancerd-lb` |

cert-manager issues TLS certs as soon as DNS resolves. Wait for all Certificates to report `READY=True`:

```bash
kubectl get certificate -A -w
```

The first certificate issuance typically takes 1 to 3 minutes per cert when using ACME (Let's Encrypt DNS-01); in-cluster self-signed certs issue near-instantly.
