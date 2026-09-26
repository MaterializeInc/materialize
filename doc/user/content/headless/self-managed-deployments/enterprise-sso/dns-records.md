---
headless: true
---
After `terraform apply`, read the load balancer addresses from the Terraform outputs:

```bash
# Ory endpoints (all clouds)
terraform output ory_lb_addresses

# Console and balancerd, Azure and GCP
terraform output console_load_balancer_ip
terraform output balancerd_load_balancer_ip

# Console and balancerd, AWS (one NLB hostname serves both)
terraform output nlb_dns_name
```

Create DNS records pointing the browser-facing hostnames at those addresses: an A record for an IP (Azure, GCP), a CNAME for a hostname (AWS):

| Hostname | Address |
|----------|---------|
| `hydra.example.com` | `ory_lb_addresses.hydra` |
| `kratos.example.com` | `ory_lb_addresses.kratos` |
| `auth.example.com` | `ory_lb_addresses.ui` |
| `polis.example.com` | `ory_lb_addresses.polis` (only when `enable_polis = true`) |
| `console.example.com` | `console_load_balancer_ip` (Azure, GCP) or `nlb_dns_name` (AWS) |
| `balancerd.example.com` | `balancerd_load_balancer_ip` (Azure, GCP) or `nlb_dns_name` (AWS) |

cert-manager issues TLS certs as soon as DNS resolves. Wait for all Certificates to report `READY=True`:

```bash
kubectl get certificate -A -w
```

The first certificate issuance typically takes 1 to 3 minutes per cert when using ACME (Let's Encrypt DNS-01); in-cluster self-signed certs issue near-instantly.
