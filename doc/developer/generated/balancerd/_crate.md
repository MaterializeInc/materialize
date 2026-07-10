---
source: src/balancerd/src/lib.rs
revision: e725791107
---

# balancerd

Implements the `balancerd` ingress router: a horizontally scalable, stateless proxy that terminates pgwire and HTTPS connections and forwards them to the correct `environmentd` instance.

For pgwire, the balancer reads the startup message, then resolves the destination via `BalancerResolver`: in multi-tenant mode it either uses the TLS SNI hostname directly (via `SniTemplate` and `TenantDnsResolver`) or falls back to Frontegg authentication to obtain a tenant ID and resolve a DNS name from it. In static mode it resolves a fixed address. The raw TCP stream is then proxied to the target.
For HTTPS, `HttpsBalancer` always resolves through a `TenantDnsResolver`; in multi-tenant mode this resolver is shared with the pgwire listener. The SNI hostname's first label is substituted into the address template to produce a Kubernetes hostname, which is resolved via CNAME to extract the tenant and via A record to get the target IP.
`BalancerService` drives three listeners (pgwire, HTTPS, internal HTTP) managed by `mz-server-core::serve`; dynamic configuration is synced from LaunchDarkly or a file at startup and periodically thereafter.
DNS resolution uses `hickory_resolver` backed by the system DNS configuration, with caching delegated to the node-local DNS infrastructure. `TenantDnsResolver` resolves CNAME records separately from A records to extract the tenant ID from the CNAME target.
Key dependencies are `mz-frontegg-auth`, `mz-server-core`, `mz-pgwire-common`, `mz-dyncfg-launchdarkly`, `mz-dyncfg-file`, and `hickory-resolver`.

## Modules

* `codec` — pgwire framing layer for the authentication handshake.
* `dyncfgs` — dynamic configuration parameters and LaunchDarkly/file sync helpers.
