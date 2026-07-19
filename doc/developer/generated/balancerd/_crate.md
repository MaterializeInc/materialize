---
source: src/balancerd/src/lib.rs
revision: d381007629
---

# balancerd

Implements the `balancerd` ingress router: a horizontally scalable, stateless proxy that terminates pgwire and HTTPS connections and forwards them to the correct `environmentd` instance.

For pgwire, the balancer reads the startup message, then resolves the destination via `BalancerResolver`: in multi-tenant mode (`BalancerResolver::MultiTenant`) it either uses the TLS SNI hostname directly via an optional `SniTemplate` (which holds an address template and a port) or falls back to Frontegg authentication (`FronteggResolver`) to obtain a tenant ID and resolve a DNS name from it. In static mode (`BalancerResolver::Static`) it resolves a fixed address using `tokio::net::lookup_host` without caching. The raw TCP stream is then proxied to the target.
Resolution errors are typed as `ResolveError`, which distinguishes `InvalidPassword` (authentication failure), `Client` (client protocol violation, logged at `warn!`), `Upstream` (tenant backend unreachable, logged at `warn!`), and `Internal` (server-side fault, logged at `error!`); each maps to an appropriate `SqlState` code in the error sent to the client.
For HTTPS, `HttpsBalancer` resolves using a `TenantDnsResolver`. In multi-tenant mode the pgwire and HTTPS listeners share one `TenantDnsResolver`; in static mode HTTPS gets its own.
`BalancerService` drives three listeners (pgwire, HTTPS, internal HTTP) managed by `mz-server-core::serve`; dynamic configuration is synced from LaunchDarkly or a file at startup and periodically thereafter.
`TenantDnsResolver` wraps a `hickory_resolver::TokioResolver` (built from the system DNS configuration, with caching disabled) and provides `resolve_sni` (substitutes an SNI label into a template to get a hostname, then calls `resolve`), `resolve` (resolves CNAME to extract tenant, then A records for the IP), and `resolve_addr` (A records only, for when the tenant is already known). IP literals are returned directly without a DNS query. The helper `extract_tenant_from_cname` parses the tenant UUID from an environmentd CNAME target of the form `<service>.environment-<tenant_id>-<index>.svc.cluster.local`.
Key dependencies are `mz-frontegg-auth`, `mz-server-core`, `mz-pgwire-common`, `mz-dyncfg-launchdarkly`, `mz-dyncfg-file`, and `hickory-resolver`.

## Modules

* `codec` — pgwire framing layer for the authentication handshake.
* `dyncfgs` — dynamic configuration parameters and LaunchDarkly/file sync helpers.
