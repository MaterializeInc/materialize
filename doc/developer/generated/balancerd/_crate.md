---
source: src/balancerd/src/lib.rs
revision: 05c9980c85
---

# balancerd

Implements the `balancerd` ingress router: a horizontally scalable, stateless proxy that terminates pgwire and HTTPS connections and forwards them to the correct `environmentd` instance.

For pgwire, the balancer reads the startup message, then resolves the destination via `Resolver`: in multi-tenant mode (`Resolver::MultiTenant`) it either uses the TLS SNI hostname directly via an optional `SniResolver` (which holds a `StubResolver`, an address template, and a port) or falls back to Frontegg authentication (`FronteggResolver`) to obtain a tenant ID and resolve a DNS name from it. In static mode (`Resolver::Static`) it resolves a fixed address. The raw TCP stream is then proxied to the target.
For HTTPS, `HttpsBalancer` resolves using a `StubResolver` backed by the system DNS configuration. The SNI hostname's first label is substituted into the address template to produce a Kubernetes hostname, which is resolved via CNAME to extract the tenant and via A record to get the target IP.
`BalancerService` drives three listeners (pgwire, HTTPS, internal HTTP) managed by `mz-server-core::serve`; dynamic configuration is synced from LaunchDarkly or a file at startup and periodically thereafter.
DNS resolution uses `hickory_resolver` (`StubResolver`) backed by the system DNS configuration. CNAME records are resolved separately from A records to extract the tenant ID from the CNAME target; the `StubResolverExt::tenant` helper encapsulates this logic.
Key dependencies are `mz-frontegg-auth`, `mz-server-core`, `mz-pgwire-common`, `mz-dyncfg-launchdarkly`, `mz-dyncfg-file`, and `hickory-resolver`.

## Modules

* `codec` — pgwire framing layer for the authentication handshake.
* `dyncfgs` — dynamic configuration parameters and LaunchDarkly/file sync helpers.
