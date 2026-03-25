---
source: src/balancerd/src/lib.rs
revision: 4267863081
---

# balancerd

Implements the `balancerd` ingress router: a horizontally scalable, stateless proxy that terminates pgwire and HTTPS connections and forwards them to the correct `environmentd` instance.

For pgwire, the balancer reads the startup message, optionally authenticates via Frontegg to obtain a tenant ID, resolves a DNS name derived from the tenant ID or SNI hostname to get a target IP, and then proxies the raw TCP stream.
For HTTPS, it uses the TLS SNI hostname to construct and resolve an internal DNS address, then forwards the connection over (optionally TLS-wrapped) TCP.
`BalancerService` drives three listeners (pgwire, HTTPS, internal HTTP) managed by `mz-server-core::serve`; dynamic configuration is synced from LaunchDarkly or a file at startup and periodically thereafter.
Key dependencies are `mz-frontegg-auth`, `mz-server-core`, `mz-pgwire-common`, `mz-dyncfg-launchdarkly`, and `mz-dyncfg-file`.

## Modules

* `codec` — pgwire framing layer for the authentication handshake.
* `dyncfgs` — dynamic configuration parameters and LaunchDarkly/file sync helpers.
