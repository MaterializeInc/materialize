---
source: src/authenticator/src/lib.rs
revision: 66acdcae9d
---

# mz-authenticator

Provides the `Authenticator` enum that unifies the authentication backends available to Materialize's pgwire layer: Frontegg, password, SASL, OIDC, and unauthenticated.
The `oidc` submodule contains the full OIDC/JWT implementation; the other backends (`FronteggAuthenticator`, `AdapterClient`) are imported from their own crates.
Consumed by the pgwire server to dispatch per-connection authentication.

## Module structure

* `oidc` — OIDC/JWT validation via JWKS
