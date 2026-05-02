---
source: src/oidc-mock/src/lib.rs
revision: 03db92b55b
---

# mz-oidc-mock

In-process OIDC mock server for integration tests, built on Axum and `jsonwebtoken`.
`OidcMockServer::start` binds a TCP listener, generates RSA key material, and serves `/.well-known/jwks.json` and `/.well-known/openid-configuration` endpoints exposing the public key.
`generate_jwt` signs RS256 tokens with configurable subject, audience, expiry, and arbitrary extra claims, allowing tests to exercise JWT validation logic without a real identity provider.
