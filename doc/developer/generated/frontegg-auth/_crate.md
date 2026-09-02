---
source: src/frontegg-auth/src/lib.rs
revision: 4061850066
---

# frontegg-auth

Provides authentication integration with Frontegg, translating Materialize `AppPassword` credentials into verified JWT claims via the Frontegg token API.
The crate's public surface is: `Authenticator` + `AuthenticatorConfig` (session authentication and token refresh), `Client` + `ApiTokenArgs` + `ApiTokenResponse` (HTTP token exchange), `AppPassword` + `AppPasswordParseError` (credential parsing), and `Error`.
`FronteggCliArgs` offers a `clap`-based argument parser for configuring Frontegg from the command line; it includes `--frontegg-oauth-issuer-url` (env `FRONTEGG_OAUTH_ISSUER_URL`) and exposes it via `oauth_issuer_url() -> Option<&str>` for use in MCP OAuth discovery.
Key dependencies are `mz-auth`, `mz-ore`, `mz-repr`, `jsonwebtoken`, `reqwest-middleware`, and `reqwest-retry`; it is consumed by `mz-pgwire` and `mz-environmentd`.
