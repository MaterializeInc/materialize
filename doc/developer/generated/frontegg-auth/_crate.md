---
source: src/frontegg-auth/src/lib.rs
revision: ad614d5d6a
---

# frontegg-auth

Provides authentication integration with Frontegg, translating Materialize `AppPassword` credentials into verified JWT claims via the Frontegg token API.
The crate's public surface is: `Authenticator` + `AuthenticatorConfig` (session authentication and token refresh), `Client` + `ApiTokenArgs` + `ApiTokenResponse` (HTTP token exchange), `AppPassword` + `AppPasswordParseError` (credential parsing), and `Error`.
`FronteggCliArgs` offers a `clap`-based argument parser for configuring Frontegg from the command line.
Key dependencies are `mz-auth`, `mz-ore`, `mz-repr`, `jsonwebtoken`, `reqwest-middleware`, and `reqwest-retry`; it is consumed by `mz-pgwire` and `mz-environmentd`.
