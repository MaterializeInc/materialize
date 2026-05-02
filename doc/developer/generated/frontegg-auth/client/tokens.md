---
source: src/frontegg-auth/src/client/tokens.rs
revision: 4267863081
---

# frontegg-auth::client::tokens

Implements `Client::exchange_client_secret_for_token`, which POSTs an `ApiTokenArgs` (client ID + secret) to the Frontegg token endpoint and returns an `ApiTokenResponse` (access token, refresh token, expiry).
Records request duration and HTTP status in `Metrics` and propagates the Frontegg trace ID in log messages for debugging.
Defines the `ApiTokenArgs` and `ApiTokenResponse` serializable structs.
