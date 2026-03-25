---
source: src/frontegg-client/src/client.rs
revision: e757b4d11b
---

# frontegg-client::client

Provides the `Client` struct — the central API client for Frontegg — along with its authentication logic (both credentials-based and app-password-based) and JWT claims verification.
Exposes three submodules (`app_password`, `role`, `user`) that each extend `Client` with typed methods for their respective Frontegg API surfaces.
Token management is handled transparently: the client acquires and refreshes access tokens as needed, sharing state across threads via a `Mutex<Option<Auth>>`.
