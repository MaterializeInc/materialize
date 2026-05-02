---
source: src/frontegg-auth/src/client.rs
revision: b725ec9d8f
---

# frontegg-auth::client

Defines `Client`, an HTTP client wrapper (backed by `reqwest-middleware`) configured with exponential-backoff retry for transient failures, used to make requests to the Frontegg API.
`Client::environmentd_default()` builds a client with 5 s timeout and retries up to 30 s total, following the defaults used in `environmentd`.
The `tokens` submodule implements the actual token-exchange operation.
