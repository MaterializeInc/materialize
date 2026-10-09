---
source: src/frontegg-auth/src/client.rs
revision: 0a070511dd
---

# frontegg-auth::client

Defines `Client`, an HTTP client wrapper (backed by `reqwest-middleware`) configured with exponential-backoff retry for transient failures, used to make requests to the Frontegg API.
`Client::environmentd_default()` builds a client with a 5 s per-request timeout and exponential backoff with bounds of 200 ms to 2 s, retrying for up to 30 s total, following the defaults used in `environmentd`.
The `tokens` submodule implements the actual token-exchange operation.
