---
source: src/frontegg-client/src/error.rs
revision: 6c50c23bea
---

# frontegg-client::error

Defines `ApiError` (HTTP status code + messages) and the crate-level `Error` enum, which unifies authentication errors from `mz-frontegg-auth`, transport errors from `reqwest`, API errors, and JWT-related errors (empty/fetch/convert JWKS, decoding claims).
