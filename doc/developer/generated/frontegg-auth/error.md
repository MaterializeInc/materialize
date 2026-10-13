---
source: src/frontegg-auth/src/error.rs
revision: 0a070511dd
---

# frontegg-auth::error

Defines the `Error` enum covering all failure modes in the crate: invalid password format (`InvalidPasswordFormat`), malformed JWT (`InvalidTokenFormat`), HTTP exchange failures (`ReqwestError`, `MiddlewareError`), missing or expired claims (`MissingClaims`, `TokenExpired`), unauthorized tenant (`UnauthorizedTenant`), invalid app password (`InvalidAppPassword`), wrong user (`WrongUser`), name-length violations (`UserNameTooLong`), an invalid tenant API token user (`InvalidTenantApiTokenUser`), request timeouts (`Timeout`), and internal errors (`Internal`).
All variants implement `Clone` so that errors can be shared across async tasks. Wrapping types (`reqwest::Error`, `anyhow::Error`, `tokio::time::error::Elapsed`) are held behind `Arc` to satisfy the `Clone` requirement.
