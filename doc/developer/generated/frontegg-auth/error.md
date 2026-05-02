---
source: src/frontegg-auth/src/error.rs
revision: 4b9700a8af
---

# frontegg-auth::error

Defines the `Error` enum covering all failure modes in the crate: invalid password format, malformed JWT, HTTP exchange failures (reqwest and middleware), missing or expired claims, unauthorized tenant, wrong user, name-length violations, and internal errors.
All variants implement `Clone` so that errors can be shared across async tasks.
