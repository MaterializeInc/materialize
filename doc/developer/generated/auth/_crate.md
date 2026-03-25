---
source: src/auth/src/lib.rs
revision: c61ef02a01
---

# mz-auth

Provides shared authentication logic for Materialize, including password representation and SCRAM-SHA-256 credential management.

## Module structure

* `password` — the `Password` newtype with redacted debug output.
* `hash` — PBKDF2-SHA-256 hashing and SCRAM-SHA-256 / SASL verification primitives.

The crate root (`lib.rs`) re-exports these two modules and defines the `Authenticated` sentinel type, a zero-sized struct that signals a successfully authenticated session.

## Key types

* `Authenticated` — constructed only by authenticators to mark a valid session; also used when authentication is disabled.
* `Password` — a `String` wrapper that never exposes its value through `Display` or `Debug`.
* `hash::HashOpts`, `hash::PasswordHash` — inputs and outputs of the PBKDF2 primitive.

## Dependencies

Depends on `openssl` (vendored), `base64`, `itertools`, `serde`, and `mz-ore`.
Consumed by authentication middleware and session-establishment code throughout Materialize.
