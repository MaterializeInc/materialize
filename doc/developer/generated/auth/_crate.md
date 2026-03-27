---
source: src/auth/src/lib.rs
revision: aa7a1afd31
---

# mz-auth

Provides shared authentication logic for Materialize, including password representation, SCRAM-SHA-256 credential management, and authenticator kind identification.

## Module structure

* `password` — the `Password` newtype with redacted debug output and zeroize-on-drop.
* `hash` — PBKDF2-SHA-256 hashing and SCRAM-SHA-256 / SASL verification primitives.

The crate root (`lib.rs`) re-exports these two modules and defines the `Authenticated` sentinel type and the `AuthenticatorKind` enum.

## Key types

* `AuthenticatorKind` — enum identifying which authentication mechanism was used: `Frontegg`, `Password`, `Sasl`, `Oidc`, or `None` (default).
* `Authenticated` — a zero-sized struct constructed only by authenticators to mark a valid session; also used when authentication is disabled.
* `Password` — a `String` wrapper that never exposes its value through `Display` or `Debug` and zeroizes on drop.
* `hash::HashOpts`, `hash::PasswordHash` — inputs and outputs of the PBKDF2 primitive.

## Dependencies

Depends on `openssl` (vendored), `base64`, `itertools`, `zeroize`, `serde`, and `mz-ore`.
Consumed by authentication middleware and session-establishment code throughout Materialize.
