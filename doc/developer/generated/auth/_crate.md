---
source: src/auth/src/lib.rs
revision: 33b8db85da
---

# mz-auth

Provides shared authentication logic for Materialize, including password representation, SCRAM-SHA-256 credential management, authenticator kind identification, and JWT group-claim extraction.

## Module structure

* `password` — the `Password` newtype with redacted debug output and zeroize-on-drop.
* `hash` — PBKDF2-SHA-256 hashing and SCRAM-SHA-256 / SASL verification primitives.
* `group_claims` — shared logic for extracting group names from JWT claims, used by both the Frontegg and OIDC authenticators.

The crate root (`lib.rs`) re-exports these three modules and defines the `Authenticated` sentinel type and the `AuthenticatorKind` enum.

## Key types

* `AuthenticatorKind` — enum identifying which authentication mechanism was used: `Frontegg`, `Password`, `Sasl`, `Oidc`, or `None` (default).
* `Authenticated` — a zero-sized struct constructed only by authenticators to mark a valid session; also used when authentication is disabled.
* `Password` — a `String` wrapper that never exposes its value through `Display` or `Debug` and zeroizes on drop.
* `hash::HashOpts`, `hash::PasswordHash` — inputs and outputs of the PBKDF2 primitive.

## Dependencies

Depends on `openssl` (vendored), `base64`, `itertools`, `zeroize`, `serde`, and `mz-ore`.
Consumed by authentication middleware and session-establishment code throughout Materialize.
