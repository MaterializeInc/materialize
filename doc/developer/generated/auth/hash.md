---
source: src/auth/src/hash.rs
revision: 3d6b1c887a
---

# mz-auth::hash

Implements PBKDF2-SHA-256 password hashing and SCRAM-SHA-256 authentication primitives via OpenSSL.

## Key types

* `HashOpts` — iteration count and salt used as input to PBKDF2.
* `PasswordHash` — the salt, iteration count, and raw 32-byte PBKDF2 output.
* `ScramSha256Hash` (private) — the derived stored key and server key formatted as `SCRAM-SHA-256$<i>:<salt>$<stored_key>:<server_key>`.
* `HashError` / `VerifyError` — error types wrapping OpenSSL failures, malformed hash strings, and invalid passwords.

## Key functions

* `hash_password` — hashes a `Password` with a random OpenSSL salt and returns a `PasswordHash`.
* `hash_password_with_opts` — hashes with caller-supplied `HashOpts` (deterministic; used during verification).
* `scram256_hash` — produces a SCRAM-SHA-256 formatted string from a password.
* `scram256_verify` — verifies a plaintext password against a stored SCRAM-SHA-256 hash using constant-time comparison.
* `scram256_parse_opts` — parses the iteration count and salt out of a SCRAM-SHA-256 hash string.
* `sasl_verify` — verifies a SASL client proof and returns the server verifier string; used during the SCRAM exchange to authenticate the client without access to the plaintext password.
* `generate_nonce` — appends 24 random bytes (base64-encoded) to a client nonce for SCRAM challenge generation.
* `mock_sasl_challenge` — derives deterministic `HashOpts` from a username and mock nonce, preventing user-enumeration attacks for nonexistent accounts.
