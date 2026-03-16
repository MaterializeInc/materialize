---
source: src/auth/src/password.rs
revision: 4267863081
---

# mz-auth::password

Defines the `Password` newtype wrapper around `String`.

`Password` deliberately omits `Display` and replaces `Debug` with a redacted `Password(****)` representation, preventing accidental credential leakage in logs or error messages.
A compile-time `assert_not_impl_all!` assertion enforces the absence of `Display`.
The type derives `Serialize`/`Deserialize` and `Arbitrary` (for property-based tests) and converts from `String` and `&str`.
