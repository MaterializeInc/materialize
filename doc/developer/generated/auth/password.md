---
source: src/auth/src/password.rs
revision: b69d9bb412
---

# mz-auth::password

Defines the `Password` newtype wrapper around `String`.

`Password` deliberately omits `Display` and replaces `Debug` with a redacted `Password(****)` representation, preventing accidental credential leakage in logs or error messages.
A compile-time `assert_not_impl_all!` assertion enforces the absence of `Display`.
The type derives `Serialize`/`Deserialize` and `Arbitrary` (for property-based tests) and converts from `String` and `&str`.
It exposes `as_bytes() -> &[u8]` and `as_str() -> &str` accessor methods.
The inner `String` is zeroized on drop via a custom `Drop` implementation using `mz_ore::secure::Zeroize`, ensuring passwords do not linger in memory after the value is freed.
