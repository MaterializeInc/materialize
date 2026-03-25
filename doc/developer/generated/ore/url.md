---
source: src/ore/src/url.rs
revision: 4f420d4b78
---

# mz-ore::url

Defines `SensitiveUrl`, a newtype around `url::Url` that redacts the password in all `Display` and `Debug` output.
`into_redacted` replaces the password with `"<redacted>"` before returning the underlying `Url`; `to_string_unredacted` bypasses redaction.
The type implements `FromStr`, `Deref<Target = Url>`, serde `Serialize`/`Deserialize`, and (behind feature flags) a `clap` value parser and `proptest::Arbitrary`.
