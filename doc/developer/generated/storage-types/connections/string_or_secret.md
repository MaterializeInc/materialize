---
source: src/storage-types/src/connections/string_or_secret.rs
revision: a375623c5b
---

# storage-types::connections::string_or_secret

Defines `StringOrSecret`, an enum representing a value that is either a plain `String` or a reference to a catalog secret (`CatalogItemId`).
Provides `get_string` for resolving the actual string value at runtime via a `SecretsReader`.
Used widely across connection definitions where configuration values may be stored as secrets.
