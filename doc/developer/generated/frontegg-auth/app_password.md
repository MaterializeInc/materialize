---
source: src/frontegg-auth/src/app_password.rs
revision: 0a4f58967e
---

# frontegg-auth::app_password

Defines `AppPassword`, a compact credential type that encodes a Frontegg client ID and secret key as a pair of UUIDs, serializable as a URL-safe base64 string (with `mzp_` prefix) or as concatenated hex-encoded UUIDs.
`FromStr` accepts both formats; `Display` always normalizes to the compact base64 form.
`AppPasswordParseError` is the parse failure type.
