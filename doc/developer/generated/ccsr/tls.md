---
source: src/ccsr/src/tls.rs
revision: ca42a663e0
---

# mz-ccsr::tls

Provides `Identity` and `Certificate`, serde-enabled wrappers around the corresponding `reqwest` types.
`Identity::from_pem` converts a PEM key/cert pair into a PKCS #12 DER archive (via `mz-tls-util`); both types round-trip through `From` impls back into their `reqwest` equivalents.
