---
source: src/ccsr/src/tls.rs
revision: c3979df0ba
---

# mz-ccsr::tls

Provides `Identity` and `Certificate`, serde-enabled wrappers around the corresponding `reqwest` types.
`Identity::from_pem` accepts a PEM private key (PKCS #8, PKCS #1 RSA, or SEC1 EC) and a PEM certificate chain, concatenates them into a single PEM buffer, validates the key against the leaf certificate using rustls and aws-lc-rs, and stores the raw PEM bytes. Both types round-trip through `From` impls back into their `reqwest` equivalents.
`Identity` implements `Zeroize` and `Drop` (which calls `zeroize()`), clearing the PEM buffer from memory when dropped.
`TlsError` gains a `Config` variant for errors constructing the rustls `ClientConfig`.
`rustls_config` builds a `rustls::ClientConfig` for clients that trust a set of root certificates and optionally present a client identity. Server certificates are verified by `rustls-platform-verifier` backed by `ExactRootMatch`: a server certificate byte-for-byte identical to one of the configured roots is accepted as long as its validity period and server name are valid, bypassing the chain, basic-constraints, and key-usage checks that webpki would otherwise apply (which rejects self-signed `CA:TRUE` certificates as `CaUsedAsEndEntity`). For exact-match certificates without a DNS or IP subjectAltName, the name check falls back to subject common names via `common_name_matches`. All other certificates go through `rustls-platform-verifier`. Handshake signatures are always verified.
