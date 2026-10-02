---
source: src/ccsr/src/tls.rs
revision: bb5c454adc01868b58a0754a26274cbef45045f0
---

# mz-ccsr::tls

Provides `Identity` and `Certificate`, serde-enabled wrappers around the corresponding `reqwest` types.
`Identity::from_pem` accepts a PEM private key (PKCS #8, PKCS #1 RSA, or SEC1 EC) and a PEM certificate chain, concatenates them into a single PEM buffer, validates the key against the leaf certificate using rustls and aws-lc-rs, and stores the raw PEM bytes. Both types round-trip through `From` impls back into their `reqwest` equivalents.
`Identity` implements `Zeroize` and `Drop` (which calls `zeroize()`), clearing the PEM buffer from memory when dropped.
