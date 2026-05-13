---
source: src/license-keys/src/signing.rs
revision: 042c213b8f
---

# mz-license-keys::signing

Provides the key-issuance side of the license-key system (behind the `signing` feature).
`make_license_key` builds and signs a JWT (PS256, version 1) using an AWS KMS key, encoding organization ID, environment ID, credit limits, expiration, and an optional list of entitlement strings.
`get_pubkey_pem` retrieves the corresponding RSA public key from KMS in PEM format for embedding in the validator.
