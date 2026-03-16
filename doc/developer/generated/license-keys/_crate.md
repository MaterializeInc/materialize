---
source: src/license-keys/src/lib.rs
revision: b5dd753ef3
---

# mz-license-keys

Validates and issues Materialize license keys encoded as JWTs signed with RSA-PSS (PS256).
The `validate` function verifies a license key against the embedded production public key(s), supports key rotation via a key list, handles graceful expiry detection, and checks a revocation list.
`ValidatedLicenseKey` carries the decoded claims including credit-consumption limits, expiration behavior, and organization/environment binding.
The optional `signing` feature adds `make_license_key` and `get_pubkey_pem` for AWS KMS-based key issuance.

## Module structure

* `signing` (feature-gated) — JWT issuance via AWS KMS
