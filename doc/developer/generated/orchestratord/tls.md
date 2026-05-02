---
source: src/orchestratord/src/tls.rs
revision: 82d92a7fad
---

# mz-orchestratord::tls

Provides TLS certificate management helpers for orchestratord.
`DefaultCertificateSpecs` holds optional default `MaterializeCertSpec` values for balancerd external, console external, and internal certificates.
`create_certificate` constructs a cert-manager `Certificate` resource by merging per-resource and default specs, validating key sizes for RSA and ECDSA, and applying resource labels; `issuer_ref_defined` tests whether a cert issuer is configured.
