---
source: src/orchestratord/src/tls.rs
revision: 38e4e9206e
---

# mz-orchestratord::tls

Provides TLS certificate management helpers for orchestratord.
`DefaultCertificateSpecs` holds optional default `MaterializeCertSpec` values for balancerd external, console external, and internal certificates.
`create_certificate` constructs a cert-manager `Certificate` resource by merging per-resource and default specs, validating key sizes for RSA and ECDSA, and applying resource labels; `issuer_ref_defined` tests whether a cert issuer is configured.
`resolved_dns_names` resolves the effective DNS names for a certificate by returning the override spec's `dns_names` if present, falling back to the default spec's `dns_names`.
