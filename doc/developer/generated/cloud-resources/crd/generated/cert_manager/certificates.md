---
source: src/cloud-resources/src/crd/generated/cert_manager/certificates.rs
revision: 4267863081
---

# cloud-resources::crd::generated::cert_manager::certificates

Kopium-generated Rust types for the cert-manager `Certificate` CRD (`certificates.cert-manager.io`, group `cert-manager.io`, version `v1`).
Defines `CertificateSpec` (the `#[derive(CustomResource)]` spec struct that generates the `Certificate` kind) along with all nested configuration types: `CertificateIssuerRef`, `CertificatePrivateKey`, `CertificateKeystores` (JKS/PKCS12), `CertificateSecretTemplate`, `CertificateSubject`, `CertificateNameConstraints`, and `CertificateStatus`.
These types are used by `MaterializeCertSpec` in the parent `crd` module to provide the user-visible subset of cert-manager certificate configuration.
