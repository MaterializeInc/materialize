---
source: src/cloud-resources/src/crd/generated/cert_manager/issuers.rs
revision: 4267863081
---

# cloud-resources::crd::generated::cert_manager::issuers

Kopium-generated Rust types for the cert-manager `Issuer` CRD (`issuers.cert-manager.io`, group `cert-manager.io`, version `v1`).
Defines `IssuerSpec` (the `#[derive(CustomResource)]` spec struct that generates the `Issuer` kind) along with all nested configuration types covering ACME, CA, Vault, Venafi, and self-signed issuer backends, their solver configurations (HTTP01/DNS01), and `IssuerStatus` with conditions.
