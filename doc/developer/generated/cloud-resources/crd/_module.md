---
source: src/cloud-resources/src/crd.rs
revision: ea7d7e5d6b
---

# cloud-resources::crd

Defines all Kubernetes custom resource definitions used by Materialize's cloud infrastructure, along with the `ManagedResource` trait and utilities for CRD registration with retry logic.
Key submodules are `materialize` (`Materialize` CRD), `balancer` (`Balancer` CRD), `console` (`Console` CRD), `vpc_endpoint` (`VpcEndpoint` CRD), and `generated` (cert-manager CRD types).
`register_versioned_crds` applies all CRDs via server-side apply and waits for establishment, using `VersionedCrd` to merge multiple schema versions.
`MaterializeCertSpec` provides the user-visible subset of cert-manager `Certificate` fields.
