---
source: src/cloud-resources/src/crd.rs
revision: b088d23e1d
---

# cloud-resources::crd

Defines all Kubernetes custom resource definitions used by Materialize's cloud infrastructure, along with the `ManagedResource` trait and utilities for CRD registration with retry logic.
Key submodules are `materialize` (`Materialize` CRD), `balancer` (`Balancer` CRD), `console` (`Console` CRD), `vpc_endpoint` (`VpcEndpoint` CRD), and `generated` (cert-manager CRD types).
`register_versioned_crds` applies all CRDs via server-side apply and waits for establishment, using `VersionedCrd` to merge multiple schema versions.
`VersionedCrd` carries an optional `conversion` field (`Option<CustomResourceConversion>`) that is applied to the merged CRD after `merge_crds` (which drops the conversion field); this is used to register a webhook conversion strategy between CRD versions.
`MaterializeCertSpec` provides the user-visible subset of cert-manager `Certificate` fields.
`recommended_k8s_labels(app_name)` builds the standard `app.kubernetes.io/*` label set (`managed-by: materialize-operator`, `part-of: materialize`, and optionally `name: <app>` along with the legacy `app` label); `ManagedResource::managed_resource_meta` calls this to attach the labels to owned resources.
