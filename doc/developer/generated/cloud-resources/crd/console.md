---
source: src/cloud-resources/src/crd/console.rs
revision: 38e4e9206e
---

# cloud-resources::crd::console

Defines the `Console` Kubernetes custom resource (group `materialize.cloud`, version `v1alpha1`) representing a Materialize web console deployment, along with spec, status, and connection configuration types including `BalancerdRef` and `HttpConnectionScheme`.
`BalancerdRef` includes an optional `dns_names: Option<Vec<String>>` field carrying the external DNS names that balancerd serves traffic for.
