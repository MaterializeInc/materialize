# mz_cloud_resources/src

Source root of `mz-cloud-resources`. Cargo-conventional location.

See [`../CONTEXT.md`](../CONTEXT.md) for the crate's role (Kubernetes
operator boundary, `vpc-endpoints` feature flag as cloud-vs-local seam,
`CloudResourceController` trait fulfilled by `mz-controller`).

## Subdirs reviewed (≥5K LOC)

- [`crd/`](crd/CONTEXT.md) — Custom Resource Definitions: `Materialize`,
  `Balancer`, `Console`, `VpcEndpoint` + `ManagedResource` trait (5,761 LOC)
