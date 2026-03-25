---
source: src/cloud-resources/src/lib.rs
revision: e757b4d11b
---

# cloud-resources

Provides Kubernetes CRD definitions and the `CloudResourceController`/`CloudResourceReader` traits for managing cloud-only resources (primarily AWS VPC endpoints) that have no equivalent in local deployments.

## Module structure

* `crd` — All Materialize Kubernetes CRDs (`Materialize`, `Balancer`, `Console`, `VpcEndpoint`) and CRD registration utilities
* `vpc_endpoint` (feature-gated `vpc-endpoints`) — `CloudResourceController`/`CloudResourceReader` traits and `VpcEndpointConfig`, `AwsExternalIdPrefix`, `VpcEndpointEvent`
* `bin/crd_writer` — Binary that generates CRD field documentation from the `MaterializeSpec` JSON schema

## Key dependencies

`kube`, `k8s-openapi`, `schemars`, `mz-ore`, `mz-server-core`, `mz-repr` (optional, for `vpc-endpoints` feature).

## Downstream consumers

`mz-orchestratord`, `mz-controller`, `mz-adapter`, `mz-environmentd`.
