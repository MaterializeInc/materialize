---
source: src/cloud-resources/src/vpc_endpoint.rs
revision: 20c10b7f8d
---

# cloud-resources::vpc_endpoint

Defines the `CloudResourceController` and `CloudResourceReader` async traits for managing AWS VPC endpoint Kubernetes objects, along with `VpcEndpointConfig`, `AwsExternalIdPrefix`, and `VpcEndpointEvent`.
Provides helper functions `vpc_endpoint_name`, `id_from_vpc_endpoint_name`, and `vpc_endpoint_host` that encode the naming contract with the cloud infrastructure layer.
`VpcEndpointEvent` converts directly to a `Row` for insertion into the PrivateLink connection status history introspection table.
