---
source: src/fivetran-destination/src/fivetran_sdk.rs
revision: 82d92a7fad
---

# mz-fivetran-destination::fivetran_sdk

Thin wrapper that includes the protobuf-generated Fivetran SDK types from the build-time output and re-exports them at the module root.
All Fivetran request/response types, data type enumerations, and the `DestinationConnectorServer` trait come from this module.
