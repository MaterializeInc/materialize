---
source: src/service/src/params.rs
revision: 82d92a7fad
---

# mz-service::params

Defines `GrpcClientParameters`, a serializable struct holding optional gRPC connection tuning values: connect timeout, HTTP/2 keep-alive interval, and keep-alive timeout.
Provides `update` (applies set fields from another instance) and `all_unset` (tests whether all fields are `None`).
