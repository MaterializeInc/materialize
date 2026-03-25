---
source: src/service/src/grpc.rs
revision: 82d92a7fad
---

# mz-service::grpc

Vestigial gRPC metrics module retained after the migration to CTP.
Defines `GrpcServerMetrics` (a registry wrapper) and `PerGrpcServerMetrics`, which implements `transport::Metrics` by recording the Unix timestamp of the last received command into a `DeleteOnDropGauge`.
New code should use the `transport` module instead.
