---
source: src/persist-client/src/internal/service.rs
revision: 95271359a5
---

# persist-client::internal::service

Thin wrapper that includes the protobuf-generated gRPC service code (client and server stubs) for the persist PubSub service.
The actual implementation lives in `rpc.rs`; this file only re-exports the generated types.
