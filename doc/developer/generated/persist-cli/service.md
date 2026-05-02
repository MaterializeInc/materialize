---
source: src/persist-cli/src/service.rs
revision: 82d92a7fad
---

# persistcli::service

Provides a minimal manual test harness for the persist gRPC PubSub service, selectable via `--role server|writer|reader`.
The server role starts a `PersistGrpcPubSubServer`; the writer role connects a `GrpcPubSubClient` and repeatedly pushes versioned diffs to a fixed shard; the reader role subscribes to the same shard and logs received messages.
This is a developer-facing utility for smoke-testing the PubSub transport outside of a full Materialize deployment.
