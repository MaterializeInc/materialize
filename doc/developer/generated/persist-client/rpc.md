---
source: src/persist-client/src/rpc.rs
revision: 6040296dc3
---

# persist-client::rpc

Implements the gRPC-based PubSub client and server that propagates shard state diffs between persist nodes in the same cluster.
When a writer commits a new diff, it pushes that diff to the PubSub server, which broadcasts it to all subscribed clients; this allows readers to update their cached state without polling consensus.
The module defines `PubSubSender` and `PubSubReceiver` traits, with a gRPC implementation and an in-process delegate for same-process communication.
