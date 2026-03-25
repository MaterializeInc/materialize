---
source: src/compute/src/server.rs
revision: 8cd889e259
---

# mz-compute::server

Provides the `serve` entry point that starts a Timely compute cluster and returns a factory for `ComputeClient` handles.
The `Worker` struct drives the per-worker event loop: it steps Timely, handles incoming `ComputeCommand`s, processes pending peeks and subscribes, and performs periodic maintenance (frontier reporting, arrangement compaction, expiration checks).
On reconnect, the `reconcile` method diffs the old command history against the new command batch to reuse compatible existing dataflows and compact or drop stale ones, ensuring workers remain consistent across client reconnects.
A `spawn_channel_adapter` task bridges the per-connection `ClusterClient` channels with the persistent worker channels, routing responses by nonce and stashing out-of-order responses.
