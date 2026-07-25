---
source: src/compute/src/server.rs
revision: 93dcb0ef5a
---

# mz-compute::server

Provides the `serve` entry point that starts a Timely compute cluster and returns a factory for `ComputeClient` handles.
`serve` accepts a `storage_log_readers` vec — one `StorageTimelyLogReader` per local worker — which is distributed to each `Worker` so that compute workers can replay storage timely log events alongside their own logging dataflow. It also registers column-pager Prometheus metrics via `mz_timely_util::column_pager::metrics::register`, wiring the process-wide `TieredPolicy` into the metrics registry, and registers pool configuration metrics via `mz_timely_util::pool_config::metrics::register`.
`StorageTimelyLogReader` is a type alias for an `Arc<EventLink<Timestamp, Vec<(Duration, TimelyEvent)>>>` shared with the corresponding storage worker.
The `Worker` struct drives the per-worker event loop: it steps Timely, handles incoming `ComputeCommand`s, processes pending peeks and subscribes, and performs periodic maintenance (frontier reporting, arrangement compaction, expiration checks).
On reconnect, the `reconcile` method diffs the old command history against the new command batch to reuse compatible existing dataflows and compact or drop stale ones, ensuring workers remain consistent across client reconnects.
`CommandReceiver` observes nonce changes in the command stream and converts them into receive errors that trigger reconciliation; `ResponseSender` tags outgoing `ComputeResponse`s with the current nonce so receivers can discard stale responses from previous connections.
