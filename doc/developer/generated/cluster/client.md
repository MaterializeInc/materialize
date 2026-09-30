---
source: src/cluster/src/client.rs
revision: c475f3b2ff
---

# client

Manages the lifecycle of a local Timely cluster and the client connection to it.
`ClusterClient` wraps a `Partitioned<LocalClient>` that fans commands out across all Timely worker threads; the first message received must be a protocol nonce that triggers `connect`, which wires up per-worker channels.
`ClusterSpec` is the trait compute and storage implement: they supply `Command`/`Response` types, a cluster name, and a `run_worker` function that drives each Timely worker.
`build_cluster` is a provided method on `ClusterSpec` that initializes Timely networking (optionally with lgalloc-backed zero-copy buffers), launches worker threads with disambiguated OS thread names, and returns a `TimelyContainer` that keeps them alive.
`GuestClusterClient` is a client to a secondary ("guest") command stream served by an existing Timely cluster. Unlike `ClusterClient`, which owns a `TimelyContainer`, `GuestClusterClient` receives its per-worker channel senders and worker thread handles from an external source, allowing additional command types to share the same Timely workers without owning them.
