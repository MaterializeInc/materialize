---
source: src/cluster/src/client.rs
revision: 445c5ce0f4
---

# client

Manages the lifecycle of a local Timely cluster and the client connection to it.
`ClusterClient` wraps a `Partitioned<LocalClient>` that fans commands out across all Timely worker threads; the first message received must be a protocol nonce that triggers `connect`, which wires up per-worker channels.
`ClusterSpec` is the trait compute and storage implement: they supply `Command`/`Response` types, a cluster name, and a `run_worker` function that drives each Timely worker.
`build_cluster` is a provided method on `ClusterSpec` that initializes Timely networking (optionally with lgalloc-backed zero-copy buffers), launches worker threads with disambiguated OS thread names, and returns a `TimelyContainer` that keeps them alive.
