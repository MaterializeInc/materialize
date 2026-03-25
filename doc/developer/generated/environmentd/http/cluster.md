---
source: src/environmentd/src/http/cluster.rs
revision: 101efde02a
---

# environmentd::http::cluster

Implements an HTTP reverse proxy that forwards requests from `environmentd`'s HTTP port to the internal HTTP endpoints of individual `clusterd` processes (profiling, metrics, tracing).
Uses `ReplicaHttpLocator` to resolve the target address for a given `(cluster_id, replica_id, process)` tuple, then opens a raw TCP/UDS connection and proxies the request via HTTP/1.1.
Also provides `handle_clusters`, which renders an HTML overview page listing all cluster replicas with links to their proxied endpoints.
