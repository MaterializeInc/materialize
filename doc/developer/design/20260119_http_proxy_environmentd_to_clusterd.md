# HTTP Proxy from environmentd to clusterd

## Summary

Add HTTP proxying capability to environmentd that allows accessing clusterd internal HTTP endpoints through a single canonical endpoint (environmentd's port 6877).

## Goals

1. Provide access to clusterd internal HTTP endpoints (profiling, metrics, tracing) without requiring direct network access to clusterd pods
2. Eliminate the need to discover and port-forward to dynamically assigned clusterd ports
3. Work with both Kubernetes orchestrator and local process orchestrator

## Non-Goals

- Replacing the existing CTP (Cluster Transport Protocol) for compute/storage commands
- Proxying WebSocket connections (can be added later if needed)
- Authentication/authorization beyond what environmentd already provides

## Problem Statement

Both environmentd and clusterd expose internal HTTP endpoints:

| Component | Port | Endpoints |
|-----------|------|-----------|
| environmentd | 6877 | `/api/sql`, `/metrics`, `/prof/*`, `/memory`, etc. |
| clusterd | 6878 | `/api/livez`, `/metrics`, `/api/tracing`, `/api/usage-metrics`, `/prof/*` |

**Current issues:**

1. **Port discovery is hard**: Clusterd ports are assigned dynamically by the orchestrator. In Kubernetes, the actual port is determined by the service. In local process mode, a free port is chosen at runtime.

2. **Direct access requires port-forwarding**: To access a clusterd's HTTP endpoint in Kubernetes, you must identify the specific pod and port-forward to it.

3. **Information loss**: The `internal-http` port address is determined by the orchestrator when provisioning a replica (see `src/controller/src/clusters.rs:772-775`) but this information is not preserved—only `storagectl` and `computectl` addresses are stored in `ClusterReplicaLocation`.

## Proposed Solution

### Overview

1. **Extend `ClusterReplicaLocation`** to include HTTP addresses
2. **Store HTTP addresses** in a dedicated in-memory state (`ReplicaHttpLocator`)
3. **Add proxy routes** in environmentd's HTTP server at `/api/cluster/{cluster_id}/replica/{replica_id}/process/{process}/{path}`
4. **Implement HTTP proxy handler** following the pattern in `console.rs`

### Route Design

```
GET/POST/etc. /api/cluster/{cluster_id}/replica/{replica_id}/process/{process}/{path}
```

Where:
- `cluster_id`: Cluster ID (e.g., `u1`, `s1`)
- `replica_id`: Replica ID (e.g., `u1`, `s1`)
- `process`: Process index within the replica (0-indexed). Replicas with `scale > 1` have multiple processes, each with its own HTTP endpoint.
- `path`: The path to proxy to clusterd (e.g., `metrics`, `prof/cpu`, `api/tracing`)

**Examples:**
```
# Single-process replica (scale=1)
GET /api/cluster/u1/replica/u1/process/0/metrics
GET /api/cluster/u1/replica/u1/process/0/prof/cpu

# Multi-process replica (scale=3) - access each process individually
GET /api/cluster/s1/replica/s1/process/0/metrics
GET /api/cluster/s1/replica/s1/process/1/metrics
GET /api/cluster/s1/replica/s1/process/2/metrics
GET /api/cluster/s1/replica/s1/process/0/api/tracing
```

### Implementation Components

#### 1. Extend `ClusterReplicaLocation` (`src/cluster-client/src/client.rs`)

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterReplicaLocation {
    /// Control plane addresses (storagectl/computectl)
    pub ctl_addrs: Vec<String>,
    /// Internal HTTP addresses for each process
    pub http_addrs: Vec<String>,
}
```

#### 2. Capture HTTP Addresses in Controller (`src/controller/src/clusters.rs`)

In `create_cluster_replica()` around line 434-441, after provisioning:

```rust
ReplicaLocation::Managed(m) => {
    let (service, metrics_task_join_handle) = self.provision_replica(...)?;
    storage_location = ClusterReplicaLocation {
        ctl_addrs: service.addresses("storagectl"),
        http_addrs: service.addresses("internal-http"),  // ADD THIS
    };
    compute_location = ClusterReplicaLocation {
        ctl_addrs: service.addresses("computectl"),
        http_addrs: service.addresses("internal-http"),  // ADD THIS
    };
    ...
}
```

#### 3. Store HTTP Addresses in Dedicated State

Create a `ReplicaHttpLocator` service that tracks HTTP addresses in memory. This avoids catalog complexity while providing the necessary address lookup for the HTTP proxy.

```rust
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use mz_controller_types::{ClusterId, ReplicaId};

/// Tracks HTTP addresses for cluster replica processes.
///
/// Each replica can have multiple processes (based on `scale`), and each
/// process has its own HTTP endpoint.
pub struct ReplicaHttpLocator {
    /// Maps (cluster_id, replica_id) -> list of HTTP addresses (one per process)
    addresses: RwLock<BTreeMap<(ClusterId, ReplicaId), Vec<String>>>,
}

impl ReplicaHttpLocator {
    pub fn new() -> Self {
        Self {
            addresses: RwLock::new(BTreeMap::new()),
        }
    }

    /// Returns the HTTP address for a specific process of a replica.
    ///
    /// Returns `None` if the replica is not found or the process index is out of bounds.
    pub fn get_http_addr(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        process: usize,
    ) -> Option<String> {
        let guard = self.addresses.read().unwrap();
        guard
            .get(&(cluster_id, replica_id))
            .and_then(|addrs| addrs.get(process).cloned())
    }

    /// Returns the number of processes for a replica, or None if not found.
    pub fn process_count(&self, cluster_id: ClusterId, replica_id: ReplicaId) -> Option<usize> {
        let guard = self.addresses.read().unwrap();
        guard.get(&(cluster_id, replica_id)).map(|addrs| addrs.len())
    }

    /// Updates the HTTP addresses for a replica.
    ///
    /// Called by the controller when a replica is provisioned.
    pub fn update_addresses(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        addrs: Vec<String>,
    ) {
        let mut guard = self.addresses.write().unwrap();
        guard.insert((cluster_id, replica_id), addrs);
    }

    /// Removes address information for a replica.
    ///
    /// Called by the controller when a replica is dropped.
    pub fn remove_replica(&self, cluster_id: ClusterId, replica_id: ReplicaId) {
        let mut guard = self.addresses.write().unwrap();
        guard.remove(&(cluster_id, replica_id));
    }
}
```

#### 4. Add HTTP Proxy Module (`src/environmentd/src/http/cluster.rs`)

New file following the `console.rs` pattern:

```rust
use std::sync::Arc;

use axum::Extension;
use axum::body::Body;
use axum::extract::Path;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use http::header::HOST;
use http::HeaderValue;
use hyper::Uri;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use mz_controller_types::{ClusterId, ReplicaId};

use crate::ReplicaHttpLocator;

pub struct ClusterProxyConfig {
    /// HTTP client for proxying (no TLS needed for internal traffic)
    client: Client<HttpConnector, Body>,
    /// Handle to look up replica HTTP addresses
    locator: Arc<ReplicaHttpLocator>,
}

impl ClusterProxyConfig {
    pub fn new(locator: Arc<ReplicaHttpLocator>) -> Self {
        let client = Client::builder(TokioExecutor::new())
            .build_http();
        Self { client, locator }
    }
}

/// Proxy handler for cluster replica HTTP endpoints.
///
/// Route: `/api/cluster/:cluster_id/replica/:replica_id/process/:process/*path`
pub async fn handle_cluster_proxy(
    Path((cluster_id, replica_id, process, path)): Path<(String, String, usize, String)>,
    config: Extension<Arc<ClusterProxyConfig>>,
    mut req: Request<Body>,
) -> Result<Response, StatusCode> {
    // Parse IDs
    let cluster_id: ClusterId = cluster_id.parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let replica_id: ReplicaId = replica_id.parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    // Look up HTTP address for this replica and process
    let http_addr = config.locator
        .get_http_addr(cluster_id, replica_id, process)
        .ok_or_else(|| {
            // Provide more specific error: replica not found vs process out of range
            if config.locator.process_count(cluster_id, replica_id).is_none() {
                tracing::debug!(
                    "Replica {cluster_id}/{replica_id} not found for HTTP proxy"
                );
                StatusCode::NOT_FOUND
            } else {
                tracing::debug!(
                    "Process {process} out of range for replica {cluster_id}/{replica_id}"
                );
                StatusCode::NOT_FOUND
            }
        })?;

    // Build target URI, preserving query string if present
    let path_query = if let Some(query) = req.uri().query() {
        format!("/{}?{}", path, query)
    } else {
        format!("/{}", path)
    };

    let uri = Uri::try_from(format!("http://{}{}", http_addr, path_query))
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    // Update request with new URI
    *req.uri_mut() = uri.clone();

    // Set Host header to target
    if let Some(host) = uri.host() {
        req.headers_mut().insert(
            HOST,
            HeaderValue::from_str(host).map_err(|_| StatusCode::BAD_REQUEST)?
        );
    }

    // Proxy the request
    config.client
        .request(req)
        .await
        .map(|r| r.into_response())
        .map_err(|err| {
            tracing::warn!(
                "Error proxying to clusterd {cluster_id}/{replica_id}/process/{process}: {err}"
            );
            StatusCode::BAD_GATEWAY
        })
}
```

#### 5. Register Routes (`src/environmentd/src/http.rs`)

Add to the router setup (around line 264, in the `routes_enabled.internal` block):

```rust
if routes_enabled.internal {
    // ... existing internal routes ...

    // Cluster HTTP proxy
    let cluster_proxy_config = Arc::new(cluster::ClusterProxyConfig::new(
        replica_http_locator.clone(),
    ));
    base_router = base_router
        .route(
            "/api/cluster/:cluster_id/replica/:replica_id/process/:process/*path",
            routing::any(cluster::handle_cluster_proxy),
        )
        .layer(Extension(cluster_proxy_config));
}
```

#### 6. Wire Up the Locator

The `ReplicaHttpLocator` needs to be:
1. Created at environmentd startup
2. Passed to both the controller (to update addresses) and HTTP server (to serve proxy requests)
3. Updated when replicas are created/dropped

### Data Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              environmentd                                 │
│  ┌──────────────┐    ┌───────────────┐    ┌──────────────────┐          │
│  │ HTTP Server  │───▶│ ReplicaHttp   │◀───│   Controller     │          │
│  │  (port 6877) │    │   Locator     │    │                  │          │
│  └──────────────┘    └───────────────┘    └────────┬─────────┘          │
│         │                                          │                     │
│         │ GET /api/cluster/u1/replica/u1/          │                     │
│         │         process/0/metrics                │                     │
│         ▼                                          │                     │
│  ┌──────────────┐                                  │                     │
│  │cluster.rs    │                                  │                     │
│  │proxy handler │                                  │                     │
│  └──────┬───────┘                                  │                     │
└─────────┼──────────────────────────────────────────┼─────────────────────┘
          │                                          │
          │ HTTP GET /metrics                        │ orchestrate
          ▼                                          ▼
    ┌───────────────────────────────────────────────────────────────┐
    │                     clusterd (process 0)                       │
    │                   (e.g., 10.0.0.5:6878)                        │
    │   /metrics  /prof/*  /api/tracing  /api/livez                 │
    └───────────────────────────────────────────────────────────────┘

For multi-process replicas (scale > 1):

    GET .../process/0/metrics  ──▶  clusterd process 0 (10.0.0.5:6878)
    GET .../process/1/metrics  ──▶  clusterd process 1 (10.0.0.6:6878)
    GET .../process/2/metrics  ──▶  clusterd process 2 (10.0.0.7:6878)
```

### Alternatives Considered

#### Alternative 1: Query Orchestrator Directly

Instead of storing addresses in `ReplicaHttpLocator`, query the orchestrator's `Service::addresses()` method on each request.

**Pros:** Always up-to-date, no synchronization needed
**Cons:** Requires keeping `Service` handles alive; process orchestrator creates new services on each `ensure_service` call

#### Alternative 2: Use Existing CTP Channel

Embed HTTP proxy requests in the existing CTP protocol.

**Pros:** Reuses existing communication channel
**Cons:** Significant protocol changes; mixes concerns; adds latency

#### Alternative 3: Kubernetes Ingress/Service

Configure Kubernetes services to expose clusterd HTTP endpoints.

**Pros:** Standard Kubernetes approach
**Cons:** Doesn't work for local process orchestrator; requires external configuration

### Security Considerations

1. **Authentication**: The proxy routes should be protected by the same authentication as other internal routes (the existing `auth_middleware`)

2. **Authorization**: Consider restricting access to certain clusterd endpoints or requiring specific roles

3. **Request Limits**: Apply the same request size limits as other routes

4. **TLS**: Internal clusterd communication is typically not TLS-encrypted; this is consistent with existing CTP traffic

### Testing Plan

1. **Unit tests**: Test URL parsing and routing logic
2. **Integration tests**: Verify proxying works with local process orchestrator
3. **Platform tests**: Verify proxying works in Kubernetes environment
4. **Error handling**: Test behavior when replica is not available

### Migration

This is a purely additive change:
- No changes to existing APIs
- No changes to stored data formats (addresses stored in-memory only)
- No protobuf schema changes required

### Work Items

1. [ ] Extend `ClusterReplicaLocation` with `http_addrs` field
2. [ ] Update controller to capture HTTP addresses from orchestrator
3. [ ] Implement `ReplicaHttpLocator` for in-memory address tracking
4. [ ] Wire `ReplicaHttpLocator` between controller and HTTP server
5. [ ] Create `src/environmentd/src/http/cluster.rs` proxy module
6. [ ] Register proxy routes in HTTP server
7. [ ] Add tests
8. [ ] Update documentation

### Open Questions

1. **Should this be enabled by default?** Probably yes for internal routes, controlled by `HttpRoutesEnabled::internal`.

2. **Should we support WebSocket proxying?** Not initially, but the pattern could be extended.

3. **Should we add a convenience endpoint listing available replicas and their process counts?**
   ```
   GET /api/cluster/{cluster_id}/replicas
   ```
   This could return a list of replica IDs and their process counts:
   ```json
   {
     "replicas": [
       {"replica_id": "u1", "process_count": 1},
       {"replica_id": "u2", "process_count": 3}
     ]
   }
   ```
