// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! HTTP proxy for cluster replica endpoints.
//!
//! This module provides an HTTP proxy that forwards requests from environmentd
//! to clusterd internal HTTP endpoints (profiling, metrics, tracing). This allows
//! accessing clusterd endpoints through environmentd's canonical HTTP port without
//! requiring direct network access to the clusterd pods.

use std::sync::Arc;
use std::time::Duration;

use askama::Template;
use axum::Extension;
use axum::body::Body;
use axum::extract::Path;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use http::HeaderValue;
use http::header::HOST;
use hyper::Uri;
use hyper_util::rt::TokioIo;
use mz_controller::ReplicaHttpLocator;
use mz_controller_types::{ClusterId, ReplicaId};
use tokio::net::TcpStream;

use crate::BUILD_INFO;
use crate::http::AuthedClient;

/// Connection timeout for proxied requests.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Overall request timeout for proxied requests.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Configuration for the cluster HTTP proxy.
pub struct ClusterProxyConfig {
    /// Handle to look up replica HTTP addresses.
    locator: Arc<ReplicaHttpLocator>,
}

impl ClusterProxyConfig {
    /// Creates a new `ClusterProxyConfig`.
    pub fn new(locator: Arc<ReplicaHttpLocator>) -> Self {
        Self { locator }
    }
}

/// Proxy handler for cluster replica HTTP endpoints (root path).
///
/// Route: `/api/cluster/:cluster_id/replica/:replica_id/process/:process/`
///
/// This handler handles requests to the root of a clusterd process's HTTP endpoint.
pub(crate) async fn handle_cluster_proxy_root(
    Path((cluster_id, replica_id, process)): Path<(String, String, usize)>,
    config: Extension<Arc<ClusterProxyConfig>>,
    req: Request<Body>,
) -> Result<Response, (StatusCode, String)> {
    handle_cluster_proxy_inner(cluster_id, replica_id, process, "", config, req).await
}

/// Proxy handler for cluster replica HTTP endpoints.
///
/// Route: `/api/cluster/:cluster_id/replica/:replica_id/process/:process/*path`
///
/// This handler proxies HTTP requests to the internal HTTP endpoint of a specific
/// clusterd process. Each replica can have multiple processes (based on `scale`),
/// and each process has its own HTTP endpoint.
pub(crate) async fn handle_cluster_proxy(
    Path((cluster_id, replica_id, process, path)): Path<(String, String, usize, String)>,
    config: Extension<Arc<ClusterProxyConfig>>,
    req: Request<Body>,
) -> Result<Response, (StatusCode, String)> {
    handle_cluster_proxy_inner(cluster_id, replica_id, process, &path, config, req).await
}

async fn handle_cluster_proxy_inner(
    cluster_id: String,
    replica_id: String,
    process: usize,
    path: &str,
    config: Extension<Arc<ClusterProxyConfig>>,
    mut req: Request<Body>,
) -> Result<Response, (StatusCode, String)> {
    // Parse cluster ID
    let cluster_id: ClusterId = cluster_id.parse().map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("Invalid cluster_id '{cluster_id}': {e}"),
        )
    })?;

    // Parse replica ID
    let replica_id: ReplicaId = replica_id.parse().map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("Invalid replica_id '{replica_id}': {e}"),
        )
    })?;

    // Look up HTTP address for this replica and process
    let http_addr = config
        .locator
        .get_http_addr(cluster_id, replica_id, process)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!(
                    "No HTTP address found for cluster {cluster_id}, replica {replica_id}, process {process}"
                ),
            )
        })?;

    // Build target URI, preserving query string if present
    let path_query = if let Some(query) = req.uri().query() {
        format!("/{path}?{query}")
    } else {
        format!("/{path}")
    };

    let uri = Uri::try_from(format!("http://{http_addr}{path_query}")).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Invalid URI 'http://{http_addr}{path_query}': {e}"),
        )
    })?;

    // Update request with new URI
    *req.uri_mut() = uri.clone();

    // Set Host header to target
    if let Some(host) = uri.host() {
        req.headers_mut().insert(
            HOST,
            HeaderValue::from_str(host).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Invalid host header '{host}': {e}"),
                )
            })?,
        );
    }

    // Connect to the target with timeout
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&*http_addr))
        .await
        .map_err(|_| {
            (
                StatusCode::GATEWAY_TIMEOUT,
                format!("Connection timeout to {http_addr} after {CONNECT_TIMEOUT:?}"),
            )
        })?
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("Failed to connect to {http_addr}: {e}"),
            )
        })?;

    // Perform HTTP/1.1 handshake
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("HTTP handshake with {http_addr} failed: {e}"),
            )
        })?;

    // Spawn task to drive the connection
    mz_ore::task::spawn(|| format!("Proxy to {http_addr}"), async move {
        if let Err(e) = conn.await {
            tracing::debug!("Connection to clusterd {http_addr} closed: {e}");
        }
    });

    // Send the request with timeout
    tokio::time::timeout(REQUEST_TIMEOUT, sender.send_request(req))
        .await
        .map_err(|_| {
            (
                StatusCode::GATEWAY_TIMEOUT,
                format!(
                    "Request timeout to clusterd {cluster_id}/{replica_id}/process/{process} after {REQUEST_TIMEOUT:?}"
                ),
            )
        })?
        .map(|r| r.into_response())
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!(
                    "Error proxying to clusterd {cluster_id}/{replica_id}/process/{process}: {e}"
                ),
            )
        })
}

/// Information about a replica for the clusters page template.
pub struct ReplicaInfo {
    pub cluster_name: String,
    pub replica_name: String,
    pub cluster_id: String,
    pub replica_id: String,
    pub process_count: usize,
    pub process_indices: Vec<usize>,
}

#[derive(Template)]
#[template(path = "clusters.html")]
struct ClustersTemplate<'a> {
    version: &'a str,
    replicas: Vec<ReplicaInfo>,
}

/// Handler for the clusters overview page.
///
/// Displays a table of all cluster replicas with links to their HTTP endpoints.
pub(crate) async fn handle_clusters(client: AuthedClient) -> impl IntoResponse {
    // Look up names from the catalog
    let catalog = client.client.catalog_snapshot("clusters_page").await;

    let mut replicas = Vec::new();
    for cluster in catalog.clusters() {
        for replica in cluster.replicas() {
            let process_count = replica.config.location.num_processes();
            replicas.push(ReplicaInfo {
                cluster_name: cluster.name.clone(),
                replica_name: replica.name.clone(),
                cluster_id: cluster.id.to_string(),
                replica_id: replica.replica_id.to_string(),
                process_count,
                process_indices: (0..process_count).collect(),
            });
        }
    }

    let _ = catalog;

    // Sort by system clusters first (cluster_id starts with 's'), then cluster name, then replica name
    replicas.sort_by(|a, b| {
        let a_is_system = a.cluster_id.starts_with('s');
        let b_is_system = b.cluster_id.starts_with('s');
        b_is_system
            .cmp(&a_is_system)
            .then_with(|| a.cluster_name.cmp(&b.cluster_name))
            .then_with(|| a.replica_name.cmp(&b.replica_name))
    });

    mz_http_util::template_response(ClustersTemplate {
        version: BUILD_INFO.version,
        replicas,
    })
}
