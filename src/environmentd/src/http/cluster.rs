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

use axum::Extension;
use axum::body::Body;
use axum::extract::Path;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use http::HeaderValue;
use http::header::HOST;
use hyper::Uri;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use mz_controller::ReplicaHttpLocator;
use mz_controller_types::{ClusterId, ReplicaId};

/// Configuration for the cluster HTTP proxy.
pub struct ClusterProxyConfig {
    /// HTTP client for proxying requests (no TLS needed for internal traffic).
    client: Client<HttpConnector, Body>,
    /// Handle to look up replica HTTP addresses.
    locator: Arc<ReplicaHttpLocator>,
}

impl ClusterProxyConfig {
    /// Creates a new `ClusterProxyConfig`.
    pub fn new(locator: Arc<ReplicaHttpLocator>) -> Self {
        let client = Client::builder(TokioExecutor::new()).build_http();
        Self { client, locator }
    }
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
    mut req: Request<Body>,
) -> Result<Response, StatusCode> {
    // Parse cluster ID
    let cluster_id: ClusterId = cluster_id.parse().map_err(|_| {
        tracing::debug!("Invalid cluster_id: {}", cluster_id);
        StatusCode::BAD_REQUEST
    })?;

    // Parse replica ID
    let replica_id: ReplicaId = replica_id.parse().map_err(|_| {
        tracing::debug!("Invalid replica_id: {}", replica_id);
        StatusCode::BAD_REQUEST
    })?;

    // Look up HTTP address for this replica and process
    let http_addr = config
        .locator
        .get_http_addr(cluster_id, replica_id, process)
        .ok_or_else(|| {
            // Provide more specific error logging
            if config.locator.process_count(cluster_id, replica_id).is_none() {
                tracing::debug!(
                    "Replica {cluster_id}/{replica_id} not found for HTTP proxy"
                );
            } else {
                tracing::debug!(
                    "Process {process} out of range for replica {cluster_id}/{replica_id}"
                );
            }
            StatusCode::NOT_FOUND
        })?;

    // Build target URI, preserving query string if present
    let path_query = if let Some(query) = req.uri().query() {
        format!("/{}?{}", path, query)
    } else {
        format!("/{}", path)
    };

    let uri = Uri::try_from(format!("http://{}{}", http_addr, path_query)).map_err(|e| {
        tracing::debug!("Invalid URI: {}", e);
        StatusCode::BAD_REQUEST
    })?;

    // Update request with new URI
    *req.uri_mut() = uri.clone();

    // Set Host header to target
    if let Some(host) = uri.host() {
        req.headers_mut().insert(
            HOST,
            HeaderValue::from_str(host).map_err(|_| StatusCode::BAD_REQUEST)?,
        );
    }

    // Proxy the request
    config
        .client
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
