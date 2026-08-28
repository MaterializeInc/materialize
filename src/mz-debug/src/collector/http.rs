// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The collector's HTTP API, which the CLI drives:
//!
//! ```text
//! GET  /api/readyz               readiness probe
//! GET  /api/snapshots            completed snapshots plus what is running or queued
//! POST /api/snapshots            request an on-demand snapshot, body: SnapshotRequest
//! GET  /api/snapshots/latest     the newest snapshot's zip
//! GET  /api/snapshots/{id}       one snapshot's zip
//! ```

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use serde::Serialize;
use tokio_util::io::ReaderStream;
use tracing::warn;

use crate::collector::snapshot::SnapshotRequest;
use crate::collector::store::SnapshotMeta;
use crate::collector::{CollectorHandle, SnapshotStatus};

/// Response header naming the snapshot a zip belongs to, so a client that
/// asked for `latest` learns which one it got.
pub const SNAPSHOT_ID_HEADER: &str = "x-mz-debug-snapshot-id";

#[derive(Debug, Serialize)]
pub struct SnapshotList {
    /// Completed snapshots, oldest first.
    pub snapshots: Vec<SnapshotMeta>,
    pub in_progress: Option<SnapshotStatus>,
    pub pending: Option<SnapshotStatus>,
    /// The failure of the most recent snapshot attempt, if it failed.
    pub last_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotRequested {
    pub id: String,
}

pub fn router(handle: Arc<CollectorHandle>) -> Router {
    Router::new()
        .route("/api/readyz", get(|| async { "ready" }))
        .route("/api/snapshots", get(list).post(request))
        .route("/api/snapshots/latest", get(latest))
        .route("/api/snapshots/{id}", get(download))
        .with_state(handle)
}

fn internal_error(err: anyhow::Error) -> Response {
    warn!("Request failed: {:#}", err);
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response()
}

async fn list(State(handle): State<Arc<CollectorHandle>>) -> Response {
    let snapshots = match handle.store.list() {
        Ok(snapshots) => snapshots,
        Err(e) => return internal_error(e),
    };
    let (in_progress, pending, last_error) = handle.status();
    Json(SnapshotList {
        snapshots,
        in_progress,
        pending,
        last_error,
    })
    .into_response()
}

async fn request(
    State(handle): State<Arc<CollectorHandle>>,
    Json(request): Json<SnapshotRequest>,
) -> Response {
    let id = handle.request_snapshot(&request);
    (StatusCode::ACCEPTED, Json(SnapshotRequested { id })).into_response()
}

async fn latest(State(handle): State<Arc<CollectorHandle>>) -> Response {
    match handle.store.latest() {
        Ok(Some(meta)) => serve_zip(&handle, &meta).await,
        Ok(None) => (StatusCode::NOT_FOUND, "no snapshots yet").into_response(),
        Err(e) => internal_error(e),
    }
}

async fn download(State(handle): State<Arc<CollectorHandle>>, Path(id): Path<String>) -> Response {
    // Ids are file name stems; anything that could escape the store directory
    // is not an id.
    if id.is_empty() || id.contains(['/', '\\']) || id.contains("..") {
        return (StatusCode::BAD_REQUEST, "invalid snapshot id").into_response();
    }
    match handle.store.get(&id) {
        Ok(Some(meta)) => serve_zip(&handle, &meta).await,
        Ok(None) => (StatusCode::NOT_FOUND, "no such snapshot").into_response(),
        Err(e) => internal_error(e),
    }
}

async fn serve_zip(handle: &CollectorHandle, meta: &SnapshotMeta) -> Response {
    let path = handle.store.zip_path(&meta.id);
    let file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(e) => {
            return internal_error(
                anyhow::Error::new(e).context(format!("Failed to open {}", path.display())),
            );
        }
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/zip"),
    );
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(meta.size_bytes));
    if let Ok(value) = HeaderValue::from_str(&meta.id) {
        headers.insert(SNAPSHOT_ID_HEADER, value);
    }
    if let Ok(value) = HeaderValue::from_str(&format!("attachment; filename=\"{}.zip\"", meta.id)) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }
    (headers, Body::from_stream(ReaderStream::new(file))).into_response()
}
