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

//! Endpoints for the allocation tracker ([`mz_ore::alloc_track`]).
//!
//! * `GET /tracked?view=live|allocated|freed&format=pprof|mzfg|flamegraph`
//!   renders a profile of the current snapshot.
//! * `GET /tracked/config` reports the tracker configuration, and
//!   `POST /tracked/config` with a JSON body of optional `active`,
//!   `track_frees`, `sample_interval`, and `reset_history` fields changes
//!   it and reports the result.

use axum::Json;
use axum::extract::Query;
use axum::response::{IntoResponse, Response};
use http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, StatusCode};
use mz_build_info::BuildInfo;
use mz_ore::alloc_track;
use mz_prof::StackProfileExt;
use mz_prof::tracking::{View, to_stack_profile};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct TrackedQuery {
    view: Option<String>,
    format: Option<String>,
}

pub async fn handle_get(
    Query(query): Query<TrackedQuery>,
    build_info: &'static BuildInfo,
) -> Result<Response, (StatusCode, String)> {
    let (view, sample_type, title) = match query.view.as_deref().unwrap_or("live") {
        "live" => (View::Live, "inuse_space", "Tracked live bytes"),
        "allocated" => (View::Allocated, "alloc_space", "Tracked allocated bytes"),
        "freed" => (View::Freed, "free_space", "Tracked freed bytes"),
        other => return Err((StatusCode::BAD_REQUEST, format!("unknown view: {other}"))),
    };
    let format = query.format.unwrap_or_else(|| "pprof".into());
    // Snapshotting and symbolizing are CPU-bound and can take a while on a
    // large profile.
    let mut profile = mz_ore::task::spawn_blocking(
        || "tracked_snapshot",
        move || to_stack_profile(&alloc_track::snapshot(), view),
    )
    .await;
    if let Some(mappings) = mappings::MAPPINGS.as_ref() {
        profile.mappings = mappings.clone();
    }
    match format.as_str() {
        "pprof" => {
            let pprof = profile.to_pprof((sample_type, "bytes"), ("space", "bytes"), None);
            Ok((
                HeaderMap::from_iter([
                    (
                        CONTENT_DISPOSITION,
                        HeaderValue::from_static("attachment; filename=\"tracked.pb.gz\""),
                    ),
                    (
                        CONTENT_TYPE,
                        HeaderValue::from_static("application/octet-stream"),
                    ),
                ]),
                pprof,
            )
                .into_response())
        }
        "mzfg" => {
            let mzfg = mz_ore::task::spawn_blocking(
                || "tracked_mzfg",
                move || profile.to_mzfg(true, &[("display_bytes", "1")]),
            )
            .await;
            Ok((
                HeaderMap::from_iter([(
                    CONTENT_DISPOSITION,
                    HeaderValue::from_static("attachment; filename=\"tracked.mzfg\""),
                )]),
                mzfg,
            )
                .into_response())
        }
        "flamegraph" => {
            Ok(super::flamegraph(profile, title, true, &[], build_info).into_response())
        }
        other => Err((StatusCode::BAD_REQUEST, format!("unknown format: {other}"))),
    }
}

/// Serves the live profile in pprof format, the tracker's equivalent of
/// jemalloc's heap profile.
pub async fn handle_get_live_pprof(
    build_info: &'static BuildInfo,
) -> Result<Response, (StatusCode, String)> {
    handle_get(
        Query(TrackedQuery {
            view: Some("live".into()),
            format: Some("pprof".into()),
        }),
        build_info,
    )
    .await
}

#[derive(Debug, Deserialize)]
pub struct AllocatorQuery {
    #[serde(default)]
    collect: bool,
}

/// Renders tracker totals next to the underlying allocator's own
/// statistics, after asking the allocator to return cached memory if
/// `collect` is set.
pub async fn handle_get_allocator(Query(query): Query<AllocatorQuery>) -> String {
    use std::fmt::Write;
    mz_ore::task::spawn_blocking(
        || "tracked_allocator",
        move || {
            let hooks = alloc_track::allocator();
            if query.collect {
                if let Some(hooks) = hooks {
                    (hooks.collect)();
                }
            }
            let snapshot = alloc_track::snapshot();
            let mut out = String::new();
            let mut live = std::collections::BTreeMap::new();
            for site in &snapshot.live {
                *live.entry(site.space.name()).or_insert(0.0) += site.bytes;
            }
            for (space, bytes) in live {
                writeln!(out, "tracked live {space}: {bytes:.0} bytes").unwrap();
            }
            writeln!(
                out,
                "tracker: {} live samples, {} stacks, {} free site pairs",
                snapshot.live_samples,
                snapshot.stacks.len(),
                snapshot.freed.len(),
            )
            .unwrap();
            match hooks {
                Some(hooks) => {
                    writeln!(out, "\n{} stats:\n{}", hooks.name, (hooks.stats)()).unwrap()
                }
                None => writeln!(out, "\nno allocator hooks registered").unwrap(),
            }
            out
        },
    )
    .await
}

#[derive(Debug, Deserialize)]
pub struct TrackedConfig {
    active: Option<bool>,
    track_frees: Option<bool>,
    sample_interval: Option<u64>,
    #[serde(default)]
    reset_history: bool,
}

#[derive(Debug, Serialize)]
pub struct TrackedStatus {
    active: bool,
    track_frees: bool,
    sample_interval: u64,
}

fn status() -> Json<TrackedStatus> {
    Json(TrackedStatus {
        active: alloc_track::is_active(),
        track_frees: alloc_track::track_frees(),
        sample_interval: alloc_track::sample_interval(),
    })
}

pub async fn handle_get_config() -> Json<TrackedStatus> {
    status()
}

pub async fn handle_post_config(Json(config): Json<TrackedConfig>) -> Json<TrackedStatus> {
    if let Some(active) = config.active {
        alloc_track::set_active(active);
    }
    if let Some(track) = config.track_frees {
        alloc_track::set_track_frees(track);
    }
    if let Some(interval) = config.sample_interval {
        alloc_track::set_sample_interval(interval);
    }
    if config.reset_history {
        alloc_track::reset_history();
    }
    status()
}
