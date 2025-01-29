// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Profiling HTTP endpoints.

use std::env;
use std::sync::LazyLock;
use std::time::Duration;

use askama::Template;
use axum::response::IntoResponse;
use axum::routing::{self, Router};
use cfg_if::cfg_if;
use http::StatusCode;
use mz_build_info::BuildInfo;
use mz_prof::StackProfileExt;
use pprof_util::{ProfStartTime, StackProfile};

cfg_if! {
    if #[cfg(any(not(feature = "jemalloc"), miri))] {
        use disabled::{handle_get, handle_post, handle_get_heap};
    } else {
        use enabled::{handle_get, handle_post, handle_get_heap};
    }
}

static EXECUTABLE: LazyLock<String> = LazyLock::new(|| {
    {
        env::current_exe()
            .ok()
            .as_ref()
            .and_then(|exe| exe.file_name())
            .map(|exe| exe.to_string_lossy().into_owned())
            .unwrap_or_else(|| "<unknown executable>".into())
    }
});

mz_http_util::make_handle_static!(
    dir_1: ::include_dir::include_dir!("$CARGO_MANIFEST_DIR/src/http/static"),
    dir_2: ::include_dir::include_dir!("$OUT_DIR/src/http/static"),
    prod_base_path: "src/http/static",
    dev_base_path: "src/http/static-dev",
);

/// Creates a router that serves the profiling endpoints.
pub fn router(build_info: &'static BuildInfo) -> Router {
    Router::new()
        .route(
            "/",
            routing::get(move |query, headers| handle_get(query, headers, build_info)),
        )
        .route(
            "/",
            routing::post(move |form| handle_post(form, build_info)),
        )
        .route("/heap", routing::get(handle_get_heap))
        .route("/static/*path", routing::get(handle_static))
}

#[allow(dead_code)]
enum MemProfilingStatus {
    Disabled,
    Enabled(Option<ProfStartTime>),
}

#[derive(Template)]
#[template(path = "prof.html")]
struct ProfTemplate<'a> {
    version: &'a str,
    executable: &'a str,
    mem_prof: MemProfilingStatus,
    ever_symbolized: bool,
}

#[derive(Template)]
#[template(path = "flamegraph.html")]
pub struct FlamegraphTemplate<'a> {
    pub version: &'a str,
    pub title: &'a str,
    pub mzfg: &'a str,
}

#[allow(dropping_copy_types)]
async fn time_prof<'a>(
    merge_threads: bool,
    build_info: &BuildInfo,
    // the time in seconds to run the profiler for
    time_secs: u64,
    // the sampling frequency in Hz
    sample_freq: u32,
) -> impl IntoResponse {
    let ctl_lock;
    cfg_if! {
        if #[cfg(any(not(feature = "jemalloc"), miri))] {
            ctl_lock = ();
        } else {
            ctl_lock = if let Some(ctl) = jemalloc_pprof::PROF_CTL.as_ref() {
                let mut borrow = ctl.lock().await;
                borrow.deactivate().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                Some(borrow)
            } else {
                None
            };
        }
    }
    // SAFETY: We ensure above that memory profiling is off.
    // Since we hold the mutex, nobody else can be turning it back on in the intervening time.
    let stacks = unsafe {
        mz_prof::time::prof_time(Duration::from_secs(time_secs), sample_freq, merge_threads)
    }
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Fail with a compile error if we weren't holding the jemalloc lock.
    drop(ctl_lock);
    let (secs_s, freq_s) = (format!("{time_secs}"), format!("{sample_freq}"));
    Ok::<_, (StatusCode, String)>(flamegraph(
        stacks,
        "CPU Time Flamegraph",
        false,
        &[
            ("Sampling time (s)", &secs_s),
            ("Sampling frequency (Hz)", &freq_s),
        ],
        build_info,
    ))
}

fn flamegraph(
    stacks: StackProfile,
    title: &str,
    display_bytes: bool,
    extras: &[(&str, &str)],
    build_info: &BuildInfo,
) -> impl IntoResponse {
    let mut header_extra = vec![];
    if display_bytes {
        header_extra.push(("display_bytes", "1"));
    }
    for (k, v) in extras {
        header_extra.push((k, v));
    }
    let mzfg = stacks.to_mzfg(true, &header_extra);
    mz_http_util::template_response(FlamegraphTemplate {
        version: build_info.version,
        title,
        mzfg: &mzfg,
    })
}

#[cfg(any(not(feature = "jemalloc"), miri))]
mod disabled {
    use axum::extract::{Form, Query};
    use axum::response::IntoResponse;
    use http::header::HeaderMap;
    use http::StatusCode;
    use mz_build_info::BuildInfo;
    use serde::Deserialize;

    use mz_prof::ever_symbolized;

    use super::{time_prof, MemProfilingStatus, ProfTemplate};

    #[derive(Deserialize)]
    pub struct ProfQuery {
        _action: Option<String>,
    }

    #[allow(clippy::unused_async)]
    pub async fn handle_get(
        _: Query<ProfQuery>,
        _: HeaderMap,
        build_info: &'static BuildInfo,
    ) -> impl IntoResponse {
        mz_http_util::template_response(ProfTemplate {
            version: build_info.version,
            executable: &super::EXECUTABLE,
            mem_prof: MemProfilingStatus::Disabled,
            ever_symbolized: ever_symbolized(),
        })
    }

    #[derive(Deserialize)]
    pub struct ProfForm {
        action: String,
        threads: Option<String>,
        time_secs: Option<u64>,
        hz: Option<u32>,
    }

    pub async fn handle_post(
        Form(ProfForm {
            action,
            threads,
            time_secs,
            hz,
        }): Form<ProfForm>,
        build_info: &'static BuildInfo,
    ) -> impl IntoResponse {
        let merge_threads = threads.as_deref() == Some("merge");
        match action.as_ref() {
            "time_fg" => {
                let time_secs = time_secs.ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        "Expected value for `time_secs`".to_owned(),
                    )
                })?;
                let hz = hz.ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        "Expected value for `hz`".to_owned(),
                    )
                })?;

                Ok(time_prof(merge_threads, build_info, time_secs, hz).await)
            }
            _ => Err((
                StatusCode::BAD_REQUEST,
                format!("unrecognized `action` parameter: {}", action),
            )),
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn handle_get_heap() -> Result<(), (StatusCode, String)> {
        Err((
            StatusCode::BAD_REQUEST,
            "This software was compiled without heap profiling support.".to_string(),
        ))
    }
}

#[cfg(all(feature = "jemalloc", not(miri)))]
mod enabled {
    use std::io::{BufReader, Read};
    use std::sync::Arc;

    use axum::extract::{Form, Query};
    use axum::response::IntoResponse;
    use axum_extra::TypedHeader;
    use bytesize::ByteSize;
    use headers::ContentType;
    use http::header::{HeaderMap, CONTENT_DISPOSITION};
    use http::{HeaderValue, StatusCode};
    use jemalloc_pprof::{JemallocProfCtl, PROF_CTL};
    use mappings::MAPPINGS;
    use mz_build_info::BuildInfo;
    use mz_ore::cast::CastFrom;
    use mz_prof::jemalloc::{JemallocProfCtlExt, JemallocStats};
    use mz_prof::{ever_symbolized, StackProfileExt};
    use pprof_util::parse_jeheap;
    use serde::Deserialize;
    use tokio::sync::Mutex;

    use super::{flamegraph, time_prof, MemProfilingStatus, ProfTemplate};

    #[derive(Deserialize)]
    pub struct ProfForm {
        action: String,
        threads: Option<String>,
        time_secs: Option<u64>,
        hz: Option<u32>,
    }

    pub async fn handle_post(
        Form(ProfForm {
            action,
            threads,
            time_secs,
            hz,
        }): Form<ProfForm>,
        build_info: &'static BuildInfo,
    ) -> impl IntoResponse {
        let prof_ctl = PROF_CTL.as_ref().unwrap();
        let merge_threads = threads.as_deref() == Some("merge");

        fn render_jemalloc_stats(stats: &JemallocStats) -> Vec<(&str, String)> {
            vec![
                (
                    "Allocated",
                    ByteSize(u64::cast_from(stats.allocated)).to_string_as(true),
                ),
                (
                    "In active pages",
                    ByteSize(u64::cast_from(stats.active)).to_string_as(true),
                ),
                (
                    "Allocated for allocator metadata",
                    ByteSize(u64::cast_from(stats.metadata)).to_string_as(true),
                ),
                (
                    "Maximum number of bytes in physically resident data pages mapped by the allocator",
                    ByteSize(u64::cast_from(stats.resident)).to_string_as(true),
                ),
                (
                    "Bytes unused, but retained by allocator",
                    ByteSize(u64::cast_from(stats.retained)).to_string_as(true),
                ),
            ]
        }

        match action.as_str() {
            "activate" => {
                {
                    let mut borrow = prof_ctl.lock().await;
                    borrow
                        .activate()
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                };
                Ok(render_template(prof_ctl, build_info).await.into_response())
            }
            "deactivate" => {
                {
                    let mut borrow = prof_ctl.lock().await;
                    borrow
                        .deactivate()
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                };
                Ok(render_template(prof_ctl, build_info).await.into_response())
            }
            "dump_jeheap" => {
                let mut borrow = prof_ctl.lock().await;
                require_profiling_activated(&borrow)?;
                let mut f = borrow
                    .dump()
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let mut s = String::new();
                f.read_to_string(&mut s)
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                Ok((
                    HeaderMap::from_iter([(
                        CONTENT_DISPOSITION,
                        HeaderValue::from_static("attachment; filename=\"jeprof.heap\""),
                    )]),
                    s,
                )
                    .into_response())
            }
            "dump_sym_mzfg" => {
                let mut borrow = prof_ctl.lock().await;
                require_profiling_activated(&borrow)?;
                let f = borrow
                    .dump()
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let r = BufReader::new(f);
                let stacks = parse_jeheap(r, MAPPINGS.as_deref())
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let stats = borrow
                    .stats()
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let stats_rendered = render_jemalloc_stats(&stats);
                let mut header = stats_rendered
                    .iter()
                    .map(|(k, v)| (*k, v.as_str()))
                    .collect::<Vec<_>>();
                header.push(("display_bytes", "1"));
                let mzfg = stacks.to_mzfg(true, &header);
                Ok((
                    HeaderMap::from_iter([(
                        CONTENT_DISPOSITION,
                        HeaderValue::from_static("attachment; filename=\"trace.mzfg\""),
                    )]),
                    mzfg,
                )
                    .into_response())
            }
            "mem_fg" => {
                let mut borrow = prof_ctl.lock().await;
                require_profiling_activated(&borrow)?;
                let f = borrow
                    .dump()
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let r = BufReader::new(f);
                let stacks = parse_jeheap(r, MAPPINGS.as_deref())
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let stats = borrow
                    .stats()
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let stats_rendered = render_jemalloc_stats(&stats);
                let stats_rendered = stats_rendered
                    .iter()
                    .map(|(k, v)| (*k, v.as_str()))
                    .collect::<Vec<_>>();
                Ok(
                    flamegraph(stacks, "Heap Flamegraph", true, &stats_rendered, build_info)
                        .into_response(),
                )
            }
            "time_fg" => {
                let time_secs = time_secs.ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        "Expected value for `time_secs`".to_owned(),
                    )
                })?;
                let hz = hz.ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        "Expected value for `hz`".to_owned(),
                    )
                })?;
                Ok(time_prof(merge_threads, build_info, time_secs, hz)
                    .await
                    .into_response())
            }
            x => Err((
                StatusCode::BAD_REQUEST,
                format!("unrecognized `action` parameter: {}", x),
            )),
        }
    }

    #[derive(Deserialize)]
    pub struct ProfQuery {
        action: Option<String>,
    }

    pub async fn handle_get(
        Query(query): Query<ProfQuery>,
        headers: HeaderMap,
        build_info: &'static BuildInfo,
    ) -> impl IntoResponse {
        let prof_ctl = PROF_CTL.as_ref().unwrap();
        match query.action.as_deref() {
            Some("dump_stats") => {
                let json = headers
                    .get("accept")
                    .map_or(false, |accept| accept.as_bytes() == b"application/json");
                let mut borrow = prof_ctl.lock().await;
                let s = borrow
                    .dump_stats(json)
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let content_type = match json {
                    false => ContentType::text(),
                    true => ContentType::json(),
                };
                Ok((TypedHeader(content_type), s).into_response())
            }
            Some(x) => Err((
                StatusCode::BAD_REQUEST,
                format!("unrecognized query: {}", x),
            )),
            None => Ok(render_template(prof_ctl, build_info).await.into_response()),
        }
    }

    pub async fn handle_get_heap() -> Result<impl IntoResponse, (StatusCode, String)> {
        let mut prof_ctl = PROF_CTL.as_ref().unwrap().lock().await;
        require_profiling_activated(&prof_ctl)?;
        let dump_file = prof_ctl
            .dump()
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        let dump_reader = BufReader::new(dump_file);
        let profile = parse_jeheap(dump_reader, MAPPINGS.as_deref())
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        let pprof = profile.to_pprof(("inuse_space", "bytes"), ("space", "bytes"), None);
        Ok(pprof)
    }

    async fn render_template(
        prof_ctl: &Arc<Mutex<JemallocProfCtl>>,
        build_info: &'static BuildInfo,
    ) -> impl IntoResponse {
        let prof_md = prof_ctl.lock().await.get_md();
        mz_http_util::template_response(ProfTemplate {
            version: build_info.version,
            executable: &super::EXECUTABLE,
            mem_prof: MemProfilingStatus::Enabled(prof_md.start_time),
            ever_symbolized: ever_symbolized(),
        })
    }

    /// Checks whether jemalloc profiling is activated an returns an error response if not.
    fn require_profiling_activated(prof_ctl: &JemallocProfCtl) -> Result<(), (StatusCode, String)> {
        if prof_ctl.activated() {
            Ok(())
        } else {
            Err((StatusCode::FORBIDDEN, "heap profiling not activated".into()))
        }
    }
}
