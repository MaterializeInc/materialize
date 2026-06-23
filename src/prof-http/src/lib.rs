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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;
use std::time::Duration;

use askama::Template;
use axum::Json;
use axum::response::IntoResponse;
use axum::routing::{self, Router};
use cfg_if::cfg_if;
use http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, StatusCode};
use mz_build_info::BuildInfo;
use mz_prof::StackProfileExt;
use pprof_util::{ProfStartTime, StackProfile};
use serde::{Deserialize, Serialize};

cfg_if! {
    if #[cfg(any(not(feature = "jemalloc"), miri))] {
        use disabled::{handle_get, handle_get_heap, handle_get_mode, handle_post, handle_post_mode};
    } else {
        use enabled::{handle_get, handle_get_heap, handle_get_mode, handle_post, handle_post_mode};
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
        .route("/cpu", routing::post(handle_post_cpu))
        .route("/mode", routing::get(handle_get_mode).post(handle_post_mode))
        .route("/heap", routing::get(handle_get_heap))
        .route("/static/{*path}", routing::get(handle_static))
}

static CPU_PROFILING_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Deserialize)]
struct CpuProfileRequest {
    seconds: u64,
    hz: u32,
    #[serde(default)]
    merge_threads: bool,
}

#[derive(Debug, Deserialize)]
struct ModeUpdateRequest {
    cpu_active: Option<bool>,
    memory_active: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ModeResponse {
    cpu_active: bool,
    memory_available: bool,
    memory_active: bool,
}

fn cpu_profiling_active() -> bool {
    CPU_PROFILING_ACTIVE.load(Ordering::SeqCst)
}

struct CpuProfilingGuard;

impl CpuProfilingGuard {
    fn acquire() -> Result<Self, (StatusCode, String)> {
        CPU_PROFILING_ACTIVE
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| {
                (
                    StatusCode::CONFLICT,
                    "CPU profiling is already running.".to_owned(),
                )
            })?;
        Ok(Self)
    }
}

impl Drop for CpuProfilingGuard {
    fn drop(&mut self) {
        CPU_PROFILING_ACTIVE.store(false, Ordering::SeqCst);
    }
}

fn validate_cpu_profile_params(
    time_secs: u64,
    sample_freq: u32,
) -> Result<(), (StatusCode, String)> {
    if time_secs == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "`seconds` must be greater than zero.".to_owned(),
        ));
    }
    if sample_freq == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "`hz` must be greater than zero.".to_owned(),
        ));
    }
    Ok(())
}

fn validate_cpu_mode_request(request: &ModeUpdateRequest) -> Result<(), (StatusCode, String)> {
    match request.cpu_active {
        Some(true) => Err((
            StatusCode::BAD_REQUEST,
            "`cpu_active=true` is not supported on `/prof/mode`; use `POST /prof/cpu` to run a capture.".to_owned(),
        )),
        Some(false) if cpu_profiling_active() => Err((
            StatusCode::CONFLICT,
            "CPU profiling is currently running and cannot be interrupted.".to_owned(),
        )),
        _ => Ok(()),
    }
}

fn cpu_pprof_response(stacks: StackProfile) -> impl IntoResponse {
    let pprof = stacks.to_pprof(("samples", "count"), ("cpu", "nanoseconds"), None);
    (
        HeaderMap::from_iter([
            (
                CONTENT_DISPOSITION,
                HeaderValue::from_static("attachment; filename=\"cpu.pb.gz\""),
            ),
            (CONTENT_TYPE, HeaderValue::from_static("application/octet-stream")),
        ]),
        pprof,
    )
}

async fn handle_post_cpu(Json(request): Json<CpuProfileRequest>) -> Result<impl IntoResponse, (StatusCode, String)> {
    let stacks = capture_cpu_profile(request.merge_threads, request.seconds, request.hz).await?;
    Ok(cpu_pprof_response(stacks))
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
async fn capture_cpu_profile(
    merge_threads: bool,
    // the time in seconds to run the profiler for
    time_secs: u64,
    // the sampling frequency in Hz
    sample_freq: u32,
) -> Result<StackProfile, (StatusCode, String)> {
    validate_cpu_profile_params(time_secs, sample_freq)?;
    let _cpu_profiling_guard = CpuProfilingGuard::acquire()?;
    let ctl_lock;
    cfg_if! {
        if #[cfg(any(not(feature = "jemalloc"), miri))] {
            ctl_lock = ();
        } else {
            ctl_lock = if let Some(ctl) = jemalloc_pprof::PROF_CTL.as_ref() {
                let mut borrow = ctl.lock().await;
                borrow.deactivate().map_err(|e| {
                    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
                })?;
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
    Ok(stacks)
}

async fn time_prof(
    merge_threads: bool,
    build_info: &'static BuildInfo,
    // the time in seconds to run the profiler for
    time_secs: u64,
    // the sampling frequency in Hz
    sample_freq: u32,
) -> impl IntoResponse + use<> {
    let stacks = capture_cpu_profile(merge_threads, time_secs, sample_freq).await?;
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

fn flamegraph<'a, 'b>(
    stacks: StackProfile,
    title: &'a str,
    display_bytes: bool,
    extras: &'b [(&'b str, &'b str)],
    build_info: &'static BuildInfo,
) -> impl IntoResponse + use<'a> {
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
    use axum::Json;
    use axum::response::IntoResponse;
    use http::StatusCode;
    use http::header::HeaderMap;
    use mz_build_info::BuildInfo;
    use serde::Deserialize;

    use mz_prof::ever_symbolized;

    use super::{
        MemProfilingStatus, ModeResponse, ModeUpdateRequest, ProfTemplate, cpu_profiling_active,
        time_prof, validate_cpu_mode_request,
    };

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
    pub async fn handle_get_mode() -> Json<ModeResponse> {
        Json(ModeResponse {
            cpu_active: cpu_profiling_active(),
            memory_available: false,
            memory_active: false,
        })
    }

    pub async fn handle_post_mode(
        Json(request): Json<ModeUpdateRequest>,
    ) -> Result<Json<ModeResponse>, (StatusCode, String)> {
        validate_cpu_mode_request(&request)?;
        if request.memory_active == Some(true) {
            return Err((
                StatusCode::FORBIDDEN,
                "Memory profiling is unavailable in this build.".to_owned(),
            ));
        }
        Ok(handle_get_mode().await)
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
    use axum::Json;
    use axum::response::IntoResponse;
    use axum_extra::TypedHeader;
    use bytesize::ByteSize;
    use headers::ContentType;
    use http::header::{CONTENT_DISPOSITION, HeaderMap};
    use http::{HeaderValue, StatusCode};
    use jemalloc_pprof::{JemallocProfCtl, PROF_CTL};
    use mappings::MAPPINGS;
    use mz_build_info::BuildInfo;
    use mz_ore::cast::CastFrom;
    use mz_prof::jemalloc::{JemallocProfCtlExt, JemallocStats};
    use mz_prof::{StackProfileExt, ever_symbolized};
    use pprof_util::parse_jeheap;
    use serde::Deserialize;
    use tokio::sync::Mutex;

    use super::{
        MemProfilingStatus, ModeResponse, ModeUpdateRequest, ProfTemplate, cpu_profiling_active,
        flamegraph, time_prof, validate_cpu_mode_request,
    };

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
            let stats = [
                ("Allocated", stats.allocated),
                ("In active pages", stats.active),
                ("Allocated for allocator metadata", stats.metadata),
                (
                    "Maximum number of bytes in physically resident data pages mapped by the allocator",
                    stats.resident,
                ),
                ("Bytes unused, but retained by allocator", stats.retained),
            ];
            stats
                .into_iter()
                .map(|(k, v)| (k, ByteSize(u64::cast_from(v)).display().si().to_string()))
                .collect()
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

    pub async fn handle_get_mode() -> Json<ModeResponse> {
        let memory_active = current_memory_mode().await;
        Json(mode_response(memory_active, prof_ctl().is_some()))
    }

    pub async fn handle_post_mode(
        Json(request): Json<ModeUpdateRequest>,
    ) -> Result<Json<ModeResponse>, (StatusCode, String)> {
        validate_cpu_mode_request(&request)?;
        if let Some(desired_memory_active) = request.memory_active {
            if desired_memory_active && cpu_profiling_active() {
                return Err((
                    StatusCode::CONFLICT,
                    "Memory profiling cannot be activated while CPU profiling is running."
                        .to_owned(),
                ));
            }
            let prof_ctl = prof_ctl().ok_or_else(|| {
                (
                    StatusCode::FORBIDDEN,
                    "Memory profiling is unavailable in this build.".to_owned(),
                )
            })?;
            let mut borrow = prof_ctl.lock().await;
            if desired_memory_active && !borrow.activated() {
                borrow
                    .activate()
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            } else if !desired_memory_active && borrow.activated() {
                borrow
                    .deactivate()
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            }
            return Ok(Json(mode_response(borrow.activated(), true)));
        }
        Ok(Json(mode_response(
            current_memory_mode().await,
            prof_ctl().is_some(),
        )))
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

    async fn current_memory_mode() -> bool {
        match prof_ctl() {
            Some(prof_ctl) => prof_ctl.lock().await.activated(),
            None => false,
        }
    }

    fn prof_ctl() -> Option<Arc<Mutex<JemallocProfCtl>>> {
        std::panic::catch_unwind(|| PROF_CTL.as_ref().cloned())
            .ok()
            .flatten()
    }

    fn mode_response(memory_active: bool, memory_available: bool) -> ModeResponse {
        ModeResponse {
            cpu_active: cpu_profiling_active(),
            memory_available,
            memory_active,
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::Json;
    use http::StatusCode;

    use super::{
        CPU_PROFILING_ACTIVE, CpuProfileRequest, ModeResponse, ModeUpdateRequest, handle_get_mode,
        handle_post_cpu, handle_post_mode,
    };

    #[tokio::test]
    async fn post_cpu_rejects_zero_seconds() {
        let result = handle_post_cpu(Json(CpuProfileRequest {
            seconds: 0,
            hz: 99,
            merge_threads: false,
        }))
        .await;
        let (status, _) = match result {
            Ok(_) => panic!("zero-second capture must fail"),
            Err(err) => err,
        };
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn post_cpu_rejects_zero_hz() {
        let result = handle_post_cpu(Json(CpuProfileRequest {
            seconds: 1,
            hz: 0,
            merge_threads: false,
        }))
        .await;
        let (status, _) = match result {
            Ok(_) => panic!("zero-hz capture must fail"),
            Err(err) => err,
        };
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn post_mode_rejects_cpu_enable() {
        let result = handle_post_mode(Json(ModeUpdateRequest {
            cpu_active: Some(true),
            memory_active: None,
        }))
        .await;
        let (status, _) = result.expect_err("enabling CPU mode via /mode must fail");
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_mode_reports_cpu_active_state() {
        CPU_PROFILING_ACTIVE.store(true, std::sync::atomic::Ordering::SeqCst);
        let Json(ModeResponse { cpu_active, .. }) = handle_get_mode().await;
        CPU_PROFILING_ACTIVE.store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(cpu_active);
    }

    #[cfg(any(not(feature = "jemalloc"), miri))]
    #[tokio::test]
    async fn post_mode_rejects_memory_activation_when_unavailable() {
        let result = handle_post_mode(Json(ModeUpdateRequest {
            cpu_active: None,
            memory_active: Some(true),
        }))
        .await;
        let (status, _) = result.expect_err("memory activation must fail without jemalloc");
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[cfg(all(feature = "jemalloc", not(miri)))]
    #[tokio::test]
    async fn get_mode_handles_optional_memory_support() {
        let Json(ModeResponse {
            memory_available,
            memory_active,
            ..
        }) = handle_get_mode().await;
        assert!(!memory_active || memory_available);
    }
}
