// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Profiling HTTP endpoints.

use std::cell::RefCell;
use std::env;
use std::fmt::Write;
use std::time::Duration;

use askama::Template;
use axum::response::IntoResponse;
use axum::routing::{self, Router};
use cfg_if::cfg_if;
use http::StatusCode;
use mz_build_info::BuildInfo;
use once_cell::sync::Lazy;

use crate::{ProfStartTime, StackProfile};

cfg_if! {
    if #[cfg(any(target_os = "macos", not(feature = "jemalloc")))] {
        use disabled::{handle_get, handle_post};
    } else {
        use enabled::{handle_get, handle_post};
    }
}

static EXECUTABLE: Lazy<String> = Lazy::new(|| {
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
    include_dir::include_dir!("$CARGO_MANIFEST_DIR/src/http/static"),
    "src/http/static",
    "src/http/static-dev"
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
        .route("/static/*path", routing::get(handle_static))
}

#[allow(dead_code)]
enum MemProfilingStatus {
    Disabled,
    Enabled(Option<ProfStartTime>),
}

#[derive(Template)]
#[template(path = "http/templates/prof.html")]
struct ProfTemplate<'a> {
    version: &'a str,
    executable: &'a str,
    mem_prof: MemProfilingStatus,
}

#[derive(Template)]
#[template(path = "http/templates/flamegraph.html")]
struct FlamegraphTemplate<'a> {
    version: &'a str,
    title: &'a str,
    data_json: &'a str,
    display_bytes: bool,
    extras: &'a [&'a str],
}

#[allow(clippy::drop_copy)]
async fn time_prof<'a>(merge_threads: bool, build_info: &BuildInfo) -> impl IntoResponse {
    let ctl_lock;
    cfg_if! {
        if #[cfg(any(target_os = "macos", not(feature = "jemalloc")))] {
            ctl_lock = ();
        } else {
            ctl_lock = if let Some(ctl) = crate::jemalloc::PROF_CTL.as_ref() {
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
    let stacks = unsafe { crate::time::prof_time(Duration::from_secs(10), 99, merge_threads) }
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Fail with a compile error if we weren't holding the jemalloc lock.
    drop(ctl_lock);
    Ok::<_, (StatusCode, String)>(flamegraph(
        stacks,
        "CPU Time Flamegraph",
        false,
        &[],
        &build_info,
    ))
}

fn flamegraph(
    stacks: StackProfile,
    title: &str,
    display_bytes: bool,
    extras: &[&str],
    build_info: &BuildInfo,
) -> impl IntoResponse {
    let collated = crate::collate_stacks(stacks);
    let data_json = RefCell::new(String::new());
    collated.dfs(
        |node| {
            write!(
                data_json.borrow_mut(),
                "{{\"name\": \"{}\",\"value\":{},\"children\":[",
                node.name,
                node.weight
            )
            .unwrap(); // String's `std::fmt::Write` implementation never fails
        },
        |_node, is_last| {
            data_json.borrow_mut().push_str("]}");
            if !is_last {
                data_json.borrow_mut().push(',');
            }
        },
    );
    let data_json = &*data_json.borrow();
    mz_http_util::template_response(FlamegraphTemplate {
        version: build_info.version,
        title,
        data_json,
        display_bytes,
        extras,
    })
}

#[cfg(any(target_os = "macos", not(feature = "jemalloc")))]
mod disabled {
    use axum::extract::{Form, Query};
    use axum::response::IntoResponse;
    use http::header::HeaderMap;
    use http::StatusCode;
    use serde::Deserialize;

    use mz_build_info::BuildInfo;

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
        })
    }

    #[derive(Deserialize)]
    pub struct ProfForm {
        action: String,
        threads: Option<String>,
    }

    pub async fn handle_post(
        Form(ProfForm { action, threads }): Form<ProfForm>,
        build_info: &'static BuildInfo,
    ) -> impl IntoResponse {
        let merge_threads = threads.as_deref() == Some("merge");
        match action.as_ref() {
            "time_fg" => Ok(time_prof(merge_threads, build_info).await),
            _ => Err((
                StatusCode::BAD_REQUEST,
                format!("unrecognized `action` parameter: {}", action),
            )),
        }
    }
}

#[cfg(all(not(target_os = "macos"), feature = "jemalloc"))]
mod enabled {
    use std::borrow::Cow;
    use std::fmt::Write;
    use std::io::{BufReader, Read};
    use std::sync::Arc;

    use axum::extract::{Form, Query};
    use axum::response::IntoResponse;
    use axum::TypedHeader;
    use headers::ContentType;
    use http::header::{HeaderMap, CONTENT_DISPOSITION};
    use http::{HeaderValue, StatusCode};
    use serde::Deserialize;
    use tokio::sync::Mutex;

    use crate::jemalloc::{parse_jeheap, JemallocProfCtl, PROF_CTL};
    use crate::symbolicate;

    use super::{flamegraph, time_prof, MemProfilingStatus, ProfTemplate};
    use mz_build_info::BuildInfo;

    struct HumanFormattedBytes(usize);
    impl std::fmt::Display for HumanFormattedBytes {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            const TOKENS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];

            let mut counter = self.0 as f64;
            let mut tok_i = 0;
            while counter >= 1024.0 && tok_i < TOKENS.len() - 1 {
                counter /= 1024.0;
                tok_i += 1;
            }

            write!(f, "{:.2} {}", counter, TOKENS[tok_i])
        }
    }

    #[derive(Deserialize)]
    pub struct ProfForm {
        action: String,
        threads: Option<String>,
    }

    pub async fn handle_post(
        Form(ProfForm { action, threads }): Form<ProfForm>,
        build_info: &'static BuildInfo,
    ) -> impl IntoResponse {
        let prof_ctl = PROF_CTL.as_ref().unwrap();
        let merge_threads = threads.as_deref() == Some("merge");
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
            "dump_file" => {
                let mut borrow = prof_ctl.lock().await;
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
            "dump_symbolicated_file" => {
                let mut borrow = prof_ctl.lock().await;
                let f = borrow
                    .dump()
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let r = BufReader::new(f);
                let stacks = parse_jeheap(r)
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let syms = symbolicate(&stacks);
                let mut s = String::new();
                // Emitting the format expected by Brendan Gregg's flamegraph tool.
                //
                // foo;bar;quux 30
                // foo;bar;asdf 40
                //
                // etc.
                for (stack, _anno) in stacks.iter() {
                    for (i, addr) in stack.addrs.iter().enumerate() {
                        let syms = match syms.get(addr) {
                            Some(syms) => Cow::Borrowed(syms),
                            None => Cow::Owned(vec!["???".to_string()]),
                        };
                        for (j, sym) in syms.iter().enumerate() {
                            if j != 0 || i != 0 {
                                s.push_str(";");
                            }
                            s.push_str(&*sym);
                        }
                    }
                    writeln!(&mut s, " {}", stack.weight).unwrap();
                }
                Ok((
                    HeaderMap::from_iter([(
                        CONTENT_DISPOSITION,
                        HeaderValue::from_static("attachment; filename=\"mz.fg\""),
                    )]),
                    s,
                )
                    .into_response())
            }
            "mem_fg" => {
                let mut borrow = prof_ctl.lock().await;
                let f = borrow
                    .dump()
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let r = BufReader::new(f);
                let stacks = parse_jeheap(r)
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let stats = borrow
                    .stats()
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let stats_rendered = &[
                    format!("Allocated: {}", HumanFormattedBytes(stats.allocated)),
                    format!("In active pages: {}", HumanFormattedBytes(stats.active)),
                    format!(
                        "Allocated for allocator metadata: {}",
                        HumanFormattedBytes(stats.metadata)
                    ),
                    // Don't print `stats.resident` since it is a bit hard to interpret;
                    // see `man jemalloc` for details.
                    format!(
                        "Bytes unused, but retained by allocator: {}",
                        HumanFormattedBytes(stats.retained)
                    ),
                ];
                let stats_rendered = stats_rendered
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                Ok(
                    flamegraph(stacks, "Heap Flamegraph", true, &stats_rendered, build_info)
                        .into_response(),
                )
            }
            "time_fg" => Ok(time_prof(merge_threads, build_info).await.into_response()),
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

    async fn render_template(
        prof_ctl: &Arc<Mutex<JemallocProfCtl>>,
        build_info: &'static BuildInfo,
    ) -> impl IntoResponse {
        let prof_md = prof_ctl.lock().await.get_md();
        mz_http_util::template_response(ProfTemplate {
            version: build_info.version,
            executable: &super::EXECUTABLE,
            mem_prof: MemProfilingStatus::Enabled(prof_md.start_time),
        })
    }
}
