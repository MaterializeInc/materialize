// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Profiling HTTP endpoints.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write;
use std::time::Duration;

use askama::Template;
use cfg_if::cfg_if;
use hyper::{Body, Request, Response};

use mz_prof::{ProfStartTime, StackProfile};

use crate::http::util;
use crate::BUILD_INFO;

pub async fn handle_prof(
    req: Request<Body>,
    _: &mut mz_coord::SessionClient,
) -> Result<Response<Body>, anyhow::Error> {
    cfg_if! {
        if #[cfg(target_os = "macos")] {
            disabled::handle(req).await
        } else {
            enabled::handle(req).await
        }
    }
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

#[allow(clippy::drop_copy, clippy::unit_arg)]
async fn time_prof<'a>(
    params: &HashMap<Cow<'a, str>, Cow<'a, str>>,
) -> anyhow::Result<Response<Body>> {
    let ctl_lock;
    cfg_if! {
        if #[cfg(target_os = "macos")] {
            ctl_lock = ();
        } else {
            ctl_lock = if let Some(ctl) = mz_prof::jemalloc::PROF_CTL.as_ref() {
                let mut borrow = ctl.lock().await;
                borrow.deactivate()?;
                Some(borrow)
            } else {
                None
            };
        }
    }
    let merge_threads = params.get("threads").map(AsRef::as_ref) == Some("merge");
    // SAFETY: We ensure above that memory profiling is off.
    // Since we hold the mutex, nobody else can be turning it back on in the intervening time.
    let stacks =
        unsafe { mz_prof::time::prof_time(Duration::from_secs(10), 99, merge_threads) }.await?;
    // Fail with a compile error if we weren't holding the jemalloc lock.
    drop(ctl_lock);
    flamegraph(stacks, "CPU Time Flamegraph", false, &[])
}

fn flamegraph(
    stacks: StackProfile,
    title: &str,
    display_bytes: bool,
    extras: &[&str],
) -> anyhow::Result<Response<Body>> {
    let collated = mz_prof::collate_stacks(stacks);
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
    Ok(util::template_response(FlamegraphTemplate {
        version: BUILD_INFO.version,
        title,
        data_json,
        display_bytes,
        extras,
    }))
}

mod disabled {
    use std::collections::HashMap;

    use hyper::{Body, Method, Request, Response, StatusCode};
    use url::form_urlencoded;

    use super::{time_prof, MemProfilingStatus, ProfTemplate};
    use crate::http::util;
    use crate::BUILD_INFO;

    pub async fn handle(req: Request<Body>) -> anyhow::Result<Response<Body>> {
        match req.method() {
            &Method::GET => Ok(util::template_response(ProfTemplate {
                version: BUILD_INFO.version,
                mem_prof: MemProfilingStatus::Disabled,
            })),
            &Method::POST => handle_post(req).await,
            method => Ok(util::error_response(
                StatusCode::BAD_REQUEST,
                format!("Unrecognized request method: {:?}", method),
            )),
        }
    }

    async fn handle_post(body: Request<Body>) -> Result<Response<Body>, anyhow::Error> {
        let body = hyper::body::to_bytes(body).await?;
        let params: HashMap<_, _> = form_urlencoded::parse(&body).collect();
        let action = match params.get("action") {
            Some(action) => action,
            None => {
                return Ok(util::error_response(
                    StatusCode::BAD_REQUEST,
                    "expected `action` parameter",
                ))
            }
        };
        match action.as_ref() {
            "time_fg" => time_prof(&params).await,
            x => Ok(util::error_response(
                StatusCode::BAD_REQUEST,
                format!("unrecognized `action` parameter: {}", x),
            )),
        }
    }
}

#[cfg(not(target_os = "macos"))]
mod enabled {
    use std::borrow::Cow;
    use std::collections::HashMap;
    use std::fmt::Write;
    use std::io::{BufReader, Read};
    use std::sync::Arc;

    use hyper::http::HeaderValue;
    use hyper::{header, Body, Method, Request, Response, StatusCode};
    use mz_prof::symbolicate;
    use tokio::sync::Mutex;
    use url::form_urlencoded;

    use mz_prof::jemalloc::{parse_jeheap, JemallocProfCtl, PROF_CTL};

    use super::{flamegraph, time_prof, MemProfilingStatus, ProfTemplate};
    use crate::http::util;
    use crate::BUILD_INFO;

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

    pub async fn handle(req: Request<Body>) -> anyhow::Result<Response<Body>> {
        let accept = req.headers().get("Accept").cloned();
        match (req.method(), &*PROF_CTL) {
            (&Method::GET, Some(prof_ctl)) => handle_get(req.uri().query(), accept, prof_ctl).await,

            (&Method::POST, Some(prof_ctl)) => handle_post(req, accept, prof_ctl).await,

            _ => super::disabled::handle(req).await,
        }
    }

    pub async fn handle_post(
        body: Request<Body>,
        accept: Option<HeaderValue>,
        prof_ctl: &Arc<Mutex<JemallocProfCtl>>,
    ) -> Result<Response<Body>, anyhow::Error> {
        let query = body.uri().query().map(str::to_string);
        let body = hyper::body::to_bytes(body).await?;
        let params: HashMap<_, _> = form_urlencoded::parse(&body).collect();
        let action = match params.get("action") {
            Some(action) => action,
            None => {
                return Ok(util::error_response(
                    StatusCode::BAD_REQUEST,
                    "expected `action` parameter",
                ))
            }
        };
        match action.as_ref() {
            "activate" => {
                {
                    let mut borrow = prof_ctl.lock().await;
                    borrow.activate()?;
                };
                handle_get(query.as_ref().map(String::as_str), accept, prof_ctl).await
            }
            "deactivate" => {
                {
                    let mut borrow = prof_ctl.lock().await;
                    borrow.deactivate()?;
                };
                handle_get(query.as_ref().map(String::as_str), accept, prof_ctl).await
            }
            "dump_file" => {
                let mut borrow = prof_ctl.lock().await;
                let mut f = borrow.dump()?;
                let mut s = String::new();
                f.read_to_string(&mut s)?;
                Ok(Response::builder()
                    .header(
                        header::CONTENT_DISPOSITION,
                        "attachment; filename=\"jeprof.heap\"",
                    )
                    .body(Body::from(s))
                    .unwrap())
            }
            "dump_stats" => handle_get(query.as_ref().map(String::as_str), accept, prof_ctl).await,
            "dump_symbolicated_file" => {
                let mut borrow = prof_ctl.lock().await;
                let f = borrow.dump()?;
                let r = BufReader::new(f);
                let stacks = parse_jeheap(r)?;
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
                Ok(Response::builder()
                    .header(
                        header::CONTENT_DISPOSITION,
                        "attachment; filename=\"mz.fg\"",
                    )
                    .body(Body::from(s))
                    .unwrap())
            }
            "mem_fg" => {
                let mut borrow = prof_ctl.lock().await;
                let f = borrow.dump()?;
                let r = BufReader::new(f);
                let stacks = parse_jeheap(r)?;
                let stats = borrow.stats()?;
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
                flamegraph(stacks, "Heap Flamegraph", true, &stats_rendered)
            }
            "time_fg" => time_prof(&params).await,
            x => Ok(util::error_response(
                StatusCode::BAD_REQUEST,
                format!("unrecognized `action` parameter: {}", x),
            )),
        }
    }

    pub async fn handle_get(
        query: Option<&str>,
        accept: Option<HeaderValue>,
        prof_ctl: &Arc<Mutex<JemallocProfCtl>>,
    ) -> anyhow::Result<Response<Body>> {
        match query {
            Some("dump_stats") => {
                let json = accept.map_or(false, |accept| accept.as_bytes() == b"application/json");
                let mut borrow = prof_ctl.lock().await;
                let s = borrow.dump_stats(json)?;
                Ok(Response::builder()
                    .header(header::CONTENT_TYPE, "text/plain")
                    .body(Body::from(s))
                    .unwrap())
            }
            Some(x) => Ok(util::error_response(
                StatusCode::BAD_REQUEST,
                format!("unrecognized query: {}", x),
            )),
            None => {
                let prof_md = prof_ctl.lock().await.get_md();
                return Ok(util::template_response(ProfTemplate {
                    version: BUILD_INFO.version,
                    mem_prof: MemProfilingStatus::Enabled(prof_md.start_time),
                }));
            }
        }
    }
}
