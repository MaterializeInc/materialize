// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Profiling HTTP endpoints.

use std::fmt::Write;
use std::{borrow::Cow, cell::RefCell, collections::HashMap, future::Future, time::Duration};

use askama::Template;
use cfg_if::cfg_if;
use hyper::{Body, Request, Response};

use super::util;
use crate::http::Server;
use prof_common::{collate_stacks, time::prof_time, ProfStartTime, StackProfile};

impl Server {
    pub fn handle_prof(
        &self,
        req: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        cfg_if! {
            if #[cfg(target_os = "macos")] {
                disabled::handle(req)
            } else {
                enabled::handle(req)
            }
        }
    }
}

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
}

async fn time_prof<'a>(
    params: &HashMap<Cow<'a, str>, Cow<'a, str>>,
) -> anyhow::Result<Response<Body>> {
    let ctl_lock;
    cfg_if! {
        if #[cfg(target_os = "macos")] {
            ctl_lock = ();
        } else {
            ctl_lock = if let Some(ctl) = prof_jemalloc::PROF_CTL.as_ref() {
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
    let stacks = unsafe { prof_time(Duration::from_secs(10), 99, merge_threads) }.await?;
    // Fail with a compile error if we weren't holding the jemalloc lock.
    drop(ctl_lock);
    flamegraph(stacks, "CPU Time Flamegraph", false)
}

fn flamegraph(
    stacks: StackProfile,
    title: &str,
    display_bytes: bool,
) -> anyhow::Result<Response<Body>> {
    let collated = collate_stacks(stacks);
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
                data_json.borrow_mut().push_str(",");
            }
        },
    );
    let data_json = &*data_json.borrow();
    Ok(util::template_response(FlamegraphTemplate {
        version: crate::VERSION,
        title,
        data_json,
        display_bytes,
    }))
}

mod disabled {
    use hyper::{Body, Method, Request, Response, StatusCode};

    use super::{time_prof, MemProfilingStatus, ProfTemplate};
    use crate::http::util;
    use std::collections::HashMap;
    use url::form_urlencoded;

    pub async fn handle(req: Request<Body>) -> anyhow::Result<Response<Body>> {
        match req.method() {
            &Method::GET => Ok(util::template_response(ProfTemplate {
                version: crate::VERSION,
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

    use std::io::{BufReader, Read};
    use std::{collections::HashMap, sync::Arc};

    use hyper::{header, Body, Method, Request, Response, StatusCode};
    use tokio::sync::Mutex;
    use url::form_urlencoded;

    use prof_jemalloc::{parse_jeheap, JemallocProfCtl, JemallocProfMetadata, PROF_CTL};

    use super::{flamegraph, time_prof, MemProfilingStatus, ProfTemplate};
    use crate::http::util;

    pub async fn handle(req: Request<Body>) -> anyhow::Result<Response<Body>> {
        match (req.method(), &*PROF_CTL) {
            (&Method::GET, Some(prof_ctl)) => {
                let prof_md = prof_ctl.lock().await.get_md();
                handle_get(prof_md)
            }

            (&Method::POST, Some(prof_ctl)) => handle_post(req, prof_ctl).await,

            _ => super::disabled::handle(req).await,
        }
    }

    pub async fn handle_post(
        body: Request<Body>,
        prof_ctl: &Arc<Mutex<JemallocProfCtl>>,
    ) -> Result<Response<Body>, anyhow::Error> {
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
                let md = {
                    let mut borrow = prof_ctl.lock().await;
                    borrow.activate()?;
                    borrow.get_md()
                };
                handle_get(md)
            }
            "deactivate" => {
                let md = {
                    let mut borrow = prof_ctl.lock().await;
                    borrow.deactivate()?;
                    borrow.get_md()
                };
                handle_get(md)
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
            "mem_fg" => {
                let mut borrow = prof_ctl.lock().await;
                let f = borrow.dump()?;
                let r = BufReader::new(f);
                let stacks = parse_jeheap(r)?;
                flamegraph(stacks, "Heap Flamegraph", true)
            }
            "time_fg" => time_prof(&params).await,
            x => Ok(util::error_response(
                StatusCode::BAD_REQUEST,
                format!("unrecognized `action` parameter: {}", x),
            )),
        }
    }

    pub fn handle_get(prof_md: JemallocProfMetadata) -> anyhow::Result<Response<Body>> {
        Ok(util::template_response(ProfTemplate {
            version: crate::VERSION,
            mem_prof: MemProfilingStatus::Enabled(prof_md.start_time),
        }))
    }
}
