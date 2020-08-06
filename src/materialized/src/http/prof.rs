// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Profiling HTTP endpoints.

use std::future::Future;

use askama::Template;
use cfg_if::cfg_if;
use hyper::{Body, Request, Response};

use crate::http::Server;
use prof::ProfStartTime;

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

#[derive(Template)]
#[template(path = "http/templates/prof.html")]
struct ProfTemplate<'a> {
    version: &'a str,
    mem_prof: Option<Option<ProfStartTime>>,
}

mod disabled {
    use hyper::{Body, Request, Response};

    use super::ProfTemplate;
    use crate::http::util;

    pub async fn handle(_: Request<Body>) -> anyhow::Result<Response<Body>> {
        Ok(util::template_response(ProfTemplate {
            version: crate::VERSION,
            mem_prof: None,
        }))
    }
}

#[cfg(not(target_os = "macos"))]
mod enabled {
    use std::fmt::Write;
    use std::io::{BufReader, Read};
    use std::{
        cell::RefCell,
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use askama::Template;
    use hyper::{header, Body, Method, Request, Response, StatusCode};
    use url::form_urlencoded;

    use prof::{
        collate_stacks, parse_jeheap, time::prof_time, JemallocProfCtl, JemallocProfMetadata,
        StackProfile, PROF_CTL,
    };

    use super::ProfTemplate;
    use crate::http::util;

    #[derive(Template)]
    #[template(path = "http/templates/flamegraph.html", escape = "none")]
    struct FlamegraphTemplate<'a> {
        version: &'a str,
        data_json: &'a str,
    }

    pub async fn handle(req: Request<Body>) -> anyhow::Result<Response<Body>> {
        match (req.method(), &*PROF_CTL) {
            (&Method::GET, Some(prof_ctl)) => {
                let prof_md = prof_ctl.lock().expect("Profiler lock poisoned").get_md();
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
                    let mut borrow = prof_ctl.lock().expect("Profiler lock poisoned");
                    borrow.activate()?;
                    borrow.get_md()
                };
                handle_get(md)
            }
            "deactivate" => {
                let md = {
                    let mut borrow = prof_ctl.lock().expect("Profiler lock poisoned");
                    borrow.deactivate()?;
                    borrow.get_md()
                };
                handle_get(md)
            }
            "dump_file" => {
                let mut borrow = prof_ctl.lock().expect("Profiler lock poisoned");
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
                let mut borrow = prof_ctl.lock().expect("Profiler lock poisoned");
                let f = borrow.dump()?;
                let r = BufReader::new(f);
                let stacks = parse_jeheap(r)?;
                flamegraph(stacks)
            }
            "time_fg" => {
                let merge_threads = params.get("threads").map(AsRef::as_ref) == Some("merge");
                let stacks = prof_time(Duration::from_secs(10), 99, merge_threads).await?;
                flamegraph(stacks)
            }
            x => Ok(util::error_response(
                StatusCode::BAD_REQUEST,
                format!("unrecognized `action` parameter: {}", x),
            )),
        }
    }

    fn flamegraph(stacks: StackProfile) -> anyhow::Result<Response<Body>> {
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
            data_json,
        }))
    }

    pub fn handle_get(prof_md: JemallocProfMetadata) -> anyhow::Result<Response<Body>> {
        Ok(util::template_response(ProfTemplate {
            version: crate::VERSION,
            mem_prof: Some(prof_md.start_time),
        }))
    }
}
