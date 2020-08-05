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

use cfg_if::cfg_if;
use hyper::{Body, Request, Response};

use crate::http::Server;

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

mod disabled {
    use askama::Template;
    use hyper::{Body, Request, Response};

    use crate::http::util;

    #[derive(Template)]
    #[template(path = "http/templates/prof-disabled.html")]
    struct ProfDisabledTemplate<'a> {
        version: &'a str,
    }

    pub async fn handle(_: Request<Body>) -> anyhow::Result<Response<Body>> {
        Ok(util::template_response(ProfDisabledTemplate {
            version: crate::VERSION,
        }))
    }
}

#[cfg(not(target_os = "macos"))]
mod enabled {
    use std::fmt::Write;
    use std::io::{BufReader, Read};
    use std::{
        cell::RefCell,
        sync::{Arc, Mutex},
    };

    use askama::Template;
    use hyper::{header, Body, Method, Request, Response, StatusCode};
    use url::form_urlencoded;

    use prof::{
        collate_stacks, parse_jeheap, JemallocProfCtl, JemallocProfMetadata, ProfStartTime,
        PROF_CTL,
    };

    use crate::http::util;

    #[derive(Template)]
    #[template(path = "http/templates/prof-enabled.html")]
    struct ProfEnabledTemplate<'a> {
        version: &'a str,
        start_time: Option<ProfStartTime>,
    }

    #[derive(Template)]
    #[template(path = "http/templates/flamegraph.html", escape = "none")]
    struct FlamegraphTemplate<'a> {
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
        let action = match form_urlencoded::parse(&body)
            .find(|(k, _v)| &**k == "action")
            .map(|(_k, v)| v)
        {
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
            "flamegraph" => {
                let mut borrow = prof_ctl.lock().expect("Profiler lock poisoned");
                let f = borrow.dump()?;
                let r = BufReader::new(f);
                let stacks = parse_jeheap(r)?;
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
                Ok(util::template_response(FlamegraphTemplate { data_json }))
            }
            x => Ok(util::error_response(
                StatusCode::BAD_REQUEST,
                format!("unrecognized `action` parameter: {}", x),
            )),
        }
    }

    pub fn handle_get(prof_md: JemallocProfMetadata) -> anyhow::Result<Response<Body>> {
        Ok(util::template_response(ProfEnabledTemplate {
            version: crate::VERSION,
            start_time: prof_md.start_time,
        }))
    }
}
