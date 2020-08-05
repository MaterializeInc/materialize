// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Profiling HTTP endpoints.

use std::io::Read;
use std::sync::{Arc, Mutex};

use askama::Template;
use futures::stream::TryStreamExt;
use hyper::header::HeaderValue;
use hyper::{Body, Method, Request, Response};
use url::form_urlencoded;

use prof::{JemallocProfCtl, JemallocProfMetadata, ProfStartTime, PROF_CTL};

use crate::http::util;

#[derive(Template)]
#[template(path = "http/templates/prof.html")]
struct ProfTemplate<'a> {
    version: &'a str,
    start_time: Option<ProfStartTime>,
}

pub async fn handle(req: Request<Body>) -> Result<Response<Body>, anyhow::Error> {
    match (req.method(), &*PROF_CTL) {
        (&Method::GET, Some(prof_ctl)) => {
            let prof_md = prof_ctl.lock().expect("Profiler lock poisoned").get_md();
            handle_get(prof_md)
        }
        (&Method::POST, Some(prof_ctl)) => handle_post(req, prof_ctl).await,
        _ => Ok(Response::builder().status(501).body(Body::from(
            "Jemalloc profiling disabled (HINT: run with _RJEM_MALLOC_CONF=prof:true)",
        ))?),
    }
}

pub async fn handle_post(
    body: Request<Body>,
    prof_ctl: &Arc<Mutex<JemallocProfCtl>>,
) -> Result<Response<Body>, anyhow::Error> {
    let body = body
        .into_body()
        .try_fold(vec![], |mut v, b| async move {
            v.extend(b);
            Ok(v)
        })
        .await?;
    let action = match form_urlencoded::parse(&body)
        .find(|(k, _v)| &**k == "action")
        .map(|(_k, v)| v)
    {
        Some(action) => action,
        None => {
            return Ok(Response::builder()
                .status(400)
                .body(Body::from("Expected `action` parameter"))?)
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
            let body = Body::from(s);
            let mut response = Response::new(body);
            response.headers_mut().append(
                "Content-Disposition",
                HeaderValue::from_static("attachment; filename=\"jeprof.heap\""),
            );
            Ok(response)
        }
        x => Ok(Response::builder().status(400).body(Body::from(format!(
            "Unrecognized `action` parameter: {}",
            x
        )))?),
    }
}

pub fn handle_get(prof_md: JemallocProfMetadata) -> anyhow::Result<Response<Body>> {
    Ok(util::template_response(ProfTemplate {
        version: crate::VERSION,
        start_time: prof_md.start_time,
    }))
}
