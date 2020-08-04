// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Profiling endpoints.

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use futures::stream::TryStreamExt;
use hyper::header::HeaderValue;
use hyper::{Body, Method, Request, Response};
use url::form_urlencoded;

use prof::{JemallocProfCtl, JemallocProfMetadata, ProfStartTime, PROF_CTL};

pub async fn handle(req: Request<Body>) -> Result<Response<Body>, anyhow::Error> {
    match (req.method(), &*PROF_CTL) {
        (&Method::GET, Some(prof_ctl)) => {
            let prof_md = prof_ctl.lock().expect("Profiler lock poisoned").get_md();
            handle_get(prof_md).await
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
            handle_get(md).await
        }
        "deactivate" => {
            let md = {
                let mut borrow = prof_ctl.lock().expect("Profiler lock poisoned");
                borrow.deactivate()?;
                borrow.get_md()
            };
            handle_get(md).await
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

pub async fn handle_get(prof_md: JemallocProfMetadata) -> Result<Response<Body>, anyhow::Error> {
    let (prof_status, can_activate, is_active) = match prof_md.start_time {
        Some(ProfStartTime::TimeImmemorial) => (
            "Jemalloc profiling active since server start".to_string(),
            false,
            true,
        ),
        Some(ProfStartTime::Instant(when)) => (
            format!(
                "Jemalloc profiling active since {:?}",
                Instant::now() - when,
            ),
            false,
            true,
        ),
        None => (
            "Jemalloc profiling enabled but inactive".to_string(),
            true,
            false,
        ),
    };
    let activate_link = if can_activate {
        r#"<form action="/prof" method="POST"><button type="submit" name="action" value="activate">Activate</button></form>"#
    } else {
        ""
    };
    let deactivate_link = if is_active {
        r#"<form action="/prof" method="POST"><button type="submit" name="action" value="deactivate">Deactivate</button></form>"#
    } else {
        ""
    };
    let dump_file_link = if is_active {
        r#"<form action="/prof" method="POST"><button type="submit" name="action" value="dump_file">Download heap profile</button></form>"#
    } else {
        ""
    };
    Ok(Response::new(Body::from(format!(
        r#"<!DOCTYPE html>
<html lang="en">
    <head>
    <meta charset="utf-8">
    <title>materialized profiling functions</title>
    </head>
    <body>
    <p>{prof_status}</p>
    {activate_link}
    {deactivate_link}
    {dump_file_link}
    </body>
</html>
"#,
        prof_status = prof_status,
        activate_link = activate_link,
        deactivate_link = deactivate_link,
        dump_file_link = dump_file_link,
    ))))
}
