// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use askama::Template;
use hyper::{Body, Request, Response};

use crate::http::util;
use crate::BUILD_INFO;

#[derive(Template)]
#[template(path = "http/templates/memory.html")]
struct MemoryTemplate<'a> {
    version: &'a str,
}

pub async fn handle_memory(
    _: Request<Body>,
    _: &mut coord::SessionClient,
) -> Result<Response<Body>, anyhow::Error> {
    Ok(util::template_response(MemoryTemplate {
        version: BUILD_INFO.version,
    }))
}
