// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use askama::Template;
use futures::future;
use hyper::{Body, Request, Response};

use crate::http::{util, Server};

#[derive(Template)]
#[template(path = "http/templates/memory.html")]
struct MemoryTemplate<'a> {
    version: &'a str,
}

impl Server {
    pub fn handle_memory(
        &self,
        _: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        future::ok(util::template_response(MemoryTemplate {
            version: crate::VERSION,
        }))
    }
}
