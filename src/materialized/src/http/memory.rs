// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use askama::Template;
use axum::response::IntoResponse;

use crate::BUILD_INFO;

#[derive(Template)]
#[template(path = "http/templates/memory.html")]
struct MemoryTemplate<'a> {
    version: &'a str,
}

pub async fn handle_memory() -> impl IntoResponse {
    mz_http_util::template_response(MemoryTemplate {
        version: BUILD_INFO.version,
    })
}

#[derive(Template)]
#[template(path = "http/templates/hierarchical-memory.html")]
struct HierarchicalMemoryTemplate<'a> {
    version: &'a str,
}

pub async fn handle_hierarchical_memory() -> impl IntoResponse {
    mz_http_util::template_response(HierarchicalMemoryTemplate {
        version: BUILD_INFO.version,
    })
}
