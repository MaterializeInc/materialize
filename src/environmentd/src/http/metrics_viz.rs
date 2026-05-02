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
#[template(path = "metrics-viz.html")]
struct MetricsVizTemplate<'a> {
    version: &'a str,
}

pub async fn handle_metrics_viz() -> impl IntoResponse {
    mz_http_util::template_response(MetricsVizTemplate {
        version: BUILD_INFO.version,
    })
}
