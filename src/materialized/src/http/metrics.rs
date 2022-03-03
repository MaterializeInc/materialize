// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics HTTP endpoints.

use askama::Template;
use hyper::{Body, Request, Response};
use mz_ore::metrics::MetricsRegistry;
use prometheus::Encoder;

use crate::http::util;
use crate::{Metrics, BUILD_INFO};

#[derive(Template)]
#[template(path = "http/templates/status.html")]
struct StatusTemplate<'a> {
    version: &'a str,
    query_count: u64,
    uptime_seconds: f64,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum MetricsVariant {
    Regular,
    ThirdPartyVisible,
}

/// Serves metrics from the selected metrics registry variant.
pub fn handle_prometheus(
    _: Request<Body>,
    registry: &MetricsRegistry,
    variant: MetricsVariant,
) -> Result<Response<Body>, anyhow::Error> {
    let metric_families = match variant {
        MetricsVariant::Regular => registry.gather(),
        MetricsVariant::ThirdPartyVisible => registry.gather_third_party_visible(),
    };
    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(Response::new(Body::from(buffer)))
}

pub fn handle_status(
    _: Request<Body>,
    _: &mut mz_coord::SessionClient,
    global_metrics: &Metrics,
    pgwire_metrics: &mz_pgwire::Metrics,
) -> Result<Response<Body>, anyhow::Error> {
    Ok(util::template_response(StatusTemplate {
        version: BUILD_INFO.version,
        query_count: pgwire_metrics.query_count.get(),
        uptime_seconds: global_metrics.uptime.get(),
    }))
}
