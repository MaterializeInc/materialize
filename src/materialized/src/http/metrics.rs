// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics HTTP endpoints.

use std::collections::{BTreeMap, BTreeSet};

use askama::Template;
use hyper::{Body, Request, Response};
use ore::metrics::MetricsRegistry;
use prometheus::Encoder;

use crate::http::util;
use crate::server_metrics::PromMetric;
use crate::{Metrics, BUILD_INFO};

#[derive(Template)]
#[template(path = "http/templates/status.html")]
struct StatusTemplate<'a> {
    version: &'a str,
    query_count: u64,
    uptime_seconds: f64,
    metrics: Vec<&'a PromMetric<'a>>,
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
    _: &mut coord::SessionClient,
    registry: &MetricsRegistry,
    _: &Metrics,
) -> Result<Response<Body>, anyhow::Error> {
    let metric_families = registry.gather();

    let desired_metrics = {
        let mut s = BTreeSet::new();
        s.insert("mz_dataflow_events_read_total");
        s.insert("mz_bytes_read_total");
        s.insert("mz_worker_command_queue_size");
        s.insert("mz_command_durations");
        s
    };

    let mut uptime_seconds = 0.0;
    let mut metrics = BTreeMap::new();
    for metric in &metric_families {
        let converted = PromMetric::from_metric_family(metric);
        match converted {
            Ok(m) => {
                for m in m {
                    match m {
                        PromMetric::Counter { name, .. } => {
                            if desired_metrics.contains(name) {
                                metrics.insert(name.to_string(), m);
                            }
                        }
                        PromMetric::Gauge { name, value, .. } => {
                            if desired_metrics.contains(name) {
                                metrics.insert(name.to_string(), m);
                            }
                            if name == "mz_server_metadata_seconds" {
                                uptime_seconds = value;
                            }
                        }
                        PromMetric::Histogram {
                            name, ref labels, ..
                        } => {
                            if desired_metrics.contains(name) {
                                metrics.insert(
                                    format!("{}:{}", name, labels.get("command").unwrap_or(&"")),
                                    m,
                                );
                            }
                        }
                    }
                }
            }
            Err(_) => continue,
        };
    }
    let mut query_count = metrics
        .get("mz_command_durations:query")
        .map(|m| {
            if let PromMetric::Histogram { count, .. } = m {
                *count
            } else {
                0
            }
        })
        .unwrap_or(0);
    query_count += metrics
        .get("mz_command_durations:execute")
        .map(|m| {
            if let PromMetric::Histogram { count, .. } = m {
                *count
            } else {
                0
            }
        })
        .unwrap_or(0);

    Ok(util::template_response(StatusTemplate {
        version: BUILD_INFO.version,
        query_count,
        uptime_seconds,
        metrics: metrics.values().collect(),
    }))
}
