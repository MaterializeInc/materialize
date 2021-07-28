// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics HTTP endpoints.

use crate::{Metrics, BUILD_INFO};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use askama::Template;
use hyper::{Body, Request, Response};
use ore::metrics::MetricsRegistry;
use prometheus::Encoder;

use crate::http::util;
use crate::server_metrics::PromMetric;

#[derive(Template)]
#[template(path = "http/templates/status.html")]
struct StatusTemplate<'a> {
    version: &'a str,
    query_count: u64,
    start_time: Instant,
    metrics: Vec<&'a PromMetric<'a>>,
}

/// Call [`prometheus::gather`], ensuring that all our metrics are up to date
fn load_prom_metrics(
    start_time: Instant,
    registry: &MetricsRegistry,
    global_metrics: &Metrics,
) -> Vec<prometheus::proto::MetricFamily> {
    let before_gather = Instant::now();
    global_metrics.update_uptime(start_time);
    let result = registry.gather();

    global_metrics
        .request_metrics_gather
        .set(Instant::elapsed(&before_gather).as_micros() as u64);
    result
}

pub fn handle_prometheus(
    _: Request<Body>,
    _: &mut coord::SessionClient,
    start_time: Instant,
    registry: &MetricsRegistry,
    global_metrics: &Metrics,
) -> Result<Response<Body>, anyhow::Error> {
    let metric_families = load_prom_metrics(start_time, registry, global_metrics);
    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    let start = Instant::now();
    encoder.encode(&metric_families, &mut buffer)?;
    global_metrics
        .request_metrics_encode
        .set(Instant::elapsed(&start).as_micros() as u64);

    Ok(Response::new(Body::from(buffer)))
}

pub fn handle_status(
    _: Request<Body>,
    _: &mut coord::SessionClient,
    start_time: Instant,
    registry: &MetricsRegistry,
    global_metrics: &Metrics,
) -> Result<Response<Body>, anyhow::Error> {
    let metric_families = load_prom_metrics(start_time, registry, global_metrics);

    let desired_metrics = {
        let mut s = BTreeSet::new();
        s.insert("mz_dataflow_events_read_total");
        s.insert("mz_bytes_read_total");
        s.insert("mz_worker_command_queue_size");
        s.insert("mz_command_durations");
        s
    };

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
                        PromMetric::Gauge { name, .. } => {
                            if desired_metrics.contains(name) {
                                metrics.insert(name.to_string(), m);
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
        start_time,
        metrics: metrics.values().collect(),
    }))
}
