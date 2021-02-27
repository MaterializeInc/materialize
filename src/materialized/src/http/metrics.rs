// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics HTTP endpoints.

use crate::BUILD_INFO;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use askama::Template;
use hyper::{Body, Request, Response};
use prometheus::Encoder;

use crate::http::util;
use crate::server_metrics::{load_prom_metrics, PromMetric, REQUEST_METRICS_ENCODE};

#[derive(Template)]
#[template(path = "http/templates/status.html")]
struct StatusTemplate<'a> {
    version: &'a str,
    query_count: u64,
    start_time: Instant,
    metrics: Vec<&'a PromMetric<'a>>,
}

pub async fn handle_prometheus(
    _: Request<Body>,
    _: &mut coord::SessionClient,
    start_time: Instant,
) -> Result<Response<Body>, anyhow::Error> {
    let metric_families = load_prom_metrics(start_time);
    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    let start = Instant::now();
    encoder.encode(&metric_families, &mut buffer)?;
    REQUEST_METRICS_ENCODE.set(Instant::now().duration_since(start).as_micros() as i64);

    Ok(Response::new(Body::from(buffer)))
}

pub async fn handle_status(
    _: Request<Body>,
    _: &mut coord::SessionClient,
    start_time: Instant,
) -> Result<Response<Body>, anyhow::Error> {
    let metric_families = load_prom_metrics(start_time);

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
