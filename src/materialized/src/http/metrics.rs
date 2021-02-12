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
use std::future::Future;
use std::time::Instant;

use askama::Template;
use futures::future;
use hyper::{Body, Request, Response};
use prometheus::Encoder;

use crate::http::{util, Server};
use crate::server_metrics::{load_prom_metrics, PromMetric, REQUEST_METRICS_ENCODE};

#[derive(Template)]
#[template(path = "http/templates/status.html")]
struct StatusTemplate<'a> {
    version: &'a str,
    query_count: u64,
    start_time: Instant,
    metrics: Vec<&'a PromMetric<'a>>,
}

impl Server {
    pub fn handle_prometheus(
        &self,
        _: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        let metric_families = load_prom_metrics(self.start_time);
        async move {
            let encoder = prometheus::TextEncoder::new();
            let mut buffer = Vec::new();

            let start = Instant::now();
            encoder.encode(&metric_families, &mut buffer)?;
            REQUEST_METRICS_ENCODE.set(Instant::now().duration_since(start).as_micros() as i64);

            Ok(Response::new(Body::from(buffer)))
        }
    }

    pub fn handle_status(
        &self,
        _: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        let metric_families = load_prom_metrics(self.start_time);

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
                                        format!(
                                            "{}:{}",
                                            name,
                                            labels.get("command").unwrap_or(&"")
                                        ),
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

        future::ok(util::template_response(StatusTemplate {
            version: BUILD_INFO.version,
            query_count,
            start_time: self.start_time,
            metrics: metrics.values().collect(),
        }))
    }
}
