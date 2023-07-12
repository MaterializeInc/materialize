// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics tracked for `environmentd`s HTTP servers.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::response::IntoResponse;
use futures::Future;
use http::Request;
use http_body::Body;
use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::result::ResultExt;
use mz_ore::stats::histogram_seconds_buckets;
use pin_project::pin_project;
use prometheus::{HistogramTimer, HistogramVec, IntCounterVec, IntGaugeVec};
use tower::Layer;
use tower::Service;

#[derive(Debug, Clone)]
pub struct Metrics {
    /// Total number of requests since process start.
    pub requests: IntCounterVec,
    /// Number of currently active/open requests.
    pub requests_active: IntGaugeVec,
    /// How long it takes for a request to complete.
    pub request_duration: HistogramVec,
}

impl Metrics {
    pub(crate) fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            requests: registry.register(metric!(
                name: "mz_http_requests",
                help: "Total number of http requests received since process start.",
                var_labels: ["path", "status"],
            )),
            requests_active: registry.register(metric!(
                name: "mz_http_requests_active",
                help: "Number of currently active/open http requests.",
                var_labels: ["path"],
            )),
            request_duration: registry.register(metric!(
                name: "mz_http_request_duration",
                help: "How long it takes for a request to complete.",
                var_labels: ["path"],
                buckets: histogram_seconds_buckets(0.000_128, 8.0)
            )),
        }
    }
}

#[derive(Clone)]
pub struct PrometheusLayer {
    metrics: Metrics,
}

impl PrometheusLayer {
    pub fn new(metrics: Metrics) -> Self {
        PrometheusLayer { metrics }
    }
}

impl<S> Layer<S> for PrometheusLayer {
    type Service = PrometheusService<S>;

    fn layer(&self, service: S) -> Self::Service {
        PrometheusService {
            metrics: self.metrics.clone(),
            service,
        }
    }
}

#[derive(Clone)]
pub struct PrometheusService<S> {
    metrics: Metrics,
    service: S,
}

impl<S, B: Body> Service<Request<B>> for PrometheusService<S>
where
    S: Service<Request<B>>,
    S::Response: IntoResponse,
    S::Error: Into<Infallible>,
    S::Future: Send,
{
    type Error = S::Error;
    type Response = axum::response::Response;
    type Future = PrometheusFuture<S::Future>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let path = req.uri().path().to_string();
        let fut = self.service.call(req);
        PrometheusFuture::new(fut, path, self.metrics.clone())
    }
}

#[pin_project]
pub struct PrometheusFuture<F> {
    /// The URI portion of the path this request originated from.
    path: String,
    /// Instant at which we started the requst.
    timer: Option<HistogramTimer>,
    /// Metrics registry used to record events.
    metrics: Metrics,
    /// Inner request future.
    #[pin]
    fut: F,
}

impl<F> PrometheusFuture<F> {
    pub fn new(fut: F, path: String, metrics: Metrics) -> Self {
        PrometheusFuture {
            path,
            timer: None,
            metrics,
            fut,
        }
    }
}

impl<F, R, E> Future for PrometheusFuture<F>
where
    R: IntoResponse,
    E: Into<Infallible>,
    F: Future<Output = Result<R, E>>,
{
    type Output = Result<axum::response::Response, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if this.timer.is_none() {
            // Start timer so we can track duration of request.
            let duration_metric = this
                .metrics
                .request_duration
                .with_label_values(&[this.path]);
            *this.timer = Some(duration_metric.start_timer());

            // Increment our counter of currently active requests.
            this.metrics
                .requests_active
                .with_label_values(&[this.path])
                .inc();
        }

        // Poll the inner future to make progress.
        match this.fut.poll(cx) {
            Poll::Ready(resp) => {
                let ok = resp.infallible_unwrap();
                let resp = ok.into_response();
                let status = resp.status();

                // Record the completion of this request.
                this.metrics
                    .requests
                    .with_label_values(&[this.path, status.as_str()])
                    .inc();

                // Record the duration of this request.
                if let Some(timer) = this.timer.take() {
                    timer.observe_duration();
                }

                // We've completed this request, so decrement the count.
                this.metrics
                    .requests_active
                    .with_label_values(&[this.path])
                    .dec();

                Poll::Ready(Ok(resp))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
