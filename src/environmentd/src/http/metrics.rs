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

use axum::extract::MatchedPath;
use axum::response::IntoResponse;
use futures::Future;
use http::Request;
use http_body::Body;
use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::result::ResultExt;
use mz_ore::stats::histogram_seconds_buckets;
use pin_project::{pin_project, pinned_drop};
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
    pub(crate) fn register_into(registry: &MetricsRegistry, component: &'static str) -> Self {
        Self {
            requests: registry.register(metric!(
                name: "requests_total",
                help: "Total number of http requests received since process start.",
                subsystem: component,
                var_labels: ["path", "status"],
            )),
            requests_active: registry.register(metric!(
                name: "requests_active",
                help: "Number of currently active/open http requests.",
                subsystem: component,
                var_labels: ["path"],
            )),
            request_duration: registry.register(metric!(
                name: "request_duration_seconds",
                help: "How long it takes for a request to complete in seconds.",
                subsystem: component,
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

impl<S, B> Service<Request<B>> for PrometheusService<S>
where
    B: Body,
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
        let path = req
            .extensions()
            .get::<MatchedPath>()
            .map(|path| path.as_str().to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let fut = self.service.call(req);
        PrometheusFuture::new(fut, path, self.metrics.clone())
    }
}

#[pin_project(PinnedDrop)]
pub struct PrometheusFuture<F> {
    /// The axum router path this request matched.
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

#[pinned_drop]
impl<F> PinnedDrop for PrometheusFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();

        if let Some(timer) = this.timer.take() {
            // Make sure to decrement the in-progress count if we weren't polled to completion.
            this.metrics
                .requests_active
                .with_label_values(&[this.path])
                .dec();

            // Our request didn't complete, so don't record the timing.
            timer.stop_and_discard();
        }
    }
}

#[cfg(test)]
mod test {
    use futures::Future;
    use http::StatusCode;
    use mz_ore::metrics::MetricsRegistry;
    use std::convert::Infallible;
    use std::pin::Pin;

    use super::{Metrics, PrometheusFuture};

    #[mz_ore::test]
    fn test_metrics_future_on_drop() {
        let registry = MetricsRegistry::new();
        let metrics = Metrics::register_into(&registry, "test");
        let waker = futures::task::noop_waker_ref();
        let mut cx = std::task::Context::from_waker(waker);

        let request_future = futures::future::pending::<Result<(StatusCode, String), Infallible>>();
        let mut future = PrometheusFuture::new(request_future, "/future/test".to_string(), metrics);

        // Poll the Future once to get metrics registered.
        assert!(Pin::new(&mut future).poll(&mut cx).is_pending());

        let metrics = registry.gather();

        // We don't log total requests until the request completes.
        let total_requests_exists = metrics
            .iter()
            .find(|metric| metric.get_name().contains("requests_total"))
            .is_some();
        assert!(!total_requests_exists);

        // We should have one request in-flight.
        let active_requests = metrics
            .iter()
            .find(|metric| metric.get_name().contains("requests_active"))
            .unwrap();
        assert_eq!(active_requests.get_metric()[0].get_gauge().get_value(), 1.0);

        // Drop the request before we finish polling it to completion.
        drop(future);

        let metrics = registry.gather();

        // Our in-flight request count should have been decremented.
        let active_requests = metrics
            .iter()
            .find(|metric| metric.get_name().contains("requests_active"))
            .unwrap();
        assert_eq!(active_requests.get_metric()[0].get_gauge().get_value(), 0.0);

        // We should have discarded the in-flight timer.
        let active_requests = metrics
            .iter()
            .find(|metric| metric.get_name().contains("request_duration_seconds"))
            .unwrap();
        assert_eq!(
            active_requests.get_metric()[0]
                .get_histogram()
                .get_sample_count(),
            0
        );
    }
}
