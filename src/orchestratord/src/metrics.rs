// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use axum::{body::Body, routing::get, Extension, Router};
use http::{Method, Request, Response, StatusCode};
use prometheus::{Encoder, TextEncoder};
use tower_http::{classify::ServerErrorsFailureClass, trace::TraceLayer};
use tracing::{Level, Span};

use mz_ore::metric;
use mz_ore::metrics::{MetricsRegistry, UIntGauge};

#[derive(Debug)]
pub struct Metrics {
    pub needs_update: UIntGauge,
}

impl Metrics {
    pub fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            needs_update: registry.register(
                metric! {
                    name: "needs_update",
                    help: "Count of organizations in this cluster which are running outdated pod templates",
                }),
        }
    }
}

pub fn router(registry: MetricsRegistry) -> Router {
    add_tracing_layer(
        Router::new()
            .route("/metrics", get(metrics))
            .layer(Extension(registry)),
    )
}

#[allow(clippy::unused_async)]
async fn metrics(Extension(registry): Extension<MetricsRegistry>) -> (StatusCode, Vec<u8>) {
    let mut buf = vec![];
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    encoder.encode(&metric_families, &mut buf).unwrap();
    (StatusCode::OK, buf)
}

///   Adds a tracing layer that reports an `INFO` level span per
///   request and reports a `WARN` event when a handler returns a
///   server error to the given Axum Router
///
///   This accepts a router instead of returning a layer itself
///   to avoid dealing with defining generics over a bunch of closures
///   (see <https://users.rust-lang.org/t/how-to-encapsulate-a-builder-that-depends-on-a-closure/71139/6>)
///
///   And this also can't be returned as a Router::new()::layer(TraceLayer)...
///   because the TraceLayer needs to be added to a Router after
///   all routes are defined, as it won't trace any routes defined
///   on the router after it's attached.
fn add_tracing_layer<S>(router: Router<S>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.layer(TraceLayer::new_for_http()
                .make_span_with(|request: &Request<Body>| {
                    // This ugly macro is needed, unfortunately (and
                    // copied from tower-http), because
                    // `tracing::span!` required the level argument to
                    // be static. Meaning we can't just pass
                    // `self.level`.
                    // Don't log Authorization headers
                    let mut headers = request.headers().clone();
                    _ = headers.remove(http::header::AUTHORIZATION);
                    macro_rules! make_span {
                        ($level:expr) => {
                            tracing::span!(
                                $level,
                                "HTTP request",
                                "request.uri" = %request.uri(),
                                "request.version" = ?request.version(),
                                "request.method" = %request.method(),
                                "request.headers" = ?headers,
                                "response.status" = tracing::field::Empty,
                                "response.status_code" = tracing::field::Empty,
                                "response.headers" = tracing::field::Empty,
                            )
                        }
                    }
                    if request.uri().path() == "/api/health" || request.method() == Method::OPTIONS {
                        return make_span!(Level::DEBUG);
                    }
                    make_span!(Level::INFO)
                })
                .on_response(|response: &Response<Body>, _latency, span: &Span| {
                    span.record(
                        "response.status",
                        &tracing::field::display(response.status()),
                    );
                    span.record(
                        "response.status_code",
                        &tracing::field::display(response.status().as_u16()),
                    );
                    span.record(
                        "response.headers",
                        &tracing::field::debug(response.headers()),
                    );
                    // Emit an event at the same level as the span. For the same reason as noted in the comment
                    // above we can't use `tracing::event!(dynamic_level, ...)` since the level argument
                    // needs to be static
                    if span.metadata().and_then(|m| Some(m.level())).unwrap_or(&Level::DEBUG) == &Level::DEBUG {
                        tracing::debug!(msg = "HTTP response generated", response = ?response, status_code = response.status().as_u16());
                    } else {
                        tracing::info!(msg = "HTTP response generated", response = ?response, status_code = response.status().as_u16());
                    }
                })
                .on_failure(
                    |error: ServerErrorsFailureClass, _latency: Duration, _span: &Span| {
                        tracing::warn!(msg = "HTTP request handling error", error = ?error);
                    },
                ))
}
