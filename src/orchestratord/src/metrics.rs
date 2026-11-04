// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{Extension, Router, body::Body, routing::get};
use http::{HeaderMap, Method, Request, Response, StatusCode};
use k8s_controller::PrometheusMetrics;
use prometheus::core::AtomicU64;
use prometheus::{Encoder, TextEncoder};
use tower_http::{classify::ServerErrorsFailureClass, trace::TraceLayer};
use tracing::{Level, Span};
use uuid::Uuid;

use mz_ore::metric;
use mz_ore::metrics::{
    DeleteOnDropGauge, MetricTag, MetricVisibility, MetricsRegistry, UIntGauge, UIntGaugeVec,
};

type EnvironmentGauge = DeleteOnDropGauge<AtomicU64, Vec<String>>;

#[derive(Debug)]
pub struct Metrics {
    pub is_leader: UIntGauge,
    pub environmentd_needs_update: UIntGauge,
    environment_info: UIntGaugeVec,
    environment_active_generation: UIntGaugeVec,
    /// The series published for each `Materialize` resource, keyed by its
    /// namespace and name. Dropping an entry removes its series.
    environments: Mutex<BTreeMap<(String, String), EnvironmentSeries>>,
    /// The reconciliation metrics of every controller, as defined by
    /// `k8s_controller`: counts and durations of reconciliation passes, and
    /// of the steps within them.
    pub reconcile: Arc<PrometheusMetrics>,
}

/// The series describing one `Materialize` resource.
#[derive(Debug)]
struct EnvironmentSeries {
    /// The label values of `info`, which `active_generation` extends with the
    /// generation.
    labels: Vec<String>,
    /// Held so that dropping it removes the series; its value is always 1.
    #[allow(dead_code)]
    info: EnvironmentGauge,
    active_generation: Option<(u64, EnvironmentGauge)>,
}

impl Metrics {
    pub fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            is_leader: registry.register(
                metric! {
                    name: "orchestratord_is_leader",
                    help: "Whether this operator replica holds the controller leadership lease, and is therefore the replica reconciling. Summed across the replicas this should be 1. A sustained 0 means no replica can take the lease, for instance because the service account lacks permission on leases, and the operator is reconciling nothing.",
                }),
            environmentd_needs_update: registry.register(
                metric! {
                    name: "environmentd_needs_update",
                    help: "Count of organizations in this cluster which are running outdated pod templates. Only the operator replica holding the leadership lease reconciles, so the others report zero.",
                }),
            environment_info: registry.register(metric!(
                name: "mz_environment_info",
                help: "Maps Materialize environment IDs to the environment's name and the namespace of its Materialize resource. Constant 1. Only the operator replica holding the leadership lease reports it.",
                var_labels: ["environment_id", "environment_name", "environment_namespace"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Environment],
            )),
            environment_active_generation: registry.register(metric!(
                name: "mz_environment_active_generation",
                help: "The deployment generation each Materialize environment serves: the one the operator last promoted, from the resource's status.activeGeneration. During a rollout it is the older generation until the new one is promoted. The generation label carries the same number, so it joins to the generation in a pod's name. Only the operator replica holding the leadership lease reports it.",
                var_labels: ["environment_id", "environment_name", "environment_namespace", "generation"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Environment],
            )),
            environments: Mutex::new(BTreeMap::new()),
            reconcile: {
                let reconcile = PrometheusMetrics::new("orchestratord")
                    .expect("valid reconciliation metric definitions");
                registry.register_collector(reconcile.clone());
                Arc::new(reconcile)
            },
        }
    }

    /// Records that this replica has taken the leadership lease.
    pub fn leadership_acquired(&self) {
        self.is_leader.set(1);
    }

    /// Records that this replica no longer holds the leadership lease, and
    /// resets the metrics that only mean anything while it is reconciling.
    ///
    /// Those are derived from what reconciliation observed, and the process
    /// outlives its own leadership: it keeps serving the conversion webhook
    /// after losing the lease. Without this, a former leader would go on
    /// publishing its last observation forever, so summing across the
    /// replicas would count the same organizations once per past leader, and
    /// each environment would have a series per past leader.
    pub fn leadership_lost(&self) {
        self.is_leader.set(0);
        self.environmentd_needs_update.set(0);
        self.environments.lock().expect("lock poisoned").clear();
    }

    /// Publishes the series describing the environment of the `Materialize`
    /// resource `name` in `namespace`, replacing those published for it
    /// before.
    ///
    /// `mz_environment_info` needs the environment's ID, which the operator
    /// assigns on its first reconciliation of a resource that has none, so
    /// nothing is published while `environment_id` is nil.
    /// `mz_environment_active_generation` is published only while the
    /// resource has a status, which `active_generation` comes from.
    pub fn observe_environment(
        &self,
        namespace: &str,
        name: &str,
        environment_id: Uuid,
        active_generation: Option<u64>,
    ) {
        let key = (namespace.to_owned(), name.to_owned());
        let mut environments = self.environments.lock().expect("lock poisoned");
        if environment_id.is_nil() {
            environments.remove(&key);
            return;
        }

        let labels = vec![
            environment_id.to_string(),
            name.to_owned(),
            namespace.to_owned(),
        ];
        let new_series = || {
            let info = self
                .environment_info
                .get_delete_on_drop_metric(labels.clone());
            info.set(1);
            EnvironmentSeries {
                labels: labels.clone(),
                info,
                active_generation: None,
            }
        };
        let series = environments.entry(key).or_insert_with(new_series);
        // A new handle on the labels a series already has would share it, and
        // dropping the old handle would then remove it, so a series is only
        // replaced when its labels change.
        if series.labels != labels {
            *series = new_series();
        }

        match active_generation {
            None => series.active_generation = None,
            Some(generation)
                if series
                    .active_generation
                    .as_ref()
                    .is_some_and(|(current, _)| *current == generation) => {}
            Some(generation) => {
                let mut labels = labels;
                labels.push(generation.to_string());
                let gauge = self
                    .environment_active_generation
                    .get_delete_on_drop_metric(labels);
                gauge.set(generation);
                series.active_generation = Some((generation, gauge));
            }
        }
    }

    /// Removes the series describing the environment of the `Materialize`
    /// resource `name` in `namespace`.
    pub fn forget_environment(&self, namespace: &str, name: &str) {
        self.environments
            .lock()
            .expect("lock poisoned")
            .remove(&(namespace.to_owned(), name.to_owned()));
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
    router.layer(
        TraceLayer::new_for_http()
            .make_span_with(|request: &Request<Body>| {
                // This ugly macro is needed, unfortunately (and
                // copied from tower-http), because
                // `tracing::span!` required the level argument to
                // be static. Meaning we can't just pass
                // `self.level`.
                macro_rules! make_span {
                        ($level:expr) => {
                            tracing::span!(
                                $level,
                                "HTTP request",
                                "request.uri" = %request.uri(),
                                "request.version" = ?request.version(),
                                "request.method" = %request.method(),
                                "request.headers" = tracing::field::Empty,
                                "response.status" = tracing::field::Empty,
                                "response.status_code" = tracing::field::Empty,
                                "response.headers" = tracing::field::Empty,
                            )
                        }
                    }
                let span = if ["/api/health", "/metrics"].contains(&request.uri().path())
                    || request.method() == Method::OPTIONS
                {
                    make_span!(Level::DEBUG)
                } else {
                    make_span!(Level::INFO)
                };

                if let Ok(s) = serde_json::to_string(&display_headers(request.headers().clone())) {
                    span.record("request.headers", s);
                }

                span
            })
            .on_response(|response: &Response<Body>, _latency, span: &Span| {
                span.record(
                    "response.status",
                    &tracing::field::display(response.status()),
                );
                span.record("response.status_code", response.status().as_u16());
                if let Ok(s) = serde_json::to_string(&display_headers(response.headers().clone())) {
                    span.record("response.headers", s);
                }

                // Emit an event at the same level as the span. For the same reason as noted in the comment
                // above we can't use `tracing::event!(dynamic_level, ...)` since the level argument
                // needs to be static
                if span
                    .metadata()
                    .and_then(|m| Some(m.level()))
                    .unwrap_or(&Level::DEBUG)
                    == &Level::DEBUG
                {
                    tracing::debug!("HTTP response generated");
                } else {
                    tracing::info!("HTTP response generated");
                }
            })
            .on_failure(
                |error: ServerErrorsFailureClass, _latency: Duration, _span: &Span| {
                    tracing::warn!(error = ?error, "HTTP request handling error");
                },
            ),
    )
}

fn display_headers(mut headers: HeaderMap) -> BTreeMap<String, String> {
    // Don't log Authorization headers
    _ = headers.remove(http::header::AUTHORIZATION);

    headers
        .into_iter()
        .filter_map(|(k, v)| {
            k.map(|k| {
                (
                    k.to_string(),
                    String::from_utf8_lossy(v.as_bytes()).to_string(),
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const NAMESPACE: &str = "materialize-environment";
    const NAME: &str = "production";
    const ENVIRONMENT_ID: &str = "4b1a2f6e-0c3d-4e5f-8a9b-0c1d2e3f4a5b";
    const OTHER_ENVIRONMENT_ID: &str = "9f8e7d6c-5b4a-4321-9876-543210fedcba";

    /// The environment series in `registry`, one line each, in the text
    /// exposition format.
    fn environment_series(registry: &MetricsRegistry) -> Vec<String> {
        let mut series: Vec<String> = registry
            .gather()
            .iter()
            .filter(|family| family.name().starts_with("mz_environment_"))
            .flat_map(|family| {
                family.get_metric().iter().map(|metric| {
                    let labels: Vec<String> = metric
                        .get_label()
                        .iter()
                        .map(|label| format!("{}=\"{}\"", label.name(), label.value()))
                        .collect();
                    format!(
                        "{}{{{}}} {}",
                        family.name(),
                        labels.join(","),
                        metric.get_gauge().value()
                    )
                })
            })
            .collect();
        series.sort();
        series
    }

    fn info(environment_id: &str, namespace: &str) -> String {
        format!(
            "mz_environment_info{{environment_id=\"{environment_id}\",\
             environment_name=\"{NAME}\",environment_namespace=\"{namespace}\"}} 1"
        )
    }

    fn active_generation(environment_id: &str, namespace: &str, generation: u64) -> String {
        format!(
            "mz_environment_active_generation{{environment_id=\"{environment_id}\",\
             environment_name=\"{NAME}\",environment_namespace=\"{namespace}\",\
             generation=\"{generation}\"}} {generation}"
        )
    }

    #[mz_ore::test]
    fn environment_series_follow_the_resource() {
        let registry = MetricsRegistry::new();
        let metrics = Metrics::register_into(&registry);
        let id = Uuid::parse_str(ENVIRONMENT_ID).unwrap();

        metrics.observe_environment(NAMESPACE, NAME, Uuid::nil(), None);
        assert_eq!(environment_series(&registry), Vec::<String>::new());

        metrics.observe_environment(NAMESPACE, NAME, id, None);
        assert_eq!(
            environment_series(&registry),
            vec![info(ENVIRONMENT_ID, NAMESPACE)]
        );

        metrics.observe_environment(NAMESPACE, NAME, id, Some(3));
        let at_generation_3 = vec![
            active_generation(ENVIRONMENT_ID, NAMESPACE, 3),
            info(ENVIRONMENT_ID, NAMESPACE),
        ];
        assert_eq!(environment_series(&registry), at_generation_3);

        // Observing the same resource again keeps its series.
        metrics.observe_environment(NAMESPACE, NAME, id, Some(3));
        assert_eq!(environment_series(&registry), at_generation_3);

        metrics.observe_environment(NAMESPACE, NAME, id, Some(4));
        assert_eq!(
            environment_series(&registry),
            vec![
                active_generation(ENVIRONMENT_ID, NAMESPACE, 4),
                info(ENVIRONMENT_ID, NAMESPACE),
            ]
        );

        // A resource of the same name in another namespace is another
        // environment.
        let other = Uuid::parse_str(OTHER_ENVIRONMENT_ID).unwrap();
        metrics.observe_environment("other", NAME, other, Some(1));
        metrics.forget_environment(NAMESPACE, NAME);
        assert_eq!(
            environment_series(&registry),
            vec![
                active_generation(OTHER_ENVIRONMENT_ID, "other", 1),
                info(OTHER_ENVIRONMENT_ID, "other"),
            ]
        );
    }

    #[mz_ore::test]
    fn environment_id_change_replaces_series() {
        let registry = MetricsRegistry::new();
        let metrics = Metrics::register_into(&registry);

        let id = Uuid::parse_str(ENVIRONMENT_ID).unwrap();
        metrics.observe_environment(NAMESPACE, NAME, id, Some(2));
        let other = Uuid::parse_str(OTHER_ENVIRONMENT_ID).unwrap();
        metrics.observe_environment(NAMESPACE, NAME, other, Some(2));
        assert_eq!(
            environment_series(&registry),
            vec![
                active_generation(OTHER_ENVIRONMENT_ID, NAMESPACE, 2),
                info(OTHER_ENVIRONMENT_ID, NAMESPACE),
            ]
        );
    }

    #[mz_ore::test]
    fn leadership_lost_clears_environment_series() {
        let registry = MetricsRegistry::new();
        let metrics = Metrics::register_into(&registry);

        let id = Uuid::parse_str(ENVIRONMENT_ID).unwrap();
        metrics.leadership_acquired();
        metrics.observe_environment(NAMESPACE, NAME, id, Some(1));
        metrics.leadership_lost();
        assert_eq!(environment_series(&registry), Vec::<String>::new());
    }
}
