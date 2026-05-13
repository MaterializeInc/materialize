// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Federated `/metrics` endpoint.
//!
//! Aggregates environmentd's local metrics with every clusterd replica's
//! `/metrics` output, adding `cluster_id`, `replica_id`, `process`,
//! `cluster_name`, and `replica_name` labels onto the clusterd-sourced
//! metrics.

use std::sync::Arc;

use axum::Extension;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use futures::future::join_all;
use headers::ContentType;
use http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use mz_adapter_types::dyncfgs::ENABLE_EXTERNAL_METRICS_ENDPOINT;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::metrics::MetricsRegistry;
use prometheus::Encoder;

use crate::http::AuthedClient;
use crate::http::cluster::{
    ClusterProxyConfig, proxy_replica_request, rewrite_request_for_replica,
};

/// The customer-facing `/metrics` endpoint.
///
/// Aggregates environmentd's local metrics with every clusterd replica's
/// `/metrics` output.
pub(crate) async fn handle_external_metrics(
    client: AuthedClient,
    Extension(config): Extension<Arc<ClusterProxyConfig>>,
    Extension(metrics_registry): Extension<MetricsRegistry>,
) -> Response {
    let catalog = client.client.catalog_snapshot("metrics_external").await;
    if !ENABLE_EXTERNAL_METRICS_ENDPOINT.get(catalog.system_config().dyncfgs()) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    // Start with env's local gather() output.
    let mut all_families: Vec<prometheus::proto::MetricFamily> = metrics_registry.gather();

    // Fan out to every (cluster, replica, process).
    let scrapes: Vec<(ClusterId, ReplicaId, usize, String)> = config
        .locator
        .list_replicas()
        .into_iter()
        .flat_map(|(cluster_id, replica_id, addrs)| {
            addrs
                .into_iter()
                .enumerate()
                .map(move |(process_id, addr)| (cluster_id, replica_id, process_id, addr))
        })
        .collect();

    let results = join_all(scrapes.into_iter().map(scrape_replica_metrics_endpoint)).await;

    for (cluster_id, replica_id, process, result) in results {
        let bytes = match result {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    %cluster_id,
                    %replica_id,
                    process,
                    "federated metrics scrape failed: {e}"
                );
                continue;
            }
        };
        let cluster_name = catalog
            .try_get_cluster(cluster_id)
            .map(|cluster| cluster.name.clone());
        let replica_name = catalog
            .try_get_cluster_replica(cluster_id, replica_id)
            .map(|replica| replica.name.clone());
        let families = match mz_prometheus_protobuf::decode_length_delimited(&bytes) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(
                    %cluster_id,
                    %replica_id,
                    process,
                    "federated metrics decode failed: {e}"
                );
                continue;
            }
        };
        for mut family in families {
            add_replica_labels(
                &mut family,
                cluster_id,
                replica_id,
                process,
                cluster_name.as_deref(),
                replica_name.as_deref(),
            );
            all_families.push(family);
        }
    }

    // Re-emit as Prometheus text.
    let mut body = Vec::new();
    if let Err(e) = prometheus::TextEncoder::new().encode(&all_families, &mut body) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {e}"),
        )
            .into_response();
    }
    (TypedHeader(ContentType::text()), body).into_response()
}

async fn scrape_replica_metrics_endpoint(
    (cluster_id, replica_id, process, addr): (ClusterId, ReplicaId, usize, String),
) -> (ClusterId, ReplicaId, usize, Result<Vec<u8>, String>) {
    let result =
        scrape_replica_metrics_endpoint_inner(cluster_id, replica_id, process, &addr).await;
    (cluster_id, replica_id, process, result)
}

async fn scrape_replica_metrics_endpoint_inner(
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    process: usize,
    addr: &str,
) -> Result<Vec<u8>, String> {
    let mut req = Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header(
            http::header::ACCEPT,
            mz_http_util::PROMETHEUS_PROTOBUF_CONTENT_TYPE,
        )
        .body(Body::empty())
        .map_err(|e| format!("building request: {e}"))?;
    rewrite_request_for_replica(&mut req, addr, cluster_id, replica_id, process, "/metrics")
        .map_err(|(status, msg)| format!("{status}: {msg}"))?;
    let resp = proxy_replica_request(addr, req)
        .await
        .map_err(|(status, msg)| format!("{status}: {msg}"))?;
    if !resp.status().is_success() {
        return Err(format!("upstream status {}", resp.status()));
    }
    let body = resp.into_body();
    let collected = body
        .collect()
        .await
        .map_err(|e| format!("collecting body: {e}"))?
        .to_bytes();

    Ok(collected.to_vec())
}

fn add_replica_labels(
    family: &mut prometheus::proto::MetricFamily,
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    process: usize,
    cluster_name: Option<&str>,
    replica_name: Option<&str>,
) {
    for metric in family.mut_metric() {
        let mut labels = metric.take_label();
        labels.push(label_pair("cluster_id", cluster_id.to_string()));
        labels.push(label_pair("replica_id", replica_id.to_string()));
        labels.push(label_pair("process", process.to_string()));
        if let Some(n) = cluster_name {
            labels.push(label_pair("cluster_name", n.to_owned()));
        }
        if let Some(n) = replica_name {
            labels.push(label_pair("replica_name", n.to_owned()));
        }
        metric.set_label(labels);
    }
}

fn label_pair(name: &str, value: String) -> prometheus::proto::LabelPair {
    let mut pair = prometheus::proto::LabelPair::default();
    pair.set_name(name.to_owned());
    pair.set_value(value);
    pair
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_family(name: &str) -> prometheus::proto::MetricFamily {
        let mut family = prometheus::proto::MetricFamily::default();
        family.set_name(name.to_owned());
        family.set_help(format!("help for {name}"));
        family.set_field_type(prometheus::proto::MetricType::COUNTER);
        let mut metric = prometheus::proto::Metric::default();
        let mut counter = prometheus::proto::Counter::default();
        counter.set_value(1.0);
        metric.set_counter(counter);
        family.set_metric(vec![metric]);
        family
    }

    #[mz_ore::test]
    fn adds_three_labels_when_names_unknown() {
        let mut family = make_family("test");
        let cluster_id: ClusterId = "u1".parse().expect("cluster id");
        let replica_id: ReplicaId = "u2".parse().expect("replica id");
        add_replica_labels(&mut family, cluster_id, replica_id, 7, None, None);
        let labels = family.get_metric()[0].get_label();
        let names: Vec<_> = labels.iter().map(|l| l.name()).collect();
        assert_eq!(names, vec!["cluster_id", "replica_id", "process"]);
        let values: Vec<_> = labels.iter().map(|l| l.value()).collect();
        assert_eq!(values, vec!["u1", "u2", "7"]);
    }

    #[mz_ore::test]
    fn shared_metric_name_emits_single_help_and_type() {
        let env_family = make_family("mz_persist_test");
        let mut clusterd_family = make_family("mz_persist_test");
        add_replica_labels(
            &mut clusterd_family,
            "u1".parse().unwrap(),
            "u2".parse().unwrap(),
            0,
            Some("quickstart"),
            Some("r1"),
        );

        let mut body = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&[env_family, clusterd_family], &mut body)
            .unwrap();
        let text = String::from_utf8(body).unwrap();

        let help = text
            .lines()
            .filter(|l| l.starts_with("# HELP mz_persist_test "))
            .count();
        let type_ = text
            .lines()
            .filter(|l| l.starts_with("# TYPE mz_persist_test "))
            .count();
        assert_eq!(help, 1, "{text}");
        assert_eq!(type_, 1, "{text}");
    }

    #[mz_ore::test]
    fn adds_five_labels_when_names_known() {
        let mut family = make_family("test");
        let cluster_id: ClusterId = "u1".parse().expect("cluster id");
        let replica_id: ReplicaId = "u2".parse().expect("replica id");
        add_replica_labels(
            &mut family,
            cluster_id,
            replica_id,
            0,
            Some("quickstart"),
            Some("r1"),
        );
        let labels = family.get_metric()[0].get_label();
        let names: Vec<_> = labels.iter().map(|l| l.name()).collect();
        assert_eq!(
            names,
            vec![
                "cluster_id",
                "replica_id",
                "process",
                "cluster_name",
                "replica_name",
            ]
        );
        let values: Vec<_> = labels.iter().map(|l| l.value()).collect();
        assert_eq!(values, vec!["u1", "u2", "0", "quickstart", "r1"]);
    }
}
