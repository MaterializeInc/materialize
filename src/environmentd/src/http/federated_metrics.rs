// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Federated `/metrics/federated` endpoint.
//!
//! Scrapes environmentd's local `MetricsRegistry` plus every clusterd
//! replica's `/metrics` endpoint in parallel, decodes them, attaches
//! human-readable name labels resolved from the catalog, and re-emits
//! standard Prometheus text. See
//! `doc/developer/design/20260427_federated_metrics_endpoint.md` for the
//! full design.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum::Extension;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use bytes::Buf;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use mz_adapter::catalog::Catalog;
use mz_catalog::memory::objects::CatalogEntry;
use mz_controller::ReplicaHttpLocator;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::metrics::{MetricsRegistry, Rule};
use mz_repr::{CatalogItemId, GlobalId};
use prometheus::Encoder;
use prometheus::proto::{LabelPair, Metric, MetricFamily};
use prost::Message as _;
use tracing::{debug, warn};

use crate::http::AuthedClient;

/// Prost-generated types for `io.prometheus.client.proto` (the same schema
/// used by the upstream `prometheus` crate, but compiled against prost
/// rather than rust-protobuf to keep a single protobuf runtime in
/// Materialize source code).
///
/// We decode the delimited-protobuf wire bytes into these types and then
/// translate to `prometheus::proto::*` so the rest of the pipeline
/// (provenance attachment, `enrich`, `TextEncoder`) stays unchanged.
#[allow(non_snake_case, clippy::all)]
mod prom_pb {
    include!(concat!(env!("OUT_DIR"), "/io.prometheus.client.rs"));
}

/// Per-replica scrape timeout. Matches the cluster proxy's overall request
/// timeout in `http/cluster.rs`.
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(60);

/// Result of scraping one clusterd `/metrics` endpoint.
struct ScrapedReplica {
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    families: Vec<MetricFamily>,
    rules: Vec<Rule>,
}

/// Handler for `GET /metrics/federated`.
pub async fn handle_federated_metrics(
    client: AuthedClient,
    Extension(metrics_registry): Extension<MetricsRegistry>,
    Extension(locator): Extension<Arc<ReplicaHttpLocator>>,
) -> impl IntoResponse {
    let catalog = client.client.catalog_snapshot("metrics_federated").await;

    // 1. Local gather — env's own metrics. Some (e.g. `replica_connects_total`)
    //    carry cluster_id/replica_id labels; the rule-driven `enrich` pass
    //    below will attach cluster/replica names for those.
    let mut families = metrics_registry.gather();
    let mut all_rules = metrics_registry.rules_snapshot();

    // 2. Enumerate replica targets from the catalog. Probe process indices
    //    upward until `get_http_addr` returns None — no need to know each
    //    replica's scale up front.
    let mut targets: Vec<(ClusterId, ReplicaId, String)> = Vec::new();
    for cluster in catalog.clusters() {
        for replica in cluster.replicas() {
            let mut process = 0;
            while let Some(addr) = locator.get_http_addr(cluster.id, replica.replica_id, process) {
                targets.push((cluster.id, replica.replica_id, addr));
                process += 1;
            }
        }
    }

    // 3. Scrape every clusterd in parallel. For each batch, env knows the
    //    (cluster_id, replica_id) it dispatched to — call the same helper
    //    `enrich` would call when a Rule::ReplicaNameLookup matches.
    let http_client = reqwest::Client::new();
    let mut scrapes = FuturesUnordered::new();
    for (cluster_id, replica_id, addr) in targets {
        scrapes.push(scrape_one(
            http_client.clone(),
            cluster_id,
            replica_id,
            addr,
            SCRAPE_TIMEOUT,
        ));
    }
    while let Some(result) = scrapes.next().await {
        match result {
            Ok(mut replica) => {
                for family in &mut replica.families {
                    for metric in family.mut_metric() {
                        attach_cluster_replica_metadata(
                            metric,
                            replica.cluster_id,
                            replica.replica_id,
                            &catalog,
                        );
                    }
                }
                families.extend(replica.families);
                all_rules.extend(replica.rules);
            }
            Err((cluster_id, replica_id, e)) => {
                warn!(
                    %cluster_id,
                    %replica_id,
                    error = %e,
                    "failed to scrape clusterd for /metrics/federated"
                );
            }
        }
    }
    dedup_rules(&mut all_rules);

    // 4. Apply rules. ClusterNameLookup / ReplicaNameLookup use the same
    //    helpers as the scrape loop; ObjectNameLookup dispatches to
    //    `try_get_entry_by_global_id`.
    enrich(&mut families, &all_rules, &catalog);

    // 5. Encode standard Prometheus text. Federated callers always get text
    //    out — the protobuf path is only used for the env→clusterd internal
    //    hop.
    let encoder = prometheus::TextEncoder::new();
    let mut body = Vec::new();
    if let Err(e) = encoder.encode(&families, &mut body) {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    Ok((headers, body))
}

/// Scrapes one clusterd `/metrics` endpoint, decodes the delimited
/// protobuf body into `Vec<MetricFamily>`, and parses the
/// `X-Mz-Enrich-Rules` JSON header into a `Vec<Rule>`.
async fn scrape_one(
    client: reqwest::Client,
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    addr: String,
    timeout: Duration,
) -> Result<ScrapedReplica, (ClusterId, ReplicaId, String)> {
    let url = format!("http://{addr}/metrics");
    let resp = client
        .get(&url)
        .header(reqwest::header::ACCEPT, prometheus::PROTOBUF_FORMAT)
        .timeout(timeout)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| (cluster_id, replica_id, e.to_string()))?;

    let rules: Vec<Rule> = match resp.headers().get("x-mz-enrich-rules") {
        Some(h) => serde_json::from_slice(h.as_bytes())
            .map_err(|e| (cluster_id, replica_id, format!("rule header decode: {e}")))?,
        None => Vec::new(),
    };

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| (cluster_id, replica_id, e.to_string()))?;

    // Each MetricFamily is varint-length-prefixed in the response body
    // (see `prometheus::ProtobufEncoder` / `Message::write_length_delimited_to_writer`).
    // Decode them in order with prost, then translate to the rust-protobuf
    // type used by the rest of the pipeline.
    let mut buf = bytes;
    let mut families: Vec<MetricFamily> = Vec::new();
    while buf.has_remaining() {
        let pb = prom_pb::MetricFamily::decode_length_delimited(&mut buf)
            .map_err(|e| (cluster_id, replica_id, format!("prost decode: {e}")))?;
        families.push(convert_family(pb));
    }

    Ok(ScrapedReplica {
        cluster_id,
        replica_id,
        families,
        rules,
    })
}

/// Translates a `prom_pb::MetricFamily` (decoded by prost) into the
/// `prometheus::proto::MetricFamily` (rust-protobuf) used by
/// `prometheus::TextEncoder` and the rest of the federated pipeline.
fn convert_family(src: prom_pb::MetricFamily) -> MetricFamily {
    let mut dst = MetricFamily::new();
    if let Some(name) = src.name {
        dst.set_name(name);
    }
    if let Some(help) = src.help {
        dst.set_help(help);
    }
    if let Some(t) = src.r#type {
        dst.set_field_type(prom_metric_type_from_int(t));
    }
    for m in src.metric {
        dst.metric.push(convert_metric(m));
    }
    dst
}

fn prom_metric_type_from_int(t: i32) -> prometheus::proto::MetricType {
    use prometheus::proto::MetricType;
    // Matches the enum values in client_model.proto exactly.
    match t {
        0 => MetricType::COUNTER,
        1 => MetricType::GAUGE,
        2 => MetricType::SUMMARY,
        3 => MetricType::UNTYPED,
        4 => MetricType::HISTOGRAM,
        _ => MetricType::UNTYPED,
    }
}

fn convert_metric(src: prom_pb::Metric) -> Metric {
    let mut dst = Metric::new();
    for lp in src.label {
        let mut p = LabelPair::new();
        if let Some(n) = lp.name {
            p.set_name(n);
        }
        if let Some(v) = lp.value {
            p.set_value(v);
        }
        dst.label.push(p);
    }
    if let Some(g) = src.gauge {
        let mut x = prometheus::proto::Gauge::new();
        if let Some(v) = g.value {
            x.set_value(v);
        }
        dst.set_gauge(x);
    }
    if let Some(c) = src.counter {
        let mut x = prometheus::proto::Counter::new();
        if let Some(v) = c.value {
            x.set_value(v);
        }
        dst.set_counter(x);
    }
    if let Some(s) = src.summary {
        let mut x = prometheus::proto::Summary::new();
        if let Some(v) = s.sample_count {
            x.set_sample_count(v);
        }
        if let Some(v) = s.sample_sum {
            x.set_sample_sum(v);
        }
        for q in s.quantile {
            let mut p = prometheus::proto::Quantile::new();
            if let Some(v) = q.quantile {
                p.set_quantile(v);
            }
            if let Some(v) = q.value {
                p.set_value(v);
            }
            x.quantile.push(p);
        }
        dst.set_summary(x);
    }
    if src.untyped.is_some() {
        // The prometheus crate's codegen doesn't generate `set_untyped` (only
        // gauge/counter/summary/histogram have shim setters via proto_ext).
        // Untyped metrics are not used in Materialize today, so we drop them
        // here rather than reaching for `protobuf::MessageField` directly.
        debug!("dropping untyped metric in federated translation");
    }
    if let Some(h) = src.histogram {
        let mut x = prometheus::proto::Histogram::new();
        if let Some(v) = h.sample_count {
            x.set_sample_count(v);
        }
        if let Some(v) = h.sample_sum {
            x.set_sample_sum(v);
        }
        for b in h.bucket {
            let mut p = prometheus::proto::Bucket::new();
            if let Some(v) = b.cumulative_count {
                p.set_cumulative_count(v);
            }
            if let Some(v) = b.upper_bound {
                p.set_upper_bound(v);
            }
            x.bucket.push(p);
        }
        dst.set_histogram(x);
    }
    if let Some(ts) = src.timestamp_ms {
        dst.set_timestamp_ms(ts);
    }
    dst
}

/// Catalog lookup for cluster name. Pushes `cluster_id` and `cluster_name`
/// onto the metric. Idempotent — uses [`push_label_if_missing`].
fn attach_cluster_name(metric: &mut Metric, cluster_id: ClusterId, catalog: &Catalog) {
    push_label_if_missing(metric, "cluster_id", &cluster_id.to_string());
    if let Some(c) = catalog.try_get_cluster(cluster_id) {
        push_label_if_missing(metric, "cluster_name", &c.name);
    }
}

/// Full cluster + replica metadata: layers `replica_id` and `replica_name`
/// on top of [`attach_cluster_name`].
fn attach_cluster_replica_metadata(
    metric: &mut Metric,
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    catalog: &Catalog,
) {
    attach_cluster_name(metric, cluster_id, catalog);
    push_label_if_missing(metric, "replica_id", &replica_id.to_string());
    if let Some(r) = catalog.try_get_cluster_replica(cluster_id, replica_id) {
        push_label_if_missing(metric, "replica_name", &r.name);
    }
}

/// Walks the rule manifest and applies each rule to every metric whose
/// labels match.
fn enrich(families: &mut [MetricFamily], rules: &[Rule], catalog: &Catalog) {
    for family in families {
        for metric in family.mut_metric() {
            for rule in rules {
                match rule {
                    Rule::ClusterNameLookup { cluster_id_label } => {
                        let Some(s) = get_label(metric, cluster_id_label) else {
                            continue;
                        };
                        let Ok(cluster_id) = ClusterId::from_str(s) else {
                            continue;
                        };
                        attach_cluster_name(metric, cluster_id, catalog);
                    }
                    Rule::ReplicaNameLookup {
                        cluster_id_label,
                        replica_id_label,
                    } => {
                        let (Some(c), Some(r)) = (
                            get_label(metric, cluster_id_label),
                            get_label(metric, replica_id_label),
                        ) else {
                            continue;
                        };
                        let (Ok(cluster_id), Ok(replica_id)) =
                            (ClusterId::from_str(c), ReplicaId::from_str(r))
                        else {
                            continue;
                        };
                        attach_cluster_replica_metadata(metric, cluster_id, replica_id, catalog);
                    }
                    Rule::ObjectNameLookup { object_id_label } => {
                        let Some(s) = get_label(metric, object_id_label) else {
                            continue;
                        };
                        let Some(entry) = lookup_entry_by_id_str(catalog, s) else {
                            continue;
                        };
                        push_label_if_missing(metric, "object_name", entry.name().item.as_str());
                    }
                }
            }
        }
    }
}

/// Resolves a string-shaped catalog ID to a `CatalogEntry`. Accepts either
/// a `CatalogItemId` or a `GlobalId`.
fn lookup_entry_by_id_str<'a>(catalog: &'a Catalog, s: &str) -> Option<&'a CatalogEntry> {
    if let Ok(item_id) = CatalogItemId::from_str(s) {
        if let Some(entry) = catalog.try_get_entry(&item_id) {
            return Some(entry);
        }
    }
    if let Ok(global_id) = GlobalId::from_str(s) {
        if let Some(entry) = catalog.try_get_entry_by_global_id(&global_id) {
            return Some(entry);
        }
    }
    None
}

/// Returns the value of the first label with the given name, if any.
fn get_label<'a>(metric: &'a Metric, name: &str) -> Option<&'a str> {
    metric
        .get_label()
        .iter()
        .find(|l| l.name() == name)
        .map(|l| l.value())
}

/// Pushes `(name, value)` onto `metric` only if no existing label by that
/// name is present.
fn push_label_if_missing(metric: &mut Metric, name: &str, value: &str) {
    if metric.get_label().iter().any(|l| l.name() == name) {
        debug!(name, "skipping duplicate label on metric");
        return;
    }
    let mut pair = LabelPair::new();
    pair.set_name(name.to_string());
    pair.set_value(value.to_string());
    metric.label.push(pair);
}

/// Removes duplicate rules — multiple replicas often emit the same rule.
fn dedup_rules(rules: &mut Vec<Rule>) {
    let mut seen = HashSet::new();
    rules.retain(|r| seen.insert(r.clone()));
}
