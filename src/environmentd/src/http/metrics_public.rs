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
//! `/metrics` output, adding `cluster_id`, `replica_id`, and `process` labels
//! drawn from the scrape context, then applying any [`Rule`]s the replica
//! advertised via the `X-Materialize-Enrich-Rules` response header.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use axum::Extension;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use futures::future::join_all;
use headers::ContentType;
use http::{HeaderValue, Method, Request, StatusCode};
use http_body_util::BodyExt;
use mz_adapter::catalog::Catalog;
use mz_adapter_types::dyncfgs::ENABLE_PUBLIC_METRICS_ENDPOINT;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::metrics::{MetricsRegistry, NameLookup, ObjectName, Rule};
use mz_repr::GlobalId;
use mz_sql::names::RawDatabaseSpecifier;
use prometheus::Encoder;
use prometheus::proto::{LabelPair, MetricFamily};

use crate::http::AuthedClient;
use crate::http::cluster::{
    ClusterProxyConfig, proxy_replica_request, rewrite_request_for_replica,
};

/// Per-metric enrichment rules carried in the `X-Materialize-Enrich-Rules`
/// response header.
type RulesByMetric = BTreeMap<String, Vec<Rule>>;

const CLUSTER_PROXY_CLUSTER_ID_LABEL: &str = "cluster_id";
const CLUSTER_PROXY_CLUSTER_NAME_LABEL: &str = "cluster_name";
const CLUSTER_PROXY_REPLICA_ID_LABEL: &str = "replica_id";
const CLUSTER_PROXY_REPLICA_NAME_LABEL: &str = "replica_name";
const CLUSTER_PROXY_PROCESS_LABEL: &str = "process";

/// The enrichment rules env applies to every replica scrape to resolve the
/// `cluster_id`/`replica_id` labels.
///
/// Replicas don't advertise these: the IDs come from environmentd, not
/// from the metric, so only environmentd knows to add them.
static CLUSTER_PROXY_RULES: LazyLock<Vec<Rule>> = LazyLock::new(|| {
    vec![
        Rule::ClusterNameLookup {
            cluster_id_label: CLUSTER_PROXY_CLUSTER_ID_LABEL.into(),

            output_label: CLUSTER_PROXY_CLUSTER_NAME_LABEL.into(),
        },
        Rule::ReplicaNameLookup {
            cluster_id_label: CLUSTER_PROXY_CLUSTER_ID_LABEL.into(),

            replica_id_label: CLUSTER_PROXY_REPLICA_ID_LABEL.into(),

            output_label: CLUSTER_PROXY_REPLICA_NAME_LABEL.into(),
        },
    ]
});

struct CatalogLookup<'a>(&'a Catalog);

/// A scrape's response body plus any [`Rule`]s the remote registry advertised
/// in the `X-Materialize-Enrich-Rules` header, keyed by the
/// fully-qualified metric name.
struct ReplicaScrape {
    body: Vec<u8>,
    rules: RulesByMetric,
}

impl NameLookup for CatalogLookup<'_> {
    fn cluster_name(&self, cluster_id: &str) -> Option<String> {
        let cid = ClusterId::from_str(cluster_id).ok()?;
        self.0.try_get_cluster(cid).map(|c| c.name.clone())
    }

    fn replica_name(&self, cluster_id: &str, replica_id: &str) -> Option<String> {
        let cid = ClusterId::from_str(cluster_id).ok()?;
        let rid = ReplicaId::from_str(replica_id).ok()?;
        self.0
            .try_get_cluster_replica(cid, rid)
            .map(|r| r.name.clone())
    }

    fn object_name(&self, global_id: &str) -> Option<ObjectName> {
        let gid = GlobalId::from_str(global_id).ok()?;
        let entry = self.0.try_get_entry_by_global_id(&gid)?;
        let full = self.0.resolve_full_name(entry.name(), None);
        let database = match full.database {
            RawDatabaseSpecifier::Name(name) => name,
            // For ambient objects, the database name is an empty string.
            RawDatabaseSpecifier::Ambient => String::new(),
        };
        Some(ObjectName {
            database,
            schema: full.schema,
            object: full.item,
        })
    }
}

/// The customer-facing `/metrics` endpoint.
///
/// Aggregates environmentd's local metrics with every clusterd replica's
/// `/metrics` output.
pub(crate) async fn handle_public_metrics(
    client: AuthedClient,
    Extension(config): Extension<Arc<ClusterProxyConfig>>,
    Extension(metrics_registry): Extension<MetricsRegistry>,
) -> Response {
    let catalog = client.client.catalog_snapshot("metrics_public").await;
    if !ENABLE_PUBLIC_METRICS_ENDPOINT.get(catalog.system_config().dyncfgs()) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    let lookup = CatalogLookup(&catalog);

    // Start with environmentd's metrics registry.
    let mut all_families: Vec<MetricFamily> = metrics_registry.gather();
    let env_rules_by_metric = metrics_registry.rules_by_metric();
    for family in &mut all_families {
        if let Some(rules) = env_rules_by_metric.get(family.name()) {
            for rule in rules {
                rule.apply(family, &lookup);
            }
        }
    }

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

    let cluster_proxy_rules = CLUSTER_PROXY_RULES.clone();

    for (cluster_id, replica_id, process, result) in results {
        let scrape = match result {
            Ok(s) => s,
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
        let families = match mz_prometheus_protobuf::decode_length_delimited(&scrape.body) {
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
            add_cluster_proxy_labels(
                &mut family,
                cluster_id,
                replica_id,
                process,
                &lookup,
                &cluster_proxy_rules,
            );

            if let Some(rules) = scrape.rules.get(family.name()) {
                for rule in rules {
                    rule.apply(&mut family, &lookup);
                }
            }
            all_families.push(family);
        }
    }

    // env and clusterd both register many of the same metric names
    // and every replica scrape may also redeclare names. The
    // text encoder emits one `# HELP`/`# TYPE` block per `MetricFamily`, so
    // without this we'd produce duplicate header lines and scrapers would
    // reject the response.
    let all_families = merge_families_by_name(all_families);

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
) -> (ClusterId, ReplicaId, usize, Result<ReplicaScrape, String>) {
    let result =
        scrape_replica_metrics_endpoint_inner(cluster_id, replica_id, process, &addr).await;
    (cluster_id, replica_id, process, result)
}

async fn scrape_replica_metrics_endpoint_inner(
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    process: usize,
    addr: &str,
) -> Result<ReplicaScrape, String> {
    let mut req = Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header(
            http::header::ACCEPT,
            mz_http_util::PROMETHEUS_PROTOBUF_CONTENT_TYPE,
        )
        .header(
            mz_http_util::MATERIALIZE_ACCEPT_ENRICH_RULES_HEADER,
            HeaderValue::from_static("1"),
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
    let rules = parse_rules_header(resp.headers());
    let body = resp.into_body();
    let collected = body
        .collect()
        .await
        .map_err(|e| format!("collecting body: {e}"))?
        .to_bytes();

    Ok(ReplicaScrape {
        body: collected.to_vec(),
        rules,
    })
}

/// Parses the `X-Materialize-Enrich-Rules` response header into a map of
/// metric-name -> rules.
fn parse_rules_header(headers: &http::HeaderMap) -> RulesByMetric {
    let Some(value) = headers.get(mz_http_util::MATERIALIZE_ENRICH_RULES_HEADER) else {
        return BTreeMap::new();
    };
    let s = match value.to_str() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("invalid utf-8 in enrich-rules header: {e}");
            return BTreeMap::new();
        }
    };
    match mz_http_util::decode_enrich_rules::<RulesByMetric>(s) {
        Ok(rules) => rules,
        Err(e) => {
            tracing::warn!("failed to decode enrich-rules header: {e}");
            BTreeMap::new()
        }
    }
}
/// Adds cluster proxy labels on every metric in `family`.
fn add_cluster_proxy_labels<L: NameLookup>(
    family: &mut MetricFamily,
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    process: usize,
    lookup: &L,
    cluster_proxy_rules: &[Rule],
) {
    for metric in family.mut_metric() {
        let mut labels = metric.take_label();
        labels.push(label_pair(
            CLUSTER_PROXY_CLUSTER_ID_LABEL,
            cluster_id.to_string(),
        ));
        labels.push(label_pair(
            CLUSTER_PROXY_REPLICA_ID_LABEL,
            replica_id.to_string(),
        ));
        labels.push(label_pair(CLUSTER_PROXY_PROCESS_LABEL, process.to_string()));
        metric.set_label(labels);
    }
    for rule in cluster_proxy_rules {
        rule.apply(family, lookup);
    }
}

/// Merges `MetricFamily` entries that share a name into a single entry, keeping
/// the first occurrence's metadata (help text, type) and appending every
/// subsequent same-named family's metrics into it.
fn merge_families_by_name(
    families: Vec<prometheus::proto::MetricFamily>,
) -> Vec<prometheus::proto::MetricFamily> {
    let mut merged_families: Vec<prometheus::proto::MetricFamily> =
        Vec::with_capacity(families.len());
    let mut first_family_index_by_name: BTreeMap<String, usize> = BTreeMap::new();
    for mut family in families {
        let name = family.name().to_owned();
        if let Some(&index) = first_family_index_by_name.get(&name) {
            for metric in family.take_metric() {
                merged_families[index].mut_metric().push(metric);
            }
        } else {
            first_family_index_by_name.insert(name, merged_families.len());
            merged_families.push(family);
        }
    }
    merged_families
}

fn label_pair(name: &str, value: String) -> LabelPair {
    let mut pair = LabelPair::default();
    pair.set_name(name.to_owned());
    pair.set_value(value);
    pair
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use prometheus::proto::{Counter, Metric, MetricType};

    use super::*;

    fn make_family(name: &str) -> MetricFamily {
        let mut family = MetricFamily::default();
        family.set_name(name.to_owned());
        family.set_help(format!("help for {name}"));
        family.set_field_type(MetricType::COUNTER);
        let mut metric = Metric::default();
        let mut counter = Counter::default();
        counter.set_value(1.0);
        metric.set_counter(counter);
        family.set_metric(vec![metric]);
        family
    }

    /// Test stand-in for [`CatalogLookup`].
    #[derive(Default)]
    struct FakeCatalog {
        clusters: BTreeMap<String, String>,
        replicas: BTreeMap<(String, String), String>,
        objects: BTreeMap<String, ObjectName>,
    }

    impl NameLookup for FakeCatalog {
        fn cluster_name(&self, cluster_id: &str) -> Option<String> {
            self.clusters.get(cluster_id).cloned()
        }
        fn replica_name(&self, cluster_id: &str, replica_id: &str) -> Option<String> {
            self.replicas
                .get(&(cluster_id.to_owned(), replica_id.to_owned()))
                .cloned()
        }
        fn object_name(&self, global_id: &str) -> Option<ObjectName> {
            self.objects.get(global_id).cloned()
        }
    }

    #[mz_ore::test]
    fn add_cluster_proxy_labels_when_names_unknown() {
        let mut family = make_family("test");
        let cluster_id: ClusterId = "u1".parse().expect("cluster id");
        let replica_id: ReplicaId = "u2".parse().expect("replica id");
        add_cluster_proxy_labels(
            &mut family,
            cluster_id,
            replica_id,
            7,
            &FakeCatalog::default(),
            &CLUSTER_PROXY_RULES.clone(),
        );
        let labels = family.get_metric()[0].get_label();
        let names: Vec<_> = labels.iter().map(|l| l.name()).collect();
        assert_eq!(names, vec!["cluster_id", "replica_id", "process"]);
        let values: Vec<_> = labels.iter().map(|l| l.value()).collect();
        assert_eq!(values, vec!["u1", "u2", "7"]);
    }

    /// The cluster-proxy name rules resolve `cluster_id`/`replica_id` to
    /// names even when the replica advertised no rules of its own.
    #[mz_ore::test]
    fn add_cluster_proxy_labels_when_names_known() {
        let mut family = make_family("test");
        let cluster_id: ClusterId = "u1".parse().expect("cluster id");
        let replica_id: ReplicaId = "u2".parse().expect("replica id");
        let catalog = FakeCatalog {
            clusters: BTreeMap::from([("u1".into(), "quickstart".into())]),
            replicas: BTreeMap::from([(("u1".into(), "u2".into()), "r1".into())]),
            ..Default::default()
        };

        // Mirror the clusterd path: stamp the IDs, then apply the implicit
        // scrape-context name rules. No rules are advertised by the replica.
        add_cluster_proxy_labels(
            &mut family,
            cluster_id,
            replica_id,
            0,
            &catalog,
            &CLUSTER_PROXY_RULES.clone(),
        );
        for rule in &CLUSTER_PROXY_RULES.clone() {
            rule.apply(&mut family, &catalog);
        }

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

    #[mz_ore::test]
    fn enriches_dataflow_wallclock_lag_metric() {
        let mut family = make_family("mz_dataflow_wallclock_lag_seconds");
        let metric = &mut family.mut_metric()[0];
        let mut labels = metric.take_label();
        labels.push(label_pair("instance_id", "u1".into()));
        labels.push(label_pair("replica_id", "u2".into()));
        labels.push(label_pair("collection_id", "s1".into()));
        labels.push(label_pair("quantile", "0".into()));
        metric.set_label(labels);

        let catalog = FakeCatalog {
            clusters: BTreeMap::from([("u1".into(), "quickstart".into())]),
            replicas: BTreeMap::from([(("u1".into(), "u2".into()), "r1".into())]),
            objects: BTreeMap::from([(
                "s1".into(),
                ObjectName {
                    database: "materialize".into(),
                    schema: "public".into(),
                    object: "my_collection".into(),
                },
            )]),
        };

        // The rules `mz_dataflow_wallclock_lag_seconds` advertises, keyed on the
        // metric's own labels (see `mz_cluster_client::metrics`).
        let advertised: RulesByMetric = BTreeMap::from([(
            "mz_dataflow_wallclock_lag_seconds".to_owned(),
            vec![
                Rule::ClusterNameLookup {
                    cluster_id_label: "instance_id".into(),
                    output_label: "cluster_name".into(),
                },
                Rule::ReplicaNameLookup {
                    cluster_id_label: "instance_id".into(),
                    replica_id_label: "replica_id".into(),
                    output_label: "replica_name".into(),
                },
                Rule::object_name_lookup_with_default_labels("collection_id", "collection_name"),
            ],
        )]);

        if let Some(rules) = advertised.get(family.name()) {
            for rule in rules {
                rule.apply(&mut family, &catalog);
            }
        }

        let labels = family.get_metric()[0].get_label();
        let value = |name: &str| labels.iter().find(|l| l.name() == name).map(|l| l.value());
        // Names resolved by the proxy rules.
        assert_eq!(value("cluster_name"), Some("quickstart"));
        assert_eq!(value("replica_name"), Some("r1"));
        // Object name resolved by the metric's advertised rule, split into the
        // object, schema, and database labels.
        assert_eq!(value("collection_name"), Some("my_collection"));
        assert_eq!(value("schema_name"), Some("public"));
        assert_eq!(value("database_name"), Some("materialize"));
    }

    #[mz_ore::test]
    fn shared_metric_name_emits_single_help_and_type() {
        let env_family = make_family("mz_persist_test");
        let mut clusterd_family = make_family("mz_persist_test");
        add_cluster_proxy_labels(
            &mut clusterd_family,
            "u1".parse().unwrap(),
            "u2".parse().unwrap(),
            0,
            &FakeCatalog::default(),
            &CLUSTER_PROXY_RULES.clone(),
        );

        let merged = merge_families_by_name(vec![env_family, clusterd_family]);
        let mut body = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&merged, &mut body)
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

    /// Mirrors `handle_public_metrics`'s clusterd path: attach the cluster
    /// proxy labels, then apply rules from a per-metric map keyed by family
    /// name.
    #[mz_ore::test]
    fn cluster_proxy_and_x_materialize_enrich_rules_labels_are_attached() {
        let mut family = make_family("test");
        let cluster_id: ClusterId = "u1".parse().expect("cluster id");
        let replica_id: ReplicaId = "u2".parse().expect("replica id");

        let catalog = FakeCatalog {
            clusters: BTreeMap::from([("u1".into(), "quickstart".into())]),
            replicas: BTreeMap::from([(("u1".into(), "u2".into()), "r1".into())]),
            ..Default::default()
        };

        // Rules advertised by the remote registry for this specific metric.
        let scraped: RulesByMetric = BTreeMap::from([(
            "test".to_owned(),
            vec![
                Rule::ClusterNameLookup {
                    cluster_id_label: "cluster_id".into(),
                    output_label: "cluster_name_2".into(),
                },
                Rule::ReplicaNameLookup {
                    cluster_id_label: "cluster_id".into(),
                    replica_id_label: "replica_id".into(),
                    output_label: "replica_name_2".into(),
                },
            ],
        )]);

        add_cluster_proxy_labels(
            &mut family,
            cluster_id,
            replica_id,
            0,
            &catalog,
            &CLUSTER_PROXY_RULES.clone(),
        );
        if let Some(rules) = scraped.get(family.name()) {
            for rule in rules {
                rule.apply(&mut family, &catalog);
            }
        }

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
                "cluster_name_2",
                "replica_name_2",
            ]
        );
        let values: Vec<_> = labels.iter().map(|l| l.value()).collect();
        assert_eq!(
            values,
            vec!["u1", "u2", "0", "quickstart", "r1", "quickstart", "r1"]
        );
    }

    /// Tests that applying duplicate rules (with identical output_label) for the same metric
    /// results in the label appearing only once, and with the value from the first-applied rule.
    #[mz_ore::test]
    fn duplicate_output_labels() {
        let mut family = make_family("test");
        let cluster_id: ClusterId = "u1".parse().expect("cluster id");
        let replica_id: ReplicaId = "u2".parse().expect("replica id");
        let catalog = FakeCatalog {
            clusters: BTreeMap::from([(cluster_id.to_string(), "one".into())]),
            replicas: BTreeMap::from([(
                (cluster_id.to_string(), replica_id.to_string()),
                "two".into(),
            )]),
            ..Default::default()
        };

        // Both rules use the same output_label ("mylabel").
        // The first will resolve to "foo", the second to "bar".
        // Our CatalogLookup returns "one" and "two", so the rule ordering determines the label value.
        let scraped: RulesByMetric = BTreeMap::from([(
            "test".to_owned(),
            vec![
                Rule::ClusterNameLookup {
                    cluster_id_label: "cluster_id".into(),
                    output_label: "mylabel".into(),
                },
                Rule::ReplicaNameLookup {
                    cluster_id_label: "cluster_id".into(),
                    replica_id_label: "replica_id".into(),
                    output_label: "mylabel".into(),
                },
            ],
        )]);

        add_cluster_proxy_labels(
            &mut family,
            cluster_id,
            replica_id,
            0,
            &catalog,
            &CLUSTER_PROXY_RULES.clone(),
        );

        if let Some(rules) = scraped.get(family.name()) {
            for rule in rules {
                rule.apply(&mut family, &catalog);
            }
        }

        let labels = family.get_metric()[0].get_label();
        let mylabel_values: Vec<_> = labels
            .iter()
            .filter(|l| l.name() == "mylabel")
            .map(|l| l.value())
            .collect();

        // should have the value of the first-applied rule ("foo").
        assert_eq!(mylabel_values, vec!["one"]);
    }

    /// When the catalog can't resolve the IDs, the per-metric rules silently
    /// no-op and the family is left with just the cluster-proxy ID labels.
    #[mz_ore::test]
    fn rules_noop_when_catalog_misses() {
        let mut family = make_family("test");
        let cluster_id: ClusterId = "u1".parse().expect("cluster id");
        let replica_id: ReplicaId = "u2".parse().expect("replica id");
        let catalog = FakeCatalog::default(); // catalog has neither cluster nor replica

        let scraped: RulesByMetric = BTreeMap::from([(
            "test".to_owned(),
            vec![
                Rule::ClusterNameLookup {
                    cluster_id_label: "cluster_id".into(),
                    output_label: "cluster_name".into(),
                },
                Rule::ReplicaNameLookup {
                    cluster_id_label: "cluster_id".into(),
                    replica_id_label: "replica_id".into(),
                    output_label: "replica_name".into(),
                },
            ],
        )]);

        add_cluster_proxy_labels(
            &mut family,
            cluster_id,
            replica_id,
            0,
            &catalog,
            &CLUSTER_PROXY_RULES.clone(),
        );
        if let Some(rules) = scraped.get(family.name()) {
            for rule in rules {
                rule.apply(&mut family, &catalog);
            }
        }

        let names: Vec<_> = family.get_metric()[0]
            .get_label()
            .iter()
            .map(|l| l.name())
            .collect();
        assert_eq!(names, vec!["cluster_id", "replica_id", "process"]);
    }

    #[mz_ore::test]
    fn parse_rules_header_missing_yields_empty() {
        let headers = http::HeaderMap::new();
        assert!(parse_rules_header(&headers).is_empty());
    }

    #[mz_ore::test]
    fn parse_rules_header_round_trips() {
        let rules: RulesByMetric = BTreeMap::from([(
            "mz_replica_connects_total".to_owned(),
            vec![Rule::ClusterNameLookup {
                cluster_id_label: "cluster_id".into(),
                output_label: "cluster_name".into(),
            }],
        )]);
        let encoded = mz_http_util::encode_enrich_rules(&rules).unwrap();
        let mut headers = http::HeaderMap::new();
        headers.insert(
            mz_http_util::MATERIALIZE_ENRICH_RULES_HEADER,
            HeaderValue::from_str(&encoded).unwrap(),
        );
        assert_eq!(parse_rules_header(&headers), rules);
    }

    #[mz_ore::test]
    fn parse_rules_header_invalid_json_yields_empty() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            mz_http_util::MATERIALIZE_ENRICH_RULES_HEADER,
            HeaderValue::from_static("not valid json"),
        );
        assert!(parse_rules_header(&headers).is_empty());
    }
}
