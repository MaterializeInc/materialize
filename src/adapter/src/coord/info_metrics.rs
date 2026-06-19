// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus `*_info` metrics describing catalog objects.
//!
//! These follow the standard Prometheus "info" pattern (`kube_pod_info`
//! style): one series per object, constant value `1`, with descriptive
//! labels. They give other metrics a stable `group_left` join target for
//! resolving object IDs to names.
//!
//! The metrics are periodically reconciled with the catalog by a
//! background task ([Coordinator::spawn_catalog_info_metrics_task], driven by an
//! interval off the coordinator's main loop. It rebuilds the series whenever the
//! catalog's [transient revision](Catalog::transient_revision) changes, from the
//! catalog. It's okay if the info metrics are not exactly up to date and
//! eventually consistent.

use std::time::{Duration, Instant};

use mz_adapter_types::dyncfgs::CATALOG_INFO_METRICS_RECONCILE_INTERVAL;
use mz_catalog::memory::objects::{
    CatalogItem, Cluster, ClusterReplica, ClusterVariant, DataSourceDesc,
};
use mz_controller::clusters::ReplicaLocation;
use mz_ore::metric;
use mz_ore::metrics::{
    DeleteOnDropGauge, Histogram, MetricVisibility, MetricsRegistry, UIntGaugeVec,
};
use mz_ore::stats::histogram_seconds_buckets;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::CatalogItemId;
use mz_sql::names::{FullItemName, RawDatabaseSpecifier};
use prometheus::core::AtomicU64;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::catalog::{Catalog, catalog_type_to_audit_object_type};
use crate::command::{CatalogSnapshot, Command};
use crate::coord::{Coordinator, Message};

/// Fallback reconcile cadence: the re-poll cadence used while reconciliation is
/// disabled (a zero [CATALOG_INFO_METRICS_RECONCILE_INTERVAL]), so it can be
/// re-enabled at runtime.
const FALLBACK_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

/// Reconciles that take longer than this are logged at warn level.
const RECONCILE_WARN_THRESHOLD: Duration = Duration::from_secs(5);

type InfoGauge = DeleteOnDropGauge<AtomicU64, Vec<String>>;

/// `*_info` metrics for catalog objects, mirroring the in-memory catalog.
///
/// System and user items, clusters, and replicas are all reported. Temporary
/// (session-scoped) items and per-cluster introspection source indexes are
/// not.
#[derive(Debug)]
pub(crate) struct CatalogInfoMetrics {
    object_info: UIntGaugeVec,
    cluster_info: UIntGaugeVec,
    replica_info: UIntGaugeVec,
    source_info: UIntGaugeVec,
    sink_info: UIntGaugeVec,
    /// Handles to all live series, held purely so that dropping them removes
    /// the series from the registry. Series are only ever created and dropped
    /// wholesale, by [CatalogInfoMetrics::populate].
    series: Vec<InfoGauge>,
    /// The [Catalog::transient_revision] the metrics were last populated
    /// from. Used to avoid rebuilding the metrics if the catalog has not changed.
    last_revision: Option<u64>,
    /// Times a full (re)build of the series in [CatalogInfoMetrics::populate].
    reconcile_seconds: Histogram,
}

impl CatalogInfoMetrics {
    pub fn new(registry: &MetricsRegistry) -> Self {
        Self {
            object_info: registry.register(metric!(
                name: "mz_object_info",
                help: "Maps catalog object IDs to the object's name, schema, database, and \
                       type. Constant 1.",
                var_labels: ["object_id", "global_id", "name", "schema_name", "database_name", "type"],
                visibility: MetricVisibility::Public
            )),
            cluster_info: registry.register(metric!(
                name: "mz_cluster_info",
                help: "Maps cluster IDs to the cluster's name and size. Constant 1.",
                var_labels: ["cluster_id", "name", "size"],
                visibility: MetricVisibility::Public
            )),
            replica_info: registry.register(metric!(
                name: "mz_replica_info",
                help: "Maps cluster replica IDs to the replica's name and size. Constant 1.",
                var_labels: ["replica_id", "cluster_id", "name", "size"],
                visibility: MetricVisibility::Public
            )),
            source_info: registry.register(metric!(
                name: "mz_source_info",
                help: "Maps user source IDs to the source's type, envelope type, and \
                       cluster. Constant 1.",
                var_labels: ["source_id", "type", "envelope_type", "cluster_id"],
                visibility: MetricVisibility::Public
            )),
            sink_info: registry.register(metric!(
                name: "mz_sink_info",
                help: "Maps user sink IDs to the sink's type, envelope type, and \
                       cluster. Constant 1.",
                var_labels: ["sink_id", "type", "envelope_type", "cluster_id"],
                visibility: MetricVisibility::Public
            )),
            series: Vec::new(),
            last_revision: None,
            reconcile_seconds: registry.register(metric!(
                name: "mz_catalog_info_metrics_reconcile_seconds",
                help: "Time taken to rebuild the catalog info metrics from a catalog snapshot.",
                buckets: histogram_seconds_buckets(0.000_128, 8.0),
            )),
        }
    }

    /// Reconciles the info metrics with the given catalog, unless they
    /// already reflect its revision.
    pub fn reconcile(&mut self, catalog: &Catalog) {
        if self.last_revision != Some(catalog.transient_revision()) {
            debug!(
                revision = catalog.transient_revision(),
                last_revision = self.last_revision,
                "reconciling catalog info metrics"
            );
            let start = Instant::now();
            self.populate(catalog);
            let elapsed = start.elapsed();
            self.reconcile_seconds.observe(elapsed.as_secs_f64());
            if elapsed > RECONCILE_WARN_THRESHOLD {
                warn!(
                    ?elapsed,
                    series = self.series.len(),
                    "catalog info metrics reconcile was slow"
                );
            }
            self.last_revision = Some(catalog.transient_revision());
        }
    }

    /// (Re)populates all info metrics from the given catalog.
    fn populate(&mut self, catalog: &Catalog) {
        // Drop all existing series before creating replacements: dropping a
        // stale handle whose labels match a newly created series would remove
        // the new series from the vector
        self.series.clear();

        for entry in catalog.entries() {
            let full_name = catalog.resolve_full_name(entry.name(), entry.conn_id());
            self.insert_item(entry.id(), entry.item(), &full_name);
            self.insert_source_or_sink(entry.id(), entry.item());
        }
        for cluster in catalog.clusters() {
            self.insert_cluster(cluster);
            for replica in cluster.replicas() {
                self.insert_replica(replica);
            }
        }
    }

    fn insert_item(&mut self, id: CatalogItemId, item: &CatalogItem, full_name: &FullItemName) {
        match id {
            // Ignore per-cluster introspection source indexes and temporary
            // (session-scoped) items.
            CatalogItemId::IntrospectionSourceIndex(_) | CatalogItemId::Transient(_) => return,
            CatalogItemId::System(_) | CatalogItemId::User(_) => (),
        }

        // System (ambient) objects don't belong to a database.
        let database_name = match &full_name.database {
            RawDatabaseSpecifier::Name(name) => name.clone(),
            RawDatabaseSpecifier::Ambient => String::new(),
        };
        let item_type = catalog_type_to_audit_object_type(item.typ()).to_string();
        for global_id in item.global_ids() {
            let series = new_series(
                &self.object_info,
                vec![
                    id.to_string(),
                    global_id.to_string(),
                    full_name.item.clone(),
                    full_name.schema.clone(),
                    database_name.clone(),
                    item_type.clone(),
                ],
            );
            self.series.push(series);
        }
    }

    /// Reports an `mz_source_info` or `mz_sink_info` series for `item` if it
    /// is a source or sink. Progress sources and subsources are not reported.
    fn insert_source_or_sink(&mut self, id: CatalogItemId, item: &CatalogItem) {
        let (info_vec, object_type, envelope_type) = match item {
            CatalogItem::Source(source) => {
                match &source.data_source {
                    DataSourceDesc::Ingestion { .. }
                    | DataSourceDesc::OldSyntaxIngestion { .. }
                    | DataSourceDesc::Webhook { .. } => (),
                    DataSourceDesc::IngestionExport { .. }
                    | DataSourceDesc::Progress
                    | DataSourceDesc::Introspection(_)
                    | DataSourceDesc::Catalog => return,
                }
                (
                    &self.source_info,
                    source.source_type(),
                    source.data_source.envelope(),
                )
            }
            CatalogItem::Sink(sink) => (&self.sink_info, sink.sink_type(), sink.envelope()),
            _ => return,
        };

        let series = new_series(
            info_vec,
            vec![
                id.to_string(),
                object_type.to_string(),
                envelope_type.map(|s| s.to_string()).unwrap_or_default(),
                item.cluster_id()
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            ],
        );
        self.series.push(series);
    }

    fn insert_cluster(&mut self, cluster: &Cluster) {
        let size = match &cluster.config.variant {
            ClusterVariant::Managed(managed) => managed.size.clone(),
            ClusterVariant::Unmanaged => String::new(),
        };
        let series = new_series(
            &self.cluster_info,
            vec![cluster.id.to_string(), cluster.name.clone(), size],
        );
        self.series.push(series);
    }

    fn insert_replica(&mut self, replica: &ClusterReplica) {
        let size = match &replica.config.location {
            ReplicaLocation::Managed(managed) => managed.size.clone(),
            ReplicaLocation::Unmanaged(_) => String::new(),
        };
        let series = new_series(
            &self.replica_info,
            vec![
                replica.replica_id.to_string(),
                replica.cluster_id.to_string(),
                replica.name.clone(),
                size,
            ],
        );
        self.series.push(series);
    }
}

impl Coordinator {
    /// Spawns a background task that keeps the catalog info metrics in sync with
    /// the catalog.
    pub(crate) fn spawn_catalog_info_metrics_task(&self) {
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let mut metrics = CatalogInfoMetrics::new(&self.catalog_info_metrics_registry);
        task::spawn(|| "catalog_info_metrics", async move {
            loop {
                let (tx, rx) = oneshot::channel();
                let send = internal_cmd_tx.send(Message::Command(
                    OpenTelemetryContext::obtain(),
                    Command::CatalogSnapshot { tx },
                ));
                // Bail if the coordinator has gone away.
                if send.is_err() {
                    break;
                }
                let Ok(CatalogSnapshot { catalog }) = rx.await else {
                    break;
                };

                // The reconcile cadence is a dyncfg; a zero interval disables
                // reconciliation.
                let interval =
                    CATALOG_INFO_METRICS_RECONCILE_INTERVAL.get(catalog.system_config().dyncfgs());
                if !interval.is_zero() {
                    metrics.reconcile(&catalog);
                }

                // When disabled, keep polling at the fallback cadence so it can
                // be re-enabled at runtime.
                let sleep = if interval.is_zero() {
                    FALLBACK_RECONCILE_INTERVAL
                } else {
                    interval
                };
                tokio::time::sleep(sleep).await;
            }
        });
    }
}

fn new_series(vec: &UIntGaugeVec, labels: Vec<String>) -> InfoGauge {
    let gauge = vec.get_delete_on_drop_metric(labels);
    gauge.set(1);
    gauge
}
#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mz_catalog::memory::objects::{
        ClusterConfig, ClusterVariantManaged, Sink, Source, Table, TableDataSource,
    };
    use mz_compute_types::config::ComputeReplicaConfig;
    use mz_controller::clusters::{
        ManagedReplicaAvailabilityZones, ManagedReplicaLocation, ReplicaAllocation, ReplicaConfig,
        UnmanagedReplicaLocation,
    };
    use mz_controller_types::{ClusterId, ReplicaId};
    use mz_repr::adt::mz_acl_item::PrivilegeMap;
    use mz_repr::role_id::RoleId;
    use mz_repr::{GlobalId, RelationDesc, RelationVersion, SqlScalarType, VersionedRelationDesc};
    use mz_sql::names::ResolvedIds;
    use mz_sql::plan::{WebhookBodyFormat, WebhookHeaders};
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
    use mz_storage_types::sinks::{
        KafkaIdStyle, KafkaSinkCompressionType, KafkaSinkConnection, KafkaSinkFormat,
        KafkaSinkFormatType, SinkEnvelope, StorageSinkConnection,
    };
    use mz_storage_types::sources::Timeline;

    use super::*;
    use crate::catalog::Op;

    /// Returns the label maps of all series of the metric `name`
    fn series(registry: &MetricsRegistry, name: &str) -> Vec<BTreeMap<String, String>> {
        registry
            .gather()
            .iter()
            .filter(|family| family.name() == name)
            .flat_map(|family| family.get_metric())
            .map(|metric| {
                // ensure every series has the value 1.
                assert_eq!(metric.get_gauge().value(), 1.0, "info series must be 1");
                metric
                    .get_label()
                    .iter()
                    .map(|label| (label.name().to_string(), label.value().to_string()))
                    .collect()
            })
            .collect()
    }

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn test_table(gid: GlobalId) -> CatalogItem {
        CatalogItem::Table(Table {
            create_sql: None,
            desc: VersionedRelationDesc::new(
                RelationDesc::builder()
                    .with_column("a", SqlScalarType::String.nullable(false))
                    .finish(),
            ),
            collections: BTreeMap::from([(RelationVersion::root(), gid)]),
            conn_id: None,
            resolved_ids: ResolvedIds::empty(),
            custom_logical_compaction_window: None,
            is_retained_metrics_object: false,
            data_source: TableDataSource::TableWrites { defaults: vec![] },
            branch_target_shard: None,
        })
    }

    fn test_webhook_source(gid: u64, cluster_id: ClusterId) -> CatalogItem {
        CatalogItem::Source(Source {
            create_sql: None,
            global_id: GlobalId::User(gid),
            data_source: DataSourceDesc::Webhook {
                validate_using: None,
                body_format: WebhookBodyFormat::Json { array: false },
                headers: WebhookHeaders::default(),
                cluster_id,
            },
            desc: RelationDesc::builder()
                .with_column("a", SqlScalarType::String.nullable(false))
                .finish(),
            timeline: Timeline::EpochMilliseconds,
            resolved_ids: ResolvedIds::empty(),
            custom_logical_compaction_window: None,
            is_retained_metrics_object: false,
        })
    }

    fn test_kafka_sink(gid: u64, cluster_id: ClusterId) -> CatalogItem {
        CatalogItem::Sink(Sink {
            create_sql: "CREATE SINK s FROM t INTO KAFKA CONNECTION c (TOPIC 'topic')".to_string(),
            global_id: GlobalId::User(gid),
            from: GlobalId::User(1),
            connection: StorageSinkConnection::Kafka(KafkaSinkConnection {
                connection_id: CatalogItemId::User(2),
                connection: CatalogItemId::User(2),
                format: KafkaSinkFormat {
                    key_format: None,
                    value_format: KafkaSinkFormatType::Json,
                },
                relation_key_indices: None,
                key_desc_and_indices: None,
                headers_index: None,
                value_desc: RelationDesc::builder()
                    .with_column("a", SqlScalarType::String.nullable(false))
                    .finish(),
                partition_by: None,
                topic: "topic".to_string(),
                topic_options: Default::default(),
                compression_type: KafkaSinkCompressionType::None,
                progress_group_id: KafkaIdStyle::Legacy,
                transactional_id: KafkaIdStyle::Legacy,
                topic_metadata_refresh_interval: Duration::from_secs(60),
            }),
            envelope: SinkEnvelope::Upsert,
            with_snapshot: true,
            version: 0,
            resolved_ids: ResolvedIds::empty(),
            cluster_id,
            commit_interval: None,
        })
    }

    fn test_cluster_config(size: &str) -> ClusterConfig {
        ClusterConfig {
            variant: ClusterVariant::Managed(ClusterVariantManaged {
                size: size.to_string(),
                availability_zones: Vec::new(),
                logging: Default::default(),
                replication_factor: 1,
                optimizer_feature_overrides: Default::default(),
                schedule: Default::default(),
            }),
            workload_class: None,
        }
    }

    fn test_cluster(id: ClusterId, name: &str, size: &str) -> Cluster {
        Cluster {
            name: name.to_string(),
            id,
            config: test_cluster_config(size),
            log_indexes: BTreeMap::new(),
            bound_objects: BTreeSet::new(),
            replica_id_by_name_: BTreeMap::new(),
            replicas_by_id_: BTreeMap::new(),
            owner_id: RoleId::User(1),
            privileges: PrivilegeMap::default(),
        }
    }

    fn test_replica(cluster_id: ClusterId, replica_id: ReplicaId, name: &str) -> ClusterReplica {
        ClusterReplica {
            name: name.to_string(),
            cluster_id,
            replica_id,
            config: ReplicaConfig {
                location: ReplicaLocation::Unmanaged(UnmanagedReplicaLocation {
                    storagectl_addrs: Vec::new(),
                    computectl_addrs: Vec::new(),
                }),
                compute: ComputeReplicaConfig {
                    logging: Default::default(),
                },
            },
            owner_id: RoleId::User(1),
        }
    }

    fn full_name(database: Option<&str>, schema: &str, item: &str) -> FullItemName {
        FullItemName {
            database: match database {
                Some(database) => RawDatabaseSpecifier::Name(database.to_string()),
                None => RawDatabaseSpecifier::Ambient,
            },
            schema: schema.to_string(),
            item: item.to_string(),
        }
    }

    #[mz_ore::test]
    fn item_insertion_creates_object_info_series() {
        let registry = MetricsRegistry::new();
        let mut metrics = CatalogInfoMetrics::new(&registry);

        metrics.insert_item(
            CatalogItemId::User(1),
            &test_table(GlobalId::User(1)),
            &full_name(Some("materialize"), "public", "t"),
        );

        assert_eq!(
            series(&registry, "mz_object_info"),
            vec![labels(&[
                ("object_id", "u1"),
                ("global_id", "u1"),
                ("name", "t"),
                ("schema_name", "public"),
                ("database_name", "materialize"),
                ("type", "table"),
            ])]
        );
    }

    #[mz_ore::test]
    fn system_items_are_reported() {
        let registry = MetricsRegistry::new();
        let mut metrics = CatalogInfoMetrics::new(&registry);

        metrics.insert_item(
            CatalogItemId::System(456),
            &test_table(GlobalId::System(456)),
            // System schemas are ambient, i.e. not in a database.
            &full_name(None, "mz_catalog", "mz_test"),
        );

        assert_eq!(
            series(&registry, "mz_object_info"),
            vec![labels(&[
                ("object_id", "s456"),
                ("global_id", "s456"),
                ("name", "mz_test"),
                ("schema_name", "mz_catalog"),
                ("database_name", ""),
                ("type", "table"),
            ])]
        );
    }

    #[mz_ore::test]
    fn introspection_indexes_and_temporary_items_are_not_reported() {
        let registry = MetricsRegistry::new();
        let mut metrics = CatalogInfoMetrics::new(&registry);

        metrics.insert_item(
            CatalogItemId::IntrospectionSourceIndex(5),
            &test_table(GlobalId::IntrospectionSourceIndex(5)),
            &full_name(None, "mz_introspection", "mz_test"),
        );
        metrics.insert_item(
            CatalogItemId::Transient(6),
            &test_table(GlobalId::Transient(6)),
            &full_name(None, "mz_temp", "t"),
        );

        assert_eq!(series(&registry, "mz_object_info"), Vec::new());
    }

    #[mz_ore::test]
    fn user_sources_get_source_info_series() {
        let registry = MetricsRegistry::new();
        let mut metrics = CatalogInfoMetrics::new(&registry);

        let cluster_id = ClusterId::user(9).expect("valid id");
        metrics.insert_source_or_sink(CatalogItemId::User(4), &test_webhook_source(4, cluster_id));

        assert_eq!(
            series(&registry, "mz_source_info"),
            vec![labels(&[
                ("source_id", "u4"),
                ("type", "webhook"),
                // Webhook sources have no envelope.
                ("envelope_type", ""),
                ("cluster_id", "u9"),
            ])]
        );
    }

    #[mz_ore::test]
    fn user_sinks_get_sink_info_series() {
        let registry = MetricsRegistry::new();
        let mut metrics = CatalogInfoMetrics::new(&registry);

        let cluster_id = ClusterId::user(9).expect("valid id");
        metrics.insert_source_or_sink(CatalogItemId::User(5), &test_kafka_sink(5, cluster_id));

        assert_eq!(
            series(&registry, "mz_sink_info"),
            vec![labels(&[
                ("sink_id", "u5"),
                ("type", "kafka"),
                ("envelope_type", "upsert"),
                ("cluster_id", "u9"),
            ])]
        );
    }

    #[mz_ore::test]
    fn cluster_and_replica_insertions_create_info_series() {
        let registry = MetricsRegistry::new();
        let mut metrics = CatalogInfoMetrics::new(&registry);

        let cluster_id = ClusterId::user(7).expect("valid id");
        let replica_id = ReplicaId::User(2);
        metrics.insert_cluster(&test_cluster(cluster_id, "prod", "123cc"));
        metrics.insert_replica(&test_replica(cluster_id, replica_id, "r1"));

        assert_eq!(
            series(&registry, "mz_cluster_info"),
            vec![labels(&[
                ("cluster_id", "u7"),
                ("name", "prod"),
                ("size", "123cc"),
            ])]
        );
        assert_eq!(
            series(&registry, "mz_replica_info"),
            vec![labels(&[
                ("replica_id", "u2"),
                ("cluster_id", "u7"),
                ("name", "r1"),
                // Replicas with unmanaged *locations* (user-specified
                // addresses) have no allocation, and so no size.
                ("size", ""),
            ])]
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault` on OS `linux`
    fn replicas_of_unmanaged_clusters_report_their_size() {
        let registry = MetricsRegistry::new();
        let mut metrics = CatalogInfoMetrics::new(&registry);

        // Replicas of unmanaged clusters (`CREATE CLUSTER c REPLICAS (..)`)
        // have a managed *location* with an allocation and a size of their
        // own, even though the cluster variant carries none.
        let allocation: ReplicaAllocation =
            serde_json::from_str(r#"{"scale": 2, "workers": 4, "credits_per_hour": "0"}"#)
                .expect("valid allocation");
        let replica = ClusterReplica {
            name: "r2".to_string(),
            cluster_id: ClusterId::user(7).expect("valid id"),
            replica_id: ReplicaId::User(3),
            config: ReplicaConfig {
                location: ReplicaLocation::Managed(ManagedReplicaLocation {
                    allocation,
                    size: "scale=2,workers=4".to_string(),
                    internal: false,
                    billed_as: None,
                    availability_zones: ManagedReplicaAvailabilityZones::FromReplica(None),
                    pending: false,
                }),
                compute: ComputeReplicaConfig {
                    logging: Default::default(),
                },
            },
            owner_id: RoleId::User(1),
        };
        metrics.insert_replica(&replica);

        assert_eq!(
            series(&registry, "mz_replica_info"),
            vec![labels(&[
                ("replica_id", "u3"),
                ("cluster_id", "u7"),
                ("name", "r2"),
                ("size", "scale=2,workers=4"),
            ])]
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn reconcile_rebuilds_from_the_catalog_when_its_revision_changes() {
        Catalog::with_debug(|mut catalog| async move {
            let registry = MetricsRegistry::new();
            let mut metrics = CatalogInfoMetrics::new(&registry);

            let has_test_cluster_series = |registry: &MetricsRegistry| {
                series(registry, "mz_cluster_info")
                    .iter()
                    .any(|s| s["name"] == "test_cluster")
            };

            metrics.reconcile(&catalog);

            // The initial reconciliation reports the catalog's contents,
            // including system items, but not introspection source indexes.
            assert!(
                series(&registry, "mz_cluster_info")
                    .iter()
                    .any(|s| s["name"] == "mz_system")
            );
            assert_ne!(series(&registry, "mz_replica_info"), Vec::new());
            let objects = series(&registry, "mz_object_info");
            // System items
            assert!(objects.iter().any(|s| s["object_id"].starts_with("s")));
            // Introspection source indexes
            assert!(!objects.iter().any(|s| s["object_id"].starts_with("si")));

            // Assert that the test cluster series is not present.
            assert!(!has_test_cluster_series(&registry));

            // Create a cluster in the catalog.
            let commit_ts = catalog.current_upper().await;
            let cluster_id = catalog
                .allocate_user_cluster_id(commit_ts)
                .await
                .expect("failed to allocate cluster id");
            let commit_ts = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    commit_ts,
                    None,
                    vec![Op::CreateCluster {
                        id: cluster_id,
                        name: "test_cluster".to_string(),
                        introspection_sources: Vec::new(),
                        owner_id: MZ_SYSTEM_ROLE_ID,
                        config: test_cluster_config("scale=1,workers=2"),
                    }],
                )
                .await
                .expect("failed to transact");

            // The metrics are stale until the next reconciliation.
            assert!(!has_test_cluster_series(&registry));

            metrics.reconcile(&catalog);
            assert!(has_test_cluster_series(&registry));

            catalog.expire().await;
        })
        .await
    }
}
