---
source: src/adapter/src/coord/info_metrics.rs
revision: 82dac06732
---

# adapter::coord::info_metrics

Prometheus `*_info` metrics for catalog objects, following the standard "info" pattern (constant value `1`, descriptive labels) that provides a stable `group_left` join target for resolving object IDs to human-readable names.

`CatalogInfoMetrics` owns five `UIntGaugeVec` families — `mz_object_info` (labeled by `object_id`, `global_id`, `name`, `schema_name`, `database_name`, `type`), `mz_cluster_info` (by `cluster_id`, `name`, `size`), `mz_replica_info` (by `replica_id`, `cluster_id`, `name`, `size`), `mz_source_info` (by `source_id`, `type`, `envelope_type`, `cluster_id`), and `mz_sink_info` (by `sink_id`, `type`, `envelope_type`, `cluster_id`) — plus a `Vec<InfoGauge>` of live `DeleteOnDropGauge` handles and a `reconcile_seconds` histogram.

`CatalogInfoMetrics::reconcile` rebuilds all series from the catalog whenever `Catalog::transient_revision` changes; a full rebuild (via `populate`) drops and re-creates all handles wholesale to avoid stale-handle collisions. Reconciliations exceeding 5 seconds are logged at warn level.

Introspection source indexes (`CatalogItemId::IntrospectionSourceIndex`) and temporary (session-scoped) items (`CatalogItemId::Transient`) are excluded from `mz_object_info`. Progress sources, subsources, and introspection sources are excluded from `mz_source_info`.

`Coordinator::spawn_catalog_info_metrics_task` drives reconciliation from a background tokio task. The reconcile cadence is controlled by the `CATALOG_INFO_METRICS_RECONCILE_INTERVAL` dyncfg; when the interval is zero (disabled) the task keeps polling at a 30-second fallback interval so the setting can be re-enabled without a restart.
