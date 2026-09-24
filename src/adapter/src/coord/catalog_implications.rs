// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to deriving and applying implications from [catalog
//! changes](ParsedStateUpdate).
//!
//! The flow from "raw" catalog changes to [CatalogImplication] works like this:
//!
//! StateUpdateKind -> ParsedStateUpdate -> CatalogImplication
//!
//! [ParsedStateUpdate] adds context to a "raw" catalog change
//! ([StateUpdateKind](mz_catalog::memory::objects::StateUpdateKind)). It
//! includes an in-memory representation of the updated object, which can in
//! theory be derived from the raw change but only when we have access to all
//! the other raw changes or to an in-memory Catalog, which represents a
//! "rollup" of all the raw changes.
//!
//! [CatalogImplication] is both the state machine that we use for absorbing
//! multiple state updates for the same object and the final command that has to
//! be applied to in-memory state and or the controller(s) after absorbing all
//! the state updates in a given batch of updates.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use differential_dataflow::lattice::Lattice;
use fail::fail_point;
use itertools::Itertools;
use mz_adapter_types::compaction::{CompactionWindow, SINCE_GRANULARITY};
use mz_catalog::expr_cache::{GlobalExpressions, latest_item_version};
use mz_catalog::memory::implications::{
    CatalogImplication, CatalogImplicationKind, CatalogImplications, ParsedStateUpdate,
    ParsedStateUpdateKind,
};
use mz_catalog::memory::objects::{
    CatalogItem, Connection, DataSourceDesc, Index, MaterializedView, MetricSink, Source, Table,
    TableDataSource,
};
use mz_cloud_resources::VpcEndpointConfig;
use mz_compute_client::logging::LogVariant;
use mz_compute_client::protocol::response::{PeekError, PeekResponse};
use mz_controller::clusters::{ClusterRole, ReplicaConfig};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::instrument;
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_repr::optimize::OverrideFrom;
use mz_repr::{CatalogItemId, Diff, GlobalId, RelationVersion, RelationVersionSelector, Timestamp};
use mz_sql::plan::ConnectionDetails;
use mz_sql::session::vars::{DISABLED_METRIC_SINKS, Var};
use mz_storage_client::controller::{CollectionDescription, DataSource};
use mz_storage_types::connections::PostgresConnection;
use mz_storage_types::connections::inline::{InlinedConnection, IntoInlineConnection};
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sinks::StorageSinkConnection;
use mz_storage_types::sources::{
    GenericSourceConnection, SourceDesc, SourceExport, SourceExportDataConfig,
};
use timely::PartialOrder;
use timely::progress::Antichain;
use tracing::{Instrument, info_span, warn};

use crate::active_compute_sink::ActiveComputeSinkRetireReason;
use crate::coord::peek::DroppedDependency;
use crate::coord::timestamp_selection::TimestampProvider;
use crate::coord::{BuiltinTableAppendNotify, Coordinator};
use crate::optimize::OptimizerConfig;
use crate::optimize::dataflows::{ComputeInstanceSnapshot, dataflow_import_id_bundle};
use crate::statement_logging::{StatementEndedExecutionReason, StatementLoggingId};
use crate::{AdapterError, CollectionIdBundle, ExecuteContext, ResultExt, flags};

impl Coordinator {
    /// Applies implications from the given bucket of [ParsedStateUpdate] to our
    /// in-memory state and our controllers. This also applies transitive
    /// implications, for example, peeks and subscribes will be cancelled when
    /// referenced objects are dropped.
    ///
    /// This _requires_ that the given updates are consolidated. There must be
    /// at most one addition and/or one retraction for a given item, as
    /// identified by that items ID type.
    #[instrument(level = "debug")]
    pub async fn apply_catalog_implications(
        &mut self,
        ctx: Option<&mut ExecuteContext>,
        catalog_updates: Vec<ParsedStateUpdate>,
    ) -> Result<(), AdapterError> {
        let start = Instant::now();

        let reconcile_metric_sinks = catalog_updates.iter().any(|update| {
            matches!(&update.kind, ParsedStateUpdateKind::SystemConfiguration { durable }
                if durable.name.eq_ignore_ascii_case(DISABLED_METRIC_SINKS.name()))
        });
        let build =
            crate::catalog::Catalog::expression_build_version(self.catalog().config().build_info)
                .to_string();
        let CatalogImplications {
            items: catalog_implications,
            clusters: cluster_commands,
            replicas: cluster_replica_commands,
            introspection_source_indexes,
            replica_scoped_config_changed,
            system_config_changed,
            compaction_bounds,
            mut retired_storage_metadata,
            written_plans,
        } = CatalogImplications::from_updates(catalog_updates, &build);
        let should_reconcile_now =
            !cluster_commands.is_empty() || !cluster_replica_commands.is_empty();

        if written_plans
            .iter()
            .any(|id| self.pending_compute_installations.contains(id))
            || !cluster_commands.is_empty()
            || !cluster_replica_commands.is_empty()
        {
            self.pending_compute_installation_retry = None;
        }
        let notice_updates = self.refresh_written_plan_notices(written_plans).await?;
        self.apply_catalog_implications_inner(
            ctx,
            catalog_implications.into_iter().collect_vec(),
            cluster_commands.into_iter().collect_vec(),
            cluster_replica_commands.into_iter().collect_vec(),
            introspection_source_indexes,
            replica_scoped_config_changed,
            system_config_changed,
            compaction_bounds,
        )
        .await?;
        if reconcile_metric_sinks {
            self.reconcile_metric_sinks().await;
        }
        if let Some(notice_updates) = notice_updates {
            notice_updates.await;
        }

        // A client can release the final reference after the SQL object's drop.
        // Retire storage from that committed metadata removal as well. Apply this
        // after ordinary implications, preserving their execution cleanup order.
        let storage_metadata = self.catalog().state().storage_metadata();
        retired_storage_metadata
            .retain(|id| !storage_metadata.collection_metadata.contains_key(id));
        if !retired_storage_metadata.is_empty() {
            self.controller
                .storage_collections
                .drop_collections_unvalidated(
                    storage_metadata,
                    retired_storage_metadata.into_iter().collect(),
                );
        }
        if should_reconcile_now || system_config_changed {
            if let Some(client) = &self.query_client {
                client.connections.sync_catalog(self.catalog());
            }
        }

        if should_reconcile_now {
            // Wake the controller to reconcile immediately rather than waiting
            // out its tick interval. A missed or spurious wake is harmless: the
            // periodic tick is the backstop, and an extra wake costs one no-op
            // reconcile.
            self.reconcile_now.notify_one();
        }

        // Query protection follows the completed installation batch. An
        // unavailable replica leaves its window pending rather than holding up
        // installation or substituting controller tokens for client grants.
        if let Err(error) = Box::pin(self.acquire_pending_query_timeline_holds()).await {
            tracing::warn!(%error, "unable to establish query timeline windows");
        }

        self.metrics
            .apply_catalog_implications_seconds
            .observe(start.elapsed().as_secs_f64());

        Ok(())
    }

    /// The writer publishes notices from committed selections, independently of
    /// installation. Catalog drop application already retracts affected notices.
    async fn refresh_written_plan_notices(
        &mut self,
        mut ids: BTreeSet<GlobalId>,
    ) -> Result<Option<BuiltinTableAppendNotify>, AdapterError> {
        if !self.catalog().state().catalog_read_protection_enabled() || ids.is_empty() {
            return Ok(None);
        }
        ids.retain(|id| {
            self.catalog()
                .try_get_entry_by_global_id(id)
                .is_some_and(|entry| match entry.item() {
                    CatalogItem::Index(index) => index.global_id() == *id,
                    CatalogItem::MaterializedView(mv) => mv.global_id_writes() == *id,
                    CatalogItem::MetricSink(sink) => sink.global_id == *id,
                    _ => false,
                })
        });
        if ids.is_empty() {
            return Ok(None);
        }
        let build =
            crate::catalog::Catalog::expression_build_version(self.catalog().config().build_info)
                .to_string();
        let revisions: Vec<_> = ids
            .iter()
            .filter_map(|id| {
                self.catalog()
                    .state()
                    .written_plan(*id, &build)
                    .map(|revision| (*id, revision))
            })
            .collect();
        let mut selected = self.catalog().read_written_plans(revisions.clone()).await?;
        if selected.len() != revisions.len() {
            return Err(AdapterError::internal(
                "publish written plan notices",
                "selected plan is missing",
            ));
        }
        let mut updates = Vec::new();
        for id in ids {
            let previous: BTreeSet<_> = self
                .catalog()
                .try_get_dataflow_metainfo(&id)
                .into_iter()
                .flat_map(|meta| meta.optimizer_notices.iter().cloned())
                .collect();
            let metainfo = selected
                .remove(&id)
                .map(|plan| plan.dataflow_metainfos)
                .unwrap_or_default();
            let current: BTreeSet<_> = metainfo.optimizer_notices.iter().cloned().collect();
            if self.catalog().system_config().enable_mz_notices() {
                self.catalog().state().pack_optimizer_notices(
                    &mut updates,
                    previous.difference(&current),
                    Diff::MINUS_ONE,
                );
                self.catalog().state().pack_optimizer_notices(
                    &mut updates,
                    current.difference(&previous),
                    Diff::ONE,
                );
            }
            self.catalog_mut().set_dataflow_metainfo(id, metainfo);
        }
        Ok((!updates.is_empty()).then(|| self.builtin_table_update().execute(updates)))
    }

    /// Refreshes runtime consumers from the committed global configuration.
    /// Called once per changed batch, including retractions that restore defaults.
    fn apply_current_system_configuration(&mut self) {
        mz_metrics::update_dyncfg(&self.catalog().system_config().dyncfg_updates());
        self.update_controller_config();
        self.update_compute_config();
        self.update_storage_config();
        self.update_timestamp_oracle_config();
        self.update_metrics_retention();
        self.update_tracing_config();
        self.update_secrets_caching_config();
        self.update_cluster_scheduling_config();
        self.update_http_config();

        // Preserve the pending tick when an unrelated configuration changes.
        let interval = self.catalog().system_config().default_timestamp_interval();
        if interval != self.advance_timelines_interval.period() {
            self.advance_timelines_interval = tokio::time::interval(interval);
        }
        let threshold = self
            .catalog()
            .system_config()
            .optimizer_e2e_latency_warning_threshold();
        self.optimizer_metrics
            .set_e2e_optimization_time_log_threshold(threshold);
        self.catalog().system_config().notify_all_callbacks();
    }

    fn update_cluster_scheduling_config(&self) {
        let config = flags::orchestrator_scheduling_config(self.catalog.system_config());
        self.controller
            .update_orchestrator_scheduling_config(config);
    }

    fn update_secrets_caching_config(&self) {
        let config = flags::caching_config(self.catalog.system_config());
        self.caching_secrets_reader.set_policy(config);
    }

    fn update_tracing_config(&self) {
        let tracing = flags::tracing_config(self.catalog().system_config());
        tracing.apply(&self.tracing_handle);
    }

    fn update_compute_config(&mut self) {
        let config_params = flags::compute_config(self.catalog().system_config());
        self.controller.compute.update_configuration(config_params);
    }

    fn update_storage_config(&mut self) {
        let config_params = flags::storage_config(self.catalog().system_config());
        self.adapter_storage.update_parameters(&config_params);
        self.storage_configuration.update(config_params.clone());
        self.controller.storage.update_parameters(config_params);
    }

    fn update_timestamp_oracle_config(&self) {
        let config_params = flags::timestamp_oracle_config(self.catalog().system_config());
        if let Some(config) = self.timestamp_oracle_config.as_ref() {
            config.apply_parameters(config_params)
        }
    }

    fn update_metrics_retention(&self) {
        let duration = self.catalog().system_config().metrics_retention();
        let policy = ReadPolicy::lag_writes_by(
            Timestamp::new(u64::try_from(duration.as_millis()).unwrap_or_else(|_e| {
                tracing::error!("Absurd metrics retention duration: {duration:?}.");
                u64::MAX
            })),
            SINCE_GRANULARITY,
        );
        let storage_policies = self
            .catalog()
            .entries()
            .filter(|entry| {
                entry.item().is_retained_metrics_object()
                    && entry.item().is_compute_object_on_cluster().is_none()
            })
            .map(|entry| (entry.id(), policy.clone()))
            .collect::<Vec<_>>();
        let compute_policies = self
            .catalog()
            .entries()
            .filter_map(|entry| {
                if let (true, Some(cluster_id)) = (
                    entry.item().is_retained_metrics_object(),
                    entry.item().is_compute_object_on_cluster(),
                ) {
                    Some((cluster_id, entry.id(), policy.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        self.update_storage_read_policies(storage_policies);
        self.update_compute_read_policies(compute_policies);
    }

    fn update_controller_config(&mut self) {
        let sys_config = self.catalog().system_config();
        self.controller
            .update_configuration(sys_config.dyncfg_updates());
    }

    fn update_http_config(&mut self) {
        let webhook_request_limit = self
            .catalog()
            .system_config()
            .webhook_concurrent_request_limit();
        self.webhook_concurrency_limit
            .set_limit(webhook_request_limit);
    }

    #[instrument(level = "debug")]
    async fn apply_catalog_implications_inner(
        &mut self,
        ctx: Option<&mut ExecuteContext>,
        implications: Vec<(CatalogItemId, CatalogImplication)>,
        cluster_commands: Vec<(ClusterId, CatalogImplication)>,
        cluster_replica_commands: Vec<((ClusterId, ReplicaId), CatalogImplication)>,
        mut introspection_source_indexes: BTreeMap<ClusterId, BTreeMap<LogVariant, GlobalId>>,
        replica_scoped_config_changed: bool,
        system_config_changed: bool,
        compaction_bounds: BTreeMap<GlobalId, Antichain<Timestamp>>,
    ) -> Result<(), AdapterError> {
        // Install configuration before other implications so newly created
        // objects use the committed settings, regardless of which node wrote them.
        if system_config_changed {
            self.apply_current_system_configuration();
        }

        // Logging indexes are installed with their cluster, so stage compute permission
        // before applying any creates. Storage receives its bounds after registration.
        let storage_metadata = self.catalog().state().storage_metadata();
        let (storage_bounds, compute_bounds): (BTreeMap<_, _>, BTreeMap<_, _>) = compaction_bounds
            .into_iter()
            .partition(|(id, _)| storage_metadata.collection_metadata.contains_key(id));
        for (id, bound) in compute_bounds {
            if self.controller.replica_owned_compute() {
                continue;
            }
            self.controller
                .compute
                .apply_compaction_bound(id, bound)
                .map_err(|error| AdapterError::Unstructured(error.into()))?;
        }

        let mut tables_to_drop = BTreeSet::new();
        let mut txn_tables_to_drop = BTreeSet::new();
        let mut sources_to_drop = vec![];
        let mut replication_slots_to_drop: Vec<(PostgresConnection, String)> = vec![];
        let mut storage_sink_gids_to_drop = vec![];
        let mut indexes_to_drop = vec![];
        let mut compute_sinks_to_drop = vec![];
        let mut view_gids_to_drop = vec![];
        let mut secrets_to_drop = vec![];
        let mut vpc_endpoints_to_drop = vec![];
        let mut clusters_to_drop = vec![];
        let mut cluster_replicas_to_drop = vec![];
        let mut cluster_replicas_to_create = vec![];
        let mut active_compute_sinks_to_drop = BTreeMap::new();
        let mut peeks_to_drop = vec![];
        let mut copies_to_drop = vec![];

        // Maps for storing names of dropped objects for error messages.
        let mut dropped_item_names: BTreeMap<GlobalId, String> = BTreeMap::new();
        let mut dropped_cluster_names: BTreeMap<ClusterId, String> = BTreeMap::new();

        // Separate collections for tables (which need write timestamps) and
        // sources (which don't).
        let mut table_collections_to_create = BTreeMap::new();
        let mut source_collections_to_create = BTreeMap::new();
        let mut sinks_to_create = Vec::new();
        let mut sinks_to_alter = Vec::new();
        let mut compute_items_to_create = BTreeSet::new();
        let mut storage_policies_to_initialize = BTreeMap::new();
        let mut execution_timestamps_to_set = BTreeSet::new();
        let mut vpc_endpoints_to_create: Vec<(CatalogItemId, VpcEndpointConfig)> = vec![];

        // Sources that shouldn't be dropped, even if we saw a `Dropped` event.
        // Used for correct handling of ALTER MV.
        let mut source_gids_to_keep = BTreeSet::new();

        // Collections for batching connection-related alterations.
        let mut source_connections_to_alter: BTreeMap<
            GlobalId,
            GenericSourceConnection<InlinedConnection>,
        > = BTreeMap::new();
        let mut sink_connections_to_alter: BTreeMap<GlobalId, StorageSinkConnection> =
            BTreeMap::new();
        let mut source_export_data_configs_to_alter: BTreeMap<GlobalId, SourceExportDataConfig> =
            BTreeMap::new();
        let mut source_descs_to_alter: BTreeMap<GlobalId, SourceDesc> = BTreeMap::new();

        // We're incrementally migrating the code that manipulates the
        // controller from closures in the sequencer. For some types of catalog
        // changes we haven't done this migration yet, so there you will see
        // just a log message. Over the next couple of PRs all of these will go
        // away.

        for (catalog_id, implication) in implications {
            tracing::trace!(?implication, "have to apply catalog implication");

            match implication {
                CatalogImplication::Table(CatalogImplicationKind::Added(table)) => {
                    self.handle_create_table(
                        &ctx,
                        &mut table_collections_to_create,
                        &mut storage_policies_to_initialize,
                        &mut execution_timestamps_to_set,
                        catalog_id,
                        table.clone(),
                    )
                    .await?
                }
                CatalogImplication::Table(CatalogImplicationKind::Altered {
                    prev: prev_table,
                    new: new_table,
                }) => {
                    self.handle_alter_table(catalog_id, prev_table, new_table)
                        .await?
                }

                CatalogImplication::Table(CatalogImplicationKind::Dropped(table, full_name)) => {
                    let txn_managed =
                        matches!(table.data_source, TableDataSource::TableWrites { .. });
                    let global_ids = table.global_ids();
                    for global_id in global_ids {
                        tables_to_drop.insert((catalog_id, global_id));
                        if txn_managed {
                            txn_tables_to_drop.insert(global_id);
                        }
                        dropped_item_names.insert(global_id, full_name.clone());
                    }
                }
                CatalogImplication::Source(CatalogImplicationKind::Added((
                    source,
                    _connection,
                ))) => {
                    // Get the compaction windows for all sources with this
                    // catalog_id This replicates the logic from
                    // sequence_create_source where it collects all item_ids and
                    // gets their compaction windows
                    let compaction_windows = self
                        .catalog()
                        .state()
                        .source_compaction_windows(vec![catalog_id]);

                    self.handle_create_source(
                        &mut source_collections_to_create,
                        &mut storage_policies_to_initialize,
                        catalog_id,
                        source,
                        compaction_windows,
                    )
                    .await?
                }
                CatalogImplication::Source(CatalogImplicationKind::Altered {
                    prev: (prev_source, _prev_connection),
                    new: (new_source, new_connection),
                }) => {
                    if prev_source.custom_logical_compaction_window
                        != new_source.custom_logical_compaction_window
                    {
                        let new_window = new_source
                            .custom_logical_compaction_window
                            .unwrap_or(CompactionWindow::Default);
                        self.update_storage_read_policies(vec![(catalog_id, new_window.into())]);
                    }
                    match (&prev_source.data_source, &new_source.data_source) {
                        (
                            DataSourceDesc::Ingestion {
                                desc: prev_desc, ..
                            }
                            | DataSourceDesc::OldSyntaxIngestion {
                                desc: prev_desc, ..
                            },
                            DataSourceDesc::Ingestion { desc: new_desc, .. }
                            | DataSourceDesc::OldSyntaxIngestion { desc: new_desc, .. },
                        ) => {
                            if prev_desc != new_desc {
                                let inlined_connection = new_connection
                                    .expect("ingestion source should have inlined connection");
                                let inlined_desc = SourceDesc {
                                    connection: inlined_connection,
                                    timestamp_interval: new_desc.timestamp_interval,
                                };
                                source_descs_to_alter.insert(new_source.global_id, inlined_desc);
                            }
                        }
                        _ => {}
                    }
                }
                CatalogImplication::Source(CatalogImplicationKind::Dropped(
                    (source, connection),
                    full_name,
                )) => {
                    let global_id = source.global_id();
                    sources_to_drop.push((catalog_id, global_id));
                    dropped_item_names.insert(global_id, full_name);

                    if let DataSourceDesc::Ingestion { desc, .. }
                    | DataSourceDesc::OldSyntaxIngestion { desc, .. } = &source.data_source
                    {
                        match &desc.connection {
                            GenericSourceConnection::Postgres(_referenced_conn) => {
                                let inline_conn = connection.expect("missing inlined connection");

                                let pg_conn = match inline_conn {
                                    GenericSourceConnection::Postgres(pg_conn) => pg_conn,
                                    other => {
                                        panic!("expected postgres connection, got: {:?}", other)
                                    }
                                };
                                let pending_drop = (
                                    pg_conn.connection.clone(),
                                    pg_conn.publication_details.slot.clone(),
                                );
                                replication_slots_to_drop.push(pending_drop);
                            }
                            _ => {}
                        }
                    }
                }
                CatalogImplication::Sink(CatalogImplicationKind::Added(sink)) => {
                    storage_policies_to_initialize
                        .entry(CompactionWindow::Default)
                        .or_default()
                        .insert(sink.global_id());
                    sinks_to_create.push(sink);
                }
                CatalogImplication::Sink(CatalogImplicationKind::Altered {
                    prev: prev_sink,
                    new: new_sink,
                }) => {
                    // Renames and privilege changes do not change the export.
                    // ALTER SINK commits a new version of its definition.
                    if prev_sink.version != new_sink.version {
                        sinks_to_alter.push(new_sink);
                    }
                }
                CatalogImplication::Sink(CatalogImplicationKind::Dropped(sink, full_name)) => {
                    storage_sink_gids_to_drop.push(sink.global_id());
                    dropped_item_names.insert(sink.global_id(), full_name);
                }
                CatalogImplication::Index(CatalogImplicationKind::Added(_index)) => {
                    compute_items_to_create.insert(catalog_id);
                }
                CatalogImplication::Index(CatalogImplicationKind::Altered {
                    prev: prev_index,
                    new: new_index,
                }) => {
                    if prev_index.custom_logical_compaction_window
                        != new_index.custom_logical_compaction_window
                    {
                        let new_window = new_index
                            .custom_logical_compaction_window
                            .unwrap_or(CompactionWindow::Default);
                        if !self
                            .pending_compute_installations
                            .contains(&new_index.global_id())
                        {
                            self.update_compute_read_policy(
                                new_index.cluster_id,
                                catalog_id,
                                new_window.into(),
                            );
                        }
                    }
                }
                CatalogImplication::Index(CatalogImplicationKind::Dropped(index, full_name)) => {
                    indexes_to_drop.push((index.cluster_id, index.global_id()));
                    dropped_item_names.insert(index.global_id(), full_name);
                }
                CatalogImplication::MetricSink(CatalogImplicationKind::Added(_metric_sink)) => {
                    compute_items_to_create.insert(catalog_id);
                }
                CatalogImplication::MetricSink(CatalogImplicationKind::Altered { .. }) => {
                    // Nothing to do: owner, privilege, and rename changes are catalog-only.
                }
                CatalogImplication::MetricSink(CatalogImplicationKind::Dropped(
                    metric_sink,
                    full_name,
                )) => {
                    // A metric sink is a non-readable leaf compute dataflow, like an MV's write
                    // side, so it drops through the same path as other compute sinks.
                    compute_sinks_to_drop.push((metric_sink.cluster_id, metric_sink.global_id));
                    dropped_item_names.insert(metric_sink.global_id, full_name);
                }
                CatalogImplication::MaterializedView(CatalogImplicationKind::Added(mv)) => {
                    source_collections_to_create
                        .extend(self.materialized_view_storage_collections(&mv));
                    storage_policies_to_initialize
                        .entry(
                            mv.custom_logical_compaction_window
                                .unwrap_or(CompactionWindow::Default),
                        )
                        .or_default()
                        .extend(mv.global_ids());
                    compute_items_to_create.insert(catalog_id);
                }
                CatalogImplication::MaterializedView(CatalogImplicationKind::Altered {
                    prev: prev_mv,
                    new: new_mv,
                }) => {
                    // We get here for three reasons:
                    //  1. Name changes, like those caused by ALTER SCHEMA.
                    //  2. Replacement application.
                    //  3. Compaction window changes (ALTER ... SET (RETAIN HISTORY ...)).
                    //
                    // 1. Name changes: We don't have to do anything here.
                    //
                    // 2. Replacement application: This is tricky: It changes the `CatalogItemId` of
                    // the target to that of the replacement and simultaneously drops the replacement.
                    // Which means when we get here `prev_mv` is the replacement that should be
                    // dropped, and `new_mv` is the target that already exists but under a different
                    // ID (which will receive a `Dropped` event separately). We can sniff out this
                    // case by checking for version differences.
                    //
                    // 3. Compaction window changes: We handle this in an `else if`, because if there
                    // is also a replacement application, then the replacement's storage collections
                    // already have the correct read policies from when they were created, so we
                    // don't need to update them here.
                    if prev_mv.collections != new_mv.collections {
                        // Sanity check: The replacement's last (and only) version must be the same
                        // as the new target's last version.
                        assert_eq!(
                            prev_mv.global_id_writes(),
                            new_mv.global_id_writes(),
                            "unexpected MV Altered implication: prev={prev_mv:?}, new={new_mv:?}",
                        );

                        let gid = new_mv.global_id_writes();
                        if !self.pending_compute_installations.contains(&gid) {
                            self.allow_writes(new_mv.cluster_id, gid);
                        }

                        // There will be a separate `Dropped` implication for the old definition of
                        // the target MV. That will drop the old compute collection, as we desire,
                        // but we need to prevent it from dropping the old storage collection as
                        // well, since that might still be depended on.
                        source_gids_to_keep.extend(new_mv.global_ids());
                    } else if prev_mv.custom_logical_compaction_window
                        != new_mv.custom_logical_compaction_window
                    {
                        let new_window = new_mv
                            .custom_logical_compaction_window
                            .unwrap_or(CompactionWindow::Default);
                        self.update_storage_read_policies(vec![(catalog_id, new_window.into())]);
                    }
                }
                CatalogImplication::MaterializedView(CatalogImplicationKind::Dropped(
                    mv,
                    full_name,
                )) => {
                    compute_sinks_to_drop.push((mv.cluster_id, mv.global_id_writes()));
                    for gid in mv.global_ids() {
                        sources_to_drop.push((catalog_id, gid));
                        dropped_item_names.insert(gid, full_name.clone());
                    }
                }
                CatalogImplication::View(CatalogImplicationKind::Added(_view)) => {
                    // No action needed: views are catalog-only objects with no
                    // storage collections or dataflows to create.
                }
                CatalogImplication::View(CatalogImplicationKind::Altered {
                    prev: _prev_view,
                    new: _new_view,
                }) => {
                    // No action needed: view alterations (e.g. renames) are
                    // catalog-only and require no controller changes.
                }
                CatalogImplication::View(CatalogImplicationKind::Dropped(view, full_name)) => {
                    view_gids_to_drop.push(view.global_id());
                    dropped_item_names.insert(view.global_id(), full_name);
                }
                CatalogImplication::Secret(CatalogImplicationKind::Added(_secret)) => {
                    // No action needed: the secret payload is stored in
                    // secrets_controller.ensure() BEFORE the catalog transaction.
                    // By the time we see this update, the secret is already stored.
                }
                CatalogImplication::Secret(CatalogImplicationKind::Altered {
                    prev: _prev_secret,
                    new: _new_secret,
                }) => {
                    // No action needed: altering a secret updates the payload via
                    // secrets_controller.ensure() without a catalog transaction.
                }
                CatalogImplication::Secret(CatalogImplicationKind::Dropped(
                    _secret,
                    _full_name,
                )) => {
                    secrets_to_drop.push(catalog_id);
                }
                CatalogImplication::Connection(CatalogImplicationKind::Added(connection)) => {
                    match &connection.details {
                        // SSH connections: key pair is stored in secrets_controller
                        // BEFORE the catalog transaction, so no action needed here.
                        ConnectionDetails::Ssh { .. } => {}
                        // AWS PrivateLink connections: create the VPC endpoint
                        ConnectionDetails::AwsPrivatelink(privatelink) => {
                            let spec = VpcEndpointConfig {
                                aws_service_name: privatelink.service_name.to_owned(),
                                availability_zone_ids: privatelink.availability_zones.to_owned(),
                            };
                            vpc_endpoints_to_create.push((catalog_id, spec));
                        }
                        // Other connection types don't require post-transaction actions
                        _ => {}
                    }
                }
                CatalogImplication::Connection(CatalogImplicationKind::Altered {
                    prev: _prev_connection,
                    new: new_connection,
                }) => {
                    self.handle_alter_connection(
                        catalog_id,
                        new_connection,
                        &mut vpc_endpoints_to_create,
                        &mut source_connections_to_alter,
                        &mut sink_connections_to_alter,
                        &mut source_export_data_configs_to_alter,
                    );
                }
                CatalogImplication::Connection(CatalogImplicationKind::Dropped(
                    connection,
                    _full_name,
                )) => {
                    match &connection.details {
                        // SSH connections have an associated secret that should be dropped
                        ConnectionDetails::Ssh { .. } => {
                            secrets_to_drop.push(catalog_id);
                        }
                        // AWS PrivateLink connections have an associated
                        // VpcEndpoint K8S resource that should be dropped
                        ConnectionDetails::AwsPrivatelink(_) => {
                            vpc_endpoints_to_drop.push(catalog_id);
                        }
                        _ => (),
                    }
                }
                CatalogImplication::None => {
                    // Nothing to do for None commands
                }
                CatalogImplication::Cluster(_) | CatalogImplication::ClusterReplica(_) => {
                    unreachable!("clusters and cluster replicas are handled below")
                }
                CatalogImplication::Table(CatalogImplicationKind::None)
                | CatalogImplication::Source(CatalogImplicationKind::None)
                | CatalogImplication::Sink(CatalogImplicationKind::None)
                | CatalogImplication::Index(CatalogImplicationKind::None)
                | CatalogImplication::MetricSink(CatalogImplicationKind::None)
                | CatalogImplication::MaterializedView(CatalogImplicationKind::None)
                | CatalogImplication::View(CatalogImplicationKind::None)
                | CatalogImplication::Secret(CatalogImplicationKind::None)
                | CatalogImplication::Connection(CatalogImplicationKind::None) => {
                    unreachable!("will never leave None in place");
                }
            }
        }

        for (cluster_id, command) in cluster_commands {
            tracing::trace!(?command, "have cluster command to apply!");

            match command {
                CatalogImplication::Cluster(CatalogImplicationKind::Added(cluster)) => {
                    // The Cluster's log_indexes is empty at parse time
                    // because IntrospectionSourceIndex updates are applied
                    // after the Cluster update. Use the separately collected
                    // introspection_source_indexes instead.
                    let arranged_logs = introspection_source_indexes
                        .remove(&cluster_id)
                        .unwrap_or_default();
                    let introspection_source_ids: Vec<_> =
                        arranged_logs.values().copied().collect();

                    self.controller
                        .create_cluster(
                            cluster_id,
                            mz_controller::clusters::ClusterConfig {
                                arranged_logs,
                                workload_class: cluster.config.workload_class.clone(),
                            },
                        )
                        .expect("creating cluster must not fail");

                    if !introspection_source_ids.is_empty() {
                        self.initialize_compute_read_policies(
                            introspection_source_ids,
                            cluster_id,
                            CompactionWindow::Default,
                        )
                        .await;
                    }
                }
                CatalogImplication::Cluster(CatalogImplicationKind::Altered {
                    prev: prev_cluster,
                    new: new_cluster,
                }) => {
                    // Replica adds/drops/renames from config changes arrive as
                    // separate AddClusterReplica/DroppedClusterReplica/
                    // AlterClusterReplica events, so the only cluster-level
                    // side effect here is updating the workload class on the
                    // controller when it changes.
                    if prev_cluster.config.workload_class != new_cluster.config.workload_class {
                        self.controller.update_cluster_workload_class(
                            cluster_id,
                            new_cluster.config.workload_class.clone(),
                        );
                    }
                }
                CatalogImplication::Cluster(CatalogImplicationKind::Dropped(
                    cluster,
                    _full_name,
                )) => {
                    clusters_to_drop.push(cluster_id);
                    dropped_cluster_names.insert(cluster_id, cluster.name);
                }
                CatalogImplication::Cluster(CatalogImplicationKind::None) => {
                    unreachable!("will never leave None in place");
                }
                command => {
                    unreachable!(
                        "we only handle cluster commands in this map, got: {:?}",
                        command
                    );
                }
            }
        }

        // Apply replica-scoped overrides after clusters are created (so their
        // compute instances exist) but before replicas are created below. The
        // override layer must be set before `create_replica`, so the new
        // replica's first configuration replays with its override, and so the
        // configuration the controller freezes into the replica's process at
        // provisioning time resolves against it. The push reads the catalog
        // working copy, which already reflects this transaction's scoped-config
        // changes.
        if replica_scoped_config_changed {
            self.push_replica_dyncfg_overrides();
        }

        for ((cluster_id, replica_id), command) in cluster_replica_commands {
            tracing::trace!(?command, "have cluster replica command to apply!");

            match command {
                CatalogImplication::ClusterReplica(CatalogImplicationKind::Added(replica)) => {
                    // Read the cluster name and role from the current catalog
                    // state. This is correct as long as implications are
                    // processed right after each catalog transaction. For a
                    // more future-proof approach that tracks cluster info
                    // locally across transactions, see the last commit of
                    // https://github.com/ggevay/materialize/tree/implications-cluster-name-tracking
                    // which removes that logic.
                    let cluster = self.catalog().get_cluster(cluster_id);
                    let cluster_name = cluster.name.clone();
                    let cluster_role = cluster.role();
                    cluster_replicas_to_create.push((
                        cluster_id,
                        replica_id,
                        cluster_role,
                        cluster_name,
                        replica.name.clone(),
                        replica.config.clone(),
                    ));
                }
                CatalogImplication::ClusterReplica(CatalogImplicationKind::Altered {
                    prev: _prev_replica,
                    new: _new_replica,
                }) => {
                    // No action needed: cluster replica alterations (e.g.
                    // renames, owner changes, pending flag changes) are
                    // catalog-only and require no controller changes.
                }
                CatalogImplication::ClusterReplica(CatalogImplicationKind::Dropped(
                    _replica,
                    _full_name,
                )) => {
                    cluster_replicas_to_drop.push((cluster_id, replica_id));
                }
                CatalogImplication::ClusterReplica(CatalogImplicationKind::None) => {
                    unreachable!("will never leave None in place");
                }
                command => {
                    unreachable!(
                        "we only handle cluster replica commands in this map, got: {:?}",
                        command
                    );
                }
            }
        }

        let clusters_with_replica_creates = cluster_replicas_to_create
            .iter()
            .map(|(cluster_id, ..)| *cluster_id)
            .collect();
        let (replacement_drops, deferred_drops) = partition_cluster_replica_drops(
            &clusters_with_replica_creates,
            cluster_replicas_to_drop,
        );
        cluster_replicas_to_drop = deferred_drops;

        // A same-cluster mixed drop/create batch replaces the replica set, as a
        // forced cut-over does. Catalog resource accounting charges its net, so
        // controller side effects must preserve the same contract. Queue the
        // old replicas' drops before creates. If orchestration needs time to
        // release a physical quota, a later ensure can retry without blocking
        // the drop behind it.
        if !replacement_drops.is_empty() {
            fail::fail_point!("after_catalog_drop_replica");
            for (cluster_id, replica_id) in replacement_drops {
                self.drop_replica(cluster_id, replica_id);
            }
        }
        for (cluster_id, replica_id, role, cluster_name, replica_name, config) in
            cluster_replicas_to_create
        {
            self.handle_create_cluster_replica(
                cluster_id,
                replica_id,
                role,
                cluster_name,
                replica_name,
                config,
            )
            .await;
        }

        if !source_collections_to_create.is_empty() || !table_collections_to_create.is_empty() {
            self.pending_compute_installation_retry = None;
        }
        if !source_collections_to_create.is_empty() {
            self.create_source_collections(source_collections_to_create)
                .await?;
        }

        // Have to create sources first and then tables, because tables within
        // one transaction can depend on sources.
        if !table_collections_to_create.is_empty() {
            self.create_table_collections(table_collections_to_create, execution_timestamps_to_set)
                .await?;
        }
        // Sink inputs must exist before exports acquire their dependency read holds.
        for sink in sinks_to_create {
            self.create_storage_export(sink.global_id(), &sink).await?;
        }
        for sink in sinks_to_alter {
            self.alter_storage_export(&sink).await?;
        }
        // Storage exists before compute imports it. Within compute, install indexes
        // immediately after their input so downstream plans can use same-batch indexes.
        if self.controller.replica_owned_compute() {
            // Serving timelines are adapter-owned. Installation, execution holds,
            // and compaction policies are applied by each replica's follower.
            let indexes: Vec<_> = compute_items_to_create
                .iter()
                .filter_map(|id| {
                    let entry = self.catalog().get_entry(id);
                    match entry.item() {
                        CatalogItem::Index(index) => Some((
                            index.global_id(),
                            index.cluster_id,
                            index.custom_logical_compaction_window.unwrap_or_default(),
                        )),
                        _ => None,
                    }
                })
                .collect();
            for (id, cluster, window) in indexes {
                self.initialize_compute_read_policies(vec![id], cluster, window)
                    .await;
            }
        } else if self.catalog().state().catalog_read_protection_enabled() {
            if !compute_items_to_create.is_empty() {
                self.pending_compute_installation_retry = None;
            }
            for id in compute_items_to_create {
                let entry = self.catalog().get_entry(&id);
                let export = match entry.item() {
                    CatalogItem::Index(index) => index.global_id(),
                    CatalogItem::MaterializedView(mv) => mv.global_id_writes(),
                    CatalogItem::MetricSink(sink) => sink.global_id,
                    _ => unreachable!("maintained compute addition"),
                };
                self.pending_compute_installations.insert(export);
            }
            self.install_pending_compute_collections().await;
        } else if !compute_items_to_create.is_empty() {
            // Traverse only these additions' dependencies rather than sorting the entire
            // catalog on each DDL. Views between maintained objects matter to ordering.
            let mut pending = compute_items_to_create.clone();
            let mut entries = BTreeMap::new();
            while let Some(id) = pending.pop_first() {
                if entries.contains_key(&id) {
                    continue;
                }
                let entry = self.catalog().get_entry(&id).clone();
                pending.extend(entry.uses());
                entries.insert(id, entry);
            }
            for entry in self.sort_catalog_entries(entries.into_values()) {
                if !compute_items_to_create.contains(&entry.id()) {
                    continue;
                }
                match entry.item() {
                    CatalogItem::Index(index) => {
                        self.create_index_from_catalog(entry.id(), index, None)
                            .await?;
                    }
                    CatalogItem::MaterializedView(mv) => {
                        self.create_materialized_view_from_catalog(entry.id(), mv, None)
                            .await?;
                    }
                    CatalogItem::MetricSink(sink) => {
                        self.create_metric_sink_from_catalog(entry.id(), sink, None)
                            .await?;
                    }
                    _ => unreachable!("only maintained compute additions are queued"),
                }
            }
        }
        // It is _very_ important that we only initialize read policies after we
        // have created all the sources/collections. Some of the sources created
        // in this collection might have dependencies on other sources, so the
        // controller must get a chance to install read holds before we set a
        // policy that might make the since advance.
        self.initialize_storage_collections(storage_policies_to_initialize)
            .await?;

        // New collections already enforce their committed bounds during creation.
        // Deliver advancements after same-batch creates, before drops release protection.
        if !storage_bounds.is_empty() {
            self.controller
                .storage_collections
                .apply_compaction_bounds(storage_bounds)?;
        }

        // Create VPC endpoints for AWS PrivateLink connections
        if !vpc_endpoints_to_create.is_empty() {
            if let Some(cloud_resource_controller) = self.cloud_resource_controller.as_ref() {
                for (connection_id, spec) in vpc_endpoints_to_create {
                    if let Err(err) = cloud_resource_controller
                        .ensure_vpc_endpoint(connection_id, spec)
                        .await
                    {
                        tracing::error!(?err, "failed to ensure vpc endpoint!");
                    }
                }
            } else {
                tracing::error!(
                    "AWS PrivateLink connections unsupported without cloud_resource_controller"
                );
            }
        }

        // Apply batched connection alterations to dependent sources/sinks/tables.
        if !source_connections_to_alter.is_empty() {
            self.controller
                .storage
                .alter_ingestion_connections(source_connections_to_alter)
                .await
                .unwrap_or_terminate("cannot fail to alter ingestion connections");
        }

        if !sink_connections_to_alter.is_empty() {
            self.controller
                .storage
                .alter_export_connections(sink_connections_to_alter)
                .await
                .unwrap_or_terminate("altering export connections after txn must succeed");
        }

        if !source_export_data_configs_to_alter.is_empty() {
            self.controller
                .storage
                .alter_ingestion_export_data_configs(source_export_data_configs_to_alter)
                .await
                .unwrap_or_terminate("altering source export data configs after txn must succeed");
        }

        if !source_descs_to_alter.is_empty() {
            self.controller
                .storage
                .alter_ingestion_source_desc(source_descs_to_alter)
                .await
                .unwrap_or_terminate("cannot fail to alter ingestion source desc");
        }

        // Apply source drop overwrites.
        sources_to_drop.retain(|(_, gid)| !source_gids_to_keep.contains(gid));

        let readable_collections_to_drop: BTreeSet<_> = sources_to_drop
            .iter()
            .map(|(_, gid)| *gid)
            .chain(tables_to_drop.iter().map(|(_, gid)| *gid))
            .chain(indexes_to_drop.iter().map(|(_, gid)| *gid))
            .chain(view_gids_to_drop.iter().copied())
            .collect();

        // Clean up any active compute sinks like subscribes or copy to-s that
        // rely on dropped relations or clusters.
        for (sink_id, sink) in &self.active_compute_sinks {
            let cluster_id = sink.cluster_id();
            if let Some(id) = sink
                .depends_on()
                .iter()
                .find(|id| readable_collections_to_drop.contains(id))
            {
                let name = dropped_item_names
                    .get(id)
                    .cloned()
                    .expect("missing relation name");
                active_compute_sinks_to_drop.insert(
                    *sink_id,
                    ActiveComputeSinkRetireReason::DependencyDropped(DroppedDependency::Relation {
                        name,
                    }),
                );
            } else if clusters_to_drop.contains(&cluster_id) {
                let name = dropped_cluster_names
                    .get(&cluster_id)
                    .cloned()
                    .expect("missing cluster name");
                active_compute_sinks_to_drop.insert(
                    *sink_id,
                    ActiveComputeSinkRetireReason::DependencyDropped(DroppedDependency::Cluster {
                        name,
                    }),
                );
            }
        }

        // Clean up any pending peeks that rely on dropped relations or clusters.
        for (uuid, pending_peek) in &self.pending_peeks {
            if let Some(id) = pending_peek
                .depends_on
                .iter()
                .find(|id| readable_collections_to_drop.contains(id))
            {
                let name = dropped_item_names
                    .get(id)
                    .cloned()
                    .expect("missing relation name");
                peeks_to_drop.push((DroppedDependency::Relation { name }, uuid.clone()));
            } else if clusters_to_drop.contains(&pending_peek.cluster_id) {
                let name = dropped_cluster_names
                    .get(&pending_peek.cluster_id)
                    .cloned()
                    .expect("missing cluster name");
                peeks_to_drop.push((DroppedDependency::Cluster { name }, uuid.clone()));
            }
        }

        // Clean up any pending `COPY` statements that rely on dropped relations or clusters.
        for (conn_id, pending_copy) in &self.active_copies {
            let dropping_table = tables_to_drop
                .iter()
                .any(|(item_id, _gid)| pending_copy.table_id == *item_id);
            let dropping_cluster = clusters_to_drop.contains(&pending_copy.cluster_id);

            if dropping_table || dropping_cluster {
                copies_to_drop.push(conn_id.clone());
            }
        }

        let storage_gids_to_drop: BTreeSet<_> = sources_to_drop
            .iter()
            .map(|(_id, gid)| gid)
            .chain(storage_sink_gids_to_drop.iter())
            .chain(tables_to_drop.iter().map(|(_id, gid)| gid))
            .copied()
            .collect();
        let compute_gids_to_drop: Vec<_> = indexes_to_drop
            .iter()
            .chain(compute_sinks_to_drop.iter())
            .copied()
            .collect();

        // Gather resources that we have to remove from timeline state and
        // pre-check if any Timelines become empty, when we drop the specified
        // storage and compute resources.
        //
        // Note: We only apply these changes below.
        let mut timeline_id_bundles = BTreeMap::new();

        for (timeline, state) in &self.global_timelines {
            let mut id_bundle = CollectionIdBundle::default();
            let associated = state.id_bundle();

            for storage_id in associated.storage_ids {
                if storage_gids_to_drop.contains(&storage_id) {
                    id_bundle.storage_ids.insert(storage_id);
                }
            }

            for (instance_id, id) in associated
                .compute_ids
                .into_iter()
                .flat_map(|(cluster, ids)| ids.into_iter().map(move |id| (cluster, id)))
            {
                if compute_gids_to_drop.contains(&(instance_id, id))
                    || clusters_to_drop.contains(&instance_id)
                {
                    id_bundle
                        .compute_ids
                        .entry(instance_id)
                        .or_default()
                        .insert(id);
                }
            }

            timeline_id_bundles.insert(timeline.clone(), id_bundle);
        }

        let mut timeline_associations = BTreeMap::new();
        for (timeline, id_bundle) in timeline_id_bundles.into_iter() {
            let state = self
                .global_timelines
                .get(&timeline)
                .expect("all timelines have a timestamp oracle");

            let empty = state.id_bundle().difference(&id_bundle).is_empty();
            timeline_associations.insert(timeline, (empty, id_bundle));
        }

        // No error returns are allowed after this point. Enforce this at compile time
        // by using this odd structure so we don't accidentally add a stray `?`.
        let _: () = async {
            if !timeline_associations.is_empty() {
                for (timeline, (should_be_empty, id_bundle)) in timeline_associations {
                    let became_empty =
                        self.remove_resources_associated_with_timeline(timeline, id_bundle);
                    assert_eq!(should_be_empty, became_empty, "emptiness did not match!");
                }
            }

            // Note that we drop tables before sources since there can be a weak
            // dependency on sources from tables in the storage controller that
            // will result in error logging that we'd prefer to avoid. This
            // isn't an actual dependency issue but we'd like to keep that error
            // logging around to indicate when an actual dependency error might
            // occur.
            if !tables_to_drop.is_empty() {
                // Forgetting follows every staged append in the adapter's FIFO.
                if !txn_tables_to_drop.is_empty() {
                    self.forget_tables_via_committer(txn_tables_to_drop.into_iter().collect())
                        .await;
                }
                self.drop_tables(tables_to_drop.into_iter().collect_vec());
            }

            if !sources_to_drop.is_empty() {
                self.drop_sources(sources_to_drop);
            }

            if !storage_sink_gids_to_drop.is_empty() {
                self.drop_storage_sinks(storage_sink_gids_to_drop);
            }

            if !active_compute_sinks_to_drop.is_empty() {
                let retire_notify = self
                    .retire_compute_sinks(active_compute_sinks_to_drop)
                    .await;
                if let Some(ctx) = ctx {
                    ctx.delay_response_until(retire_notify);
                }
            }

            if !peeks_to_drop.is_empty() {
                for (dep, uuid) in peeks_to_drop {
                    if let Some(pending_peek) = self.remove_pending_peek(&uuid) {
                        let cancel_reason = PeekResponse::Error(PeekError::unstructured(
                            dep.query_terminated_error(),
                        ));
                        self.cancel_compute_peek(pending_peek.cluster_id, uuid, cancel_reason)
                            .unwrap_or_terminate("unable to cancel peek");
                        self.retire_execution(
                            StatementEndedExecutionReason::Canceled,
                            pending_peek.ctx_extra.defuse(),
                        );
                    }
                }
            }

            if !copies_to_drop.is_empty() {
                for conn_id in copies_to_drop {
                    self.cancel_pending_copy(&conn_id);
                }
            }

            if !compute_gids_to_drop.is_empty() {
                self.drop_compute_collections(compute_gids_to_drop);
            }

            if !vpc_endpoints_to_drop.is_empty() {
                self.drop_vpc_endpoints_in_background(vpc_endpoints_to_drop)
            }

            let clusters_losing_replicas: BTreeSet<_> = cluster_replicas_to_drop
                .iter()
                .map(|(cluster_id, _)| *cluster_id)
                .collect();
            if !cluster_replicas_to_drop.is_empty() {
                fail::fail_point!("after_catalog_drop_replica");

                for (cluster_id, replica_id) in cluster_replicas_to_drop {
                    self.drop_replica(cluster_id, replica_id);
                }
            }
            if !clusters_to_drop.is_empty() {
                for cluster_id in &clusters_to_drop {
                    self.controller.drop_cluster(*cluster_id);
                }
            }
            // A dropped cluster, or one left without replicas, cannot serve
            // peeks, so its peek series are stale. They come back on the first
            // peek once a cluster has a replica again.
            for cluster_id in clusters_losing_replicas.into_iter().chain(clusters_to_drop) {
                let has_replicas = self
                    .catalog()
                    .try_get_cluster(cluster_id)
                    .is_some_and(|cluster| cluster.replicas().next().is_some());
                if !has_replicas {
                    self.metrics.by_cluster.remove_cluster(cluster_id);
                }
            }

            // We don't want to block the main coordinator thread on cleaning
            // up external resources (PostgreSQL replication slots and secrets),
            // so we perform that cleanup in a background task.
            //
            // TODO(14551): This is inherently best effort. An ill-timed crash
            // means we'll never clean these resources up. Safer cleanup for non-Materialize resources.
            // See <https://github.com/MaterializeInc/materialize/issues/14551>
            task::spawn(|| "drop_replication_slots_and_secrets", {
                let ssh_tunnel_manager = self.connection_context().ssh_tunnel_manager.clone();
                let caching_secrets_reader = self.caching_secrets_reader.clone();
                let secrets_controller = Arc::clone(&self.secrets_controller);
                let secrets_reader = Arc::clone(self.secrets_reader());
                let storage_config = self.storage_configuration.clone();

                async move {
                    for (connection, replication_slot_name) in replication_slots_to_drop {
                        tracing::info!(?replication_slot_name, "dropping replication slot");

                        // Try to drop the replication slots, but give up after
                        // a while. The PostgreSQL server may no longer be
                        // healthy. Users often drop PostgreSQL sources
                        // *because* the PostgreSQL server has been
                        // decomissioned.
                        let result: Result<(), anyhow::Error> = Retry::default()
                            .max_duration(Duration::from_secs(60))
                            .retry_async(|_state| async {
                                let config = connection
                                    .config(&secrets_reader, &storage_config, InTask::No)
                                    .await
                                    .map_err(|e| {
                                        anyhow::anyhow!(
                                            "error creating Postgres client for \
                                            dropping acquired slots: {}",
                                            e.display_with_causes()
                                        )
                                    })?;

                                mz_postgres_util::drop_replication_slots(
                                    &ssh_tunnel_manager,
                                    config.clone(),
                                    &[(&replication_slot_name, true)],
                                )
                                .await?;

                                Ok(())
                            })
                            .await;

                        if let Err(err) = result {
                            tracing::warn!(
                                ?replication_slot_name,
                                ?err,
                                "failed to drop replication slot"
                            );
                        }
                    }

                    // Drop secrets *after* dropping the replication slots,
                    // because dropping replication slots may rely on those
                    // secrets still being present.
                    //
                    // It's okay if we crash before processing the secret drops,
                    // as we look for and remove any orphaned secrets during
                    // startup.
                    fail_point!("drop_secrets");
                    for secret in secrets_to_drop {
                        if let Err(e) = secrets_controller.delete(secret).await {
                            warn!("Dropping secrets has encountered an error: {}", e);
                        } else {
                            caching_secrets_reader.invalidate(secret);
                        }
                    }
                }
            });
        }
        .instrument(info_span!(
            "coord::apply_catalog_implications_inner::finalize"
        ))
        .await;

        Ok(())
    }

    /// Install every ready committed export, revisiting physical dependencies after
    /// each pass. Pending work retains identities, not stale catalog snapshots, so
    /// selection changes, drops, and replacement application take effect on retry.
    pub(super) async fn install_pending_compute_collections(&mut self) {
        self.metrics.pending_compute_installations.set(
            self.pending_compute_installations
                .len()
                .try_into()
                .expect("fits u64"),
        );
        if self.pending_compute_installations.is_empty() {
            self.pending_compute_installation_retry = None;
            return;
        }
        if self
            .pending_compute_installation_retry
            .is_some_and(|(deadline, _)| Instant::now() < deadline)
        {
            return;
        }
        let result = self.try_install_pending_compute_collections().await;
        self.metrics.pending_compute_installations.set(
            self.pending_compute_installations
                .len()
                .try_into()
                .expect("fits u64"),
        );
        if self.pending_compute_installations.is_empty() {
            self.pending_compute_installation_retry = None;
            return;
        }
        let delay = self
            .pending_compute_installation_retry
            .map(|(_, delay)| delay.saturating_mul(2))
            .unwrap_or(Duration::from_secs(1))
            .min(Duration::from_secs(30));
        self.pending_compute_installation_retry = Some((Instant::now() + delay, delay));
        self.metrics.compute_installation_retries.inc();
        warn!(
            pending = ?self.pending_compute_installations,
            retry_after = ?delay,
            error = ?result.err(),
            "committed compute installation remains pending; bound publication and client reclamation are deferred"
        );
    }

    async fn try_install_pending_compute_collections(&mut self) -> Result<(), AdapterError> {
        let build =
            crate::catalog::Catalog::expression_build_version(self.catalog().config().build_info)
                .to_string();
        let revisions = self
            .pending_compute_installations
            .iter()
            .filter_map(|id| {
                self.catalog()
                    .state()
                    .written_plan(*id, &build)
                    .map(|revision| (*id, revision))
            })
            .collect();
        let plans = self.catalog().read_written_plans(revisions).await?;
        let mut first_error = None;
        loop {
            let pending: Vec<_> = self.pending_compute_installations.iter().copied().collect();
            let mut progressed = false;
            for id in pending {
                let Some(entry) = self.catalog().try_get_entry_by_global_id(&id).cloned() else {
                    // The drop path consumes this marker before issuing physical
                    // drops. Retrying earlier in that batch must not erase it.
                    continue;
                };
                if entry.item().cluster_id().is_some_and(|cluster| {
                    self.controller
                        .compute
                        .collection_frontiers(id, Some(cluster))
                        .is_ok()
                }) {
                    self.pending_compute_installations.remove(&id);
                    progressed = true;
                    continue;
                }
                let result = match entry.item() {
                    CatalogItem::Index(index) if index.global_id() == id => {
                        self.create_index_from_catalog(entry.id(), index, plans.get(&id))
                            .await
                    }
                    CatalogItem::MaterializedView(mv) if mv.global_id_writes() == id => {
                        self.create_materialized_view_from_catalog(entry.id(), mv, plans.get(&id))
                            .await
                    }
                    CatalogItem::MetricSink(sink) if sink.global_id == id => {
                        self.create_metric_sink_from_catalog(entry.id(), sink, plans.get(&id))
                            .await
                    }
                    // Retired MV writers can remain as readable aliases. Their
                    // physical drop likewise owns removing the pending marker.
                    _ => Ok(false),
                };
                match result {
                    Ok(true) => {
                        self.pending_compute_installations.remove(&id);
                        progressed = true;
                    }
                    Ok(false) => (),
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
            if !progressed {
                return first_error.map_or(Ok(()), Err);
            }
        }
    }

    /// A selected plan is executable only after its physical imports exist in this
    /// lifecycle instance. Missing selections or imports require retry, not planning.
    fn written_installation_plan(
        &self,
        plan: Option<&GlobalExpressions>,
        item_version: RelationVersion,
        cluster: ClusterId,
    ) -> Option<GlobalExpressions> {
        if !self.controller.compute.instance_exists(cluster) {
            return None;
        }
        let plan = plan?;
        if plan.item_version != item_version
            || !plan
                .collection_imports()
                .all(|input| self.catalog().try_get_entry_by_global_id(input).is_some())
            || !plan.physical_plan.index_imports.keys().all(|input| {
                self.controller
                    .compute
                    .collection_frontiers(*input, Some(cluster))
                    .is_ok()
            })
            || !plan.physical_plan.source_imports.keys().all(|input| {
                self.controller
                    .storage_collections
                    .collection_frontiers(*input)
                    .is_ok()
            })
        {
            return None;
        }
        Some(plan.clone())
    }

    /// Cached plans are optional. Imports must belong to the committed catalog and
    /// to the installation snapshot, not merely to compute's not-yet-dropped state.
    async fn cached_installation_plan(
        &self,
        global_id: GlobalId,
        item_version: RelationVersion,
        compute_instance: &ComputeInstanceSnapshot,
        optimizer_config: &OptimizerConfig,
    ) -> Option<GlobalExpressions> {
        let cached = self.catalog().cached_global_expressions(global_id).await;
        cached.filter(|expressions| {
            expressions.item_version == item_version
                && expressions.optimizer_features == optimizer_config.features
                // A dropped index can still be in compute until this batch's drops run.
                // Conversely, a committed index may not have been installed yet.
                && expressions.global_mir.index_imports.keys()
                    .chain(expressions.physical_plan.index_imports.keys())
                    .all(|id| {
                        self.catalog().try_get_entry_by_global_id(id).is_some()
                            && compute_instance.contains_collection(id)
                    })
                && expressions.dataflow_metainfos.optimizer_notices.iter().all(|notice| {
                    notice.dependencies.iter().all(|id| {
                        self.catalog().try_get_entry_by_global_id(id).is_some()
                    })
                })
        })
    }

    async fn create_index_from_catalog(
        &mut self,
        catalog_id: CatalogItemId,
        index: &Index,
        written: Option<&GlobalExpressions>,
    ) -> Result<bool, AdapterError> {
        let global_id = index.global_id();
        let expressions = if self.catalog().state().catalog_read_protection_enabled() {
            let Some(plan) =
                self.written_installation_plan(written, RelationVersion::root(), index.cluster_id)
            else {
                return Ok(false);
            };
            plan
        } else {
            let compute_instance = self
                .instance_snapshot(index.cluster_id)
                .expect("index cluster must exist before installation");
            let optimizer_config = OptimizerConfig::from(self.catalog().system_config())
                .override_from(
                    &self
                        .catalog()
                        .get_cluster(index.cluster_id)
                        .config
                        .features(),
                )
                .override_from(&self.cluster_scoped_optimizer_overrides(index.cluster_id));
            let cached = self
                .cached_installation_plan(
                    global_id,
                    RelationVersion::root(),
                    &compute_instance,
                    &optimizer_config,
                )
                .await;
            match cached {
                Some(expressions) => expressions,
                None => {
                    let name = self.catalog().get_entry(&catalog_id).name();
                    self.build_index_dataflow_plan(
                        Arc::new(self.catalog().state().clone()),
                        name,
                        index,
                        compute_instance,
                        optimizer_config,
                    )?
                }
            }
        };
        let GlobalExpressions {
            global_mir,
            physical_plan,
            dataflow_metainfos,
            ..
        } = expressions;
        let id_bundle = dataflow_import_id_bundle(&physical_plan, index.cluster_id);
        self.catalog_mut().set_optimized_plan(global_id, global_mir);
        self.catalog_mut()
            .set_physical_plan(global_id, physical_plan.clone());
        let notice_updates = self.persist_dataflow_metainfo(dataflow_metainfos, global_id);
        self.ship_new_dataflow(&id_bundle, physical_plan, index.cluster_id, notice_updates)
            .await;
        self.update_compute_read_policy(
            index.cluster_id,
            catalog_id,
            index
                .custom_logical_compaction_window
                .unwrap_or_default()
                .into(),
        );
        Ok(true)
    }

    async fn create_metric_sink_from_catalog(
        &mut self,
        catalog_id: CatalogItemId,
        sink: &MetricSink,
        written: Option<&GlobalExpressions>,
    ) -> Result<bool, AdapterError> {
        let expressions = if self.catalog().state().catalog_read_protection_enabled() {
            let Some(plan) =
                self.written_installation_plan(written, RelationVersion::root(), sink.cluster_id)
            else {
                return Ok(false);
            };
            plan
        } else {
            let snapshot = self
                .instance_snapshot(sink.cluster_id)
                .expect("metric sink cluster exists before installation");
            let config = OptimizerConfig::from(self.catalog().system_config())
                .override_from(
                    &self
                        .catalog()
                        .get_cluster(sink.cluster_id)
                        .config
                        .features(),
                )
                .override_from(&self.cluster_scoped_optimizer_overrides(sink.cluster_id));
            match self
                .cached_installation_plan(
                    sink.global_id,
                    RelationVersion::root(),
                    &snapshot,
                    &config,
                )
                .await
            {
                Some(expressions) => expressions,
                None => self.build_metric_sink_dataflow_plan(
                    Arc::new(self.catalog().state().clone()),
                    self.catalog().get_entry(&catalog_id).name(),
                    sink,
                    snapshot,
                    config,
                )?,
            }
        };
        let GlobalExpressions {
            global_mir,
            physical_plan,
            dataflow_metainfos,
            ..
        } = expressions;
        let imports = dataflow_import_id_bundle(&physical_plan, sink.cluster_id);
        self.catalog_mut()
            .set_optimized_plan(sink.global_id, global_mir);
        self.catalog_mut()
            .set_physical_plan(sink.global_id, physical_plan.clone());
        let notices = self.persist_dataflow_metainfo(dataflow_metainfos, sink.global_id);
        // Metric exports are process-local and need neither historical recovery nor allow_writes.
        self.ship_new_dataflow(&imports, physical_plan, sink.cluster_id, notices)
            .await;
        Ok(true)
    }

    async fn create_materialized_view_from_catalog(
        &mut self,
        catalog_id: CatalogItemId,
        mv: &MaterializedView,
        written: Option<&GlobalExpressions>,
    ) -> Result<bool, AdapterError> {
        let global_id = mv.global_id_writes();
        let output = self
            .controller
            .storage_collections
            .collection_frontiers(global_id)
            .expect("MV storage exists before compute installation");
        // A pending replacement does not own output writes yet. Its creation promise
        // is independent of the target's progress on their shared shard.
        // An existing writer instead recovers from durable output progress, not its
        // initial visibility frontier.
        let upper = if mv.replacement_target.is_some() {
            mv.initial_as_of
                .clone()
                .expect("pending replacement has an initial visibility frontier")
        } else if PartialOrder::less_equal(&output.write_frontier, &output.read_capabilities) {
            output.read_capabilities
        } else {
            output
                .write_frontier
                .iter()
                .map(|t| t.step_back().unwrap_or(Timestamp::MIN))
                .collect()
        };
        let expressions = if self.catalog().state().catalog_read_protection_enabled() {
            let Some(plan) = self.written_installation_plan(
                written,
                latest_item_version(&mv.collections),
                mv.cluster_id,
            ) else {
                return Ok(false);
            };
            plan
        } else {
            // Equivalent indexes need not retain equivalent history. Restrict both cached
            // and reconstructed plans to installed paths that can satisfy the output promise.
            let indexes = self
                .controller
                .compute
                .collection_ids(mv.cluster_id)
                .expect("MV cluster exists before installation")
                .filter(|id| self.catalog().try_get_entry_by_global_id(id).is_some())
                .filter(|id| {
                    self.controller
                        .compute
                        .collection_frontiers(*id, Some(mv.cluster_id))
                        .is_ok_and(|f| PartialOrder::less_equal(&f.read_frontier, &upper))
                })
                .collect();
            let snapshot = ComputeInstanceSnapshot::new_from_parts(mv.cluster_id, indexes);
            let config = OptimizerConfig::from(self.catalog().system_config())
                .override_from(&self.catalog().get_cluster(mv.cluster_id).config.features())
                .override_from(&self.cluster_scoped_optimizer_overrides(mv.cluster_id));
            match self
                .cached_installation_plan(
                    global_id,
                    latest_item_version(&mv.collections),
                    &snapshot,
                    &config,
                )
                .await
            {
                Some(expressions) => expressions,
                None => self.build_materialized_view_dataflow_plan(
                    Arc::new(self.catalog().state().clone()),
                    self.catalog().get_entry(&catalog_id).name(),
                    mv,
                    snapshot,
                    config,
                )?,
            }
        };
        let GlobalExpressions {
            global_mir,
            mut physical_plan,
            dataflow_metainfos,
            ..
        } = expressions;
        let imports = dataflow_import_id_bundle(&physical_plan, mv.cluster_id);
        // Installation owns these holds, independently of DDL or its chosen plan.
        // Keep them until compute has established its transitive execution protection.
        let holds = self.acquire_read_holds(&imports);
        let mut as_of = holds.least_valid_read();
        if !PartialOrder::less_equal(&as_of, &upper) {
            return Err(AdapterError::internal(
                "install materialized view",
                format!("inputs are readable from {as_of:?}, beyond recovery frontier {upper:?}"),
            ));
        }
        if mv.refresh_schedule.is_some() {
            // Permit warmup before the first refresh without skipping historical output.
            as_of.join_assign(&self.greatest_available_read(&imports).meet(&upper));
        } else {
            as_of = upper;
        }
        self.catalog_mut().set_optimized_plan(global_id, global_mir);
        self.catalog_mut()
            .set_physical_plan(global_id, physical_plan.clone());
        let notices = self.persist_dataflow_metainfo(dataflow_metainfos, global_id);
        physical_plan.set_as_of(as_of);
        mv.apply_execution_bounds(&mut physical_plan);
        self.ship_dataflow_and_notice_builtin_table_updates(
            physical_plan,
            mv.cluster_id,
            notices,
            mv.target_replica,
        )
        .await;
        if mv.replacement_target.is_none() {
            self.allow_writes(mv.cluster_id, global_id);
        }
        drop(holds);
        Ok(true)
    }

    /// Describe adapter-owned table writes from committed shard metadata.
    pub(super) fn table_registration(
        &self,
        id: GlobalId,
        relation_desc: mz_repr::RelationDesc,
    ) -> crate::table_writer::TableRegistration {
        crate::table_writer::TableRegistration {
            id,
            data_shard: self
                .catalog()
                .state()
                .storage_metadata()
                .get_collection_shard(id)
                .expect("table has committed shard metadata"),
            relation_desc,
        }
    }

    #[instrument(level = "debug")]
    async fn create_table_collections(
        &mut self,
        table_collections_to_create: BTreeMap<GlobalId, CollectionDescription>,
        execution_timestamps_to_set: BTreeSet<StatementLoggingId>,
    ) -> Result<(), AdapterError> {
        let registrations = table_collections_to_create
            .iter()
            .filter_map(|(id, collection)| {
                matches!(collection.data_source, DataSource::Table)
                    .then(|| self.table_registration(*id, collection.desc.clone()))
            })
            .collect::<Vec<_>>();
        let collections = table_collections_to_create.into_iter().collect_vec();
        self.register_adapter_storage_collections(&collections, &BTreeSet::new())
            .await;

        // Confirm leadership after allocating the collections' initial timestamp.
        let write_ts = self.get_local_write_ts().await;
        let register_ts = write_ts.timestamp;
        self.catalog
            .advance_upper(write_ts.advance_to)
            .await
            .unwrap_or_terminate("unable to advance catalog upper");

        {
            let storage_metadata = self.catalog.state().storage_metadata();
            self.controller
                .storage
                .create_collections(storage_metadata, Some(register_ts), collections)
                .await
                .unwrap_or_terminate("cannot fail to create collections");
        }

        // Registration can choose a later timestamp than the collections' initial since. Reads
        // remain above the applied registration timestamp.
        let table_ts = if registrations.is_empty() {
            // Without txn-wal registration, this timestamp still makes the collections readable.
            self.apply_local_write(register_ts).await;
            register_ts
        } else {
            self.register_tables_via_committer(registrations).await
        };

        for id in execution_timestamps_to_set {
            self.set_statement_execution_timestamp(id, table_ts);
        }

        Ok(())
    }

    /// Describe an MV's storage collections from committed catalog state.
    pub(super) fn materialized_view_storage_collections(
        &self,
        mv: &MaterializedView,
    ) -> Vec<(GlobalId, CollectionDescription)> {
        // The oldest collection owns the shard. Applied replacements point to
        // their predecessor, and pending replacements to their target's latest
        // collection. NOTE: Versioned tables chain in the opposite direction.
        let mut primary = mv
            .replacement_target
            .map(|target_id| self.catalog().get_entry(&target_id).latest_global_id());
        mv.collection_descs()
            .map(|(gid, _version, desc)| {
                // Applied replacements retain the original shard and its history.
                // Their SQL visibility frontier is not an instruction to initialize
                // that shard again. Compute receives it separately as initial_as_of.
                let since = if mv.collections.len() > 1 {
                    None
                } else {
                    mv.initial_as_of.clone()
                };
                let mut collection_desc = CollectionDescription::for_other(desc, since);
                collection_desc.primary = primary;
                primary = Some(gid);
                (gid, collection_desc)
            })
            .collect()
    }

    #[instrument(level = "debug")]
    async fn create_source_collections(
        &mut self,
        source_collections_to_create: BTreeMap<GlobalId, CollectionDescription>,
    ) -> Result<(), AdapterError> {
        let collections = source_collections_to_create.into_iter().collect_vec();
        self.register_adapter_storage_collections(&collections, &BTreeSet::new())
            .await;
        let storage_metadata = self.catalog.state().storage_metadata();

        self.controller
            .storage
            .create_collections(
                storage_metadata,
                None, // Sources don't need a write timestamp
                collections,
            )
            .await
            .unwrap_or_terminate("cannot fail to create collections");

        Ok(())
    }

    /// Start request-side writers from committed shard identities, independently of storage enactment.
    pub(super) async fn register_adapter_storage_collections(
        &self,
        collections: &[(GlobalId, CollectionDescription)],
        migrated_storage_collections: &BTreeSet<GlobalId>,
    ) {
        for (id, collection) in collections {
            let history = match collection.data_source {
                DataSource::Webhook => None,
                DataSource::Introspection(typ) if typ.is_statement_history() => Some(typ),
                _ => continue,
            };
            let shard = self
                .catalog
                .state()
                .storage_metadata()
                .get_collection_shard(*id)
                .expect("adapter-written collection has committed shard metadata");
            let writer = self
                .persist_client
                .open_writer(
                    shard,
                    Arc::new(collection.desc.clone()),
                    Arc::new(mz_persist_types::codec_impls::UnitSchema),
                    mz_persist_client::Diagnostics {
                        shard_name: id.to_string(),
                        handle_purpose: "adapter storage writes".to_owned(),
                    },
                )
                .await
                .expect("adapter-written schema matches committed description");
            if let Some(typ) = history {
                let force_writable =
                    self.controller.read_only() && migrated_storage_collections.contains(id);
                self.adapter_storage
                    .register_history(typ, *id, writer, force_writable);
            } else {
                self.adapter_storage.register_webhook(*id, writer);
            }
        }
    }

    #[instrument(level = "debug")]
    async fn initialize_storage_collections(
        &mut self,
        storage_policies_to_initialize: BTreeMap<CompactionWindow, BTreeSet<GlobalId>>,
    ) -> Result<(), AdapterError> {
        for (compaction_window, global_ids) in storage_policies_to_initialize {
            self.initialize_read_policies(
                &CollectionIdBundle {
                    storage_ids: global_ids,
                    compute_ids: BTreeMap::new(),
                },
                compaction_window,
            )
            .await;
        }

        Ok(())
    }

    #[instrument(level = "debug")]
    async fn handle_create_table(
        &self,
        ctx: &Option<&mut ExecuteContext>,
        storage_collections_to_create: &mut BTreeMap<GlobalId, CollectionDescription>,
        storage_policies_to_initialize: &mut BTreeMap<CompactionWindow, BTreeSet<GlobalId>>,
        execution_timestamps_to_set: &mut BTreeSet<StatementLoggingId>,
        table_id: CatalogItemId,
        table: Table,
    ) -> Result<(), AdapterError> {
        // The table data_source determines whether this table will be written to
        // by environmentd (e.g. with INSERT INTO statements) or by the storage layer
        // (e.g. a source-fed table).
        match &table.data_source {
            TableDataSource::TableWrites { defaults: _ } => {
                let versions: BTreeMap<_, _> = table
                    .collection_descs()
                    .map(|(gid, version, desc)| (version, (gid, desc)))
                    .collect();
                let collection_descs = versions.iter().map(|(_version, (gid, desc))| {
                    let collection_desc = CollectionDescription::for_table(desc.clone());

                    (*gid, collection_desc)
                });

                let compaction_window = table
                    .custom_logical_compaction_window
                    .unwrap_or(CompactionWindow::Default);
                let ids_to_initialize = storage_policies_to_initialize
                    .entry(compaction_window)
                    .or_default();

                for (gid, collection_desc) in collection_descs {
                    storage_collections_to_create.insert(gid, collection_desc);
                    ids_to_initialize.insert(gid);
                }

                if let Some(id) = ctx.as_ref().and_then(|ctx| ctx.extra().contents()) {
                    execution_timestamps_to_set.insert(id);
                }
            }
            TableDataSource::DataSource {
                desc: data_source_desc,
                timeline,
            } => {
                match data_source_desc {
                    DataSourceDesc::IngestionExport {
                        ingestion_id,
                        external_reference: _,
                        details,
                        data_config,
                    } => {
                        let global_ingestion_id =
                            self.catalog().get_entry(ingestion_id).latest_global_id();

                        let collection_desc = CollectionDescription {
                            desc: table.desc.latest(),
                            data_source: DataSource::IngestionExport {
                                ingestion_id: global_ingestion_id,
                                details: details.clone(),
                                data_config: data_config
                                    .clone()
                                    .into_inline_connection(self.catalog.state()),
                            },
                            since: None,
                            timeline: Some(timeline.clone()),
                            primary: None,
                        };

                        let global_id = table
                            .global_ids()
                            .expect_element(|| "subsources cannot have multiple versions");

                        storage_collections_to_create.insert(global_id, collection_desc);

                        let read_policies = self
                            .catalog()
                            .state()
                            .source_compaction_windows(vec![table_id]);
                        for (compaction_window, catalog_ids) in read_policies {
                            let compaction_ids = storage_policies_to_initialize
                                .entry(compaction_window)
                                .or_default();

                            let gids = catalog_ids
                                .into_iter()
                                .map(|item_id| self.catalog().get_entry(&item_id).global_ids())
                                .flatten();
                            compaction_ids.extend(gids);
                        }
                    }
                    DataSourceDesc::Webhook {
                        validate_using: _,
                        body_format: _,
                        headers: _,
                        cluster_id: _,
                    } => {
                        // Create the underlying collection with the latest schema from the Table.
                        assert_eq!(
                            table.desc.latest_version(),
                            RelationVersion::root(),
                            "found webhook with more than 1 relation version, {:?}",
                            table.desc
                        );
                        let desc = table.desc.latest();

                        let collection_desc = CollectionDescription {
                            desc,
                            data_source: DataSource::Webhook,
                            since: None,
                            timeline: Some(timeline.clone()),
                            primary: None,
                        };

                        let global_id = table
                            .global_ids()
                            .expect_element(|| "webhooks cannot have multiple versions");

                        storage_collections_to_create.insert(global_id, collection_desc);

                        let read_policies = self
                            .catalog()
                            .state()
                            .source_compaction_windows(vec![table_id]);

                        for (compaction_window, catalog_ids) in read_policies {
                            let compaction_ids = storage_policies_to_initialize
                                .entry(compaction_window)
                                .or_default();

                            let gids = catalog_ids
                                .into_iter()
                                .map(|item_id| self.catalog().get_entry(&item_id).global_ids())
                                .flatten();
                            compaction_ids.extend(gids);
                        }
                    }
                    _ => unreachable!("CREATE TABLE data source got {:?}", data_source_desc),
                }
            }
        }

        Ok(())
    }

    #[instrument(level = "debug")]
    async fn handle_alter_table(
        &mut self,
        catalog_id: CatalogItemId,
        prev_table: Table,
        new_table: Table,
    ) -> Result<(), AdapterError> {
        let existing_gid = prev_table.global_id_writes();
        let new_gid = new_table.global_id_writes();

        if existing_gid == new_gid {
            // It's not an ALTER TABLE ADD COLUMN, because we still have the
            // same GlobalId. It might be a compaction window change.
            if prev_table.custom_logical_compaction_window
                != new_table.custom_logical_compaction_window
            {
                let new_window = new_table
                    .custom_logical_compaction_window
                    .unwrap_or(CompactionWindow::Default);
                self.update_storage_read_policies(vec![(catalog_id, new_window.into())]);
            }
            return Ok(());
        }

        // Acquire a read hold on the original table for the duration of
        // the alter to prevent the since of the original table from
        // getting advanced, while the ALTER is running.
        let existing_table = crate::CollectionIdBundle {
            storage_ids: BTreeSet::from([existing_gid]),
            compute_ids: BTreeMap::new(),
        };
        let existing_table_read_hold = self.acquire_read_holds(&existing_table);

        let expected_version = prev_table.desc.latest_version();
        let new_version = new_table.desc.latest_version();
        let new_desc = new_table
            .desc
            .at_version(RelationVersionSelector::Specific(new_version));
        let registration = matches!(new_table.data_source, TableDataSource::TableWrites { .. })
            .then(|| self.table_registration(new_gid, new_desc.clone()));

        // Confirm leadership before mutating controller state.
        let write_ts = self.get_local_write_ts().await;
        self.catalog
            .advance_upper(write_ts.advance_to)
            .await
            .unwrap_or_terminate("unable to advance catalog upper");

        self.controller
            .storage
            .alter_table_desc(
                self.catalog.state().storage_metadata(),
                existing_gid,
                new_gid,
                new_desc,
                expected_version,
            )
            .await
            .unwrap_or_terminate("failed to alter desc of table");

        // FIFO registration follows all staged writes to the old collection.
        if let Some(registration) = registration {
            self.register_tables_via_committer(vec![registration]).await;
        }

        // Initialize the ReadPolicy which ensures we have the correct read holds.
        let compaction_window = new_table
            .custom_logical_compaction_window
            .unwrap_or(CompactionWindow::Default);
        self.initialize_read_policies(
            &crate::CollectionIdBundle {
                storage_ids: BTreeSet::from([new_gid]),
                compute_ids: BTreeMap::new(),
            },
            compaction_window,
        )
        .await;

        // Alter is complete! We can drop our read hold.
        drop(existing_table_read_hold);

        Ok(())
    }

    #[instrument(level = "debug")]
    async fn handle_create_source(
        &self,
        storage_collections_to_create: &mut BTreeMap<GlobalId, CollectionDescription>,
        storage_policies_to_initialize: &mut BTreeMap<CompactionWindow, BTreeSet<GlobalId>>,
        item_id: CatalogItemId,
        source: Source,
        compaction_windows: BTreeMap<CompactionWindow, BTreeSet<CatalogItemId>>,
    ) -> Result<(), AdapterError> {
        let data_source = match source.data_source {
            DataSourceDesc::Ingestion { desc, cluster_id } => {
                let desc = desc.into_inline_connection(self.catalog().state());
                let item_global_id = self.catalog().get_entry(&item_id).latest_global_id();

                let ingestion = mz_storage_types::sources::IngestionDescription::new(
                    desc,
                    cluster_id,
                    item_global_id,
                );

                DataSource::Ingestion(ingestion)
            }
            DataSourceDesc::OldSyntaxIngestion {
                desc,
                progress_subsource,
                data_config,
                details,
                cluster_id,
            } => {
                let desc = desc.into_inline_connection(self.catalog().state());
                let data_config = data_config.into_inline_connection(self.catalog().state());

                // TODO(parkmycar): We should probably check the type here, but I'm not
                // sure if this will always be a Source or a Table.
                let progress_subsource = self
                    .catalog()
                    .get_entry(&progress_subsource)
                    .latest_global_id();

                let mut ingestion = mz_storage_types::sources::IngestionDescription::new(
                    desc,
                    cluster_id,
                    progress_subsource,
                );

                let legacy_export = SourceExport {
                    storage_metadata: (),
                    data_config,
                    details,
                };

                ingestion
                    .source_exports
                    .insert(source.global_id, legacy_export);

                DataSource::Ingestion(ingestion)
            }
            DataSourceDesc::IngestionExport {
                ingestion_id,
                external_reference: _,
                details,
                data_config,
            } => {
                // TODO(parkmycar): We should probably check the type here, but I'm not sure if
                // this will always be a Source or a Table.
                let ingestion_id = self.catalog().get_entry(&ingestion_id).latest_global_id();

                DataSource::IngestionExport {
                    ingestion_id,
                    details,
                    data_config: data_config.into_inline_connection(self.catalog().state()),
                }
            }
            DataSourceDesc::Progress => DataSource::Progress,
            DataSourceDesc::Webhook { .. } => DataSource::Webhook,
            DataSourceDesc::Introspection(_) | DataSourceDesc::Catalog => {
                unreachable!("cannot create sources with internal data sources")
            }
        };

        storage_collections_to_create.insert(
            source.global_id,
            CollectionDescription {
                desc: source.desc.clone(),
                data_source,
                timeline: Some(source.timeline),
                since: None,
                primary: None,
            },
        );

        // Initialize read policies for the source
        for (compaction_window, catalog_ids) in compaction_windows {
            let compaction_ids = storage_policies_to_initialize
                .entry(compaction_window)
                .or_default();

            let gids = catalog_ids
                .into_iter()
                .map(|item_id| self.catalog().get_entry(&item_id).global_ids())
                .flatten();
            compaction_ids.extend(gids);
        }

        Ok(())
    }

    /// Handles altering a connection by collecting all the dependent sources,
    /// sinks, and tables that need their connection updated.
    ///
    /// This mirrors the logic from `sequence_alter_connection_stage_finish` but
    /// collects the changes into batched collections for application after all
    /// implications are processed.
    #[instrument(level = "debug")]
    fn handle_alter_connection(
        &self,
        connection_id: CatalogItemId,
        connection: Connection,
        vpc_endpoints_to_create: &mut Vec<(CatalogItemId, VpcEndpointConfig)>,
        source_connections_to_alter: &mut BTreeMap<
            GlobalId,
            GenericSourceConnection<InlinedConnection>,
        >,
        sink_connections_to_alter: &mut BTreeMap<GlobalId, StorageSinkConnection>,
        source_export_data_configs_to_alter: &mut BTreeMap<GlobalId, SourceExportDataConfig>,
    ) {
        use std::collections::VecDeque;

        // Handle AWS PrivateLink connections by queueing VPC endpoint creation.
        if let ConnectionDetails::AwsPrivatelink(ref privatelink) = connection.details {
            let spec = VpcEndpointConfig {
                aws_service_name: privatelink.service_name.to_owned(),
                availability_zone_ids: privatelink.availability_zones.to_owned(),
            };
            vpc_endpoints_to_create.push((connection_id, spec));
        }

        // Walk the dependency graph to find all sources, sinks, and tables
        // that depend on this connection (directly or transitively through
        // other connections).
        let mut connections_to_process = VecDeque::new();
        connections_to_process.push_front(connection_id.clone());

        while let Some(id) = connections_to_process.pop_front() {
            for dependent_id in self.catalog().get_entry(&id).used_by() {
                let dependent_entry = self.catalog().get_entry(dependent_id);
                match dependent_entry.item() {
                    CatalogItem::Connection(_) => {
                        // Connections can depend on other connections (e.g., a
                        // Kafka connection using an SSH tunnel connection).
                        // Process these transitively.
                        connections_to_process.push_back(*dependent_id);
                    }
                    CatalogItem::Source(source) => {
                        let desc = match &dependent_entry
                            .source()
                            .expect("known to be source")
                            .data_source
                        {
                            DataSourceDesc::Ingestion { desc, .. }
                            | DataSourceDesc::OldSyntaxIngestion { desc, .. } => {
                                desc.clone().into_inline_connection(self.catalog().state())
                            }
                            DataSourceDesc::IngestionExport { .. }
                            | DataSourceDesc::Introspection(_)
                            | DataSourceDesc::Progress
                            | DataSourceDesc::Webhook { .. }
                            | DataSourceDesc::Catalog => {
                                // Only ingestions reference connections directly.
                                continue;
                            }
                        };

                        source_connections_to_alter.insert(source.global_id, desc.connection);
                    }
                    CatalogItem::Sink(sink) => {
                        let export = dependent_entry.sink().expect("known to be sink");
                        sink_connections_to_alter.insert(
                            sink.global_id,
                            export
                                .connection
                                .clone()
                                .into_inline_connection(self.catalog().state()),
                        );
                    }
                    CatalogItem::Table(table) => {
                        // This is a source-fed table that references a schema
                        // registry connection as part of its encoding/data
                        // config.
                        if let Some((_, _, _, export_data_config)) =
                            dependent_entry.source_export_details()
                        {
                            let data_config = export_data_config.clone();
                            source_export_data_configs_to_alter.insert(
                                table.global_id_writes(),
                                data_config.into_inline_connection(self.catalog().state()),
                            );
                        }
                    }
                    CatalogItem::Log(_)
                    | CatalogItem::View(_)
                    | CatalogItem::MaterializedView(_)
                    | CatalogItem::Index(_)
                    | CatalogItem::Type(_)
                    | CatalogItem::Func(_)
                    | CatalogItem::Secret(_)
                    | CatalogItem::MetricSink(_) => {
                        // Other item types don't have connection dependencies
                        // that need updating.
                    }
                }
            }
        }
    }

    async fn handle_create_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        role: ClusterRole,
        cluster_name: String,
        replica_name: String,
        replica_config: ReplicaConfig,
    ) {
        let enable_worker_core_affinity =
            self.catalog().system_config().enable_worker_core_affinity();

        // This replica's scoped (replica-local) overrides were pushed into the
        // controller's per-replica layer before this loop, by the
        // replica-scoped-configuration implication, so the replica's first
        // configuration replays with them. Render-frozen flags make a later push
        // too late, which is why the push precedes `create_replica`.

        self.controller
            .create_replica(
                cluster_id,
                replica_id,
                cluster_name,
                replica_name,
                role,
                replica_config,
                enable_worker_core_affinity,
            )
            .expect("creating replicas must not fail");

        self.install_introspection_subscribes(cluster_id, replica_id)
            .await;
        self.install_metric_sinks(cluster_id, replica_id).await;
    }
}

fn partition_cluster_replica_drops(
    clusters_with_creates: &BTreeSet<ClusterId>,
    drops: Vec<(ClusterId, ReplicaId)>,
) -> (Vec<(ClusterId, ReplicaId)>, Vec<(ClusterId, ReplicaId)>) {
    drops
        .into_iter()
        .partition(|(cluster_id, _)| clusters_with_creates.contains(cluster_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn mixed_replica_drops_are_applied_before_creates() {
        let c1 = ClusterId::user(1).expect("valid id");
        let c2 = ClusterId::user(2).expect("valid id");
        let r1 = ReplicaId::User(1);
        let r2 = ReplicaId::User(2);
        let creates = BTreeSet::from([c1]);

        let (before_creates, deferred) =
            partition_cluster_replica_drops(&creates, vec![(c1, r1), (c2, r2)]);

        assert_eq!(before_creates, vec![(c1, r1)]);
        assert_eq!(deferred, vec![(c2, r2)]);
    }
}
