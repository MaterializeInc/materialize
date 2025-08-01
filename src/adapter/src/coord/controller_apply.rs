// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to applying [CatalogSideEffects](CatalogSideEffect) to
//! controller(s).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use fail::fail_point;
use itertools::Itertools;
use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::memory::objects::{DataSourceDesc, Table, TableDataSource};
use mz_compute_client::protocol::response::PeekResponse;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::instrument;
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_postgres_util::tunnel::PostgresFlavor;
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use mz_sql::plan::ConnectionDetails;
use mz_storage_client::controller::{CollectionDescription, DataSource};
use mz_storage_types::connections::PostgresConnection;
use mz_storage_types::connections::inline::IntoInlineConnection;
use tracing::{Instrument, info_span, warn};

use crate::active_compute_sink::ActiveComputeSinkRetireReason;
use crate::catalog::side_effects::CatalogSideEffect;
use crate::coord::Coordinator;
use crate::coord::statement_logging::StatementLoggingId;
use crate::coord::timeline::TimelineState;
use crate::statement_logging::StatementEndedExecutionReason;
use crate::{AdapterError, CollectionIdBundle, ExecuteContext, ResultExt};

impl Coordinator {
    #[instrument(level = "debug")]
    pub async fn controller_apply_side_effects(
        &mut self,
        mut ctx: Option<&mut ExecuteContext>,
        updates: Vec<CatalogSideEffect>,
    ) -> Result<(), AdapterError> {
        let mut tables_to_drop = BTreeSet::new();
        let mut sources_to_drop = vec![];
        let mut replication_slots_to_drop: Vec<(PostgresConnection, String)> = vec![];
        let mut storage_sink_gids_to_drop = vec![];
        let mut indexes_to_drop = vec![];
        let mut materialized_views_to_drop = vec![];
        let mut continual_tasks_to_drop = vec![];
        let mut view_gids_to_drop = vec![];
        let mut secrets_to_drop = vec![];
        let mut vpc_endpoints_to_drop = vec![];
        let mut clusters_to_drop = vec![];
        let mut cluster_replicas_to_drop = vec![];
        let mut compute_sinks_to_drop = BTreeMap::new();
        let mut peeks_to_drop = vec![];
        let mut copies_to_drop = vec![];

        let mut storage_collections_to_create = BTreeMap::new();
        let mut storage_policies_to_initialize = BTreeMap::new();
        let mut execution_timestamps_to_set = BTreeSet::new();

        let dropped_items = Self::collect_dropped_items(&updates);

        for update in updates {
            tracing::info!(?update, "have to apply!");
            match update {
                CatalogSideEffect::CreateTable(id, global_id, table) => {
                    if dropped_items.contains(&id) {
                        continue;
                    }

                    self.handle_create_table(
                        &mut ctx,
                        &mut storage_collections_to_create,
                        &mut storage_policies_to_initialize,
                        &mut execution_timestamps_to_set,
                        id,
                        global_id,
                        table,
                    )
                    .await?
                }
                CatalogSideEffect::DropTable(id, global_id) => {
                    if !dropped_items.contains(&id) {
                        continue;
                    }
                    tables_to_drop.insert((id, global_id));
                }
                CatalogSideEffect::DropSource(id, gid, _source) => {
                    sources_to_drop.push((id, gid));
                }
                CatalogSideEffect::DropReplicationSlot(connection, slot) => {
                    replication_slots_to_drop.push((connection, slot));
                }
                CatalogSideEffect::DropSink(_id, gid) => {
                    storage_sink_gids_to_drop.push(gid);
                }
                CatalogSideEffect::DropIndex(_id, gid, index) => {
                    indexes_to_drop.push((index.cluster_id, gid));
                }
                CatalogSideEffect::DropMaterializedView(_id, gid, mv) => {
                    materialized_views_to_drop.push((mv.cluster_id, gid));
                }
                CatalogSideEffect::DropContinualTask(id, gid, ct) => {
                    continual_tasks_to_drop.push((id, ct.cluster_id, gid));
                }
                CatalogSideEffect::DropView(_id, gid) => {
                    view_gids_to_drop.push(gid);
                }
                CatalogSideEffect::DropSecret(id, _gid) => {
                    secrets_to_drop.push(id);
                }
                CatalogSideEffect::DropConnection(id, _gid, connection) => {
                    match connection.details {
                        // SSH connections have an associated secret that should be dropped
                        ConnectionDetails::Ssh { .. } => {
                            secrets_to_drop.push(id);
                        }
                        // AWS PrivateLink connections have an associated
                        // VpcEndpoint K8S resource that should be dropped
                        ConnectionDetails::AwsPrivatelink(_) => {
                            vpc_endpoints_to_drop.push(id);
                        }
                        _ => (),
                    }
                }
                CatalogSideEffect::DropCluster(cluster_id) => {
                    clusters_to_drop.push(cluster_id);
                }
                CatalogSideEffect::DropClusterReplica {
                    cluster_id,
                    replica_id,
                } => {
                    // Drop the cluster replica itself.
                    cluster_replicas_to_drop.push((cluster_id, replica_id));
                }
            }
        }

        self.create_storage_collections(
            storage_collections_to_create,
            storage_policies_to_initialize,
            execution_timestamps_to_set,
        )
        .await?;

        let collections_to_drop: BTreeSet<_> = sources_to_drop
            .iter()
            .map(|(_, gid)| *gid)
            .chain(tables_to_drop.iter().map(|(_, gid)| *gid))
            .chain(storage_sink_gids_to_drop.iter().copied())
            .chain(indexes_to_drop.iter().map(|(_, id)| *id))
            .chain(materialized_views_to_drop.iter().map(|(_, id)| *id))
            .chain(continual_tasks_to_drop.iter().map(|(_, _, gid)| *gid))
            .chain(view_gids_to_drop.iter().copied())
            .collect();

        // Clean up any active compute sinks like subscribes or copy to-s that rely on dropped relations or clusters.
        for (sink_id, sink) in &self.active_compute_sinks {
            let cluster_id = sink.cluster_id();
            //let conn_id = &sink.connection_id();
            if let Some(id) = sink
                .depends_on()
                .iter()
                .find(|id| collections_to_drop.contains(id))
            {
                // WIP: We can't get the full name of the referenced collection
                // because it has been dropped from the catalog by now. Is this
                // okay?
                //let entry = self.catalog().get_entry_by_global_id(id);
                //let name = self
                //    .catalog()
                //    .resolve_full_name(entry.name(), Some(conn_id))
                //    .to_string();
                compute_sinks_to_drop.insert(
                    *sink_id,
                    ActiveComputeSinkRetireReason::DependencyDropped(format!("relation {}", id,)),
                );
            } else if clusters_to_drop.contains(&cluster_id) {
                // WIP: We can't get the full name of the referenced collection
                // because it has been dropped from the catalog by now. Is this
                // okay?
                //let name = self.catalog().get_cluster(cluster_id).name();
                compute_sinks_to_drop.insert(
                    *sink_id,
                    ActiveComputeSinkRetireReason::DependencyDropped(format!(
                        "cluster {}",
                        cluster_id,
                    )),
                );
            }
        }

        // Clean up any pending peeks that rely on dropped relations or clusters.
        for (uuid, pending_peek) in &self.pending_peeks {
            if let Some(id) = pending_peek
                .depends_on
                .iter()
                .find(|id| collections_to_drop.contains(id))
            {
                // WIP: We can't get the full name of the referenced collection
                // because it has been dropped from the catalog by now. Is this
                // okay?
                //let entry = self.catalog().get_entry_by_global_id(id);
                //let name = self
                //    .catalog()
                //    .resolve_full_name(entry.name(), Some(&pending_peek.conn_id));
                peeks_to_drop.push((format!("relation {}", id), uuid.clone()));
            } else if clusters_to_drop.contains(&pending_peek.cluster_id) {
                // WIP: We can't get the full name of the referenced collection
                // because it has been dropped from the catalog by now. Is this
                // okay?
                //let name = self.catalog().get_cluster(pending_peek.cluster_id).name();
                peeks_to_drop.push((format!("cluster {}", pending_peek.cluster_id), uuid.clone()));
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

        let storage_ids_to_drop: BTreeSet<_> = sources_to_drop
            .iter()
            .map(|(_id, gid)| gid)
            .chain(storage_sink_gids_to_drop.iter())
            .chain(tables_to_drop.iter().map(|(_id, gid)| gid))
            .chain(materialized_views_to_drop.iter().map(|(_, id)| id))
            .chain(continual_tasks_to_drop.iter().map(|(_, _, gid)| gid))
            .copied()
            .collect();

        let compute_ids_to_drop: BTreeSet<_> = indexes_to_drop
            .iter()
            .copied()
            .chain(materialized_views_to_drop.iter().copied())
            .chain(
                continual_tasks_to_drop
                    .iter()
                    .map(|(_, cluster_id, gid)| (*cluster_id, *gid)),
            )
            .collect();

        // Gather resources that we have to remove from timeline state and
        // pre-check if any Timelines become empty, when we drop the specified
        // storage and compute resources.
        //
        // Note: We only apply these changes below.
        let mut timeline_id_bundles = BTreeMap::new();

        for (timeline, TimelineState { read_holds, .. }) in &self.global_timelines {
            let mut id_bundle = CollectionIdBundle::default();

            for storage_id in read_holds.storage_ids() {
                if storage_ids_to_drop.contains(&storage_id) {
                    id_bundle.storage_ids.insert(storage_id);
                }
            }

            for (instance_id, id) in read_holds.compute_ids() {
                if compute_ids_to_drop.contains(&(instance_id, id))
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
            let TimelineState { read_holds, .. } = self
                .global_timelines
                .get(&timeline)
                .expect("all timeslines have a timestamp oracle");

            let empty = read_holds.id_bundle().difference(&id_bundle).is_empty();
            timeline_associations.insert(timeline, (empty, id_bundle));
        }

        // No error returns are allowed after this point. Enforce this at compile time
        // by using this odd structure so we don't accidentally add a stray `?`.
        //
        // WIP: Do we need to cargo cult this structure?
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
                let ts = self.get_local_write_ts().await;
                self.drop_tables(tables_to_drop.into_iter().collect_vec(), ts.timestamp);
            }

            if !sources_to_drop.is_empty() {
                self.drop_sources(sources_to_drop);
            }

            if !storage_sink_gids_to_drop.is_empty() {
                self.drop_storage_sinks(storage_sink_gids_to_drop);
            }

            if !compute_sinks_to_drop.is_empty() {
                self.retire_compute_sinks(compute_sinks_to_drop).await;
            }

            if !peeks_to_drop.is_empty() {
                for (dropped_name, uuid) in peeks_to_drop {
                    if let Some(pending_peek) = self.remove_pending_peek(&uuid) {
                        let cancel_reason = PeekResponse::Error(format!(
                            "query could not complete because {dropped_name} was dropped"
                        ));
                        self.controller
                            .compute
                            .cancel_peek(pending_peek.cluster_id, uuid, cancel_reason)
                            .unwrap_or_terminate("unable to cancel peek");
                        self.retire_execution(
                            StatementEndedExecutionReason::Canceled,
                            pending_peek.ctx_extra,
                        );
                    }
                }
            }

            if !copies_to_drop.is_empty() {
                for conn_id in copies_to_drop {
                    self.cancel_pending_copy(&conn_id);
                }
            }

            if !indexes_to_drop.is_empty() {
                self.drop_indexes(indexes_to_drop);
            }

            if !materialized_views_to_drop.is_empty() {
                self.drop_materialized_views(materialized_views_to_drop);
            }

            if !continual_tasks_to_drop.is_empty() {
                self.drop_continual_tasks(continual_tasks_to_drop);
            }

            if !vpc_endpoints_to_drop.is_empty() {
                self.drop_vpc_endpoints_in_background(vpc_endpoints_to_drop)
            }

            if !cluster_replicas_to_drop.is_empty() {
                for (cluster_id, replica_id) in cluster_replicas_to_drop {
                    self.drop_replica(cluster_id, replica_id);
                }
            }
            if !clusters_to_drop.is_empty() {
                for cluster_id in clusters_to_drop {
                    self.controller.drop_cluster(cluster_id);
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
                let secrets_controller = Arc::clone(&self.secrets_controller);
                let secrets_reader = Arc::clone(self.secrets_reader());
                let storage_config = self.controller.storage.config().clone();

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

                                // Yugabyte does not support waiting for the replication to become
                                // inactive before dropping it. That is fine though because in that
                                // case dropping will fail and we'll go around the retry loop which
                                // is effectively the same as waiting 60 seconds.
                                let should_wait = match connection.flavor {
                                    PostgresFlavor::Vanilla => true,
                                    PostgresFlavor::Yugabyte => false,
                                };
                                mz_postgres_util::drop_replication_slots(
                                    &ssh_tunnel_manager,
                                    config.clone(),
                                    &[(&replication_slot_name, should_wait)],
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
                    // because those replication slots may.
                    //
                    // It's okay if we crash before processing the secret drops,
                    // as we look for and remove any orphaned secrets during
                    // startup.
                    fail_point!("drop_secrets");
                    for secret in secrets_to_drop {
                        if let Err(e) = secrets_controller.delete(secret).await {
                            warn!("Dropping secrets has encountered an error: {}", e);
                        }
                    }
                }
            });
        }
        .instrument(info_span!("coord::controller_apply_updates::finalize"))
        .await;

        Ok(())
    }

    fn collect_dropped_items(side_effects: &[CatalogSideEffect]) -> BTreeSet<CatalogItemId> {
        let mut liveness_counts = BTreeMap::new();

        for side_effect in side_effects {
            match side_effect {
                CatalogSideEffect::CreateTable(catalog_item_id, _global_id, _table) => {
                    liveness_counts
                        .entry(*catalog_item_id)
                        .and_modify(|count| *count += 1)
                        .or_insert_with(|| 1);
                }
                CatalogSideEffect::DropTable(catalog_item_id, ..)
                | CatalogSideEffect::DropSource(catalog_item_id, ..)
                | CatalogSideEffect::DropView(catalog_item_id, ..)
                | CatalogSideEffect::DropMaterializedView(catalog_item_id, ..)
                | CatalogSideEffect::DropContinualTask(catalog_item_id, ..)
                | CatalogSideEffect::DropSink(catalog_item_id, ..)
                | CatalogSideEffect::DropIndex(catalog_item_id, ..)
                | CatalogSideEffect::DropSecret(catalog_item_id, ..)
                | CatalogSideEffect::DropConnection(catalog_item_id, ..) => {
                    liveness_counts
                        .entry(*catalog_item_id)
                        .and_modify(|count| *count -= 1)
                        .or_insert_with(|| -1);
                }
                CatalogSideEffect::DropReplicationSlot(_pg_conn, ..) => (),
                CatalogSideEffect::DropCluster(_storage_instance_id) => (),
                CatalogSideEffect::DropClusterReplica {
                    cluster_id: _,
                    replica_id: _,
                } => (),
            }
        }

        liveness_counts
            .into_iter()
            .filter(|(_item, count)| *count < 0)
            .map(|(item, _count)| item)
            .collect()
    }

    #[instrument(level = "debug")]
    async fn create_storage_collections(
        &mut self,
        storage_collections_to_create: BTreeMap<GlobalId, CollectionDescription<Timestamp>>,
        storage_policies_to_initialize: BTreeMap<CompactionWindow, BTreeSet<CatalogItemId>>,
        execution_timestamps_to_set: BTreeSet<StatementLoggingId>,
    ) -> Result<(), AdapterError> {
        // If we have tables, determine the initial validity for the table.
        let register_ts = self.get_local_write_ts().await.timestamp;

        // After acquiring `register_ts` but before using it, we need to
        // be sure we're still the leader. Otherwise a new generation
        // may also be trying to use `register_ts` for a different
        // purpose.
        //
        // See #28216.
        self.catalog
            .confirm_leadership()
            .await
            .unwrap_or_terminate("unable to confirm leadership");

        for id in execution_timestamps_to_set {
            self.set_statement_execution_timestamp(id, register_ts);
        }

        let storage_metadata = self.catalog.state().storage_metadata();

        self.controller
            .storage
            .create_collections(
                storage_metadata,
                Some(register_ts),
                storage_collections_to_create.into_iter().collect_vec(),
            )
            .await
            .unwrap_or_terminate("cannot fail to create collections");

        self.apply_local_write(register_ts).await;

        for (compaction_window, global_ids) in storage_policies_to_initialize {
            self.initialize_storage_read_policies(global_ids, compaction_window)
                .await;
        }

        Ok(())
    }

    #[instrument(level = "debug")]
    async fn handle_create_table(
        &mut self,
        ctx: &mut Option<&mut ExecuteContext>,
        storage_collections_to_create: &mut BTreeMap<GlobalId, CollectionDescription<Timestamp>>,
        storage_policies_to_initialize: &mut BTreeMap<CompactionWindow, BTreeSet<CatalogItemId>>,
        execution_timestamps_to_set: &mut BTreeSet<StatementLoggingId>,
        table_id: CatalogItemId,
        global_id: GlobalId,
        table: Table,
    ) -> Result<(), AdapterError> {
        // The table data_source determines whether this table will be written to
        // by environmentd (e.g. with INSERT INTO statements) or by the storage layer
        // (e.g. a source-fed table).
        match table.data_source {
            TableDataSource::TableWrites { defaults: _ } => {
                let collection_desc = CollectionDescription::for_table(table.desc.latest(), None);

                storage_collections_to_create.insert(global_id, collection_desc);
                if let Some(id) = ctx.as_ref().and_then(|ctx| ctx.extra().contents()) {
                    execution_timestamps_to_set.insert(id);
                }

                let compaction_window = table
                    .custom_logical_compaction_window
                    .unwrap_or(CompactionWindow::Default);
                let ids = storage_policies_to_initialize
                    .entry(compaction_window)
                    .or_default();
                ids.insert(table_id);
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
                        // TODO: It's a little weird that a table will be present in this
                        // source status collection, we might want to split out into a separate
                        // status collection.
                        let status_collection_id =
                            self.catalog().resolve_builtin_storage_collection(
                                &mz_catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
                            );

                        let global_ingestion_id =
                            self.catalog().get_entry(&ingestion_id).latest_global_id();
                        let global_status_collection_id = self
                            .catalog()
                            .get_entry(&status_collection_id)
                            .latest_global_id();

                        let collection_desc = CollectionDescription::<Timestamp> {
                            desc: table.desc.latest(),
                            data_source: DataSource::IngestionExport {
                                ingestion_id: global_ingestion_id,
                                details,
                                data_config: data_config
                                    .into_inline_connection(self.catalog.state()),
                            },
                            since: None,
                            status_collection_id: Some(global_status_collection_id),
                            timeline: Some(timeline),
                        };

                        storage_collections_to_create.insert(global_id, collection_desc);

                        let read_policies = self
                            .catalog()
                            .state()
                            .source_compaction_windows(vec![table_id]);
                        for (compaction_window, global_ids) in read_policies {
                            let compaction_ids = storage_policies_to_initialize
                                .entry(compaction_window)
                                .or_default();

                            compaction_ids.extend(global_ids.into_iter());
                        }
                    }
                    _ => unreachable!("CREATE TABLE data source got {:?}", data_source_desc),
                }
            }
        }

        Ok(())
    }

    /// A convenience method for dropping tables.
    pub(crate) fn drop_tables(&mut self, tables: Vec<(CatalogItemId, GlobalId)>, ts: Timestamp) {
        for (item_id, _gid) in &tables {
            self.active_webhooks.remove(item_id);
        }

        let storage_metadata = self.catalog.state().storage_metadata();
        let table_gids = tables.into_iter().map(|(_id, gid)| gid).collect();
        self.controller
            .storage
            .drop_tables(storage_metadata, table_gids, ts)
            .unwrap_or_terminate("cannot fail to drop tables");
    }
}
