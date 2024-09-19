// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to deriving controller commands from [catalog
//! changes](ParsedStateUpdate) and applying them to the controller(s).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::consolidation::consolidate_updates;
use fail::fail_point;
use itertools::Itertools;
use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::builtin;
use mz_catalog::memory::objects::{
    CatalogItem, Cluster, ClusterReplica, Connection, ContinualTask, DataSourceDesc, Index,
    MaterializedView, Secret, Sink, Source, StateDiff, Table, TableDataSource, View,
};
use mz_compute_client::protocol::response::PeekResponse;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::instrument;
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_postgres_util::tunnel::PostgresFlavor;
use mz_repr::{CatalogItemId, GlobalId, RelationVersion, RelationVersionSelector, Timestamp};
use mz_sql::plan::{ConnectionDetails, Ingestion};
use mz_storage_client::controller::{CollectionDescription, DataSource};
use mz_storage_types::connections::PostgresConnection;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::sources::GenericSourceConnection;
use tracing::{Instrument, info_span, warn};

use crate::active_compute_sink::ActiveComputeSinkRetireReason;
use crate::coord::Coordinator;
use crate::coord::controller_commands::controller_state_updates::{
    ParsedStateUpdate, ParsedStateUpdateKind,
};
use crate::coord::statement_logging::StatementLoggingId;
use crate::coord::timeline::TimelineState;
use crate::statement_logging::StatementEndedExecutionReason;
use crate::{AdapterError, CollectionIdBundle, ExecuteContext, ResultExt};

pub mod controller_state_updates;

impl Coordinator {
    #[instrument(level = "debug")]
    pub async fn controller_apply_catalog_updates(
        &mut self,
        ctx: Option<&mut ExecuteContext>,
        catalog_updates: Vec<ParsedStateUpdate>,
    ) -> Result<(), AdapterError> {
        let mut controller_commands: BTreeMap<CatalogItemId, ControllerCommand> = BTreeMap::new();
        let mut cluster_commands: BTreeMap<ClusterId, ControllerCommand> = BTreeMap::new();
        let mut cluster_replica_commands: BTreeMap<(ClusterId, ReplicaId), ControllerCommand> =
            BTreeMap::new();

        let catalog_updates = Self::consolidate_updates(catalog_updates);

        for update in catalog_updates {
            tracing::info!(?update, "parsed state update");
            match &update.kind {
                ParsedStateUpdateKind::Item {
                    durable_item,
                    parsed_item: _,
                    connection: _,
                } => {
                    let entry = controller_commands
                        .entry(durable_item.id.clone())
                        .or_insert_with(|| ControllerCommand::None);
                    entry.absorb(update);
                }
                ParsedStateUpdateKind::TemporaryItem {
                    parsed_item,
                    connection: _,
                } => {
                    let entry = controller_commands
                        .entry(parsed_item.id.clone())
                        .or_insert_with(|| ControllerCommand::None);
                    entry.absorb(update);
                }
                ParsedStateUpdateKind::Cluster {
                    durable_cluster,
                    parsed_cluster: _,
                } => {
                    let entry = cluster_commands
                        .entry(durable_cluster.id)
                        .or_insert_with(|| ControllerCommand::None);
                    entry.absorb(update.clone());
                }
                ParsedStateUpdateKind::ClusterReplica {
                    durable_cluster_replica,
                    parsed_cluster_replica: _,
                } => {
                    let entry = cluster_replica_commands
                        .entry((
                            durable_cluster_replica.cluster_id,
                            durable_cluster_replica.replica_id,
                        ))
                        .or_insert_with(|| ControllerCommand::None);
                    entry.absorb(update.clone());
                }
            }
        }

        self.controller_apply_commands(
            ctx,
            controller_commands.into_iter().collect_vec(),
            cluster_commands.into_iter().collect_vec(),
            cluster_replica_commands.into_iter().collect_vec(),
        )
        .await?;

        Ok(())
    }

    /// It can happen that the sequencing logic creates "fluctuating" updates
    /// for a given catalog ID. For example, when doing a `DROP OWNED BY ...`,
    /// for a table, there will be a retraction of the original table state,
    /// then an addition for the same table but stripped of some of the roles
    /// and access things, and then a retraction for that intermediate table
    /// state. By consolidating, the intermediate state addition/retraction will
    /// cancel out and we'll only see the retraction for the original state.
    fn consolidate_updates(catalog_updates: Vec<ParsedStateUpdate>) -> Vec<ParsedStateUpdate> {
        let mut updates: Vec<(ParsedStateUpdateKind, Timestamp, mz_repr::Diff)> = catalog_updates
            .into_iter()
            .map(|update| (update.kind, update.ts, update.diff.into()))
            .collect_vec();

        consolidate_updates(&mut updates);

        updates
            .into_iter()
            .filter(|(_kind, _ts, diff)| *diff != 0.into())
            .map(|(kind, ts, diff)| ParsedStateUpdate {
                kind,
                ts,
                diff: diff
                    .try_into()
                    .expect("catalog state cannot have diff other than -1 or 1"),
            })
            .collect_vec()
    }

    #[instrument(level = "debug")]
    async fn controller_apply_commands(
        &mut self,
        mut ctx: Option<&mut ExecuteContext>,
        commands: Vec<(CatalogItemId, ControllerCommand)>,
        cluster_commands: Vec<(ClusterId, ControllerCommand)>,
        cluster_replica_commands: Vec<((ClusterId, ReplicaId), ControllerCommand)>,
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

        // Separate collections for tables (which need write timestamps) and sources (which don't)
        let mut table_collections_to_create = BTreeMap::new();
        let mut source_collections_to_create = BTreeMap::new();
        let mut storage_policies_to_initialize = BTreeMap::new();
        let mut execution_timestamps_to_set = BTreeSet::new();

        for (catalog_id, command) in commands {
            tracing::info!(?command, "have to apply!");
            match command {
                ControllerCommand::Table(TableControllerCommand::AddTable(table)) => {
                    let global_ids = table.global_ids();
                    // WIP: Handle versions!
                    for global_id in global_ids {
                        self.handle_create_table(
                            &mut ctx,
                            &mut table_collections_to_create,
                            &mut storage_policies_to_initialize,
                            &mut execution_timestamps_to_set,
                            catalog_id,
                            global_id,
                            table.clone(),
                        )
                        .await?
                    }
                }
                ControllerCommand::Table(TableControllerCommand::AlterTable {
                    prev_table,
                    new_table,
                }) => self.handle_alter_table(prev_table, new_table).await?,

                ControllerCommand::Table(TableControllerCommand::DropTable(table)) => {
                    let global_ids = table.global_ids();
                    // WIP: Handle versions!
                    for global_id in global_ids {
                        tables_to_drop.insert((catalog_id, global_id));
                    }
                }
                ControllerCommand::Source(SourceControllerCommand::AddSource(
                    source,
                    _connection,
                )) => {
                    // Get the compaction windows for all sources with this catalog_id
                    // This replicates the logic from sequence_create_source where it collects
                    // all item_ids and gets their compaction windows
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
                ControllerCommand::Source(SourceControllerCommand::AlterSource {
                    prev_source,
                    prev_connection: _,
                    new_source,
                    new_connection: _,
                }) => {
                    tracing::info!(
                        ?prev_source,
                        ?new_source,
                        "not handling AlterSource in here yet"
                    );
                }
                ControllerCommand::Source(SourceControllerCommand::DropSource(
                    source,
                    connection,
                )) => {
                    let global_id = source.global_id();
                    sources_to_drop.push((catalog_id, global_id));

                    if let DataSourceDesc::Ingestion { ingestion_desc, .. } = &source.data_source {
                        match &ingestion_desc.desc.connection {
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
                ControllerCommand::Sink(SinkControllerCommand::AddSink(sink)) => {
                    tracing::info!(?sink, "not handling AddSink in here yet");
                }
                ControllerCommand::Sink(SinkControllerCommand::AlterSink {
                    prev_sink,
                    new_sink,
                }) => {
                    tracing::info!(?prev_sink, ?new_sink, "not handling AlterSink in here yet");
                }
                ControllerCommand::Sink(SinkControllerCommand::DropSink(sink)) => {
                    storage_sink_gids_to_drop.push(sink.global_id());
                }
                ControllerCommand::Index(IndexControllerCommand::AddIndex(index)) => {
                    tracing::info!(?index, "not handling AddIndex in here yet");
                }
                ControllerCommand::Index(IndexControllerCommand::AlterIndex {
                    prev_index,
                    new_index,
                }) => {
                    tracing::info!(
                        ?prev_index,
                        ?new_index,
                        "not handling AlterIndex in here yet"
                    );
                }
                ControllerCommand::Index(IndexControllerCommand::DropIndex(index)) => {
                    indexes_to_drop.push((index.cluster_id, index.global_id()));
                }
                ControllerCommand::MaterializedView(
                    MaterializedViewControllerCommand::AddMaterializedView(mv),
                ) => {
                    tracing::info!(?mv, "not handling AddMaterializedView in here yet");
                }
                ControllerCommand::MaterializedView(
                    MaterializedViewControllerCommand::AlterMaterializedView { prev_mv, new_mv },
                ) => {
                    tracing::info!(
                        ?prev_mv,
                        ?new_mv,
                        "not handling AlterMaterializedView in here yet"
                    );
                }
                ControllerCommand::MaterializedView(
                    MaterializedViewControllerCommand::DropMaterializedView(mv),
                ) => {
                    materialized_views_to_drop.push((mv.cluster_id, mv.global_id()));
                }
                ControllerCommand::View(ViewControllerCommand::AddView(view)) => {
                    tracing::info!(?view, "not handling AddView in here yet");
                }
                ControllerCommand::View(ViewControllerCommand::AlterView {
                    prev_view,
                    new_view,
                }) => {
                    tracing::info!(?prev_view, ?new_view, "not handling AlterView in here yet");
                }
                ControllerCommand::View(ViewControllerCommand::DropView(view)) => {
                    view_gids_to_drop.push(view.global_id());
                }
                ControllerCommand::ContinualTask(
                    ContinualTaskControllerCommand::AddContinualTask(ct),
                ) => {
                    tracing::info!(?ct, "not handling AddContinualTask in here yet");
                }
                ControllerCommand::ContinualTask(
                    ContinualTaskControllerCommand::AlterContinualTask { prev_ct, new_ct },
                ) => {
                    tracing::info!(
                        ?prev_ct,
                        ?new_ct,
                        "not handling AlterContinualTask in here yet"
                    );
                }
                ControllerCommand::ContinualTask(
                    ContinualTaskControllerCommand::DropContinualTask(ct),
                ) => {
                    continual_tasks_to_drop.push((catalog_id, ct.cluster_id, ct.global_id()));
                }
                ControllerCommand::Secret(SecretControllerCommand::AddSecret(secret)) => {
                    tracing::info!(?secret, "not handling AddSecret in here yet");
                }
                ControllerCommand::Secret(SecretControllerCommand::AlterSecret {
                    prev_secret,
                    new_secret,
                }) => {
                    tracing::info!(
                        ?prev_secret,
                        ?new_secret,
                        "not handling AlterSecret in here yet"
                    );
                }
                ControllerCommand::Secret(SecretControllerCommand::DropSecret(_secret)) => {
                    secrets_to_drop.push(catalog_id);
                }
                ControllerCommand::Connection(ConnectionControllerCommand::AddConnection(
                    connection,
                )) => {
                    tracing::info!(?connection, "not handling AddConnection in here yet");
                }
                ControllerCommand::Connection(ConnectionControllerCommand::AlterConnection {
                    prev_connection,
                    new_connection,
                }) => {
                    tracing::info!(
                        ?prev_connection,
                        ?new_connection,
                        "not handling AlterConnection in here yet"
                    );
                }
                ControllerCommand::Connection(ConnectionControllerCommand::DropConnection(
                    connection,
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
                command => {
                    tracing::info!(?command, "todo");
                }
            }
        }

        for (cluster_id, command) in cluster_commands {
            tracing::info!(?command, "have cluster command to apply!");
            match command {
                ControllerCommand::Cluster(ClusterControllerCommand::AddCluster(cluster)) => {
                    tracing::info!(?cluster, "not handling AddCluster in here yet");
                }
                ControllerCommand::Cluster(ClusterControllerCommand::AlterCluster {
                    prev_cluster,
                    new_cluster,
                }) => {
                    tracing::info!(
                        ?prev_cluster,
                        ?new_cluster,
                        "not handling AlterCluster in here yet"
                    );
                }
                ControllerCommand::Cluster(ClusterControllerCommand::DropCluster(_cluster)) => {
                    clusters_to_drop.push(cluster_id);
                }
                command => {
                    tracing::info!(?command, "unexpected cluster command");
                }
            }
        }

        for ((cluster_id, replica_id), command) in cluster_replica_commands {
            tracing::info!(?command, "have cluster replica command to apply!");
            match command {
                ControllerCommand::ClusterReplica(
                    ClusterReplicaControllerCommand::AddClusterReplica(replica),
                ) => {
                    tracing::info!(?replica, "not handling AddClusterReplica in here yet");
                }
                ControllerCommand::ClusterReplica(
                    ClusterReplicaControllerCommand::AlterClusterReplica {
                        prev_replica,
                        new_replica,
                    },
                ) => {
                    tracing::info!(
                        ?prev_replica,
                        ?new_replica,
                        "not handling AlterClusterReplica in here yet"
                    );
                }
                ControllerCommand::ClusterReplica(
                    ClusterReplicaControllerCommand::DropClusterReplica(_replica),
                ) => {
                    cluster_replicas_to_drop.push((cluster_id, replica_id));
                }
                command => {
                    tracing::info!(?command, "unexpected cluster replica command");
                }
            }
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
        // have created all the sources/collections. Some of the sources created
        // in this collection might have dependencies on other sources, so the
        // controller must get a chance to install read holds before we set a
        // policy that might make the since advance.
        self.initialize_storage_collections(storage_policies_to_initialize)
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

        // Clean up any active compute sinks like subscribes or copy to-s that
        // rely on dropped relations or clusters.
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
                // WIP: We can't get the full name of the used cluster because
                // it has been dropped from the catalog by now. Is this okay?
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

    #[instrument(level = "debug")]
    async fn create_table_collections(
        &mut self,
        table_collections_to_create: BTreeMap<GlobalId, CollectionDescription<Timestamp>>,
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
                table_collections_to_create.into_iter().collect_vec(),
            )
            .await
            .unwrap_or_terminate("cannot fail to create collections");

        self.apply_local_write(register_ts).await;

        Ok(())
    }

    #[instrument(level = "debug")]
    async fn create_source_collections(
        &mut self,
        source_collections_to_create: BTreeMap<GlobalId, CollectionDescription<Timestamp>>,
    ) -> Result<(), AdapterError> {
        let storage_metadata = self.catalog.state().storage_metadata();

        self.controller
            .storage
            .create_collections(
                storage_metadata,
                None, // Sources don't need a write timestamp
                source_collections_to_create.into_iter().collect_vec(),
            )
            .await
            .unwrap_or_terminate("cannot fail to create collections");

        Ok(())
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
        &mut self,
        ctx: &mut Option<&mut ExecuteContext>,
        storage_collections_to_create: &mut BTreeMap<GlobalId, CollectionDescription<Timestamp>>,
        storage_policies_to_initialize: &mut BTreeMap<CompactionWindow, BTreeSet<GlobalId>>,
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
                ids.insert(global_id);
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
                        let status_collection_id = self
                            .catalog()
                            .resolve_builtin_storage_collection(&builtin::MZ_SOURCE_STATUS_HISTORY);

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

                        let collection_desc = CollectionDescription::<Timestamp> {
                            desc,
                            data_source: DataSource::Webhook,
                            since: None,
                            status_collection_id: None, // Webhook tables don't use status collections
                            timeline: Some(timeline),
                        };

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
        prev_table: Table,
        new_table: Table,
    ) -> Result<(), AdapterError> {
        let existing_gid = prev_table.global_id_writes();
        let new_gid = new_table.global_id_writes();

        if existing_gid == new_gid {
            // It's not an ALTER TABLE as far as the controller is concerned,
            // because we still have the same GlobalId. This is likely a change
            // from an ALTER SWAP.
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

        let register_ts = self.get_local_write_ts().await.timestamp;

        // Alter the table description, creating a "new" collection.
        self.controller
            .storage
            .alter_table_desc(
                existing_gid,
                new_gid,
                new_desc,
                expected_version,
                register_ts,
            )
            .await
            .expect("failed to alter desc of table");

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

        self.apply_local_write(register_ts).await;

        // Alter is complete! We can drop our read hold.
        drop(existing_table_read_hold);

        Ok(())
    }

    #[instrument(level = "debug")]
    async fn handle_create_source(
        &mut self,
        storage_collections_to_create: &mut BTreeMap<GlobalId, CollectionDescription<Timestamp>>,
        storage_policies_to_initialize: &mut BTreeMap<CompactionWindow, BTreeSet<GlobalId>>,
        _item_id: CatalogItemId,
        source: Source,
        compaction_windows: BTreeMap<CompactionWindow, BTreeSet<CatalogItemId>>,
    ) -> Result<(), AdapterError> {
        let source_status_item_id = self
            .catalog()
            .resolve_builtin_storage_collection(&builtin::MZ_SOURCE_STATUS_HISTORY);
        let source_status_collection_id = Some(
            self.catalog()
                .get_entry(&source_status_item_id)
                .latest_global_id(),
        );

        let (data_source, status_collection_id) = match source.data_source {
            DataSourceDesc::Ingestion {
                ingestion_desc:
                    Ingestion {
                        desc,
                        progress_subsource,
                    },
                cluster_id,
            } => {
                let desc = desc.into_inline_connection(self.catalog().state());
                // TODO(parkmycar): We should probably check the type here, but I'm not
                // sure if this will always be a Source or a Table.
                let progress_subsource = self
                    .catalog()
                    .get_entry(&progress_subsource)
                    .latest_global_id();

                let ingestion = mz_storage_types::sources::IngestionDescription::new(
                    desc,
                    cluster_id,
                    progress_subsource,
                );

                (
                    DataSource::Ingestion(ingestion),
                    source_status_collection_id,
                )
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
                (
                    DataSource::IngestionExport {
                        ingestion_id,
                        details,
                        data_config: data_config.into_inline_connection(self.catalog().state()),
                    },
                    source_status_collection_id,
                )
            }
            DataSourceDesc::Progress => (DataSource::Progress, None),
            DataSourceDesc::Webhook { .. } => (DataSource::Webhook, None),
            DataSourceDesc::Introspection(_) => {
                unreachable!("cannot create sources with introspection data sources")
            }
        };

        storage_collections_to_create.insert(
            source.global_id,
            CollectionDescription::<Timestamp> {
                desc: source.desc.clone(),
                data_source,
                timeline: Some(source.timeline),
                since: None,
                status_collection_id,
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
}

#[derive(Debug, Clone)]
enum ControllerCommand {
    None,
    Table(TableControllerCommand),
    Source(SourceControllerCommand),
    Sink(SinkControllerCommand),
    Index(IndexControllerCommand),
    MaterializedView(MaterializedViewControllerCommand),
    View(ViewControllerCommand),
    ContinualTask(ContinualTaskControllerCommand),
    Secret(SecretControllerCommand),
    Connection(ConnectionControllerCommand),
    Cluster(ClusterControllerCommand),
    ClusterReplica(ClusterReplicaControllerCommand),
}

#[derive(Debug, Clone)]
enum TableControllerCommand {
    AddTable(Table),
    AlterTable { prev_table: Table, new_table: Table },
    DropTable(Table),
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum SourceControllerCommand {
    AddSource(Source, Option<GenericSourceConnection>),
    AlterSource {
        prev_source: Source,
        prev_connection: Option<GenericSourceConnection>,
        new_source: Source,
        new_connection: Option<GenericSourceConnection>,
    },
    DropSource(Source, Option<GenericSourceConnection>),
}

#[derive(Debug, Clone)]
enum SinkControllerCommand {
    AddSink(Sink),
    AlterSink { prev_sink: Sink, new_sink: Sink },
    DropSink(Sink),
}

#[derive(Debug, Clone)]
enum IndexControllerCommand {
    AddIndex(Index),
    AlterIndex { prev_index: Index, new_index: Index },
    DropIndex(Index),
}

#[derive(Debug, Clone)]
enum MaterializedViewControllerCommand {
    AddMaterializedView(MaterializedView),
    AlterMaterializedView {
        prev_mv: MaterializedView,
        new_mv: MaterializedView,
    },
    DropMaterializedView(MaterializedView),
}

#[derive(Debug, Clone)]
enum ViewControllerCommand {
    AddView(View),
    AlterView { prev_view: View, new_view: View },
    DropView(View),
}

#[derive(Debug, Clone)]
enum ContinualTaskControllerCommand {
    AddContinualTask(ContinualTask),
    AlterContinualTask {
        prev_ct: ContinualTask,
        new_ct: ContinualTask,
    },
    DropContinualTask(ContinualTask),
}

#[derive(Debug, Clone)]
enum SecretControllerCommand {
    AddSecret(Secret),
    AlterSecret {
        prev_secret: Secret,
        new_secret: Secret,
    },
    DropSecret(Secret),
}

#[derive(Debug, Clone)]
enum ConnectionControllerCommand {
    AddConnection(Connection),
    AlterConnection {
        prev_connection: Connection,
        new_connection: Connection,
    },
    DropConnection(Connection),
}

#[derive(Debug, Clone)]
enum ClusterControllerCommand {
    AddCluster(Cluster),
    AlterCluster {
        prev_cluster: Cluster,
        new_cluster: Cluster,
    },
    DropCluster(Cluster),
}

#[derive(Debug, Clone)]
enum ClusterReplicaControllerCommand {
    AddClusterReplica(ClusterReplica),
    AlterClusterReplica {
        prev_replica: ClusterReplica,
        new_replica: ClusterReplica,
    },
    DropClusterReplica(ClusterReplica),
}

impl ControllerCommand {
    fn absorb(&mut self, catalog_update: ParsedStateUpdate) {
        match catalog_update.kind {
            ParsedStateUpdateKind::Item {
                durable_item: _,
                parsed_item,
                connection,
            } => match parsed_item {
                CatalogItem::Table(table) => {
                    self.absorb_table(table, catalog_update.ts, catalog_update.diff)
                }
                CatalogItem::Source(source) => {
                    self.absorb_source(source, connection, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Sink(sink) => {
                    self.absorb_sink(sink, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Index(index) => {
                    self.absorb_index(index, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::MaterializedView(mv) => {
                    self.absorb_materialized_view(mv, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::View(view) => {
                    self.absorb_view(view, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::ContinualTask(ct) => {
                    self.absorb_continual_task(ct, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Secret(secret) => {
                    self.absorb_secret(secret, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Connection(connection) => {
                    self.absorb_connection(connection, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Log(_) => {}
                CatalogItem::Type(_) => {}
                CatalogItem::Func(_) => {}
            },
            ParsedStateUpdateKind::TemporaryItem {
                parsed_item,
                connection,
            } => match parsed_item.item {
                CatalogItem::Table(table) => {
                    self.absorb_table(table, catalog_update.ts, catalog_update.diff)
                }
                CatalogItem::Source(source) => {
                    self.absorb_source(source, connection, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Sink(sink) => {
                    self.absorb_sink(sink, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Index(index) => {
                    self.absorb_index(index, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::MaterializedView(mv) => {
                    self.absorb_materialized_view(mv, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::View(view) => {
                    self.absorb_view(view, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::ContinualTask(ct) => {
                    self.absorb_continual_task(ct, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Secret(secret) => {
                    self.absorb_secret(secret, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Connection(connection) => {
                    self.absorb_connection(connection, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Log(_) => {}
                CatalogItem::Type(_) => {}
                CatalogItem::Func(_) => {}
            },
            ParsedStateUpdateKind::Cluster {
                durable_cluster: _,
                parsed_cluster,
            } => {
                self.absorb_cluster(parsed_cluster, catalog_update.ts, catalog_update.diff);
            }
            ParsedStateUpdateKind::ClusterReplica {
                durable_cluster_replica: _,
                parsed_cluster_replica,
            } => {
                self.absorb_cluster_replica(
                    parsed_cluster_replica,
                    catalog_update.ts,
                    catalog_update.diff,
                );
            }
        }
    }

    fn absorb_table(&mut self, table: Table, _ts: Timestamp, diff: StateDiff) {
        match self {
            ControllerCommand::None => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::Table(TableControllerCommand::AddTable(table));
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Table(TableControllerCommand::DropTable(table));
                }
            },
            ControllerCommand::Table(TableControllerCommand::DropTable(existing_table)) => {
                match diff {
                    StateDiff::Addition => {
                        *self = ControllerCommand::Table(TableControllerCommand::AlterTable {
                            prev_table: existing_table.clone(),
                            new_table: table,
                        });
                    }
                    StateDiff::Retraction => {
                        panic!("retraction for already dropped table");
                    }
                }
            }
            ControllerCommand::Table(TableControllerCommand::AddTable(new_table)) => match diff {
                StateDiff::Addition => {
                    panic!("addition for already added table");
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Table(TableControllerCommand::AlterTable {
                        prev_table: table,
                        new_table: new_table.clone(),
                    });
                }
            },
            _ => {
                tracing::info!(?self, "todo");
            }
        }
    }

    fn absorb_source(
        &mut self,
        source: Source,
        connection: Option<GenericSourceConnection>,
        _ts: Timestamp,
        diff: StateDiff,
    ) {
        match self {
            ControllerCommand::None => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::Source(SourceControllerCommand::AddSource(
                        source, connection,
                    ));
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Source(SourceControllerCommand::DropSource(
                        source, connection,
                    ));
                }
            },
            ControllerCommand::Source(SourceControllerCommand::DropSource(
                existing_source,
                existing_connection,
            )) => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::Source(SourceControllerCommand::AlterSource {
                        prev_source: existing_source.clone(),
                        prev_connection: existing_connection.clone(),
                        new_source: source,
                        new_connection: connection,
                    });
                }
                StateDiff::Retraction => {
                    panic!("retraction for already dropped source");
                }
            },
            ControllerCommand::Source(SourceControllerCommand::AddSource(
                new_source,
                new_connection,
            )) => match diff {
                StateDiff::Addition => {
                    panic!("addition for already added source");
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Source(SourceControllerCommand::AlterSource {
                        prev_source: source,
                        prev_connection: connection,
                        new_source: new_source.clone(),
                        new_connection: new_connection.clone(),
                    });
                }
            },
            _ => {
                tracing::info!(?self, "todo");
            }
        }
    }

    fn absorb_sink(&mut self, sink: Sink, _ts: Timestamp, diff: StateDiff) {
        match self {
            ControllerCommand::None => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::Sink(SinkControllerCommand::AddSink(sink));
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Sink(SinkControllerCommand::DropSink(sink));
                }
            },
            ControllerCommand::Sink(SinkControllerCommand::DropSink(existing_sink)) => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::Sink(SinkControllerCommand::AlterSink {
                        prev_sink: existing_sink.clone(),
                        new_sink: sink,
                    });
                }
                StateDiff::Retraction => {
                    panic!("retraction for already dropped sink");
                }
            },
            ControllerCommand::Sink(SinkControllerCommand::AddSink(new_sink)) => match diff {
                StateDiff::Addition => {
                    panic!("addition for already added sink");
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Sink(SinkControllerCommand::AlterSink {
                        prev_sink: sink,
                        new_sink: new_sink.clone(),
                    });
                }
            },
            _ => {
                tracing::info!(?self, "todo");
            }
        }
    }

    fn absorb_index(&mut self, index: Index, _ts: Timestamp, diff: StateDiff) {
        match self {
            ControllerCommand::None => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::Index(IndexControllerCommand::AddIndex(index));
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Index(IndexControllerCommand::DropIndex(index));
                }
            },
            ControllerCommand::Index(IndexControllerCommand::DropIndex(existing_index)) => {
                match diff {
                    StateDiff::Addition => {
                        *self = ControllerCommand::Index(IndexControllerCommand::AlterIndex {
                            prev_index: existing_index.clone(),
                            new_index: index,
                        });
                    }
                    StateDiff::Retraction => {
                        panic!("retraction for already dropped index");
                    }
                }
            }
            ControllerCommand::Index(IndexControllerCommand::AddIndex(new_index)) => match diff {
                StateDiff::Addition => {
                    panic!("addition for already added index");
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Index(IndexControllerCommand::AlterIndex {
                        prev_index: index,
                        new_index: new_index.clone(),
                    });
                }
            },
            _ => {
                tracing::info!(?self, "todo");
            }
        }
    }

    fn absorb_materialized_view(&mut self, mv: MaterializedView, _ts: Timestamp, diff: StateDiff) {
        match self {
            ControllerCommand::None => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::MaterializedView(
                        MaterializedViewControllerCommand::AddMaterializedView(mv),
                    );
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::MaterializedView(
                        MaterializedViewControllerCommand::DropMaterializedView(mv),
                    );
                }
            },
            ControllerCommand::MaterializedView(
                MaterializedViewControllerCommand::DropMaterializedView(existing_mv),
            ) => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::MaterializedView(
                        MaterializedViewControllerCommand::AlterMaterializedView {
                            prev_mv: existing_mv.clone(),
                            new_mv: mv,
                        },
                    );
                }
                StateDiff::Retraction => {
                    panic!("retraction for already dropped materialized view");
                }
            },
            ControllerCommand::MaterializedView(
                MaterializedViewControllerCommand::AddMaterializedView(new_mv),
            ) => match diff {
                StateDiff::Addition => {
                    panic!("addition for already added materialized view");
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::MaterializedView(
                        MaterializedViewControllerCommand::AlterMaterializedView {
                            prev_mv: mv,
                            new_mv: new_mv.clone(),
                        },
                    );
                }
            },
            _ => {
                tracing::info!(?self, "todo");
            }
        }
    }

    fn absorb_view(&mut self, view: View, _ts: Timestamp, diff: StateDiff) {
        match self {
            ControllerCommand::None => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::View(ViewControllerCommand::AddView(view));
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::View(ViewControllerCommand::DropView(view));
                }
            },
            ControllerCommand::View(ViewControllerCommand::DropView(existing_view)) => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::View(ViewControllerCommand::AlterView {
                        prev_view: existing_view.clone(),
                        new_view: view,
                    });
                }
                StateDiff::Retraction => {
                    panic!("retraction for already dropped view");
                }
            },
            ControllerCommand::View(ViewControllerCommand::AddView(new_view)) => match diff {
                StateDiff::Addition => {
                    panic!("addition for already added view");
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::View(ViewControllerCommand::AlterView {
                        prev_view: view,
                        new_view: new_view.clone(),
                    });
                }
            },
            _ => {
                tracing::info!(?self, "todo");
            }
        }
    }

    fn absorb_continual_task(&mut self, ct: ContinualTask, _ts: Timestamp, diff: StateDiff) {
        match self {
            ControllerCommand::None => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::ContinualTask(
                        ContinualTaskControllerCommand::AddContinualTask(ct),
                    );
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::ContinualTask(
                        ContinualTaskControllerCommand::DropContinualTask(ct),
                    );
                }
            },
            ControllerCommand::ContinualTask(
                ContinualTaskControllerCommand::DropContinualTask(existing_ct),
            ) => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::ContinualTask(
                        ContinualTaskControllerCommand::AlterContinualTask {
                            prev_ct: existing_ct.clone(),
                            new_ct: ct,
                        },
                    );
                }
                StateDiff::Retraction => {
                    panic!("retraction for already dropped continual task");
                }
            },
            ControllerCommand::ContinualTask(ContinualTaskControllerCommand::AddContinualTask(
                new_ct,
            )) => match diff {
                StateDiff::Addition => {
                    panic!("addition for already added continual task");
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::ContinualTask(
                        ContinualTaskControllerCommand::AlterContinualTask {
                            prev_ct: ct,
                            new_ct: new_ct.clone(),
                        },
                    );
                }
            },
            _ => {
                tracing::info!(?self, "todo");
            }
        }
    }

    fn absorb_secret(&mut self, secret: Secret, _ts: Timestamp, diff: StateDiff) {
        match self {
            ControllerCommand::None => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::Secret(SecretControllerCommand::AddSecret(secret));
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Secret(SecretControllerCommand::DropSecret(secret));
                }
            },
            ControllerCommand::Secret(SecretControllerCommand::DropSecret(existing_secret)) => {
                match diff {
                    StateDiff::Addition => {
                        *self = ControllerCommand::Secret(SecretControllerCommand::AlterSecret {
                            prev_secret: existing_secret.clone(),
                            new_secret: secret,
                        });
                    }
                    StateDiff::Retraction => {
                        panic!("retraction for already dropped secret");
                    }
                }
            }
            ControllerCommand::Secret(SecretControllerCommand::AddSecret(new_secret)) => match diff
            {
                StateDiff::Addition => {
                    panic!("addition for already added secret");
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Secret(SecretControllerCommand::AlterSecret {
                        prev_secret: secret,
                        new_secret: new_secret.clone(),
                    });
                }
            },
            _ => {
                tracing::info!(?self, "todo");
            }
        }
    }

    fn absorb_connection(&mut self, connection: Connection, _ts: Timestamp, diff: StateDiff) {
        match self {
            ControllerCommand::None => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::Connection(
                        ConnectionControllerCommand::AddConnection(connection),
                    );
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Connection(
                        ConnectionControllerCommand::DropConnection(connection),
                    );
                }
            },
            ControllerCommand::Connection(ConnectionControllerCommand::DropConnection(
                existing_connection,
            )) => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::Connection(
                        ConnectionControllerCommand::AlterConnection {
                            prev_connection: existing_connection.clone(),
                            new_connection: connection,
                        },
                    );
                }
                StateDiff::Retraction => {
                    panic!("retraction for already dropped connection");
                }
            },
            ControllerCommand::Connection(ConnectionControllerCommand::AddConnection(
                new_connection,
            )) => match diff {
                StateDiff::Addition => {
                    panic!("addition for already added connection");
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::Connection(
                        ConnectionControllerCommand::AlterConnection {
                            prev_connection: connection,
                            new_connection: new_connection.clone(),
                        },
                    );
                }
            },
            _ => {
                tracing::info!(?self, "todo");
            }
        }
    }

    fn absorb_cluster(&mut self, cluster: Cluster, _ts: Timestamp, diff: StateDiff) {
        match self {
            ControllerCommand::None => match diff {
                StateDiff::Addition => {
                    *self =
                        ControllerCommand::Cluster(ClusterControllerCommand::AddCluster(cluster));
                }
                StateDiff::Retraction => {
                    *self =
                        ControllerCommand::Cluster(ClusterControllerCommand::DropCluster(cluster));
                }
            },
            ControllerCommand::Cluster(ClusterControllerCommand::DropCluster(existing_cluster)) => {
                match diff {
                    StateDiff::Addition => {
                        *self =
                            ControllerCommand::Cluster(ClusterControllerCommand::AlterCluster {
                                prev_cluster: existing_cluster.clone(),
                                new_cluster: cluster,
                            });
                    }
                    StateDiff::Retraction => {
                        panic!("retraction for already dropped cluster");
                    }
                }
            }
            ControllerCommand::Cluster(ClusterControllerCommand::AddCluster(new_cluster)) => {
                match diff {
                    StateDiff::Addition => {
                        panic!("addition for already added cluster");
                    }
                    StateDiff::Retraction => {
                        *self =
                            ControllerCommand::Cluster(ClusterControllerCommand::AlterCluster {
                                prev_cluster: cluster,
                                new_cluster: new_cluster.clone(),
                            });
                    }
                }
            }
            _ => {
                tracing::info!(?self, "todo");
            }
        }
    }

    fn absorb_cluster_replica(
        &mut self,
        cluster_replica: ClusterReplica,
        _ts: Timestamp,
        diff: StateDiff,
    ) {
        match self {
            ControllerCommand::None => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::ClusterReplica(
                        ClusterReplicaControllerCommand::AddClusterReplica(cluster_replica),
                    );
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::ClusterReplica(
                        ClusterReplicaControllerCommand::DropClusterReplica(cluster_replica),
                    );
                }
            },
            ControllerCommand::ClusterReplica(
                ClusterReplicaControllerCommand::DropClusterReplica(existing_replica),
            ) => match diff {
                StateDiff::Addition => {
                    *self = ControllerCommand::ClusterReplica(
                        ClusterReplicaControllerCommand::AlterClusterReplica {
                            prev_replica: existing_replica.clone(),
                            new_replica: cluster_replica,
                        },
                    );
                }
                StateDiff::Retraction => {
                    panic!("retraction for already dropped cluster replica");
                }
            },
            ControllerCommand::ClusterReplica(
                ClusterReplicaControllerCommand::AddClusterReplica(new_replica),
            ) => match diff {
                StateDiff::Addition => {
                    panic!("addition for already added cluster replica");
                }
                StateDiff::Retraction => {
                    *self = ControllerCommand::ClusterReplica(
                        ClusterReplicaControllerCommand::AlterClusterReplica {
                            prev_replica: cluster_replica,
                            new_replica: new_replica.clone(),
                        },
                    );
                }
            },
            _ => {
                tracing::info!(?self, "todo");
            }
        }
    }
}
