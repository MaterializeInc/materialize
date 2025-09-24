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
//!
//! The flow from "raw" catalog changes to [ControllerCommand] works like this:
//!
//! StateUpdateKind -> ParsedStateUpdate -> ControllerCommand
//!
//! [ParsedStateUpdate] adds context to a "raw" catalog change
//! ([StateUpdateKind](mz_catalog::memory::objects::StateUpdateKind)). It
//! includes an in-memory representation of the updated object, which can in
//! theory be derived from the raw change but only when we have access to all
//! the other raw changes or to an in-memory Catalog, which represents a
//! "rollup" of all the raw changes.
//!
//! [ControllerCommand] is both the state machine that we use for absorbing
//! multiple state updates for the same object and the final command that has to
//! be applied to the controller(s) after absorbing all the state updates in a
//! given batch of updates.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

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
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::instrument;
use mz_ore::retry::Retry;
use mz_ore::str::StrExt;
use mz_ore::task;
use mz_repr::{CatalogItemId, GlobalId, RelationVersion, RelationVersionSelector, Timestamp};
use mz_sql::plan::ConnectionDetails;
use mz_storage_client::controller::{CollectionDescription, DataSource};
use mz_storage_types::connections::PostgresConnection;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::sources::{GenericSourceConnection, SourceExport};
use tracing::{Instrument, info_span, warn};

use crate::active_compute_sink::ActiveComputeSinkRetireReason;
use crate::coord::Coordinator;
use crate::coord::controller_commands::parsed_state_updates::{
    ParsedStateUpdate, ParsedStateUpdateKind,
};
use crate::coord::statement_logging::StatementLoggingId;
use crate::coord::timeline::TimelineState;
use crate::statement_logging::StatementEndedExecutionReason;
use crate::{AdapterError, CollectionIdBundle, ExecuteContext, ResultExt};

pub mod parsed_state_updates;

impl Coordinator {
    /// Applies the given bucket of [ParsedStateUpdate] to our in-memory state
    /// and our controllers. This will also apply implications that arise from
    /// catalog changes. For example, peeks and subscribes will be cancelled
    /// when referenced objects are dropped.
    ///
    /// This _requires_ that the given updates are consolidated. There must be
    /// at most one addition and/or one retraction for a given item, as
    /// identified by that items ID type.
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

        for update in catalog_updates {
            tracing::trace!(?update, "got parsed state update");
            match &update.kind {
                ParsedStateUpdateKind::Item {
                    durable_item,
                    parsed_item: _,
                    connection: _,
                    parsed_full_name: _,
                } => {
                    let entry = controller_commands
                        .entry(durable_item.id.clone())
                        .or_insert_with(|| ControllerCommand::None);
                    entry.absorb(update);
                }
                ParsedStateUpdateKind::TemporaryItem {
                    durable_item,
                    parsed_item: _,
                    connection: _,
                    parsed_full_name: _,
                } => {
                    let entry = controller_commands
                        .entry(durable_item.id.clone())
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

        // Maps for storing names of dropped objects for error messages.
        let mut dropped_item_names: BTreeMap<GlobalId, String> = BTreeMap::new();
        let mut dropped_cluster_names: BTreeMap<ClusterId, String> = BTreeMap::new();

        // Separate collections for tables (which need write timestamps) and
        // sources (which don't).
        let mut table_collections_to_create = BTreeMap::new();
        let mut source_collections_to_create = BTreeMap::new();
        let mut storage_policies_to_initialize = BTreeMap::new();
        let mut execution_timestamps_to_set = BTreeSet::new();

        // We're incrementally migrating the code that manipulates the
        // controller from closures in the sequencer. For some types of catalog
        // changes we haven't done this migration yet, so there you will see
        // just a log message. Over the next couple of PRs all of these will go
        // away.

        for (catalog_id, command) in commands {
            tracing::trace!(?command, "have to apply controller command");

            match command {
                ControllerCommand::Table(ControllerCommandKind::Added(table)) => {
                    self.handle_create_table(
                        &mut ctx,
                        &mut table_collections_to_create,
                        &mut storage_policies_to_initialize,
                        &mut execution_timestamps_to_set,
                        catalog_id,
                        table.clone(),
                    )
                    .await?
                }
                ControllerCommand::Table(ControllerCommandKind::Altered {
                    prev: prev_table,
                    new: new_table,
                }) => self.handle_alter_table(prev_table, new_table).await?,

                ControllerCommand::Table(ControllerCommandKind::Dropped(table, full_name)) => {
                    let global_ids = table.global_ids();
                    for global_id in global_ids {
                        tables_to_drop.insert((catalog_id, global_id));
                        dropped_item_names.insert(global_id, full_name.clone());
                    }
                }
                ControllerCommand::Source(ControllerCommandKind::Added((source, _connection))) => {
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
                ControllerCommand::Source(ControllerCommandKind::Altered {
                    prev: (prev_source, _prev_connection),
                    new: (new_source, _new_connection),
                }) => {
                    tracing::debug!(
                        ?prev_source,
                        ?new_source,
                        "not handling AlterSource in here yet"
                    );
                }
                ControllerCommand::Source(ControllerCommandKind::Dropped(
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
                ControllerCommand::Sink(ControllerCommandKind::Added(sink)) => {
                    tracing::debug!(?sink, "not handling AddSink in here yet");
                }
                ControllerCommand::Sink(ControllerCommandKind::Altered {
                    prev: prev_sink,
                    new: new_sink,
                }) => {
                    tracing::debug!(?prev_sink, ?new_sink, "not handling AlterSink in here yet");
                }
                ControllerCommand::Sink(ControllerCommandKind::Dropped(sink, full_name)) => {
                    storage_sink_gids_to_drop.push(sink.global_id());
                    dropped_item_names.insert(sink.global_id(), full_name);
                }
                ControllerCommand::Index(ControllerCommandKind::Added(index)) => {
                    tracing::debug!(?index, "not handling AddIndex in here yet");
                }
                ControllerCommand::Index(ControllerCommandKind::Altered {
                    prev: prev_index,
                    new: new_index,
                }) => {
                    tracing::debug!(
                        ?prev_index,
                        ?new_index,
                        "not handling AlterIndex in here yet"
                    );
                }
                ControllerCommand::Index(ControllerCommandKind::Dropped(index, full_name)) => {
                    indexes_to_drop.push((index.cluster_id, index.global_id()));
                    dropped_item_names.insert(index.global_id(), full_name);
                }
                ControllerCommand::MaterializedView(ControllerCommandKind::Added(mv)) => {
                    tracing::debug!(?mv, "not handling AddMaterializedView in here yet");
                }
                ControllerCommand::MaterializedView(ControllerCommandKind::Altered {
                    prev: prev_mv,
                    new: new_mv,
                }) => {
                    tracing::debug!(
                        ?prev_mv,
                        ?new_mv,
                        "not handling AlterMaterializedView in here yet"
                    );
                }
                ControllerCommand::MaterializedView(ControllerCommandKind::Dropped(
                    mv,
                    full_name,
                )) => {
                    materialized_views_to_drop.push((mv.cluster_id, mv.global_id()));
                    dropped_item_names.insert(mv.global_id(), full_name);
                }
                ControllerCommand::View(ControllerCommandKind::Added(view)) => {
                    tracing::debug!(?view, "not handling AddView in here yet");
                }
                ControllerCommand::View(ControllerCommandKind::Altered {
                    prev: prev_view,
                    new: new_view,
                }) => {
                    tracing::debug!(?prev_view, ?new_view, "not handling AlterView in here yet");
                }
                ControllerCommand::View(ControllerCommandKind::Dropped(view, full_name)) => {
                    view_gids_to_drop.push(view.global_id());
                    dropped_item_names.insert(view.global_id(), full_name);
                }
                ControllerCommand::ContinualTask(ControllerCommandKind::Added(ct)) => {
                    tracing::debug!(?ct, "not handling AddContinualTask in here yet");
                }
                ControllerCommand::ContinualTask(ControllerCommandKind::Altered {
                    prev: prev_ct,
                    new: new_ct,
                }) => {
                    tracing::debug!(
                        ?prev_ct,
                        ?new_ct,
                        "not handling AlterContinualTask in here yet"
                    );
                }
                ControllerCommand::ContinualTask(ControllerCommandKind::Dropped(
                    ct,
                    _full_name,
                )) => {
                    continual_tasks_to_drop.push((catalog_id, ct.cluster_id, ct.global_id()));
                }
                ControllerCommand::Secret(ControllerCommandKind::Added(secret)) => {
                    tracing::debug!(?secret, "not handling AddSecret in here yet");
                }
                ControllerCommand::Secret(ControllerCommandKind::Altered {
                    prev: prev_secret,
                    new: new_secret,
                }) => {
                    tracing::debug!(
                        ?prev_secret,
                        ?new_secret,
                        "not handling AlterSecret in here yet"
                    );
                }
                ControllerCommand::Secret(ControllerCommandKind::Dropped(_secret, _full_name)) => {
                    secrets_to_drop.push(catalog_id);
                }
                ControllerCommand::Connection(ControllerCommandKind::Added(connection)) => {
                    tracing::debug!(?connection, "not handling AddConnection in here yet");
                }
                ControllerCommand::Connection(ControllerCommandKind::Altered {
                    prev: prev_connection,
                    new: new_connection,
                }) => {
                    tracing::debug!(
                        ?prev_connection,
                        ?new_connection,
                        "not handling AlterConnection in here yet"
                    );
                }
                ControllerCommand::Connection(ControllerCommandKind::Dropped(
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
                ControllerCommand::None => {
                    // Nothing to do for None commands
                }
                ControllerCommand::Cluster(_) | ControllerCommand::ClusterReplica(_) => {
                    unreachable!("clusters and cluster replicas are handled below")
                }
                ControllerCommand::Table(ControllerCommandKind::None)
                | ControllerCommand::Source(ControllerCommandKind::None)
                | ControllerCommand::Sink(ControllerCommandKind::None)
                | ControllerCommand::Index(ControllerCommandKind::None)
                | ControllerCommand::MaterializedView(ControllerCommandKind::None)
                | ControllerCommand::View(ControllerCommandKind::None)
                | ControllerCommand::ContinualTask(ControllerCommandKind::None)
                | ControllerCommand::Secret(ControllerCommandKind::None)
                | ControllerCommand::Connection(ControllerCommandKind::None) => {
                    unreachable!("will never leave None in place");
                }
            }
        }

        for (cluster_id, command) in cluster_commands {
            tracing::trace!(?command, "have cluster command to apply!");

            match command {
                ControllerCommand::Cluster(ControllerCommandKind::Added(cluster)) => {
                    tracing::debug!(?cluster, "not handling AddCluster in here yet");
                }
                ControllerCommand::Cluster(ControllerCommandKind::Altered {
                    prev: prev_cluster,
                    new: new_cluster,
                }) => {
                    tracing::debug!(
                        ?prev_cluster,
                        ?new_cluster,
                        "not handling AlterCluster in here yet"
                    );
                }
                ControllerCommand::Cluster(ControllerCommandKind::Dropped(cluster, _full_name)) => {
                    clusters_to_drop.push(cluster_id);
                    dropped_cluster_names.insert(cluster_id, cluster.name);
                }
                ControllerCommand::Cluster(ControllerCommandKind::None) => {
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

        for ((cluster_id, replica_id), command) in cluster_replica_commands {
            tracing::trace!(?command, "have cluster replica command to apply!");

            match command {
                ControllerCommand::ClusterReplica(ControllerCommandKind::Added(replica)) => {
                    tracing::debug!(?replica, "not handling AddClusterReplica in here yet");
                }
                ControllerCommand::ClusterReplica(ControllerCommandKind::Altered {
                    prev: prev_replica,
                    new: new_replica,
                }) => {
                    tracing::debug!(
                        ?prev_replica,
                        ?new_replica,
                        "not handling AlterClusterReplica in here yet"
                    );
                }
                ControllerCommand::ClusterReplica(ControllerCommandKind::Dropped(
                    _replica,
                    _full_name,
                )) => {
                    cluster_replicas_to_drop.push((cluster_id, replica_id));
                }
                ControllerCommand::ClusterReplica(ControllerCommandKind::None) => {
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
        // It is _very_ important that we only initialize read policies after we
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
            if let Some(id) = sink
                .depends_on()
                .iter()
                .find(|id| collections_to_drop.contains(id))
            {
                let name = dropped_item_names
                    .get(id)
                    .map(|n| format!("relation {}", n.quoted()))
                    .expect("missing relation name");
                compute_sinks_to_drop.insert(
                    *sink_id,
                    ActiveComputeSinkRetireReason::DependencyDropped(name),
                );
            } else if clusters_to_drop.contains(&cluster_id) {
                let name = dropped_cluster_names
                    .get(&cluster_id)
                    .map(|n| format!("cluster {}", n.quoted()))
                    .expect("missing cluster name");
                compute_sinks_to_drop.insert(
                    *sink_id,
                    ActiveComputeSinkRetireReason::DependencyDropped(name),
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
                let name = dropped_item_names
                    .get(id)
                    .map(|n| format!("relation {}", n.quoted()))
                    .expect("missing relation name");
                peeks_to_drop.push((name, uuid.clone()));
            } else if clusters_to_drop.contains(&pending_peek.cluster_id) {
                let name = dropped_cluster_names
                    .get(&pending_peek.cluster_id)
                    .map(|n| format!("cluster {}", n.quoted()))
                    .expect("missing cluster name");
                peeks_to_drop.push((name, uuid.clone()));
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
                let collection_descs = versions.iter().map(|(version, (gid, desc))| {
                    let next_version = version.bump();
                    let primary_collection =
                        versions.get(&next_version).map(|(gid, _desc)| gid).copied();
                    let collection_desc =
                        CollectionDescription::for_table(desc.clone(), primary_collection);

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
                        // TODO: It's a little weird that a table will be present in this
                        // source status collection, we might want to split out into a separate
                        // status collection.
                        let status_collection_id = self
                            .catalog()
                            .resolve_builtin_storage_collection(&builtin::MZ_SOURCE_STATUS_HISTORY);

                        let global_ingestion_id =
                            self.catalog().get_entry(ingestion_id).latest_global_id();
                        let global_status_collection_id = self
                            .catalog()
                            .get_entry(&status_collection_id)
                            .latest_global_id();

                        let collection_desc = CollectionDescription::<Timestamp> {
                            desc: table.desc.latest(),
                            data_source: DataSource::IngestionExport {
                                ingestion_id: global_ingestion_id,
                                details: details.clone(),
                                data_config: data_config
                                    .clone()
                                    .into_inline_connection(self.catalog.state()),
                            },
                            since: None,
                            status_collection_id: Some(global_status_collection_id),
                            timeline: Some(timeline.clone()),
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

                        let collection_desc = CollectionDescription::<Timestamp> {
                            desc,
                            data_source: DataSource::Webhook,
                            since: None,
                            status_collection_id: None, // Webhook tables don't use status collections
                            timeline: Some(timeline.clone()),
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
        item_id: CatalogItemId,
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
            DataSourceDesc::Ingestion { desc, cluster_id } => {
                let desc = desc.into_inline_connection(self.catalog().state());
                let item_global_id =
                    self.catalog().get_entry(&item_id).latest_global_id();

                let ingestion = mz_storage_types::sources::IngestionDescription::new(
                    desc, cluster_id, item_global_id,
                );

                (
                    DataSource::Ingestion(ingestion),
                    source_status_collection_id,
                )
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

/// A state machine for building controller commands from catalog updates.
///
/// Once all [ParsedStateUpdate] of a timestamp are ingested this is a command
/// that has to be applied to the controller(s).
#[derive(Debug, Clone)]
enum ControllerCommand {
    None,
    Table(ControllerCommandKind<Table>),
    Source(ControllerCommandKind<(Source, Option<GenericSourceConnection>)>),
    Sink(ControllerCommandKind<Sink>),
    Index(ControllerCommandKind<Index>),
    MaterializedView(ControllerCommandKind<MaterializedView>),
    View(ControllerCommandKind<View>),
    ContinualTask(ControllerCommandKind<ContinualTask>),
    Secret(ControllerCommandKind<Secret>),
    Connection(ControllerCommandKind<Connection>),
    Cluster(ControllerCommandKind<Cluster>),
    ClusterReplica(ControllerCommandKind<ClusterReplica>),
}

#[derive(Debug, Clone)]
enum ControllerCommandKind<T> {
    /// No operations seen yet.
    None,
    /// Item was added.
    Added(T),
    /// Item was dropped (with its name retained for error messages).
    Dropped(T, String),
    /// Item is being altered from one state to another.
    Altered { prev: T, new: T },
}

impl<T: Clone> ControllerCommandKind<T> {
    /// Apply a state transition based on a diff. Returns an error message if
    /// the transition is invalid.
    fn transition(&mut self, item: T, name: Option<String>, diff: StateDiff) -> Result<(), String> {
        use ControllerCommandKind::*;
        use StateDiff::*;

        let new_state = match (&*self, diff) {
            // Initial state transitions
            (None, Addition) => Added(item),
            (None, Retraction) => Dropped(item, name.unwrap_or_else(|| "<unknown>".to_string())),

            // From Added state
            (Added(existing), Retraction) => {
                // Add -> Drop means the item is being altered
                Altered {
                    prev: item,
                    new: existing.clone(),
                }
            }
            (Added(_), Addition) => {
                return Err("Cannot add an already added object".to_string());
            }

            // From Dropped state
            (Dropped(existing, _), Addition) => {
                // Drop -> Add means the item is being altered
                Altered {
                    prev: existing.clone(),
                    new: item,
                }
            }
            (Dropped(_, _), Retraction) => {
                return Err("Cannot drop an already dropped object".to_string());
            }

            // From Altered state
            (Altered { .. }, _) => {
                return Err(format!(
                    "Cannot apply {:?} to an object in Altered state",
                    diff
                ));
            }
        };

        *self = new_state;
        Ok(())
    }
}

/// Macro to generate absorb methods for each item type.
macro_rules! impl_absorb_method {
    (
        $method_name:ident,
        $variant:ident,
        $item_type:ty
    ) => {
        fn $method_name(
            &mut self,
            item: $item_type,
            parsed_full_name: Option<String>,
            diff: StateDiff,
        ) {
            let state = match self {
                ControllerCommand::$variant(state) => state,
                ControllerCommand::None => {
                    *self = ControllerCommand::$variant(ControllerCommandKind::None);
                    match self {
                        ControllerCommand::$variant(state) => state,
                        _ => unreachable!(),
                    }
                }
                _ => {
                    panic!(
                        "Unexpected command type for {:?}: {} {:?}",
                        self,
                        stringify!($variant),
                        diff,
                    );
                }
            };

            if let Err(e) = state.transition(item, parsed_full_name, diff) {
                panic!(
                    "Invalid state transition for {}: {}",
                    stringify!($variant),
                    e
                );
            }
        }
    };
}

impl ControllerCommand {
    /// Absorbs the given catalog update into this [ControllerCommand], causing
    /// a state transition or error.
    fn absorb(&mut self, catalog_update: ParsedStateUpdate) {
        match catalog_update.kind {
            ParsedStateUpdateKind::Item {
                durable_item: _,
                parsed_item,
                connection,
                parsed_full_name,
            } => match parsed_item {
                CatalogItem::Table(table) => {
                    self.absorb_table(table, Some(parsed_full_name), catalog_update.diff)
                }
                CatalogItem::Source(source) => {
                    self.absorb_source(
                        (source, connection),
                        Some(parsed_full_name),
                        catalog_update.diff,
                    );
                }
                CatalogItem::Sink(sink) => {
                    self.absorb_sink(sink, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::Index(index) => {
                    self.absorb_index(index, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::MaterializedView(mv) => {
                    self.absorb_materialized_view(mv, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::View(view) => {
                    self.absorb_view(view, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::ContinualTask(ct) => {
                    self.absorb_continual_task(ct, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::Secret(secret) => {
                    self.absorb_secret(secret, None, catalog_update.diff);
                }
                CatalogItem::Connection(connection) => {
                    self.absorb_connection(connection, None, catalog_update.diff);
                }
                CatalogItem::Log(_) => {}
                CatalogItem::Type(_) => {}
                CatalogItem::Func(_) => {}
            },
            ParsedStateUpdateKind::TemporaryItem {
                durable_item: _,
                parsed_item,
                connection,
                parsed_full_name,
            } => match parsed_item {
                CatalogItem::Table(table) => {
                    self.absorb_table(table, Some(parsed_full_name), catalog_update.diff)
                }
                CatalogItem::Source(source) => {
                    self.absorb_source(
                        (source, connection),
                        Some(parsed_full_name),
                        catalog_update.diff,
                    );
                }
                CatalogItem::Sink(sink) => {
                    self.absorb_sink(sink, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::Index(index) => {
                    self.absorb_index(index, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::MaterializedView(mv) => {
                    self.absorb_materialized_view(mv, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::View(view) => {
                    self.absorb_view(view, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::ContinualTask(ct) => {
                    self.absorb_continual_task(ct, None, catalog_update.diff);
                }
                CatalogItem::Secret(secret) => {
                    self.absorb_secret(secret, None, catalog_update.diff);
                }
                CatalogItem::Connection(connection) => {
                    self.absorb_connection(connection, None, catalog_update.diff);
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

    impl_absorb_method!(absorb_table, Table, Table);
    impl_absorb_method!(
        absorb_source,
        Source,
        (Source, Option<GenericSourceConnection>)
    );
    impl_absorb_method!(absorb_sink, Sink, Sink);
    impl_absorb_method!(absorb_index, Index, Index);
    impl_absorb_method!(absorb_materialized_view, MaterializedView, MaterializedView);
    impl_absorb_method!(absorb_view, View, View);

    impl_absorb_method!(absorb_continual_task, ContinualTask, ContinualTask);
    impl_absorb_method!(absorb_secret, Secret, Secret);
    impl_absorb_method!(absorb_connection, Connection, Connection);

    // Special case for cluster which uses the cluster's name field.
    fn absorb_cluster(&mut self, cluster: Cluster, _ts: Timestamp, diff: StateDiff) {
        let state = match self {
            ControllerCommand::Cluster(state) => state,
            ControllerCommand::None => {
                *self = ControllerCommand::Cluster(ControllerCommandKind::None);
                match self {
                    ControllerCommand::Cluster(state) => state,
                    _ => unreachable!(),
                }
            }
            _ => {
                panic!("Unexpected command type for {:?}: Cluster {:?}", self, diff);
            }
        };

        if let Err(e) = state.transition(cluster.clone(), Some(cluster.name), diff) {
            panic!("invalid state transition for cluster: {}", e);
        }
    }

    // Special case for cluster replica which uses the cluster replica's name field.
    fn absorb_cluster_replica(
        &mut self,
        cluster_replica: ClusterReplica,
        _ts: Timestamp,
        diff: StateDiff,
    ) {
        let state = match self {
            ControllerCommand::ClusterReplica(state) => state,
            ControllerCommand::None => {
                *self = ControllerCommand::ClusterReplica(ControllerCommandKind::None);
                match self {
                    ControllerCommand::ClusterReplica(state) => state,
                    _ => unreachable!(),
                }
            }
            _ => {
                panic!(
                    "Unexpected command type for {:?}: ClusterReplica {:?}",
                    self, diff
                );
            }
        };

        if let Err(e) = state.transition(cluster_replica.clone(), Some(cluster_replica.name), diff)
        {
            panic!("invalid state transition for cluster replica: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::{GlobalId, RelationDesc, RelationVersion, VersionedRelationDesc};
    use mz_sql::names::ResolvedIds;
    use std::collections::BTreeMap;

    fn create_test_table(name: &str) -> Table {
        Table {
            desc: VersionedRelationDesc::new(
                RelationDesc::builder()
                    .with_column(name, mz_repr::SqlScalarType::String.nullable(false))
                    .finish(),
            ),
            create_sql: None,
            collections: BTreeMap::from([(RelationVersion::root(), GlobalId::System(1))]),
            conn_id: None,
            resolved_ids: ResolvedIds::empty(),
            custom_logical_compaction_window: None,
            is_retained_metrics_object: false,
            data_source: TableDataSource::TableWrites { defaults: vec![] },
        }
    }

    #[mz_ore::test]
    fn test_item_state_transitions() {
        // Test None -> Added
        let mut state = ControllerCommandKind::None;
        assert!(
            state
                .transition("item1".to_string(), None, StateDiff::Addition)
                .is_ok()
        );
        assert!(matches!(state, ControllerCommandKind::Added(_)));

        // Test Added -> Altered (via retraction)
        let mut state = ControllerCommandKind::Added("new_item".to_string());
        assert!(
            state
                .transition("old_item".to_string(), None, StateDiff::Retraction)
                .is_ok()
        );
        match &state {
            ControllerCommandKind::Altered { prev, new } => {
                // The retracted item is the OLD state
                assert_eq!(prev, "old_item");
                // The existing Added item is the NEW state
                assert_eq!(new, "new_item");
            }
            _ => panic!("Expected Altered state"),
        }

        // Test None -> Dropped
        let mut state = ControllerCommandKind::None;
        assert!(
            state
                .transition(
                    "item1".to_string(),
                    Some("test_name".to_string()),
                    StateDiff::Retraction
                )
                .is_ok()
        );
        assert!(matches!(state, ControllerCommandKind::Dropped(_, _)));

        // Test Dropped -> Altered (via addition)
        let mut state = ControllerCommandKind::Dropped("old_item".to_string(), "name".to_string());
        assert!(
            state
                .transition("new_item".to_string(), None, StateDiff::Addition)
                .is_ok()
        );
        match &state {
            ControllerCommandKind::Altered { prev, new } => {
                // The existing Dropped item is the OLD state
                assert_eq!(prev, "old_item");
                // The added item is the NEW state
                assert_eq!(new, "new_item");
            }
            _ => panic!("Expected Altered state"),
        }

        // Test invalid transitions
        let mut state = ControllerCommandKind::Added("item".to_string());
        assert!(
            state
                .transition("item2".to_string(), None, StateDiff::Addition)
                .is_err()
        );

        let mut state = ControllerCommandKind::Dropped("item".to_string(), "name".to_string());
        assert!(
            state
                .transition("item2".to_string(), None, StateDiff::Retraction)
                .is_err()
        );
    }

    #[mz_ore::test]
    fn test_table_absorb_state_machine() {
        let table1 = create_test_table("table1");
        let table2 = create_test_table("table2");

        // Test None -> AddTable
        let mut cmd = ControllerCommand::None;
        cmd.absorb_table(
            table1.clone(),
            Some("schema.table1".to_string()),
            StateDiff::Addition,
        );
        // Check that we have an Added state
        match &cmd {
            ControllerCommand::Table(state) => match state {
                ControllerCommandKind::Added(t) => {
                    assert_eq!(t.desc.latest().arity(), table1.desc.latest().arity())
                }
                _ => panic!("Expected Added state"),
            },
            _ => panic!("Expected Table command"),
        }

        // Test AddTable -> AlterTable (via retraction)
        // This tests the bug fix: when we have AddTable(table1) and receive Retraction(table2),
        // table2 is the old state being removed, table1 is the new state
        cmd.absorb_table(
            table2.clone(),
            Some("schema.table2".to_string()),
            StateDiff::Retraction,
        );
        match &cmd {
            ControllerCommand::Table(state) => match state {
                ControllerCommandKind::Altered { prev, new } => {
                    // Verify the fix: prev should be the retracted table, new should be the added table
                    assert_eq!(prev.desc.latest().arity(), table2.desc.latest().arity());
                    assert_eq!(new.desc.latest().arity(), table1.desc.latest().arity());
                }
                _ => panic!("Expected Altered state"),
            },
            _ => panic!("Expected Table command"),
        }

        // Test None -> DropTable
        let mut cmd = ControllerCommand::None;
        cmd.absorb_table(
            table1.clone(),
            Some("schema.table1".to_string()),
            StateDiff::Retraction,
        );
        match &cmd {
            ControllerCommand::Table(state) => match state {
                ControllerCommandKind::Dropped(t, name) => {
                    assert_eq!(t.desc.latest().arity(), table1.desc.latest().arity());
                    assert_eq!(name, "schema.table1");
                }
                _ => panic!("Expected Dropped state"),
            },
            _ => panic!("Expected Table command"),
        }

        // Test DropTable -> AlterTable (via addition)
        cmd.absorb_table(
            table2.clone(),
            Some("schema.table2".to_string()),
            StateDiff::Addition,
        );
        match &cmd {
            ControllerCommand::Table(state) => match state {
                ControllerCommandKind::Altered { prev, new } => {
                    // prev should be the dropped table, new should be the added table
                    assert_eq!(prev.desc.latest().arity(), table1.desc.latest().arity());
                    assert_eq!(new.desc.latest().arity(), table2.desc.latest().arity());
                }
                _ => panic!("Expected Altered state"),
            },
            _ => panic!("Expected Table command"),
        }
    }

    #[mz_ore::test]
    #[should_panic(expected = "Cannot add an already added object")]
    fn test_invalid_double_add() {
        let table = create_test_table("table");
        let mut cmd = ControllerCommand::None;

        // First addition
        cmd.absorb_table(
            table.clone(),
            Some("schema.table".to_string()),
            StateDiff::Addition,
        );

        // Second addition should panic
        cmd.absorb_table(
            table.clone(),
            Some("schema.table".to_string()),
            StateDiff::Addition,
        );
    }

    #[mz_ore::test]
    #[should_panic(expected = "Cannot drop an already dropped object")]
    fn test_invalid_double_drop() {
        let table = create_test_table("table");
        let mut cmd = ControllerCommand::None;

        // First drop
        cmd.absorb_table(
            table.clone(),
            Some("schema.table".to_string()),
            StateDiff::Retraction,
        );

        // Second drop should panic
        cmd.absorb_table(
            table.clone(),
            Some("schema.table".to_string()),
            StateDiff::Retraction,
        );
    }
}
