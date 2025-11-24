// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module encapsulates all of the [`Coordinator`]'s logic for creating, dropping,
//! and altering objects.

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use fail::fail_point;
use maplit::{btreemap, btreeset};
use mz_adapter_types::compaction::SINCE_GRANULARITY;
use mz_adapter_types::connection::ConnectionId;
use mz_audit_log::VersionedEvent;
use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::memory::objects::{CatalogItem, Connection, DataSourceDesc, Sink};
use mz_compute_client::protocol::response::PeekResponse;
use mz_controller::clusters::ReplicaLocation;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::instrument;
use mz_ore::now::to_datetime;
use mz_ore::retry::Retry;
use mz_ore::str::StrExt;
use mz_ore::task;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use mz_sql::catalog::{CatalogCluster, CatalogClusterReplica, CatalogSchema};
use mz_sql::names::ResolvedDatabaseSpecifier;
use mz_sql::plan::ConnectionDetails;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::{
    self, MAX_AWS_PRIVATELINK_CONNECTIONS, MAX_CLUSTERS, MAX_CONTINUAL_TASKS,
    MAX_CREDIT_CONSUMPTION_RATE, MAX_DATABASES, MAX_KAFKA_CONNECTIONS, MAX_MATERIALIZED_VIEWS,
    MAX_MYSQL_CONNECTIONS, MAX_NETWORK_POLICIES, MAX_OBJECTS_PER_SCHEMA, MAX_POSTGRES_CONNECTIONS,
    MAX_REPLICAS_PER_CLUSTER, MAX_ROLES, MAX_SCHEMAS_PER_DATABASE, MAX_SECRETS, MAX_SINKS,
    MAX_SOURCES, MAX_SQL_SERVER_CONNECTIONS, MAX_TABLES, SystemVars, Var,
};
use mz_storage_client::controller::{CollectionDescription, DataSource, ExportDescription};
use mz_storage_types::connections::PostgresConnection;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sources::GenericSourceConnection;
use mz_storage_types::sources::kafka::KAFKA_PROGRESS_DESC;
use serde_json::json;
use tracing::{Instrument, Level, event, info_span, warn};

use crate::active_compute_sink::{ActiveComputeSink, ActiveComputeSinkRetireReason};
use crate::catalog::{DropObjectInfo, Op, ReplicaCreateDropReason, TransactionResult};
use crate::coord::Coordinator;
use crate::coord::appends::BuiltinTableAppendNotify;
use crate::coord::timeline::{TimelineContext, TimelineState};
use crate::session::{Session, Transaction, TransactionOps};
use crate::statement_logging::StatementEndedExecutionReason;
use crate::telemetry::{EventDetails, SegmentClientExt};
use crate::util::ResultExt;
use crate::{AdapterError, ExecuteContext, catalog, flags};

impl Coordinator {
    /// Same as [`Self::catalog_transact_conn`] but takes a [`Session`].
    #[instrument(name = "coord::catalog_transact")]
    pub(crate) async fn catalog_transact(
        &mut self,
        session: Option<&Session>,
        ops: Vec<catalog::Op>,
    ) -> Result<(), AdapterError> {
        self.catalog_transact_conn(session.map(|session| session.conn_id()), ops)
            .await
    }

    /// Same as [`Self::catalog_transact_conn`] but takes a [`Session`] and runs
    /// builtin table updates concurrently with any side effects (e.g. creating
    /// collections).
    #[instrument(name = "coord::catalog_transact_with_side_effects")]
    pub(crate) async fn catalog_transact_with_side_effects<F>(
        &mut self,
        ctx: Option<&mut ExecuteContext>,
        ops: Vec<catalog::Op>,
        side_effect: F,
    ) -> Result<(), AdapterError>
    where
        F: for<'a> FnOnce(
                &'a mut Coordinator,
                Option<&'a mut ExecuteContext>,
            ) -> Pin<Box<dyn Future<Output = ()> + 'a>>
            + 'static,
    {
        let table_updates = self
            .catalog_transact_inner(ctx.as_ref().map(|ctx| ctx.session().conn_id()), ops)
            .await?;
        let side_effects_fut = side_effect(self, ctx);

        // Run our side effects concurrently with the table updates.
        let ((), ()) = futures::future::join(
            side_effects_fut.instrument(info_span!(
                "coord::catalog_transact_with_side_effects::side_effects_fut"
            )),
            table_updates.instrument(info_span!(
                "coord::catalog_transact_with_side_effects::table_updates"
            )),
        )
        .await;

        Ok(())
    }

    /// Same as [`Self::catalog_transact_inner`] but awaits the table updates.
    #[instrument(name = "coord::catalog_transact_conn")]
    pub(crate) async fn catalog_transact_conn(
        &mut self,
        conn_id: Option<&ConnectionId>,
        ops: Vec<catalog::Op>,
    ) -> Result<(), AdapterError> {
        let table_updates = self.catalog_transact_inner(conn_id, ops).await?;
        table_updates
            .instrument(info_span!("coord::catalog_transact_conn::table_updates"))
            .await;
        Ok(())
    }

    /// Executes a Catalog transaction with handling if the provided [`Session`]
    /// is in a SQL transaction that is executing DDL.
    #[instrument(name = "coord::catalog_transact_with_ddl_transaction")]
    pub(crate) async fn catalog_transact_with_ddl_transaction<F>(
        &mut self,
        ctx: &mut ExecuteContext,
        ops: Vec<catalog::Op>,
        side_effect: F,
    ) -> Result<(), AdapterError>
    where
        F: for<'a> FnOnce(
                &'a mut Coordinator,
                Option<&'a mut ExecuteContext>,
            ) -> Pin<Box<dyn Future<Output = ()> + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let Some(Transaction {
            ops:
                TransactionOps::DDL {
                    ops: txn_ops,
                    revision: txn_revision,
                    side_effects: _,
                    state: _,
                },
            ..
        }) = ctx.session().transaction().inner()
        else {
            return self
                .catalog_transact_with_side_effects(Some(ctx), ops, side_effect)
                .await;
        };

        // Make sure our Catalog hasn't changed since openning the transaction.
        if self.catalog().transient_revision() != *txn_revision {
            return Err(AdapterError::DDLTransactionRace);
        }

        // Combine the existing ops with the new ops so we can replay them.
        let mut all_ops = Vec::with_capacity(ops.len() + txn_ops.len() + 1);
        all_ops.extend(txn_ops.iter().cloned());
        all_ops.extend(ops.clone());
        all_ops.push(Op::TransactionDryRun);

        // Run our Catalog transaction, but abort before committing.
        let result = self.catalog_transact(Some(ctx.session()), all_ops).await;

        match result {
            // We purposefully fail with this error to prevent committing the transaction.
            Err(AdapterError::TransactionDryRun { new_ops, new_state }) => {
                // Sets these ops to our transaction, bailing if the Catalog has changed since we
                // ran the transaction.
                ctx.session_mut()
                    .transaction_mut()
                    .add_ops(TransactionOps::DDL {
                        ops: new_ops,
                        state: new_state,
                        side_effects: vec![Box::new(side_effect)],
                        revision: self.catalog().transient_revision(),
                    })?;
                Ok(())
            }
            Ok(_) => unreachable!("unexpected success!"),
            Err(e) => Err(e),
        }
    }

    /// Perform a catalog transaction. [`Coordinator::ship_dataflow`] must be
    /// called after this function successfully returns on any built
    /// [`DataflowDesc`](mz_compute_types::dataflows::DataflowDesc).
    #[instrument(name = "coord::catalog_transact_inner")]
    pub(crate) async fn catalog_transact_inner<'a>(
        &mut self,
        conn_id: Option<&ConnectionId>,
        ops: Vec<catalog::Op>,
    ) -> Result<BuiltinTableAppendNotify, AdapterError> {
        if self.controller.read_only() {
            return Err(AdapterError::ReadOnly);
        }

        event!(Level::TRACE, ops = format!("{:?}", ops));

        let mut sources_to_drop = vec![];
        let mut webhook_sources_to_restart = BTreeSet::new();
        let mut table_gids_to_drop = vec![];
        let mut storage_sink_gids_to_drop = vec![];
        let mut indexes_to_drop = vec![];
        let mut materialized_views_to_drop = vec![];
        let mut continual_tasks_to_drop = vec![];
        let mut views_to_drop = vec![];
        let mut replication_slots_to_drop: Vec<(PostgresConnection, String)> = vec![];
        let mut secrets_to_drop = vec![];
        let mut vpc_endpoints_to_drop = vec![];
        let mut clusters_to_drop = vec![];
        let mut cluster_replicas_to_drop = vec![];
        let mut compute_sinks_to_drop = BTreeMap::new();
        let mut peeks_to_drop = vec![];
        let mut copies_to_drop = vec![];
        let mut clusters_to_create = vec![];
        let mut cluster_replicas_to_create = vec![];
        let mut update_metrics_config = false;
        let mut update_tracing_config = false;
        let mut update_controller_config = false;
        let mut update_compute_config = false;
        let mut update_storage_config = false;
        let mut update_pg_timestamp_oracle_config = false;
        let mut update_metrics_retention = false;
        let mut update_secrets_caching_config = false;
        let mut update_cluster_scheduling_config = false;
        let mut update_http_config = false;

        for op in &ops {
            match op {
                catalog::Op::DropObjects(drop_object_infos) => {
                    for drop_object_info in drop_object_infos {
                        match &drop_object_info {
                            catalog::DropObjectInfo::Item(id) => {
                                match self.catalog().get_entry(id).item() {
                                    CatalogItem::Table(table) => {
                                        table_gids_to_drop
                                            .extend(table.global_ids().map(|gid| (*id, gid)));
                                    }
                                    CatalogItem::Source(source) => {
                                        sources_to_drop.push((*id, source.global_id()));
                                        if let DataSourceDesc::Ingestion { desc, .. }
                                        | DataSourceDesc::OldSyntaxIngestion { desc, .. } =
                                            &source.data_source
                                        {
                                            match &desc.connection {
                                                GenericSourceConnection::Postgres(conn) => {
                                                    let conn = conn.clone().into_inline_connection(
                                                        self.catalog().state(),
                                                    );
                                                    let pending_drop = (
                                                        conn.connection.clone(),
                                                        conn.publication_details.slot.clone(),
                                                    );
                                                    replication_slots_to_drop.push(pending_drop);
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                    CatalogItem::Sink(sink) => {
                                        storage_sink_gids_to_drop.push(sink.global_id());
                                    }
                                    CatalogItem::Index(index) => {
                                        indexes_to_drop.push((index.cluster_id, index.global_id()));
                                    }
                                    CatalogItem::MaterializedView(mv) => {
                                        materialized_views_to_drop
                                            .push((mv.cluster_id, mv.global_id_writes()));
                                    }
                                    CatalogItem::View(view) => {
                                        views_to_drop.push((*id, view.clone()))
                                    }
                                    CatalogItem::ContinualTask(ct) => {
                                        continual_tasks_to_drop.push((
                                            *id,
                                            ct.cluster_id,
                                            ct.global_id(),
                                        ));
                                    }
                                    CatalogItem::Secret(_) => {
                                        secrets_to_drop.push(*id);
                                    }
                                    CatalogItem::Connection(Connection { details, .. }) => {
                                        match details {
                                            // SSH connections have an associated secret that should be dropped
                                            ConnectionDetails::Ssh { .. } => {
                                                secrets_to_drop.push(*id);
                                            }
                                            // AWS PrivateLink connections have an associated
                                            // VpcEndpoint K8S resource that should be dropped
                                            ConnectionDetails::AwsPrivatelink(_) => {
                                                vpc_endpoints_to_drop.push(*id);
                                            }
                                            _ => (),
                                        }
                                    }
                                    _ => (),
                                }
                            }
                            catalog::DropObjectInfo::Cluster(id) => {
                                clusters_to_drop.push(*id);
                            }
                            catalog::DropObjectInfo::ClusterReplica((
                                cluster_id,
                                replica_id,
                                _reason,
                            )) => {
                                // Drop the cluster replica itself.
                                cluster_replicas_to_drop.push((*cluster_id, *replica_id));
                            }
                            _ => (),
                        }
                    }
                }
                catalog::Op::ResetSystemConfiguration { name }
                | catalog::Op::UpdateSystemConfiguration { name, .. } => {
                    update_metrics_config |= self
                        .catalog
                        .state()
                        .system_config()
                        .is_metrics_config_var(name);
                    update_tracing_config |= vars::is_tracing_var(name);
                    update_controller_config |= self
                        .catalog
                        .state()
                        .system_config()
                        .is_controller_config_var(name);
                    update_compute_config |= self
                        .catalog
                        .state()
                        .system_config()
                        .is_compute_config_var(name);
                    update_storage_config |= self
                        .catalog
                        .state()
                        .system_config()
                        .is_storage_config_var(name);
                    update_pg_timestamp_oracle_config |=
                        vars::is_pg_timestamp_oracle_config_var(name);
                    update_metrics_retention |= name == vars::METRICS_RETENTION.name();
                    update_secrets_caching_config |= vars::is_secrets_caching_var(name);
                    update_cluster_scheduling_config |= vars::is_cluster_scheduling_var(name);
                    update_http_config |= vars::is_http_config_var(name);
                }
                catalog::Op::ResetAllSystemConfiguration => {
                    // Assume they all need to be updated.
                    // We could see if the config's have actually changed, but
                    // this is simpler.
                    update_tracing_config = true;
                    update_controller_config = true;
                    update_compute_config = true;
                    update_storage_config = true;
                    update_pg_timestamp_oracle_config = true;
                    update_metrics_retention = true;
                    update_secrets_caching_config = true;
                    update_cluster_scheduling_config = true;
                    update_http_config = true;
                }
                catalog::Op::RenameItem { id, .. } => {
                    let item = self.catalog().get_entry(id);
                    let is_webhook_source = item
                        .source()
                        .map(|s| matches!(s.data_source, DataSourceDesc::Webhook { .. }))
                        .unwrap_or(false);
                    if is_webhook_source {
                        webhook_sources_to_restart.insert(*id);
                    }
                }
                catalog::Op::RenameSchema {
                    database_spec,
                    schema_spec,
                    ..
                } => {
                    let schema = self.catalog().get_schema(
                        database_spec,
                        schema_spec,
                        conn_id.unwrap_or(&SYSTEM_CONN_ID),
                    );
                    let webhook_sources = schema.item_ids().filter(|id| {
                        let item = self.catalog().get_entry(id);
                        item.source()
                            .map(|s| matches!(s.data_source, DataSourceDesc::Webhook { .. }))
                            .unwrap_or(false)
                    });
                    webhook_sources_to_restart.extend(webhook_sources);
                }
                catalog::Op::CreateCluster { id, .. } => {
                    clusters_to_create.push(*id);
                }
                catalog::Op::CreateClusterReplica {
                    cluster_id,
                    name,
                    config,
                    ..
                } => {
                    cluster_replicas_to_create.push((
                        *cluster_id,
                        name.clone(),
                        config.location.num_processes(),
                    ));
                }
                _ => (),
            }
        }

        let collections_to_drop: BTreeSet<GlobalId> = sources_to_drop
            .iter()
            .map(|(_, gid)| *gid)
            .chain(table_gids_to_drop.iter().map(|(_, gid)| *gid))
            .chain(storage_sink_gids_to_drop.iter().copied())
            .chain(indexes_to_drop.iter().map(|(_, gid)| *gid))
            .chain(materialized_views_to_drop.iter().map(|(_, gid)| *gid))
            .chain(continual_tasks_to_drop.iter().map(|(_, _, gid)| *gid))
            .chain(views_to_drop.iter().map(|(_id, view)| view.global_id()))
            .collect();

        // Clean up any active compute sinks like subscribes or copy to-s that rely on dropped relations or clusters.
        for (sink_id, sink) in &self.active_compute_sinks {
            let cluster_id = sink.cluster_id();
            let conn_id = &sink.connection_id();
            if let Some(id) = sink
                .depends_on()
                .iter()
                .find(|id| collections_to_drop.contains(id))
            {
                let entry = self.catalog().get_entry_by_global_id(id);
                let name = self
                    .catalog()
                    .resolve_full_name(entry.name(), Some(conn_id))
                    .to_string();
                compute_sinks_to_drop.insert(
                    *sink_id,
                    ActiveComputeSinkRetireReason::DependencyDropped(format!(
                        "relation {}",
                        name.quoted()
                    )),
                );
            } else if clusters_to_drop.contains(&cluster_id) {
                let name = self.catalog().get_cluster(cluster_id).name();
                compute_sinks_to_drop.insert(
                    *sink_id,
                    ActiveComputeSinkRetireReason::DependencyDropped(format!(
                        "cluster {}",
                        name.quoted()
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
                let entry = self.catalog().get_entry_by_global_id(id);
                let name = self
                    .catalog()
                    .resolve_full_name(entry.name(), Some(&pending_peek.conn_id));
                peeks_to_drop.push((
                    format!("relation {}", name.to_string().quoted()),
                    uuid.clone(),
                ));
            } else if clusters_to_drop.contains(&pending_peek.cluster_id) {
                let name = self.catalog().get_cluster(pending_peek.cluster_id).name();
                peeks_to_drop.push((format!("cluster {}", name.quoted()), uuid.clone()));
            }
        }

        // Clean up any pending `COPY` statements that rely on dropped relations or clusters.
        for (conn_id, pending_copy) in &self.active_copies {
            let dropping_table = table_gids_to_drop
                .iter()
                .any(|(item_id, _gid)| pending_copy.table_id == *item_id);
            let dropping_cluster = clusters_to_drop.contains(&pending_copy.cluster_id);

            if dropping_table || dropping_cluster {
                copies_to_drop.push(conn_id.clone());
            }
        }

        let storage_ids_to_drop = sources_to_drop
            .iter()
            .map(|(_, gid)| *gid)
            .chain(storage_sink_gids_to_drop.iter().copied())
            .chain(table_gids_to_drop.iter().map(|(_, gid)| *gid))
            .chain(materialized_views_to_drop.iter().map(|(_, gid)| *gid))
            .chain(continual_tasks_to_drop.iter().map(|(_, _, gid)| *gid));
        let compute_ids_to_drop = indexes_to_drop
            .iter()
            .copied()
            .chain(materialized_views_to_drop.iter().copied())
            .chain(
                continual_tasks_to_drop
                    .iter()
                    .map(|(_, cluster_id, gid)| (*cluster_id, *gid)),
            );

        // Check if any Timelines would become empty, if we dropped the specified storage or
        // compute resources.
        //
        // Note: only after a Transaction succeeds do we actually drop the timeline
        let collection_id_bundle = self.build_collection_id_bundle(
            storage_ids_to_drop,
            compute_ids_to_drop,
            clusters_to_drop.clone(),
        );
        let timeline_associations: BTreeMap<_, _> = self
            .catalog()
            .partition_ids_by_timeline_context(&collection_id_bundle)
            .filter_map(|(context, bundle)| {
                let TimelineContext::TimelineDependent(timeline) = context else {
                    return None;
                };
                let TimelineState { read_holds, .. } = self
                    .global_timelines
                    .get(&timeline)
                    .expect("all timeslines have a timestamp oracle");

                let empty = read_holds.id_bundle().difference(&bundle).is_empty();

                Some((timeline, (empty, bundle)))
            })
            .collect();

        self.validate_resource_limits(&ops, conn_id.unwrap_or(&SYSTEM_CONN_ID))?;

        // This will produce timestamps that are guaranteed to increase on each
        // call, and also never be behind the system clock. If the system clock
        // hasn't advanced (or has gone backward), it will increment by 1. For
        // the audit log, we need to balance "close (within 10s or so) to the
        // system clock" and "always goes up". We've chosen here to prioritize
        // always going up, and believe we will always be close to the system
        // clock because it is well configured (chrony) and so may only rarely
        // regress or pause for 10s.
        let oracle_write_ts = self.get_local_write_ts().await.timestamp;

        let Coordinator {
            catalog,
            active_conns,
            controller,
            cluster_replica_statuses,
            ..
        } = self;
        let catalog = Arc::make_mut(catalog);
        let conn = conn_id.map(|id| active_conns.get(id).expect("connection must exist"));

        let TransactionResult {
            builtin_table_updates,
            audit_events,
        } = catalog
            .transact(
                Some(&mut controller.storage_collections),
                oracle_write_ts,
                conn,
                ops,
            )
            .await?;

        for (cluster_id, replica_id) in &cluster_replicas_to_drop {
            cluster_replica_statuses.remove_cluster_replica_statuses(cluster_id, replica_id);
        }
        for cluster_id in &clusters_to_drop {
            cluster_replica_statuses.remove_cluster_statuses(cluster_id);
        }
        for cluster_id in clusters_to_create {
            cluster_replica_statuses.initialize_cluster_statuses(cluster_id);
        }
        let now = to_datetime((catalog.config().now)());
        for (cluster_id, replica_name, num_processes) in cluster_replicas_to_create {
            let replica_id = catalog
                .resolve_replica_in_cluster(&cluster_id, &replica_name)
                .expect("just created")
                .replica_id();
            cluster_replica_statuses.initialize_cluster_replica_statuses(
                cluster_id,
                replica_id,
                num_processes,
                now,
            );
        }

        // Append our builtin table updates, then return the notify so we can run other tasks in
        // parallel.
        let (builtin_update_notify, _) = self
            .builtin_table_update()
            .execute(builtin_table_updates)
            .await;

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
            if !table_gids_to_drop.is_empty() {
                let ts = self.get_local_write_ts().await;
                self.drop_tables(table_gids_to_drop, ts.timestamp);
            }
            // Note that we drop tables before sources since there can be a weak dependency
            // on sources from tables in the storage controller that will result in error
            // logging that we'd prefer to avoid. This isn't an actual dependency issue but
            // we'd like to keep that error logging around to indicate when an actual
            // dependency error might occur.
            if !sources_to_drop.is_empty() {
                self.drop_sources(sources_to_drop);
            }
            if !webhook_sources_to_restart.is_empty() {
                self.restart_webhook_sources(webhook_sources_to_restart);
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
                fail::fail_point!("after_catalog_drop_replica");
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
            // TODO(database-issues#4154): This is inherently best effort. An ill-timed crash
            // means we'll never clean these resources up. Safer cleanup for non-Materialize resources.
            // See <https://github.com/MaterializeInc/database-issues/issues/4154>
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
                                // TODO (maz): since this is always true now, can we drop it?
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

            if update_metrics_config {
                mz_metrics::update_dyncfg(&self.catalog().system_config().dyncfg_updates());
            }
            if update_controller_config {
                self.update_controller_config();
            }
            if update_compute_config {
                self.update_compute_config();
            }
            if update_storage_config {
                self.update_storage_config();
            }
            if update_pg_timestamp_oracle_config {
                self.update_pg_timestamp_oracle_config();
            }
            if update_metrics_retention {
                self.update_metrics_retention();
            }
            if update_tracing_config {
                self.update_tracing_config();
            }
            if update_secrets_caching_config {
                self.update_secrets_caching_config();
            }
            if update_cluster_scheduling_config {
                self.update_cluster_scheduling_config();
            }
            if update_http_config {
                self.update_http_config();
            }
        }
        .instrument(info_span!("coord::catalog_transact_with::finalize"))
        .await;

        let conn = conn_id.and_then(|id| self.active_conns.get(id));
        if let Some(segment_client) = &self.segment_client {
            for VersionedEvent::V1(event) in audit_events {
                let event_type = format!(
                    "{} {}",
                    event.object_type.as_title_case(),
                    event.event_type.as_title_case()
                );
                segment_client.environment_track(
                    &self.catalog().config().environment_id,
                    event_type,
                    json!({ "details": event.details.as_json() }),
                    EventDetails {
                        user_id: conn
                            .and_then(|c| c.user().external_metadata.as_ref())
                            .map(|m| m.user_id),
                        application_name: conn.map(|c| c.application_name()),
                        ..Default::default()
                    },
                );
            }
        }

        // Note: It's important that we keep the function call inside macro, this way we only run
        // the consistency checks if sort assertions are enabled.
        mz_ore::soft_assert_eq_no_log!(
            self.check_consistency(),
            Ok(()),
            "coordinator inconsistency detected"
        );

        Ok(builtin_update_notify)
    }

    fn drop_replica(&mut self, cluster_id: ClusterId, replica_id: ReplicaId) {
        self.drop_introspection_subscribes(replica_id);

        self.controller
            .drop_replica(cluster_id, replica_id)
            .expect("dropping replica must not fail");
    }

    /// A convenience method for dropping sources.
    fn drop_sources(&mut self, sources: Vec<(CatalogItemId, GlobalId)>) {
        for (item_id, _gid) in &sources {
            self.active_webhooks.remove(item_id);
        }
        let storage_metadata = self.catalog.state().storage_metadata();
        let source_gids = sources.into_iter().map(|(_id, gid)| gid).collect();
        self.controller
            .storage
            .drop_sources(storage_metadata, source_gids)
            .unwrap_or_terminate("cannot fail to drop sources");
    }

    fn drop_tables(&mut self, tables: Vec<(CatalogItemId, GlobalId)>, ts: Timestamp) {
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

    fn restart_webhook_sources(&mut self, sources: impl IntoIterator<Item = CatalogItemId>) {
        for id in sources {
            self.active_webhooks.remove(&id);
        }
    }

    /// Like `drop_compute_sinks`, but for a single compute sink.
    ///
    /// Returns the controller's state for the compute sink if the identified
    /// sink was known to the controller. It is the caller's responsibility to
    /// retire the returned sink. Consider using `retire_compute_sinks` instead.
    #[must_use]
    pub async fn drop_compute_sink(&mut self, sink_id: GlobalId) -> Option<ActiveComputeSink> {
        self.drop_compute_sinks([sink_id]).await.remove(&sink_id)
    }

    /// Drops a batch of compute sinks.
    ///
    /// For each sink that exists, the coordinator and controller's state
    /// associated with the sink is removed.
    ///
    /// Returns a map containing the controller's state for each sink that was
    /// removed. It is the caller's responsibility to retire the returned sinks.
    /// Consider using `retire_compute_sinks` instead.
    #[must_use]
    pub async fn drop_compute_sinks(
        &mut self,
        sink_ids: impl IntoIterator<Item = GlobalId>,
    ) -> BTreeMap<GlobalId, ActiveComputeSink> {
        let mut by_id = BTreeMap::new();
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for sink_id in sink_ids {
            let sink = match self.remove_active_compute_sink(sink_id).await {
                None => {
                    tracing::error!(%sink_id, "drop_compute_sinks called on nonexistent sink");
                    continue;
                }
                Some(sink) => sink,
            };

            by_cluster
                .entry(sink.cluster_id())
                .or_default()
                .push(sink_id);
            by_id.insert(sink_id, sink);
        }
        for (cluster_id, ids) in by_cluster {
            let compute = &mut self.controller.compute;
            // A cluster could have been dropped, so verify it exists.
            if compute.instance_exists(cluster_id) {
                compute
                    .drop_collections(cluster_id, ids)
                    .unwrap_or_terminate("cannot fail to drop collections");
            }
        }
        by_id
    }

    /// Retires a batch of sinks with disparate reasons for retirement.
    ///
    /// Each sink identified in `reasons` is dropped (see `drop_compute_sinks`),
    /// then retired with its corresponding reason.
    pub async fn retire_compute_sinks(
        &mut self,
        mut reasons: BTreeMap<GlobalId, ActiveComputeSinkRetireReason>,
    ) {
        let sink_ids = reasons.keys().cloned();
        for (id, sink) in self.drop_compute_sinks(sink_ids).await {
            let reason = reasons
                .remove(&id)
                .expect("all returned IDs are in `reasons`");
            sink.retire(reason);
        }
    }

    /// Drops all pending replicas for a set of clusters
    /// that are undergoing reconfiguration.
    pub async fn drop_reconfiguration_replicas(
        &mut self,
        cluster_ids: BTreeSet<ClusterId>,
    ) -> Result<(), AdapterError> {
        let pending_cluster_ops: Vec<Op> = cluster_ids
            .iter()
            .map(|c| {
                self.catalog()
                    .get_cluster(c.clone())
                    .replicas()
                    .filter_map(|r| match r.config.location {
                        ReplicaLocation::Managed(ref l) if l.pending => {
                            Some(DropObjectInfo::ClusterReplica((
                                c.clone(),
                                r.replica_id,
                                ReplicaCreateDropReason::Manual,
                            )))
                        }
                        _ => None,
                    })
                    .collect::<Vec<DropObjectInfo>>()
            })
            .filter_map(|pending_replica_drop_ops_by_cluster| {
                match pending_replica_drop_ops_by_cluster.len() {
                    0 => None,
                    _ => Some(Op::DropObjects(pending_replica_drop_ops_by_cluster)),
                }
            })
            .collect();
        if !pending_cluster_ops.is_empty() {
            self.catalog_transact(None, pending_cluster_ops).await?;
        }
        Ok(())
    }

    /// Cancels all active compute sinks for the identified connection.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn cancel_compute_sinks_for_conn(&mut self, conn_id: &ConnectionId) {
        self.retire_compute_sinks_for_conn(conn_id, ActiveComputeSinkRetireReason::Canceled)
            .await
    }

    /// Cancels all active cluster reconfigurations sinks for the identified connection.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn cancel_cluster_reconfigurations_for_conn(
        &mut self,
        conn_id: &ConnectionId,
    ) {
        self.retire_cluster_reconfigurations_for_conn(conn_id).await
    }

    /// Retires all active compute sinks for the identified connection with the
    /// specified reason.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn retire_compute_sinks_for_conn(
        &mut self,
        conn_id: &ConnectionId,
        reason: ActiveComputeSinkRetireReason,
    ) {
        let drop_sinks = self
            .active_conns
            .get_mut(conn_id)
            .expect("must exist for active session")
            .drop_sinks
            .iter()
            .map(|sink_id| (*sink_id, reason.clone()))
            .collect();
        self.retire_compute_sinks(drop_sinks).await;
    }

    /// Cleans pending cluster reconfiguraiotns for the identified connection
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn retire_cluster_reconfigurations_for_conn(
        &mut self,
        conn_id: &ConnectionId,
    ) {
        let reconfiguring_clusters = self
            .active_conns
            .get(conn_id)
            .expect("must exist for active session")
            .pending_cluster_alters
            .clone();
        // try to drop reconfig replicas
        self.drop_reconfiguration_replicas(reconfiguring_clusters)
            .await
            .unwrap_or_terminate("cannot fail to drop reconfiguration replicas");

        self.active_conns
            .get_mut(conn_id)
            .expect("must exist for active session")
            .pending_cluster_alters
            .clear();
    }

    pub(crate) fn drop_storage_sinks(&mut self, sink_gids: Vec<GlobalId>) {
        let storage_metadata = self.catalog.state().storage_metadata();
        self.controller
            .storage
            .drop_sinks(storage_metadata, sink_gids)
            .unwrap_or_terminate("cannot fail to drop sinks");
    }

    pub(crate) fn drop_indexes(&mut self, indexes: Vec<(ClusterId, GlobalId)>) {
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for (cluster_id, gid) in indexes {
            by_cluster.entry(cluster_id).or_default().push(gid);
        }
        for (cluster_id, gids) in by_cluster {
            let compute = &mut self.controller.compute;
            // A cluster could have been dropped, so verify it exists.
            if compute.instance_exists(cluster_id) {
                compute
                    .drop_collections(cluster_id, gids)
                    .unwrap_or_terminate("cannot fail to drop collections");
            }
        }
    }

    /// A convenience method for dropping materialized views.
    fn drop_materialized_views(&mut self, mviews: Vec<(ClusterId, GlobalId)>) {
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        let mut mv_gids = Vec::new();
        for (cluster_id, gid) in mviews {
            by_cluster.entry(cluster_id).or_default().push(gid);
            mv_gids.push(gid);
        }

        // Drop compute sinks.
        for (cluster_id, ids) in by_cluster {
            let compute = &mut self.controller.compute;
            // A cluster could have been dropped, so verify it exists.
            if compute.instance_exists(cluster_id) {
                compute
                    .drop_collections(cluster_id, ids)
                    .unwrap_or_terminate("cannot fail to drop collections");
            }
        }

        // Drop storage resources.
        let storage_metadata = self.catalog.state().storage_metadata();
        self.controller
            .storage
            .drop_sources(storage_metadata, mv_gids)
            .unwrap_or_terminate("cannot fail to drop sources");
    }

    /// A convenience method for dropping continual tasks.
    fn drop_continual_tasks(&mut self, cts: Vec<(CatalogItemId, ClusterId, GlobalId)>) {
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        let mut source_ids = Vec::new();
        for (item_id, cluster_id, gid) in cts {
            by_cluster.entry(cluster_id).or_default().push(gid);
            source_ids.push((item_id, gid));
        }

        // Drop compute sinks.
        for (cluster_id, ids) in by_cluster {
            let compute = &mut self.controller.compute;
            // A cluster could have been dropped, so verify it exists.
            if compute.instance_exists(cluster_id) {
                compute
                    .drop_collections(cluster_id, ids)
                    .unwrap_or_terminate("cannot fail to drop collections");
            }
        }

        // Drop storage sources.
        self.drop_sources(source_ids)
    }

    fn drop_vpc_endpoints_in_background(&self, vpc_endpoints: Vec<CatalogItemId>) {
        let cloud_resource_controller = Arc::clone(self.cloud_resource_controller
            .as_ref()
            .ok_or(AdapterError::Unsupported("AWS PrivateLink connections"))
            .expect("vpc endpoints should only be dropped in CLOUD, where `cloud_resource_controller` is `Some`"));
        // We don't want to block the coordinator on an external delete api
        // calls, so move the drop vpc_endpoint to a separate task. This does
        // mean that a failed drop won't bubble up to the user as an error
        // message. However, even if it did (and how the code previously
        // worked), mz has already dropped it from our catalog, and so we
        // wouldn't be able to retry anyway. Any orphaned vpc_endpoints will
        // eventually be cleaned during restart via coord bootstrap.
        task::spawn(
            || "drop_vpc_endpoints",
            async move {
                for vpc_endpoint in vpc_endpoints {
                    let _ = Retry::default()
                        .max_duration(Duration::from_secs(60))
                        .retry_async(|_state| async {
                            fail_point!("drop_vpc_endpoint", |r| {
                                Err(anyhow::anyhow!("Fail point error {:?}", r))
                            });
                            match cloud_resource_controller
                                .delete_vpc_endpoint(vpc_endpoint)
                                .await
                            {
                                Ok(_) => Ok(()),
                                Err(e) => {
                                    warn!("Dropping VPC Endpoints has encountered an error: {}", e);
                                    Err(e)
                                }
                            }
                        })
                        .await;
                }
            }
            .instrument(info_span!(
                "coord::catalog_transact_inner::drop_vpc_endpoints"
            )),
        );
    }

    /// Removes all temporary items created by the specified connection, though
    /// not the temporary schema itself.
    pub(crate) async fn drop_temp_items(&mut self, conn_id: &ConnectionId) {
        let temp_items = self.catalog().state().get_temp_items(conn_id).collect();
        let all_items = self.catalog().object_dependents(&temp_items, conn_id);

        if all_items.is_empty() {
            return;
        }
        let op = Op::DropObjects(
            all_items
                .into_iter()
                .map(DropObjectInfo::manual_drop_from_object_id)
                .collect(),
        );

        self.catalog_transact_conn(Some(conn_id), vec![op])
            .await
            .expect("unable to drop temporary items for conn_id");
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
        self.controller.storage.update_parameters(config_params);
    }

    fn update_pg_timestamp_oracle_config(&self) {
        let config_params = flags::pg_timstamp_oracle_config(self.catalog().system_config());
        if let Some(config) = self.pg_timestamp_oracle_config.as_ref() {
            config_params.apply(config)
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

    pub(crate) async fn create_storage_export(
        &mut self,
        id: GlobalId,
        sink: &Sink,
    ) -> Result<(), AdapterError> {
        // Validate `sink.from` is in fact a storage collection
        self.controller.storage.check_exists(sink.from)?;

        // The AsOf is used to determine at what time to snapshot reading from
        // the persist collection.  This is primarily relevant when we do _not_
        // want to include the snapshot in the sink.
        //
        // We choose the smallest as_of that is legal, according to the sinked
        // collection's since.
        let id_bundle = crate::CollectionIdBundle {
            storage_ids: btreeset! {sink.from},
            compute_ids: btreemap! {},
        };

        // We're putting in place read holds, such that create_exports, below,
        // which calls update_read_capabilities, can successfully do so.
        // Otherwise, the since of dependencies might move along concurrently,
        // pulling the rug from under us!
        //
        // TODO: Maybe in the future, pass those holds on to storage, to hold on
        // to them and downgrade when possible?
        let read_holds = self.acquire_read_holds(&id_bundle);
        let as_of = read_holds.least_valid_read();

        let storage_sink_from_entry = self.catalog().get_entry_by_global_id(&sink.from);
        let storage_sink_desc = mz_storage_types::sinks::StorageSinkDesc {
            from: sink.from,
            from_desc: storage_sink_from_entry
                .desc(&self.catalog().resolve_full_name(
                    storage_sink_from_entry.name(),
                    storage_sink_from_entry.conn_id(),
                ))
                .expect("sinks can only be built on items with descs")
                .into_owned(),
            connection: sink
                .connection
                .clone()
                .into_inline_connection(self.catalog().state()),
            envelope: sink.envelope,
            as_of,
            with_snapshot: sink.with_snapshot,
            version: sink.version,
            from_storage_metadata: (),
            to_storage_metadata: (),
        };

        let collection_desc = CollectionDescription {
            // TODO(sinks): make generic once we have more than one sink type.
            desc: KAFKA_PROGRESS_DESC.clone(),
            data_source: DataSource::Sink {
                desc: ExportDescription {
                    sink: storage_sink_desc,
                    instance_id: sink.cluster_id,
                },
            },
            since: None,
            status_collection_id: None,
            timeline: None,
            primary: None,
        };
        let collections = vec![(id, collection_desc)];

        // Create the collections.
        let storage_metadata = self.catalog.state().storage_metadata();
        let res = self
            .controller
            .storage
            .create_collections(storage_metadata, None, collections)
            .await;

        // Drop read holds after the export has been created, at which point
        // storage will have put in its own read holds.
        drop(read_holds);

        Ok(res?)
    }

    /// Validate all resource limits in a catalog transaction and return an error if that limit is
    /// exceeded.
    fn validate_resource_limits(
        &self,
        ops: &Vec<catalog::Op>,
        conn_id: &ConnectionId,
    ) -> Result<(), AdapterError> {
        let mut new_kafka_connections = 0;
        let mut new_postgres_connections = 0;
        let mut new_mysql_connections = 0;
        let mut new_sql_server_connections = 0;
        let mut new_aws_privatelink_connections = 0;
        let mut new_tables = 0;
        let mut new_sources = 0;
        let mut new_sinks = 0;
        let mut new_materialized_views = 0;
        let mut new_clusters = 0;
        let mut new_replicas_per_cluster = BTreeMap::new();
        let mut new_credit_consumption_rate = Numeric::zero();
        let mut new_databases = 0;
        let mut new_schemas_per_database = BTreeMap::new();
        let mut new_objects_per_schema = BTreeMap::new();
        let mut new_secrets = 0;
        let mut new_roles = 0;
        let mut new_continual_tasks = 0;
        let mut new_network_policies = 0;
        for op in ops {
            match op {
                Op::CreateDatabase { .. } => {
                    new_databases += 1;
                }
                Op::CreateSchema { database_id, .. } => {
                    if let ResolvedDatabaseSpecifier::Id(database_id) = database_id {
                        *new_schemas_per_database.entry(database_id).or_insert(0) += 1;
                    }
                }
                Op::CreateRole { .. } => {
                    new_roles += 1;
                }
                Op::CreateNetworkPolicy { .. } => {
                    new_network_policies += 1;
                }
                Op::CreateCluster { .. } => {
                    // TODO(benesch): having deprecated linked clusters, remove
                    // the `max_sources` and `max_sinks` limit, and set a higher
                    // max cluster limit?
                    new_clusters += 1;
                }
                Op::CreateClusterReplica {
                    cluster_id, config, ..
                } => {
                    if cluster_id.is_user() {
                        *new_replicas_per_cluster.entry(*cluster_id).or_insert(0) += 1;
                        if let ReplicaLocation::Managed(location) = &config.location {
                            let replica_allocation = self
                                .catalog()
                                .cluster_replica_sizes()
                                .0
                                .get(location.size_for_billing())
                                .expect(
                                    "location size is validated against the cluster replica sizes",
                                );
                            new_credit_consumption_rate += replica_allocation.credits_per_hour
                        }
                    }
                }
                Op::CreateItem { name, item, .. } => {
                    *new_objects_per_schema
                        .entry((
                            name.qualifiers.database_spec.clone(),
                            name.qualifiers.schema_spec.clone(),
                        ))
                        .or_insert(0) += 1;
                    match item {
                        CatalogItem::Connection(connection) => match connection.details {
                            ConnectionDetails::Kafka(_) => new_kafka_connections += 1,
                            ConnectionDetails::Postgres(_) => new_postgres_connections += 1,
                            ConnectionDetails::MySql(_) => new_mysql_connections += 1,
                            ConnectionDetails::SqlServer(_) => new_sql_server_connections += 1,
                            ConnectionDetails::AwsPrivatelink(_) => {
                                new_aws_privatelink_connections += 1
                            }
                            ConnectionDetails::Csr(_)
                            | ConnectionDetails::Ssh { .. }
                            | ConnectionDetails::Aws(_)
                            | ConnectionDetails::IcebergCatalog(_) => {}
                        },
                        CatalogItem::Table(_) => {
                            new_tables += 1;
                        }
                        CatalogItem::Source(source) => {
                            new_sources += source.user_controllable_persist_shard_count()
                        }
                        CatalogItem::Sink(_) => new_sinks += 1,
                        CatalogItem::MaterializedView(_) => {
                            new_materialized_views += 1;
                        }
                        CatalogItem::Secret(_) => {
                            new_secrets += 1;
                        }
                        CatalogItem::ContinualTask(_) => {
                            new_continual_tasks += 1;
                        }
                        CatalogItem::Log(_)
                        | CatalogItem::View(_)
                        | CatalogItem::Index(_)
                        | CatalogItem::Type(_)
                        | CatalogItem::Func(_) => {}
                    }
                }
                Op::DropObjects(drop_object_infos) => {
                    for drop_object_info in drop_object_infos {
                        match drop_object_info {
                            DropObjectInfo::Cluster(_) => {
                                new_clusters -= 1;
                            }
                            DropObjectInfo::ClusterReplica((cluster_id, replica_id, _reason)) => {
                                if cluster_id.is_user() {
                                    *new_replicas_per_cluster.entry(*cluster_id).or_insert(0) -= 1;
                                    let cluster = self
                                        .catalog()
                                        .get_cluster_replica(*cluster_id, *replica_id);
                                    if let ReplicaLocation::Managed(location) =
                                        &cluster.config.location
                                    {
                                        let replica_allocation = self
                                            .catalog()
                                            .cluster_replica_sizes()
                                            .0
                                            .get(location.size_for_billing())
                                            .expect(
                                                "location size is validated against the cluster replica sizes",
                                            );
                                        new_credit_consumption_rate -=
                                            replica_allocation.credits_per_hour
                                    }
                                }
                            }
                            DropObjectInfo::Database(_) => {
                                new_databases -= 1;
                            }
                            DropObjectInfo::Schema((database_spec, _)) => {
                                if let ResolvedDatabaseSpecifier::Id(database_id) = database_spec {
                                    *new_schemas_per_database.entry(database_id).or_insert(0) -= 1;
                                }
                            }
                            DropObjectInfo::Role(_) => {
                                new_roles -= 1;
                            }
                            DropObjectInfo::NetworkPolicy(_) => {
                                new_network_policies -= 1;
                            }
                            DropObjectInfo::Item(id) => {
                                let entry = self.catalog().get_entry(id);
                                *new_objects_per_schema
                                    .entry((
                                        entry.name().qualifiers.database_spec.clone(),
                                        entry.name().qualifiers.schema_spec.clone(),
                                    ))
                                    .or_insert(0) -= 1;
                                match entry.item() {
                                    CatalogItem::Connection(connection) => match connection.details
                                    {
                                        ConnectionDetails::AwsPrivatelink(_) => {
                                            new_aws_privatelink_connections -= 1;
                                        }
                                        _ => (),
                                    },
                                    CatalogItem::Table(_) => {
                                        new_tables -= 1;
                                    }
                                    CatalogItem::Source(source) => {
                                        new_sources -=
                                            source.user_controllable_persist_shard_count()
                                    }
                                    CatalogItem::Sink(_) => new_sinks -= 1,
                                    CatalogItem::MaterializedView(_) => {
                                        new_materialized_views -= 1;
                                    }
                                    CatalogItem::Secret(_) => {
                                        new_secrets -= 1;
                                    }
                                    CatalogItem::ContinualTask(_) => {
                                        new_continual_tasks -= 1;
                                    }
                                    CatalogItem::Log(_)
                                    | CatalogItem::View(_)
                                    | CatalogItem::Index(_)
                                    | CatalogItem::Type(_)
                                    | CatalogItem::Func(_) => {}
                                }
                            }
                        }
                    }
                }
                Op::UpdateItem {
                    name: _,
                    id,
                    to_item,
                } => match to_item {
                    CatalogItem::Source(source) => {
                        let current_source = self
                            .catalog()
                            .get_entry(id)
                            .source()
                            .expect("source update is for source item");

                        new_sources += source.user_controllable_persist_shard_count()
                            - current_source.user_controllable_persist_shard_count();
                    }
                    CatalogItem::Connection(_)
                    | CatalogItem::Table(_)
                    | CatalogItem::Sink(_)
                    | CatalogItem::MaterializedView(_)
                    | CatalogItem::Secret(_)
                    | CatalogItem::Log(_)
                    | CatalogItem::View(_)
                    | CatalogItem::Index(_)
                    | CatalogItem::Type(_)
                    | CatalogItem::Func(_)
                    | CatalogItem::ContinualTask(_) => {}
                },
                Op::AlterRole { .. }
                | Op::AlterRetainHistory { .. }
                | Op::AlterNetworkPolicy { .. }
                | Op::AlterAddColumn { .. }
                | Op::UpdatePrivilege { .. }
                | Op::UpdateDefaultPrivilege { .. }
                | Op::GrantRole { .. }
                | Op::RenameCluster { .. }
                | Op::RenameClusterReplica { .. }
                | Op::RenameItem { .. }
                | Op::RenameSchema { .. }
                | Op::UpdateOwner { .. }
                | Op::RevokeRole { .. }
                | Op::UpdateClusterConfig { .. }
                | Op::UpdateClusterReplicaConfig { .. }
                | Op::UpdateSourceReferences { .. }
                | Op::UpdateSystemConfiguration { .. }
                | Op::ResetSystemConfiguration { .. }
                | Op::ResetAllSystemConfiguration { .. }
                | Op::Comment { .. }
                | Op::WeirdStorageUsageUpdates { .. }
                | Op::TransactionDryRun => {}
            }
        }

        let mut current_aws_privatelink_connections = 0;
        let mut current_postgres_connections = 0;
        let mut current_mysql_connections = 0;
        let mut current_sql_server_connections = 0;
        let mut current_kafka_connections = 0;
        for c in self.catalog().user_connections() {
            let connection = c
                .connection()
                .expect("`user_connections()` only returns connection objects");

            match connection.details {
                ConnectionDetails::AwsPrivatelink(_) => current_aws_privatelink_connections += 1,
                ConnectionDetails::Postgres(_) => current_postgres_connections += 1,
                ConnectionDetails::MySql(_) => current_mysql_connections += 1,
                ConnectionDetails::SqlServer(_) => current_sql_server_connections += 1,
                ConnectionDetails::Kafka(_) => current_kafka_connections += 1,
                ConnectionDetails::Csr(_)
                | ConnectionDetails::Ssh { .. }
                | ConnectionDetails::Aws(_)
                | ConnectionDetails::IcebergCatalog(_) => {}
            }
        }
        self.validate_resource_limit(
            current_kafka_connections,
            new_kafka_connections,
            SystemVars::max_kafka_connections,
            "Kafka Connection",
            MAX_KAFKA_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            current_postgres_connections,
            new_postgres_connections,
            SystemVars::max_postgres_connections,
            "PostgreSQL Connection",
            MAX_POSTGRES_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            current_mysql_connections,
            new_mysql_connections,
            SystemVars::max_mysql_connections,
            "MySQL Connection",
            MAX_MYSQL_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            current_sql_server_connections,
            new_sql_server_connections,
            SystemVars::max_sql_server_connections,
            "SQL Server Connection",
            MAX_SQL_SERVER_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            current_aws_privatelink_connections,
            new_aws_privatelink_connections,
            SystemVars::max_aws_privatelink_connections,
            "AWS PrivateLink Connection",
            MAX_AWS_PRIVATELINK_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().user_tables().count(),
            new_tables,
            SystemVars::max_tables,
            "table",
            MAX_TABLES.name(),
        )?;

        let current_sources: usize = self
            .catalog()
            .user_sources()
            .filter_map(|source| source.source())
            .map(|source| source.user_controllable_persist_shard_count())
            .sum::<i64>()
            .try_into()
            .expect("non-negative sum of sources");

        self.validate_resource_limit(
            current_sources,
            new_sources,
            SystemVars::max_sources,
            "source",
            MAX_SOURCES.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().user_sinks().count(),
            new_sinks,
            SystemVars::max_sinks,
            "sink",
            MAX_SINKS.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().user_materialized_views().count(),
            new_materialized_views,
            SystemVars::max_materialized_views,
            "materialized view",
            MAX_MATERIALIZED_VIEWS.name(),
        )?;
        self.validate_resource_limit(
            // Linked compute clusters don't count against the limit, since
            // we have a separate sources and sinks limit.
            //
            // TODO(benesch): remove the `max_sources` and `max_sinks` limit,
            // and set a higher max cluster limit?
            self.catalog().user_clusters().count(),
            new_clusters,
            SystemVars::max_clusters,
            "cluster",
            MAX_CLUSTERS.name(),
        )?;
        for (cluster_id, new_replicas) in new_replicas_per_cluster {
            // It's possible that the cluster hasn't been created yet.
            let current_amount = self
                .catalog()
                .try_get_cluster(cluster_id)
                .map(|instance| instance.user_replicas().count())
                .unwrap_or(0);
            self.validate_resource_limit(
                current_amount,
                new_replicas,
                SystemVars::max_replicas_per_cluster,
                "cluster replica",
                MAX_REPLICAS_PER_CLUSTER.name(),
            )?;
        }
        self.validate_resource_limit_numeric(
            self.current_credit_consumption_rate(),
            new_credit_consumption_rate,
            |system_vars| {
                self.license_key
                    .max_credit_consumption_rate()
                    .map_or_else(|| system_vars.max_credit_consumption_rate(), Numeric::from)
            },
            "cluster replica",
            MAX_CREDIT_CONSUMPTION_RATE.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().databases().count(),
            new_databases,
            SystemVars::max_databases,
            "database",
            MAX_DATABASES.name(),
        )?;
        for (database_id, new_schemas) in new_schemas_per_database {
            self.validate_resource_limit(
                self.catalog().get_database(database_id).schemas_by_id.len(),
                new_schemas,
                SystemVars::max_schemas_per_database,
                "schema",
                MAX_SCHEMAS_PER_DATABASE.name(),
            )?;
        }
        for ((database_spec, schema_spec), new_objects) in new_objects_per_schema {
            self.validate_resource_limit(
                self.catalog()
                    .get_schema(&database_spec, &schema_spec, conn_id)
                    .items
                    .len(),
                new_objects,
                SystemVars::max_objects_per_schema,
                "object",
                MAX_OBJECTS_PER_SCHEMA.name(),
            )?;
        }
        self.validate_resource_limit(
            self.catalog().user_secrets().count(),
            new_secrets,
            SystemVars::max_secrets,
            "secret",
            MAX_SECRETS.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().user_roles().count(),
            new_roles,
            SystemVars::max_roles,
            "role",
            MAX_ROLES.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().user_continual_tasks().count(),
            new_continual_tasks,
            SystemVars::max_continual_tasks,
            "continual_task",
            MAX_CONTINUAL_TASKS.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().user_continual_tasks().count(),
            new_network_policies,
            SystemVars::max_network_policies,
            "network_policy",
            MAX_NETWORK_POLICIES.name(),
        )?;
        Ok(())
    }

    /// Validate a specific type of resource limit and return an error if that limit is exceeded.
    pub(crate) fn validate_resource_limit<F>(
        &self,
        current_amount: usize,
        new_instances: i64,
        resource_limit: F,
        resource_type: &str,
        limit_name: &str,
    ) -> Result<(), AdapterError>
    where
        F: Fn(&SystemVars) -> u32,
    {
        if new_instances <= 0 {
            return Ok(());
        }

        let limit: i64 = resource_limit(self.catalog().system_config()).into();
        let current_amount: Option<i64> = current_amount.try_into().ok();
        let desired =
            current_amount.and_then(|current_amount| current_amount.checked_add(new_instances));

        let exceeds_limit = if let Some(desired) = desired {
            desired > limit
        } else {
            true
        };

        let desired = desired
            .map(|desired| desired.to_string())
            .unwrap_or_else(|| format!("more than {}", i64::MAX));
        let current = current_amount
            .map(|current| current.to_string())
            .unwrap_or_else(|| format!("more than {}", i64::MAX));
        if exceeds_limit {
            Err(AdapterError::ResourceExhaustion {
                resource_type: resource_type.to_string(),
                limit_name: limit_name.to_string(),
                desired,
                limit: limit.to_string(),
                current,
            })
        } else {
            Ok(())
        }
    }

    /// Validate a specific type of float resource limit and return an error if that limit is exceeded.
    ///
    /// This is very similar to [`Self::validate_resource_limit`] but for numerics.
    pub(crate) fn validate_resource_limit_numeric<F>(
        &self,
        current_amount: Numeric,
        new_amount: Numeric,
        resource_limit: F,
        resource_type: &str,
        limit_name: &str,
    ) -> Result<(), AdapterError>
    where
        F: Fn(&SystemVars) -> Numeric,
    {
        if new_amount <= Numeric::zero() {
            return Ok(());
        }

        let limit = resource_limit(self.catalog().system_config());
        // Floats will overflow to infinity instead of panicking, which has the correct comparison
        // semantics.
        // NaN should be impossible here since both values are positive.
        let desired = current_amount + new_amount;
        if desired > limit {
            Err(AdapterError::ResourceExhaustion {
                resource_type: resource_type.to_string(),
                limit_name: limit_name.to_string(),
                desired: desired.to_string(),
                limit: limit.to_string(),
                current: current_amount.to_string(),
            })
        } else {
            Ok(())
        }
    }
}
