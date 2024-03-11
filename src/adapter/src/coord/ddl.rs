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
use std::sync::Arc;
use std::time::Duration;

use fail::fail_point;
use futures::Future;
use maplit::{btreemap, btreeset};
use mz_adapter_types::compaction::SINCE_GRANULARITY;
use mz_adapter_types::connection::ConnectionId;
use mz_audit_log::VersionedEvent;
use mz_catalog::memory::objects::{
    CatalogItem, Connection, DataSourceDesc, Index, MaterializedView, Sink,
};
use mz_catalog::SYSTEM_CONN_ID;
use mz_compute_client::protocol::response::PeekResponse;
use mz_controller::clusters::ReplicaLocation;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::error::ErrorExt;
use mz_ore::instrument;
use mz_ore::retry::Retry;
use mz_ore::str::StrExt;
use mz_ore::task;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::{CatalogCluster, CatalogSchema};
use mz_sql::names::{ObjectId, ResolvedDatabaseSpecifier};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::{
    self, SystemVars, Var, MAX_AWS_PRIVATELINK_CONNECTIONS, MAX_CLUSTERS,
    MAX_CREDIT_CONSUMPTION_RATE, MAX_DATABASES, MAX_KAFKA_CONNECTIONS, MAX_MATERIALIZED_VIEWS,
    MAX_OBJECTS_PER_SCHEMA, MAX_POSTGRES_CONNECTIONS, MAX_REPLICAS_PER_CLUSTER, MAX_ROLES,
    MAX_SCHEMAS_PER_DATABASE, MAX_SECRETS, MAX_SINKS, MAX_SOURCES, MAX_TABLES,
};
use mz_storage_client::controller::ExportDescription;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::controller::StorageError;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sources::GenericSourceConnection;
use serde_json::json;
use tracing::{event, info_span, warn, Instrument, Level};

use crate::active_compute_sink::{ActiveComputeSink, ActiveComputeSinkRetireReason};
use crate::catalog::{Op, TransactionResult};
use crate::coord::appends::BuiltinTableAppendNotify;
use crate::coord::timeline::{TimelineContext, TimelineState};
use crate::coord::{Coordinator, ReplicaMetadata};
use crate::session::{Session, Transaction, TransactionOps};
use crate::statement_logging::StatementEndedExecutionReason;
use crate::telemetry::SegmentClientExt;
use crate::util::ResultExt;
use crate::{catalog, flags, AdapterError, TimestampProvider};

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
    pub(crate) async fn catalog_transact_with_side_effects<'c, F, Fut>(
        &'c mut self,
        session: Option<&Session>,
        ops: Vec<catalog::Op>,
        side_effect: F,
    ) -> Result<(), AdapterError>
    where
        F: FnOnce(&'c mut Coordinator) -> Fut,
        Fut: Future<Output = ()>,
    {
        let table_updates = self
            .catalog_transact_inner(session.map(|session| session.conn_id()), ops)
            .await?;
        let side_effects_fut = side_effect(self);

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
    pub(crate) async fn catalog_transact_with_ddl_transaction(
        &mut self,
        session: &mut Session,
        ops: Vec<catalog::Op>,
    ) -> Result<(), AdapterError> {
        let Some(Transaction {
            ops:
                TransactionOps::DDL {
                    ops: txn_ops,
                    revision: txn_revision,
                    state: _,
                },
            ..
        }) = session.transaction().inner()
        else {
            return self.catalog_transact(Some(session), ops).await;
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
        let result = self.catalog_transact(Some(session), all_ops).await;

        match result {
            // We purposefully fail with this error to prevent committing the transaction.
            Err(AdapterError::TransactionDryRun { new_ops, new_state }) => {
                // Adds these ops to our transaction, bailing if the Catalog has changed since we
                // ran the transaction.
                session.transaction_mut().add_ops(TransactionOps::DDL {
                    ops: new_ops,
                    state: new_state,
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
    /// [`DataflowDesc`].
    #[instrument(name = "coord::catalog_transact_inner")]
    pub(crate) async fn catalog_transact_inner<'a>(
        &mut self,
        conn_id: Option<&ConnectionId>,
        ops: Vec<catalog::Op>,
    ) -> Result<BuiltinTableAppendNotify, AdapterError> {
        event!(Level::TRACE, ops = format!("{:?}", ops));

        let mut sources_to_drop = vec![];
        let mut webhook_sources_to_restart = BTreeSet::new();
        let mut tables_to_drop = vec![];
        let mut storage_sinks_to_drop = vec![];
        let mut indexes_to_drop = vec![];
        let mut materialized_views_to_drop = vec![];
        let mut views_to_drop = vec![];
        let mut replication_slots_to_drop: Vec<(mz_postgres_util::Config, String)> = vec![];
        let mut secrets_to_drop = vec![];
        let mut vpc_endpoints_to_drop = vec![];
        let mut clusters_to_drop = vec![];
        let mut cluster_replicas_to_drop = vec![];
        let mut compute_sinks_to_drop = BTreeMap::new();
        let mut peeks_to_drop = vec![];
        let mut update_tracing_config = false;
        let mut update_compute_config = false;
        let mut update_storage_config = false;
        let mut update_pg_timestamp_oracle_config = false;
        let mut update_metrics_retention = false;
        let mut update_secrets_caching_config = false;
        let mut update_cluster_scheduling_config = false;
        let mut update_default_arrangement_merge_options = false;
        let mut update_http_config = false;
        let mut log_indexes_to_drop = Vec::new();

        for op in &ops {
            match op {
                catalog::Op::DropObject(ObjectId::Item(id)) => {
                    match self.catalog().get_entry(id).item() {
                        CatalogItem::Table(_) => {
                            tables_to_drop.push(*id);
                        }
                        CatalogItem::Source(source) => {
                            sources_to_drop.push(*id);
                            if let DataSourceDesc::Ingestion(ingestion) = &source.data_source {
                                match &ingestion.desc.connection {
                                    GenericSourceConnection::Postgres(conn) => {
                                        let conn = conn
                                            .clone()
                                            .into_inline_connection(self.catalog().state());
                                        let config = conn
                                            .connection
                                            .config(
                                                self.secrets_reader(),
                                                self.controller.storage.config(),
                                            )
                                            .await
                                            .map_err(|e| {
                                                AdapterError::Storage(StorageError::Generic(
                                                    anyhow::anyhow!(
                                                        "error creating Postgres client for \
                                                        dropping acquired slots: {}",
                                                        e.display_with_causes()
                                                    ),
                                                ))
                                            })?;

                                        replication_slots_to_drop
                                            .push((config, conn.publication_details.slot.clone()));
                                    }
                                    _ => {}
                                }
                            }
                        }
                        CatalogItem::Sink(Sink { .. }) => {
                            storage_sinks_to_drop.push(*id);
                        }
                        CatalogItem::Index(Index { cluster_id, .. }) => {
                            indexes_to_drop.push((*cluster_id, *id));
                        }
                        CatalogItem::MaterializedView(MaterializedView { cluster_id, .. }) => {
                            materialized_views_to_drop.push((*cluster_id, *id));
                        }
                        CatalogItem::View(_) => views_to_drop.push(*id),
                        CatalogItem::Secret(_) => {
                            secrets_to_drop.push(*id);
                        }
                        CatalogItem::Connection(Connection { connection, .. }) => {
                            match connection {
                                // SSH connections have an associated secret that should be dropped
                                mz_storage_types::connections::Connection::Ssh(_) => {
                                    secrets_to_drop.push(*id);
                                }
                                // AWS PrivateLink connections have an associated
                                // VpcEndpoint K8S resource that should be dropped
                                mz_storage_types::connections::Connection::AwsPrivatelink(_) => {
                                    vpc_endpoints_to_drop.push(*id);
                                }
                                _ => (),
                            }
                        }
                        _ => (),
                    }
                }
                catalog::Op::DropObject(ObjectId::Cluster(id)) => {
                    clusters_to_drop.push(*id);
                    log_indexes_to_drop.extend(
                        self.catalog()
                            .get_cluster(*id)
                            .log_indexes
                            .values()
                            .cloned(),
                    );
                }
                catalog::Op::DropObject(ObjectId::ClusterReplica((cluster_id, replica_id))) => {
                    // Drop the cluster replica itself.
                    cluster_replicas_to_drop.push((*cluster_id, *replica_id));
                }
                catalog::Op::ResetSystemConfiguration { name }
                | catalog::Op::UpdateSystemConfiguration { name, .. } => {
                    update_tracing_config |= vars::is_tracing_var(name);
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
                    update_default_arrangement_merge_options |=
                        name == vars::DEFAULT_IDLE_ARRANGEMENT_MERGE_EFFORT.name();
                    update_default_arrangement_merge_options |=
                        name == vars::DEFAULT_ARRANGEMENT_EXERT_PROPORTIONALITY.name();
                    update_http_config |= vars::is_http_config_var(name);
                }
                catalog::Op::ResetAllSystemConfiguration => {
                    // Assume they all need to be updated.
                    // We could see if the config's have actually changed, but
                    // this is simpler.
                    update_tracing_config = true;
                    update_compute_config = true;
                    update_storage_config = true;
                    update_pg_timestamp_oracle_config = true;
                    update_metrics_retention = true;
                    update_secrets_caching_config = true;
                    update_cluster_scheduling_config = true;
                    update_default_arrangement_merge_options = true;
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
                _ => (),
            }
        }

        let relations_to_drop: BTreeSet<_> = sources_to_drop
            .iter()
            .chain(tables_to_drop.iter())
            .chain(storage_sinks_to_drop.iter())
            .chain(indexes_to_drop.iter().map(|(_, id)| id))
            .chain(materialized_views_to_drop.iter().map(|(_, id)| id))
            .chain(views_to_drop.iter())
            .collect();

        // Clean up any active compute sinks like subscribes or copy to-s that rely on dropped relations or clusters.
        for (sink_id, sink) in &self.active_compute_sinks {
            let cluster_id = sink.cluster_id();
            let conn_id = &sink.connection_id();
            if let Some(id) = sink
                .depends_on()
                .iter()
                .find(|id| relations_to_drop.contains(id))
            {
                let entry = self.catalog().get_entry(id);
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
                .find(|id| relations_to_drop.contains(id))
            {
                let entry = self.catalog().get_entry(id);
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

        let storage_ids_to_drop = sources_to_drop
            .iter()
            .chain(storage_sinks_to_drop.iter())
            .chain(tables_to_drop.iter())
            .chain(materialized_views_to_drop.iter().map(|(_, id)| id))
            .cloned();
        let compute_ids_to_drop = indexes_to_drop
            .iter()
            .chain(materialized_views_to_drop.iter())
            .cloned();

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
            ..
        } = self;
        let catalog = Arc::make_mut(catalog);
        let conn = conn_id.map(|id| active_conns.get(id).expect("connection must exist"));

        let TransactionResult {
            builtin_table_updates,
            audit_events,
        } = catalog
            .transact(Some(&mut *controller.storage), oracle_write_ts, conn, ops)
            .await?;

        // Append our builtin table updates, then return the notify so we can run other tasks in
        // parallel.
        let builtin_update_notify = self
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
            if !sources_to_drop.is_empty() {
                self.drop_sources(sources_to_drop);
            }
            if !tables_to_drop.is_empty() {
                self.drop_sources(tables_to_drop);
            }
            if !webhook_sources_to_restart.is_empty() {
                self.restart_webhook_sources(webhook_sources_to_restart);
            }
            if !storage_sinks_to_drop.is_empty() {
                self.drop_storage_sinks(storage_sinks_to_drop);
            }
            if !compute_sinks_to_drop.is_empty() {
                self.retire_compute_sinks(compute_sinks_to_drop).await;
            }
            if !peeks_to_drop.is_empty() {
                for (dropped_name, uuid) in peeks_to_drop {
                    if let Some(pending_peek) = self.remove_pending_peek(&uuid) {
                        self.controller
                            .active_compute()
                            .cancel_peek(pending_peek.cluster_id, uuid)
                            .unwrap_or_terminate("unable to cancel peek");
                        self.retire_execution(
                            StatementEndedExecutionReason::Canceled,
                            pending_peek.ctx_extra,
                        );
                        // Client may have left.
                        let _ = pending_peek.sender.send(PeekResponse::Error(format!(
                            "query could not complete because {dropped_name} was dropped"
                        )));
                    }
                }
            }
            if !indexes_to_drop.is_empty() {
                self.drop_indexes(indexes_to_drop);
            }
            if !materialized_views_to_drop.is_empty() {
                self.drop_materialized_views(materialized_views_to_drop);
            }
            if !secrets_to_drop.is_empty() {
                self.drop_secrets(secrets_to_drop).await;
            }
            if !vpc_endpoints_to_drop.is_empty() {
                self.drop_vpc_endpoints(vpc_endpoints_to_drop).await;
            }
            if !cluster_replicas_to_drop.is_empty() {
                fail::fail_point!("after_catalog_drop_replica");
                for (cluster_id, replica_id) in cluster_replicas_to_drop {
                    self.drop_replica(cluster_id, replica_id).await;
                }
            }
            if !log_indexes_to_drop.is_empty() {
                for id in log_indexes_to_drop {
                    self.drop_compute_read_policy(&id);
                }
            }
            if !clusters_to_drop.is_empty() {
                for cluster_id in clusters_to_drop {
                    self.controller.drop_cluster(cluster_id);
                }
            }

            // We don't want to block the coordinator on an external postgres server, so
            // move the drop slots to a separate task. This does mean that a failed drop
            // slot won't bubble up to the user as an error message. However, even if it
            // did (and how the code previously worked), mz has already dropped it from our
            // catalog, and so we wouldn't be able to retry anyway.
            let ssh_tunnel_manager = self.connection_context().ssh_tunnel_manager.clone();
            if !replication_slots_to_drop.is_empty() {
                // TODO(guswynn): see if there is more relevant info to add to this name
                task::spawn(|| "drop_replication_slots", async move {
                    for (config, slot_name) in replication_slots_to_drop {
                        // Try to drop the replication slots, but give up after a while.
                        let _ = Retry::default()
                            .max_duration(Duration::from_secs(60))
                            .retry_async(|_state| async {
                                mz_postgres_util::drop_replication_slots(
                                    &ssh_tunnel_manager,
                                    config.clone(),
                                    &[&slot_name],
                                )
                                .await
                            })
                            .await;
                    }
                });
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
            if update_default_arrangement_merge_options {
                self.update_default_arrangement_merge_options();
            }
            if update_http_config {
                self.update_http_config();
            }
        }
        .instrument(info_span!("coord::catalog_transact_with::finalize"))
        .await;

        let conn = conn_id.and_then(|id| self.active_conns.get(id));
        if let (Some(segment_client), Some(user_metadata)) = (
            &self.segment_client,
            conn.and_then(|s| s.user().external_metadata.as_ref()),
        ) {
            for VersionedEvent::V1(event) in audit_events {
                let event_type = format!(
                    "{} {}",
                    event.object_type.as_title_case(),
                    event.event_type.as_title_case()
                );
                // Note: when there is no ConnMeta, that means something internal to
                // environmentd initiated the transaction, hence the default name.
                let application_name = conn.map(|s| s.application_name()).unwrap_or("environmentd");
                segment_client.environment_track(
                    &self.catalog().config().environment_id,
                    application_name,
                    user_metadata.user_id,
                    event_type,
                    json!({ "details": event.details.as_json() }),
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

    async fn drop_replica(&mut self, cluster_id: ClusterId, replica_id: ReplicaId) {
        if let Some(Some(ReplicaMetadata { metrics })) =
            self.transient_replica_metadata.insert(replica_id, None)
        {
            let mut updates = vec![];
            if let Some(metrics) = metrics {
                let retraction = self
                    .catalog()
                    .state()
                    .pack_replica_metric_updates(replica_id, &metrics, -1);
                updates.extend(retraction.into_iter());
            }
            self.builtin_table_update().background(updates);
        }
        self.controller
            .drop_replica(cluster_id, replica_id)
            .await
            .expect("dropping replica must not fail");
    }

    fn drop_sources(&mut self, sources: Vec<GlobalId>) {
        for id in &sources {
            self.active_webhooks.remove(id);
            self.drop_storage_read_policy(id);
        }
        self.controller
            .storage
            .drop_sources(sources)
            .unwrap_or_terminate("cannot fail to drop sources");
    }

    fn restart_webhook_sources(&mut self, sources: impl IntoIterator<Item = GlobalId>) {
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

            if !self
                .controller
                .compute
                .enable_aggressive_readhold_downgrades()
            {
                // If aggressive downgrades are disabled, compute sinks have read policies that we
                // must drop.
                if !self.drop_compute_read_policy(&sink_id) {
                    tracing::error!("Instructed to drop a compute sink that isn't one");
                    continue;
                }
            }

            by_cluster
                .entry(sink.cluster_id())
                .or_default()
                .push(sink_id);
            by_id.insert(sink_id, sink);
        }
        let mut compute = self.controller.active_compute();
        for (cluster_id, ids) in by_cluster {
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

    /// Cancels all active compute sinks for the identified connection.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn cancel_compute_sinks_for_conn(&mut self, conn_id: &ConnectionId) {
        self.retire_compute_sinks_for_conn(conn_id, ActiveComputeSinkRetireReason::Canceled)
            .await
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

    pub(crate) fn drop_storage_sinks(&mut self, sinks: Vec<GlobalId>) {
        for id in &sinks {
            self.drop_storage_read_policy(id);
        }
        self.controller
            .storage
            .drop_sinks(sinks)
            .unwrap_or_terminate("cannot fail to drop sinks");
    }

    pub(crate) fn drop_indexes(&mut self, indexes: Vec<(ClusterId, GlobalId)>) {
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for (cluster_id, id) in indexes {
            if self.drop_compute_read_policy(&id) {
                by_cluster.entry(cluster_id).or_default().push(id);
            } else {
                tracing::error!("Instructed to drop a non-index index");
            }
        }
        let mut compute = self.controller.active_compute();
        for (cluster_id, ids) in by_cluster {
            // A cluster could have been dropped, so verify it exists.
            if compute.instance_exists(cluster_id) {
                compute
                    .drop_collections(cluster_id, ids)
                    .unwrap_or_terminate("cannot fail to drop collections");
            }
        }
    }

    fn drop_materialized_views(&mut self, mviews: Vec<(ClusterId, GlobalId)>) {
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        let mut source_ids = Vec::new();
        for (cluster_id, id) in mviews {
            if !self
                .controller
                .compute
                .enable_aggressive_readhold_downgrades()
            {
                // If aggressive downgrades are disabled, MV dataflows have read policies that we
                // must drop.
                if !self.drop_compute_read_policy(&id) {
                    tracing::error!("Instructed to drop a materialized view that isn't one");
                    continue;
                }
            }

            by_cluster.entry(cluster_id).or_default().push(id);
            source_ids.push(id);
        }

        // Drop compute sinks.
        let mut compute = self.controller.active_compute();
        for (cluster_id, ids) in by_cluster {
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

    async fn drop_secrets(&mut self, secrets: Vec<GlobalId>) {
        fail_point!("drop_secrets");
        for secret in secrets {
            if let Err(e) = self.secrets_controller.delete(secret).await {
                warn!("Dropping secrets has encountered an error: {}", e);
            }
        }
    }

    async fn drop_vpc_endpoints(&mut self, vpc_endpoints: Vec<GlobalId>) {
        for vpc_endpoint in vpc_endpoints {
            if let Err(e) = self
                .cloud_resource_controller
                .as_ref()
                .ok_or(AdapterError::Unsupported("AWS PrivateLink connections"))
                .expect("vpc endpoints should only be dropped in CLOUD, where `cloud_resource_controller` is `Some`")
                .delete_vpc_endpoint(vpc_endpoint)
                .await
            {
                warn!("Dropping VPC Endpoints has encountered an error: {}", e);
                // TODO reschedule this https://github.com/MaterializeInc/cloud/issues/4407
            }
        }
    }

    /// Removes all temporary items created by the specified connection, though
    /// not the temporary schema itself.
    pub(crate) async fn drop_temp_items(&mut self, conn_id: &ConnectionId) {
        let ops = self.catalog_mut().drop_temp_item_ops(conn_id);
        if ops.is_empty() {
            return;
        }
        self.catalog_transact_conn(Some(conn_id), ops)
            .await
            .expect("unable to drop temporary items for conn_id");
    }

    fn update_cluster_scheduling_config(&mut self) {
        let config = flags::orchestrator_scheduling_config(self.catalog.system_config());
        self.controller
            .update_orchestrator_scheduling_config(config);
    }

    fn update_secrets_caching_config(&mut self) {
        let config = flags::caching_config(self.catalog.system_config());
        self.caching_secrets_reader.set_policy(config);
    }

    fn update_tracing_config(&mut self) {
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

    fn update_pg_timestamp_oracle_config(&mut self) {
        let config_params = flags::pg_timstamp_oracle_config(self.catalog().system_config());
        if let Some(config) = self.pg_timestamp_oracle_config.as_ref() {
            config_params.apply(config)
        }
    }

    fn update_metrics_retention(&mut self) {
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
        self.update_storage_base_read_policies(storage_policies);
        self.update_compute_base_read_policies(compute_policies);
    }

    fn update_default_arrangement_merge_options(&mut self) {
        let effort = self
            .catalog()
            .system_config()
            .default_idle_arrangement_merge_effort();
        self.controller
            .compute
            .set_default_idle_arrangement_merge_effort(effort);

        let prop = self
            .catalog()
            .system_config()
            .default_arrangement_exert_proportionality();
        self.controller
            .compute
            .set_default_arrangement_exert_proportionality(prop);
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
        self.controller.storage.collection(sink.from)?;

        let status_id = Some(
            self.catalog()
                .resolve_builtin_storage_collection(&mz_catalog::builtin::MZ_SINK_STATUS_HISTORY),
        );

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
        let as_of = self.least_valid_read(&id_bundle);

        let storage_sink_from_entry = self.catalog().get_entry(&sink.from);
        let storage_sink_desc = mz_storage_types::sinks::StorageSinkDesc {
            from: sink.from,
            from_desc: storage_sink_from_entry
                .desc(&self.catalog().resolve_full_name(
                    storage_sink_from_entry.name(),
                    storage_sink_from_entry.conn_id(),
                ))
                .expect("indexes can only be built on items with descs")
                .into_owned(),
            connection: sink
                .connection
                .clone()
                .into_inline_connection(self.catalog().state()),
            envelope: sink.envelope,
            as_of,
            with_snapshot: sink.with_snapshot,
            status_id,
            from_storage_metadata: (),
        };

        Ok(self
            .controller
            .storage
            .create_exports(vec![(
                id,
                ExportDescription {
                    sink: storage_sink_desc,
                    instance_id: sink.cluster_id,
                },
            )])
            .await?)
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
                Op::CreateCluster { .. } => {
                    // TODO(benesch): having deprecated linked clusters, remove
                    // the `max_sources` and `max_sinks` limit, and set a higher
                    // max cluster limit?
                    new_clusters += 1;
                }
                Op::CreateClusterReplica {
                    cluster_id, config, ..
                } => {
                    *new_replicas_per_cluster.entry(*cluster_id).or_insert(0) += 1;
                    if let ReplicaLocation::Managed(location) = &config.location {
                        let replica_allocation = self
                            .catalog()
                            .cluster_replica_sizes()
                            .0
                            .get(location.size_for_billing())
                            .expect("location size is validated against the cluster replica sizes");
                        new_credit_consumption_rate += replica_allocation.credits_per_hour
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
                        CatalogItem::Connection(connection) => {
                            use mz_storage_types::connections::Connection;
                            match connection.connection {
                                Connection::Kafka(_) => new_kafka_connections += 1,
                                Connection::Postgres(_) => new_postgres_connections += 1,
                                Connection::AwsPrivatelink(_) => {
                                    new_aws_privatelink_connections += 1
                                }
                                // TODO(roshan): Implement limits for MySQL
                                Connection::Csr(_)
                                | Connection::Ssh(_)
                                | Connection::Aws(_)
                                | Connection::MySql(_) => {}
                            }
                        }
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
                        CatalogItem::Log(_)
                        | CatalogItem::View(_)
                        | CatalogItem::Index(_)
                        | CatalogItem::Type(_)
                        | CatalogItem::Func(_) => {}
                    }
                }
                Op::DropObject(id) => match id {
                    ObjectId::Cluster(_) => {
                        new_clusters -= 1;
                    }
                    ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                        *new_replicas_per_cluster.entry(*cluster_id).or_insert(0) -= 1;
                        let cluster = self.catalog().get_cluster_replica(*cluster_id, *replica_id);
                        if let ReplicaLocation::Managed(location) = &cluster.config.location {
                            let replica_allocation = self
                                .catalog()
                                .cluster_replica_sizes()
                                .0
                                .get(location.size_for_billing())
                                .expect(
                                    "location size is validated against the cluster replica sizes",
                                );
                            new_credit_consumption_rate -= replica_allocation.credits_per_hour
                        }
                    }
                    ObjectId::Database(_) => {
                        new_databases -= 1;
                    }
                    ObjectId::Schema((database_spec, _)) => {
                        if let ResolvedDatabaseSpecifier::Id(database_id) = database_spec {
                            *new_schemas_per_database.entry(database_id).or_insert(0) -= 1;
                        }
                    }
                    ObjectId::Role(_) => {
                        new_roles -= 1;
                    }
                    ObjectId::Item(id) => {
                        let entry = self.catalog().get_entry(id);
                        *new_objects_per_schema
                            .entry((
                                entry.name().qualifiers.database_spec.clone(),
                                entry.name().qualifiers.schema_spec.clone(),
                            ))
                            .or_insert(0) -= 1;
                        match entry.item() {
                            CatalogItem::Connection(connection) => match connection.connection {
                                mz_storage_types::connections::Connection::AwsPrivatelink(_) => {
                                    new_aws_privatelink_connections -= 1;
                                }
                                _ => (),
                            },
                            CatalogItem::Table(_) => {
                                new_tables -= 1;
                            }
                            CatalogItem::Source(source) => {
                                new_sources -= source.user_controllable_persist_shard_count()
                            }
                            CatalogItem::Sink(_) => new_sinks -= 1,
                            CatalogItem::MaterializedView(_) => {
                                new_materialized_views -= 1;
                            }
                            CatalogItem::Secret(_) => {
                                new_secrets -= 1;
                            }
                            CatalogItem::Log(_)
                            | CatalogItem::View(_)
                            | CatalogItem::Index(_)
                            | CatalogItem::Type(_)
                            | CatalogItem::Func(_) => {}
                        }
                    }
                },
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
                    | CatalogItem::Func(_) => {}
                },
                Op::AlterRole { .. }
                | Op::AlterSetCluster { .. }
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
                | Op::UpdateClusterReplicaStatus { .. }
                | Op::UpdateStorageUsage { .. }
                | Op::UpdateSystemConfiguration { .. }
                | Op::ResetSystemConfiguration { .. }
                | Op::ResetAllSystemConfiguration { .. }
                | Op::UpdateRotatedKeys { .. }
                | Op::Comment { .. }
                | Op::TransactionDryRun => {}
            }
        }

        let mut current_aws_privatelink_connections = 0;
        let mut current_postgres_connections = 0;
        let mut current_kafka_connections = 0;
        for c in self.catalog().user_connections() {
            let connection = c
                .connection()
                .expect("`user_connections()` only returns connection objects");

            use mz_storage_types::connections::Connection;
            match connection.connection {
                Connection::AwsPrivatelink(_) => current_aws_privatelink_connections += 1,
                Connection::Postgres(_) => current_postgres_connections += 1,
                Connection::Kafka(_) => current_kafka_connections += 1,
                // TODO(roshan): Implement limits for MySQL
                Connection::Csr(_)
                | Connection::Ssh(_)
                | Connection::Aws(_)
                | Connection::MySql(_) => {}
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
                .map(|instance| instance.replicas().count())
                .unwrap_or(0);
            self.validate_resource_limit(
                current_amount,
                new_replicas,
                SystemVars::max_replicas_per_cluster,
                "cluster replica",
                MAX_REPLICAS_PER_CLUSTER.name(),
            )?;
        }
        let current_credit_consumption_rate = self
            .catalog()
            .user_cluster_replicas()
            .filter_map(|replica| match &replica.config.location {
                ReplicaLocation::Managed(location) => Some(location.size_for_billing()),
                ReplicaLocation::Unmanaged(_) => None,
            })
            .map(|size| {
                self.catalog()
                    .cluster_replica_sizes()
                    .0
                    .get(size)
                    .expect("location size is validated against the cluster replica sizes")
                    .credits_per_hour
            })
            .sum();
        self.validate_resource_limit_numeric(
            current_credit_consumption_rate,
            new_credit_consumption_rate,
            SystemVars::max_credit_consumption_rate,
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
    fn validate_resource_limit_numeric<F>(
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
