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
use std::time::Duration;

use fail::fail_point;
use serde_json::json;
use timely::progress::Antichain;
use tracing::Level;
use tracing::{event, warn};

use mz_audit_log::VersionedEvent;
use mz_compute_client::protocol::response::PeekResponse;
use mz_controller::clusters::{ClusterId, ReplicaId};
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::names::ResolvedDatabaseSpecifier;
use mz_sql::session::vars::{self, SystemVars, Var};
use mz_storage_client::controller::{CreateExportToken, ExportDescription, ReadPolicy};
use mz_storage_client::types::sinks::{SinkAsOf, StorageSinkConnection};
use mz_storage_client::types::sources::{GenericSourceConnection, Timeline};

use crate::catalog::{
    CatalogItem, CatalogState, DataSourceDesc, Op, Sink, StorageSinkConnectionState,
    TransactionResult, SYSTEM_CONN_ID,
};
use crate::client::ConnectionId;
use crate::coord::appends::BuiltinTableUpdateSource;
use crate::coord::{Coordinator, ReplicaMetadata};
use crate::session::Session;
use crate::telemetry::SegmentClientExt;
use crate::util::{ComputeSinkId, ResultExt};
use crate::{catalog, AdapterError, AdapterNotice};

use super::timeline::{TimelineContext, TimelineState};

/// State provided to a catalog transaction closure.
pub struct CatalogTxn<'a, T> {
    pub(crate) dataflow_client: &'a mz_controller::Controller<T>,
    pub(crate) catalog: &'a CatalogState,
}

impl Coordinator {
    /// Same as [`Self::catalog_transact_with`] without a closure passed in.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn catalog_transact(
        &mut self,
        session: Option<&Session>,
        ops: Vec<catalog::Op>,
    ) -> Result<(), AdapterError> {
        self.catalog_transact_with(session, ops, |_| Ok(())).await
    }

    /// Perform a catalog transaction. The closure is passed a [`CatalogTxn`]
    /// made from the prospective [`CatalogState`] (i.e., the `Catalog` with `ops`
    /// applied but before the transaction is committed). The closure can return
    /// an error to abort the transaction, or otherwise return a value that is
    /// returned by this function. This allows callers to error while building
    /// [`DataflowDesc`]s. [`Coordinator::ship_dataflow`] must be called after this
    /// function successfully returns on any built `DataflowDesc`.
    ///
    /// [`CatalogState`]: crate::catalog::CatalogState
    /// [`DataflowDesc`]: mz_compute_client::types::dataflows::DataflowDesc
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn catalog_transact_with<F, R>(
        &mut self,
        session: Option<&Session>,
        mut ops: Vec<catalog::Op>,
        f: F,
    ) -> Result<R, AdapterError>
    where
        F: FnOnce(CatalogTxn<Timestamp>) -> Result<R, AdapterError>,
    {
        event!(Level::TRACE, ops = format!("{:?}", ops));

        let mut sources_to_drop = vec![];
        let mut tables_to_drop = vec![];
        let mut storage_sinks_to_drop = vec![];
        let mut indexes_to_drop = vec![];
        let mut materialized_views_to_drop = vec![];
        let mut replication_slots_to_drop: Vec<(mz_postgres_util::Config, String)> = vec![];
        let mut secrets_to_drop = vec![];
        let mut timelines_to_drop = vec![];
        let mut vpc_endpoints_to_drop = vec![];
        let mut clusters_to_drop = vec![];
        let mut cluster_replicas_to_drop = vec![];
        let mut peeks_to_drop = vec![];
        let mut update_compute_config = false;
        let mut update_storage_config = false;
        let mut update_metrics_retention = false;

        for op in &ops {
            match op {
                catalog::Op::DropItem(id) => {
                    match self.catalog().get_entry(id).item() {
                        CatalogItem::Table(_) => {
                            tables_to_drop.push(*id);
                        }
                        CatalogItem::Source(source) => {
                            sources_to_drop.push(*id);
                            if let DataSourceDesc::Ingestion(ingestion) = &source.data_source {
                                match &ingestion.desc.connection {
                                    GenericSourceConnection::Postgres(conn) => {
                                        let config = conn
                                            .connection
                                            .config(&*self.connection_context.secrets_reader)
                                            .await
                                            .unwrap_or_else(|e| {
                                                panic!("Postgres source {id} missing secrets: {e}")
                                            });
                                        replication_slots_to_drop
                                            .push((config, conn.publication_details.slot.clone()));
                                    }
                                    _ => {}
                                }
                            }
                        }
                        CatalogItem::Sink(catalog::Sink { connection, .. }) => match connection {
                            StorageSinkConnectionState::Ready(_) => {
                                storage_sinks_to_drop.push(*id);
                            }
                            StorageSinkConnectionState::Pending(_) => (),
                        },
                        CatalogItem::Index(catalog::Index { cluster_id, .. }) => {
                            indexes_to_drop.push((*cluster_id, *id));
                        }
                        CatalogItem::MaterializedView(catalog::MaterializedView {
                            cluster_id,
                            ..
                        }) => {
                            materialized_views_to_drop.push((*cluster_id, *id));
                        }
                        CatalogItem::Secret(_) => {
                            secrets_to_drop.push(*id);
                        }
                        CatalogItem::Connection(catalog::Connection { connection, .. }) => {
                            match connection {
                                // SSH connections have an associated secret that should be dropped
                                mz_storage_client::types::connections::Connection::Ssh(_) => {
                                    secrets_to_drop.push(*id);
                                }
                                // AWS PrivateLink connections have an associated
                                // VpcEndpoint K8S resource that should be dropped
                                mz_storage_client::types::connections::Connection::AwsPrivatelink(
                                    _,
                                ) => {
                                    vpc_endpoints_to_drop.push(*id);
                                }
                                _ => (),
                            }
                        }
                        _ => (),
                    }
                }
                catalog::Op::DropCluster { id } => {
                    clusters_to_drop.push(*id);
                }
                catalog::Op::DropClusterReplica {
                    cluster_id,
                    replica_id,
                } => {
                    // Drop the cluster replica itself.
                    cluster_replicas_to_drop.push((*cluster_id, *replica_id));
                }
                catalog::Op::ResetSystemConfiguration { name }
                | catalog::Op::UpdateSystemConfiguration { name, .. } => {
                    update_compute_config |= vars::is_compute_config_var(name);
                    update_storage_config |= vars::is_storage_config_var(name);
                    update_metrics_retention |= name == vars::METRICS_RETENTION.name();
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
            .collect();

        // Clean up any active subscribes that rely on dropped relations.
        let subscribe_sinks_to_drop: Vec<_> = self
            .active_subscribes
            .iter()
            .filter(|(_id, sub)| !sub.dropping)
            .filter_map(|(sink_id, sub)| {
                sub.depends_on
                    .iter()
                    .find(|id| relations_to_drop.contains(id))
                    .map(|dependent_id| (dependent_id, sink_id, sub))
            })
            .map(|(dependent_id, sink_id, active_subscribe)| {
                let conn_id = active_subscribe.conn_id;
                let entry = self.catalog().get_entry(dependent_id);
                let name = self
                    .catalog()
                    .resolve_full_name(entry.name(), Some(conn_id));

                (
                    (conn_id, name.to_string()),
                    ComputeSinkId {
                        cluster_id: active_subscribe.cluster_id,
                        global_id: *sink_id,
                    },
                )
            })
            .collect();

        // Clean up any pending peeks that rely on dropped relations.
        for (uuid, pending_peek) in &self.pending_peeks {
            if let Some(id) = pending_peek
                .depends_on
                .iter()
                .find(|id| relations_to_drop.contains(id))
            {
                let entry = self.catalog().get_entry(id);
                let name = self
                    .catalog()
                    .resolve_full_name(entry.name(), Some(pending_peek.conn_id));
                peeks_to_drop.push((name.to_string(), uuid.clone()));
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
        timelines_to_drop.extend(
            timeline_associations
                .iter()
                .filter_map(|(timeline, (is_empty, _))| is_empty.then_some(timeline))
                .cloned(),
        );
        ops.extend(
            timelines_to_drop
                .iter()
                .cloned()
                .map(catalog::Op::DropTimeline),
        );

        self.validate_resource_limits(
            &ops,
            session
                .map(|session| session.conn_id())
                .unwrap_or(SYSTEM_CONN_ID),
        )?;

        // This will produce timestamps that are guaranteed to increase on each
        // call, and also never be behind the system clock. If the system clock
        // hasn't advanced (or has gone backward), it will increment by 1. For
        // the audit log, we need to balance "close (within 10s or so) to the
        // system clock" and "always goes up". We've chosen here to prioritize
        // always going up, and believe we will always be close to the system
        // clock because it is well configured (chrony) and so may only rarely
        // regress or pause for 10s.
        let oracle_write_ts = self.get_local_write_ts().await.timestamp;

        let (catalog, controller) = self.catalog_and_controller_mut();
        let TransactionResult {
            builtin_table_updates,
            audit_events,
            result,
        } = catalog
            .transact(oracle_write_ts, session, ops, |catalog| {
                f(CatalogTxn {
                    dataflow_client: controller,
                    catalog,
                })
            })
            .await?;

        // No error returns are allowed after this point. Enforce this at compile time
        // by using this odd structure so we don't accidentally add a stray `?`.
        let _: () = async {
            self.send_builtin_table_updates(builtin_table_updates, BuiltinTableUpdateSource::DDL)
                .await;

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
            if !storage_sinks_to_drop.is_empty() {
                self.drop_storage_sinks(storage_sinks_to_drop);
            }
            if !subscribe_sinks_to_drop.is_empty() {
                let (dropped_metadata, subscribe_sinks_to_drop): (Vec<_>, BTreeSet<_>) =
                    subscribe_sinks_to_drop.into_iter().unzip();
                for (conn_id, dropped_name) in dropped_metadata {
                    if let Some(conn_meta) = self.active_conns.get_mut(&conn_id) {
                        conn_meta
                            .drop_sinks
                            .retain(|sink| !subscribe_sinks_to_drop.contains(sink));
                        // Send notice on a best effort basis.
                        let _ = conn_meta
                            .notice_tx
                            .send(AdapterNotice::DroppedSubscribe { dropped_name });
                    }
                }
                self.drop_compute_sinks(subscribe_sinks_to_drop);
            }
            if !peeks_to_drop.is_empty() {
                for (dropped_name, uuid) in peeks_to_drop {
                    if let Some(pending_peek) = self.remove_pending_peek(&uuid) {
                        self.controller
                            .active_compute()
                            .cancel_peeks(pending_peek.cluster_id, vec![uuid].into_iter().collect())
                            .unwrap_or_terminate("unable to cancel peek");
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
            if !replication_slots_to_drop.is_empty() {
                // TODO(guswynn): see if there is more relevant info to add to this name
                task::spawn(|| "drop_replication_slots", async move {
                    for (config, slot_name) in replication_slots_to_drop {
                        // Try to drop the replication slots, but give up after a while.
                        let _ = Retry::default()
                            .max_duration(Duration::from_secs(30))
                            .retry_async(|_state| async {
                                mz_postgres_util::drop_replication_slots(
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
            if update_metrics_retention {
                self.update_metrics_retention();
            }
        }
        .await;

        if let (Some(segment_client), Some(user_metadata)) = (
            &self.segment_client,
            session.and_then(|s| s.user().external_metadata.as_ref()),
        ) {
            for VersionedEvent::V1(event) in audit_events {
                let event_type = format!(
                    "{} {}",
                    event.object_type.as_title_case(),
                    event.event_type.as_title_case()
                );
                segment_client.environment_track(
                    &self.catalog().config().environment_id,
                    user_metadata.user_id,
                    event_type,
                    json!({ "details": event.details.as_json() }),
                );
            }
        }

        Ok(result)
    }

    async fn drop_replica(&mut self, cluster_id: ClusterId, replica_id: ReplicaId) {
        if let Some(Some(ReplicaMetadata {
            last_heartbeat,
            metrics,
            write_frontiers,
        })) = self.transient_replica_metadata.insert(replica_id, None)
        {
            let mut updates = vec![];
            if let Some(last_heartbeat) = last_heartbeat {
                let retraction = self.catalog().state().pack_replica_heartbeat_update(
                    replica_id,
                    last_heartbeat,
                    -1,
                );
                updates.push(retraction);
            }
            if let Some(metrics) = metrics {
                let retraction = self
                    .catalog()
                    .state()
                    .pack_replica_metric_updates(replica_id, &metrics, -1);
                updates.extend(retraction.into_iter());
            }
            let retraction = self.catalog().state().pack_replica_write_frontiers_updates(
                replica_id,
                &write_frontiers,
                -1,
            );
            updates.extend(retraction.into_iter());
            self.send_builtin_table_updates(updates, BuiltinTableUpdateSource::Background)
                .await;
        }
        self.controller
            .drop_replica(cluster_id, replica_id)
            .await
            .expect("dropping replica must not fail");
    }

    fn drop_sources(&mut self, sources: Vec<GlobalId>) {
        for id in &sources {
            self.drop_storage_read_policy(id);
        }
        self.controller
            .storage
            .drop_sources(sources)
            .unwrap_or_terminate("cannot fail to drop sources");
    }

    pub(crate) fn drop_compute_sinks(&mut self, sinks: impl IntoIterator<Item = ComputeSinkId>) {
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for sink in sinks {
            // Filter out sinks that are currently being dropped. When dropping a sink
            // we send a request to compute to drop it, but don't actually remove it from
            // `active_subscribes` until compute responds, hence the `dropping` flag.
            //
            // Note: Ideally we'd use .filter(...) on the iterator, but that would
            // require simultaneously getting an immutable and mutable borrow of self.
            let need_to_drop = self
                .active_subscribes
                .get(&sink.global_id)
                .map(|meta| !meta.dropping)
                .unwrap_or(false);
            if !need_to_drop {
                continue;
            }

            if self.drop_compute_read_policy(&sink.global_id) {
                by_cluster
                    .entry(sink.cluster_id)
                    .or_default()
                    .push(sink.global_id);

                // Mark the sink as dropped so we don't try to drop it again.
                if let Some(sink) = self.active_subscribes.get_mut(&sink.global_id) {
                    sink.dropping = true;
                }
            } else {
                tracing::error!("Instructed to drop a compute sink that isn't one");
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
            if self.drop_compute_read_policy(&id) {
                by_cluster.entry(cluster_id).or_default().push(id);
                source_ids.push(id);
            } else {
                tracing::error!("Instructed to drop a materialized view that isn't one");
            }
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
    pub(crate) async fn drop_temp_items(&mut self, session: &Session) {
        let ops = self.catalog_mut().drop_temp_item_ops(&session.conn_id());
        if ops.is_empty() {
            return;
        }
        self.catalog_transact(Some(session), ops)
            .await
            .expect("unable to drop temporary items for conn_id");
    }

    fn update_compute_config(&mut self) {
        let config_params = self.catalog().compute_config();
        self.controller.compute.update_configuration(config_params);
    }

    fn update_storage_config(&mut self) {
        let config_params = self.catalog().storage_config();
        self.controller.storage.update_configuration(config_params);
    }

    fn update_metrics_retention(&mut self) {
        let duration = self.catalog().system_config().metrics_retention();
        let policy = ReadPolicy::lag_writes_by(Timestamp::new(
            u64::try_from(duration.as_millis()).unwrap_or_else(|_e| {
                tracing::error!("Absurd metrics retention duration: {duration:?}.");
                u64::MAX
            }),
        ));
        let policies = self
            .catalog()
            .entries()
            .filter(|entry| entry.item().is_retained_metrics_relation())
            .map(|entry| (entry.id(), policy.clone()))
            .collect::<Vec<_>>();
        self.update_storage_base_read_policies(policies)
    }

    async fn create_storage_export(
        &mut self,
        create_export_token: CreateExportToken,
        sink: &Sink,
        connection: StorageSinkConnection,
    ) -> Result<(), AdapterError> {
        // Validate `sink.from` is in fact a storage collection
        self.controller.storage.collection(sink.from)?;

        let status_id =
            Some(self.catalog().resolve_builtin_storage_collection(
                &crate::catalog::builtin::MZ_SINK_STATUS_HISTORY,
            ));

        // The AsOf is used to determine at what time to snapshot reading from the persist collection.  This is
        // primarily relevant when we do _not_ want to include the snapshot in the sink.  Choosing now will mean
        // that only things going forward are exported.
        let timeline = self
            .get_timeline_context(sink.from)
            .timeline()
            .cloned()
            .unwrap_or(Timeline::EpochMilliseconds);
        let now = self.ensure_timeline_state(&timeline).await.oracle.read_ts();
        let frontier = Antichain::from_elem(now);
        let as_of = SinkAsOf {
            frontier,
            strict: !sink.with_snapshot,
        };

        let storage_sink_from_entry = self.catalog().get_entry(&sink.from);
        let storage_sink_desc = mz_storage_client::types::sinks::StorageSinkDesc {
            from: sink.from,
            from_desc: storage_sink_from_entry
                .desc(&self.catalog().resolve_full_name(
                    storage_sink_from_entry.name(),
                    storage_sink_from_entry.conn_id(),
                ))
                .expect("indexes can only be built on items with descs")
                .into_owned(),
            connection,
            envelope: Some(sink.envelope),
            as_of,
            status_id,
            from_storage_metadata: (),
        };

        Ok(self
            .controller
            .storage
            .create_exports(vec![(
                create_export_token,
                ExportDescription {
                    sink: storage_sink_desc,
                    instance_id: sink.cluster_id,
                },
            )])
            .await?)
    }

    pub(crate) async fn handle_sink_connection_ready(
        &mut self,
        id: GlobalId,
        oid: u32,
        connection: StorageSinkConnection,
        create_export_token: CreateExportToken,
        session: Option<&Session>,
    ) -> Result<(), AdapterError> {
        // Update catalog entry with sink connection.
        let entry = self.catalog().get_entry(&id);
        let name = entry.name().clone();
        let owner_id = entry.owner_id().clone();
        let sink = match entry.item() {
            CatalogItem::Sink(sink) => sink,
            _ => unreachable!(),
        };
        let sink = catalog::Sink {
            connection: StorageSinkConnectionState::Ready(connection.clone()),
            ..sink.clone()
        };

        // We always need to drop the already existing item: either because we fail to create it or we're replacing it.
        let mut ops = vec![catalog::Op::DropItem(id)];

        // Speculatively create the storage export before confirming in the catalog.  We chose this order of operations
        // for the following reasons:
        // - We want to avoid ever putting into the catalog a sink in `StorageSinkConnectionState::Ready`
        //   if we're not able to actually create the sink for some reason
        // - Dropping the sink will either succeed (or panic) so it's easier to reason about rolling that change back
        //   than it is rolling back a catalog change.
        match self
            .create_storage_export(create_export_token, &sink, connection)
            .await
        {
            Ok(()) => {
                ops.push(catalog::Op::CreateItem {
                    id,
                    oid,
                    name,
                    item: CatalogItem::Sink(sink.clone()),
                    owner_id,
                });
                match self.catalog_transact(session, ops).await {
                    Ok(()) => (),
                    catalog_err @ Err(_) => {
                        let () = self.drop_storage_sinks(vec![id]);
                        catalog_err?
                    }
                }
            }
            storage_err @ Err(_) => match self.catalog_transact(session, ops).await {
                Ok(()) => storage_err?,
                catalog_err @ Err(_) => catalog_err?,
            },
        };
        Ok(())
    }

    /// Validate all resource limits in a catalog transaction and return an error if that limit is
    /// exceeded.
    fn validate_resource_limits(
        &self,
        ops: &Vec<catalog::Op>,
        conn_id: ConnectionId,
    ) -> Result<(), AdapterError> {
        let mut new_aws_privatelink_connections = 0;
        let mut new_tables = 0;
        let mut new_sources = 0;
        let mut new_sinks = 0;
        let mut new_materialized_views = 0;
        let mut new_clusters = 0;
        let mut new_replicas_per_cluster = BTreeMap::new();
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
                    // Users can't create schemas in the ambient database.
                    if let ResolvedDatabaseSpecifier::Id(database_id) = database_id {
                        *new_schemas_per_database.entry(database_id).or_insert(0) += 1;
                    }
                }
                Op::CreateRole { .. } => {
                    new_roles += 1;
                }
                Op::CreateCluster {
                    linked_object_id, ..
                } => {
                    // Linked compute clusters don't count against the limit,
                    // since we have a separate sources and sinks limit.
                    //
                    // TODO(benesch): remove the `max_sources` and `max_sinks`
                    // limit, and set a higher max cluster limit?
                    if linked_object_id.is_none() {
                        new_clusters += 1;
                    }
                }
                Op::CreateClusterReplica { cluster_id, .. } => {
                    *new_replicas_per_cluster.entry(*cluster_id).or_insert(0) += 1;
                }
                Op::CreateItem { name, item, .. } => {
                    *new_objects_per_schema
                        .entry((
                            name.qualifiers.database_spec.clone(),
                            name.qualifiers.schema_spec.clone(),
                        ))
                        .or_insert(0) += 1;
                    match item {
                        CatalogItem::Connection(connection) => match connection.connection {
                            mz_storage_client::types::connections::Connection::AwsPrivatelink(
                                _,
                            ) => {
                                new_aws_privatelink_connections += 1;
                            }
                            _ => (),
                        },
                        CatalogItem::Table(_) => {
                            new_tables += 1;
                        }
                        CatalogItem::Source(source) => {
                            if source.is_external() {
                                // Only sources that ingest data from an external system count
                                // towards resource limits.
                                new_sources += 1
                            }
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
                Op::DropDatabase { .. } => {
                    new_databases -= 1;
                }
                Op::DropSchema { database_id, .. } => {
                    *new_schemas_per_database.entry(database_id).or_insert(0) -= 1;
                }
                Op::DropRole { .. } => {
                    new_roles -= 1;
                }
                Op::DropCluster { .. } => {
                    new_clusters -= 1;
                }
                Op::DropClusterReplica { cluster_id, .. } => {
                    *new_replicas_per_cluster.entry(*cluster_id).or_insert(0) -= 1;
                }
                Op::DropItem(id) => {
                    let entry = self.catalog().get_entry(id);
                    *new_objects_per_schema
                        .entry((
                            entry.name().qualifiers.database_spec.clone(),
                            entry.name().qualifiers.schema_spec.clone(),
                        ))
                        .or_insert(0) -= 1;
                    match entry.item() {
                        CatalogItem::Connection(connection) => match connection.connection {
                            mz_storage_client::types::connections::Connection::AwsPrivatelink(
                                _,
                            ) => {
                                new_aws_privatelink_connections -= 1;
                            }
                            _ => (),
                        },
                        CatalogItem::Table(_) => {
                            new_tables -= 1;
                        }
                        CatalogItem::Source(source) => {
                            if source.is_external() {
                                // Only sources that ingest data from an external system count
                                // towards resource limits.
                                new_sources -= 1;
                            }
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
                Op::AlterRole { .. }
                | Op::AlterSink { .. }
                | Op::AlterSource { .. }
                | Op::DropTimeline(_)
                | Op::GrantRole { .. }
                | Op::RenameItem { .. }
                | Op::ClusterOwner { .. }
                | Op::ClusterReplicaOwner { .. }
                | Op::DatabaseOwner { .. }
                | Op::SchemaOwner { .. }
                | Op::ItemOwner { .. }
                | Op::RevokeRole { .. }
                | Op::UpdateClusterReplicaStatus { .. }
                | Op::UpdateStorageUsage { .. }
                | Op::UpdateSystemConfiguration { .. }
                | Op::ResetSystemConfiguration { .. }
                | Op::ResetAllSystemConfiguration { .. }
                | Op::UpdateItem { .. }
                | Op::UpdateRotatedKeys { .. } => {}
            }
        }

        self.validate_resource_limit(
            self.catalog()
                .user_connections()
                .filter(|c| {
                    matches!(
                        c.connection()
                            .expect("`user_connections()` only returns connection objects")
                            .connection,
                        mz_storage_client::types::connections::Connection::AwsPrivatelink(_),
                    )
                })
                .count(),
            new_aws_privatelink_connections,
            SystemVars::max_aws_privatelink_connections,
            "AWS PrivateLink Connection",
        )?;
        self.validate_resource_limit(
            self.catalog().user_tables().count(),
            new_tables,
            SystemVars::max_tables,
            "Table",
        )?;
        // Only sources that ingest data from an external system count
        // towards resource limits.
        let current_sources = self
            .catalog()
            .user_sources()
            .filter_map(|source| source.source())
            .filter(|source| source.is_external())
            .count();
        self.validate_resource_limit(
            current_sources,
            new_sources,
            SystemVars::max_sources,
            "Source",
        )?;
        self.validate_resource_limit(
            self.catalog().user_sinks().count(),
            new_sinks,
            SystemVars::max_sinks,
            "Sink",
        )?;
        self.validate_resource_limit(
            self.catalog().user_materialized_views().count(),
            new_materialized_views,
            SystemVars::max_materialized_views,
            "Materialized view",
        )?;
        self.validate_resource_limit(
            // Linked compute clusters don't count against the limit, since
            // we have a separate sources and sinks limit.
            //
            // TODO(benesch): remove the `max_sources` and `max_sinks` limit,
            // and set a higher max cluster limit?
            self.catalog()
                .user_clusters()
                .filter(|c| c.linked_object_id.is_none())
                .count(),
            new_clusters,
            SystemVars::max_clusters,
            "Cluster",
        )?;
        for (cluster_id, new_replicas) in new_replicas_per_cluster {
            // It's possible that the cluster hasn't been created yet.
            let current_amount = self
                .catalog()
                .try_get_cluster(cluster_id)
                .map(|instance| instance.replicas_by_id.len())
                .unwrap_or(0);
            self.validate_resource_limit(
                current_amount,
                new_replicas,
                SystemVars::max_replicas_per_cluster,
                "Replicas per cluster",
            )?;
        }
        self.validate_resource_limit(
            self.catalog().databases().count(),
            new_databases,
            SystemVars::max_databases,
            "Database",
        )?;
        for (database_id, new_schemas) in new_schemas_per_database {
            self.validate_resource_limit(
                self.catalog().get_database(database_id).schemas_by_id.len(),
                new_schemas,
                SystemVars::max_schemas_per_database,
                "Schemas per database",
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
                "Objects per schema",
            )?;
        }
        self.validate_resource_limit(
            self.catalog().user_secrets().count(),
            new_secrets,
            SystemVars::max_secrets,
            "Secret",
        )?;
        self.validate_resource_limit(
            self.catalog().user_roles().count(),
            new_roles,
            SystemVars::max_roles,
            "Role",
        )?;
        Ok(())
    }

    /// Validate a specific type of resource limit and return an error if that limit is exceeded.
    fn validate_resource_limit<F>(
        &self,
        current_amount: usize,
        new_instances: i32,
        resource_limit: F,
        resource_type: &str,
    ) -> Result<(), AdapterError>
    where
        F: Fn(&SystemVars) -> u32,
    {
        let limit = resource_limit(self.catalog().system_config());
        let exceeds_limit = match (u32::try_from(current_amount), u32::try_from(new_instances)) {
            // 0 new instances are always ok.
            (_, Ok(new_instances)) if new_instances == 0 => false,
            // negative instances are always ok.
            (_, Err(_)) => false,
            // more than u32 for the current amount is too much.
            (Err(_), _) => true,
            (Ok(current_amount), Ok(new_instances)) => {
                match current_amount.checked_add(new_instances) {
                    Some(new_amount) => new_amount > limit,
                    None => true,
                }
            }
        };
        if exceeds_limit {
            Err(AdapterError::ResourceExhaustion {
                resource_type: resource_type.to_string(),
                limit,
                current_amount,
                new_instances,
            })
        } else {
            Ok(())
        }
    }
}
