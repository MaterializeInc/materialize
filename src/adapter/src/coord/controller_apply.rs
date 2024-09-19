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
use maplit::btreeset;
use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::memory::objects::{DataSourceDesc, Table, TableDataSource};
use mz_cluster_client::ReplicaId;
use mz_compute_client::protocol::response::PeekResponse;
use mz_controller_types::ClusterId;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::instrument;
use mz_ore::retry::Retry;
use mz_ore::str::StrExt;
use mz_ore::task;
use mz_postgres_util::tunnel::PostgresFlavor;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::CatalogCluster;
use mz_storage_client::controller::{CollectionDescription, DataSource, DataSourceOther};
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::connections::PostgresConnection;
use mz_storage_types::sources::GenericSourceConnection;
use tracing::{info_span, warn, Instrument};

use crate::active_compute_sink::ActiveComputeSinkRetireReason;
use crate::catalog::side_effects::CatalogSideEffect;
use crate::coord::timeline::TimelineState;
use crate::coord::Coordinator;
use crate::statement_logging::StatementEndedExecutionReason;
use crate::{AdapterError, ExecuteContext, ResultExt, TimelineContext};

impl Coordinator {
    #[instrument(level = "debug")]
    pub async fn controller_apply_side_effects(
        &mut self,
        mut ctx: Option<&mut ExecuteContext>,
        updates: Vec<CatalogSideEffect>,
    ) -> Result<(), AdapterError> {
        let mut tables_to_drop = Vec::new();
        let mut sources_to_drop = vec![];
        let mut replication_slots_to_drop: Vec<(PostgresConnection, String)> = vec![];
        let mut storage_sinks_to_drop = vec![];
        let mut indexes_to_drop = vec![];
        let mut materialized_views_to_drop = vec![];
        let mut views_to_drop = vec![];
        let mut secrets_to_drop = vec![];
        let mut vpc_endpoints_to_drop = vec![];
        let mut clusters_to_drop = vec![];
        let mut cluster_replicas_to_drop = vec![];
        let mut compute_sinks_to_drop = BTreeMap::new();
        let mut peeks_to_drop = vec![];

        for update in updates {
            tracing::info!(?update, "have to apply!");
            match update {
                CatalogSideEffect::CreateTable(id, table) => {
                    self.controller_create_table(&mut ctx, id, table).await?
                }
                CatalogSideEffect::DropTable(id) => {
                    tables_to_drop.push(id);
                }
                CatalogSideEffect::DropSource(id, source) => {
                    sources_to_drop.push(id);
                    if let DataSourceDesc::Ingestion { ingestion_desc, .. } = &source.data_source {
                        match &ingestion_desc.desc.connection {
                            GenericSourceConnection::Postgres(conn) => {
                                let conn =
                                    conn.clone().into_inline_connection(self.catalog().state());
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
                CatalogSideEffect::DropSink(id) => {
                    storage_sinks_to_drop.push(id);
                }
                CatalogSideEffect::DropIndex(id, index) => {
                    indexes_to_drop.push((index.cluster_id, id));
                }
                CatalogSideEffect::DropMaterializedView(id, mv) => {
                    materialized_views_to_drop.push((mv.cluster_id, id));
                }
                CatalogSideEffect::DropView(id) => {
                    views_to_drop.push(id);
                }
                CatalogSideEffect::DropSecret(id) => {
                    secrets_to_drop.push(id);
                }
                CatalogSideEffect::DropConnection(id, connection) => {
                    match connection.connection {
                        // SSH connections have an associated secret that should be dropped
                        mz_storage_types::connections::Connection::Ssh(_) => {
                            secrets_to_drop.push(id);
                        }
                        // AWS PrivateLink connections have an associated
                        // VpcEndpoint K8S resource that should be dropped
                        mz_storage_types::connections::Connection::AwsPrivatelink(_) => {
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
                self.drop_tables(tables_to_drop, ts.timestamp);
            }

            if !sources_to_drop.is_empty() {
                self.drop_sources(sources_to_drop);
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
                            .compute
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
    async fn controller_create_table(
        &mut self,
        ctx: &mut Option<&mut ExecuteContext>,
        table_id: GlobalId,
        table: Table,
    ) -> Result<(), AdapterError> {
        // The table data_source determines whether this table will be written to
        // by environmentd (e.g. with INSERT INTO statements) or by the storage layer
        // (e.g. a source-fed table).
        match table.data_source {
            TableDataSource::TableWrites { defaults: _ } => {
                // Determine the initial validity for the table.
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

                if let Some(id) = ctx.as_ref().and_then(|ctx| ctx.extra().contents()) {
                    self.set_statement_execution_timestamp(id, register_ts);
                }

                let collection_desc = CollectionDescription::from_desc(
                    table.desc.clone(),
                    DataSourceOther::TableWrites,
                );
                let storage_metadata = self.catalog.state().storage_metadata();
                self.controller
                    .storage
                    .create_collections(
                        storage_metadata,
                        Some(register_ts),
                        vec![(table_id, collection_desc)],
                    )
                    .await
                    .unwrap_or_terminate("cannot fail to create collections");
                self.apply_local_write(register_ts).await;

                self.initialize_storage_read_policies(
                    btreeset![table_id],
                    table
                        .custom_logical_compaction_window
                        .unwrap_or(CompactionWindow::Default),
                )
                .await;
            }
            TableDataSource::DataSource(data_source) => {
                match data_source {
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
                            Some(self.catalog().resolve_builtin_storage_collection(
                                &mz_catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
                            ));
                        let collection_desc = CollectionDescription::<Timestamp> {
                            desc: table.desc.clone(),
                            data_source: DataSource::IngestionExport {
                                ingestion_id,
                                details,
                                data_config: data_config
                                    .into_inline_connection(self.catalog.state()),
                            },
                            since: None,
                            status_collection_id,
                        };
                        let storage_metadata = self.catalog.state().storage_metadata();
                        self.controller
                            .storage
                            .create_collections(
                                storage_metadata,
                                None,
                                vec![(table_id, collection_desc)],
                            )
                            .await
                            .unwrap_or_terminate("cannot fail to create collections");

                        let read_policies = self
                            .catalog()
                            .state()
                            .source_compaction_windows(vec![table_id]);
                        for (compaction_window, storage_policies) in read_policies {
                            self.initialize_storage_read_policies(
                                storage_policies,
                                compaction_window,
                            )
                            .await;
                        }
                    }
                    _ => unreachable!("CREATE TABLE data source got {:?}", data_source),
                }
            }
        }
        Ok(())
    }

    fn drop_replica(&mut self, cluster_id: ClusterId, replica_id: ReplicaId) {
        self.drop_introspection_subscribes(replica_id);

        self.controller
            .drop_replica(cluster_id, replica_id)
            .expect("dropping replica must not fail");
    }

    fn drop_tables(&mut self, tables: Vec<GlobalId>, ts: Timestamp) {
        let storage_metadata = self.catalog.state().storage_metadata();
        self.controller
            .storage
            .drop_tables(storage_metadata, tables, ts)
            .unwrap_or_terminate("cannot fail to drop tables");
    }

    /// A convenience method for dropping sources.
    fn drop_sources(&mut self, sources: Vec<GlobalId>) {
        for id in &sources {
            self.active_webhooks.remove(id);
        }
        let storage_metadata = self.catalog.state().storage_metadata();
        self.controller
            .storage
            .drop_sources(storage_metadata, sources)
            .unwrap_or_terminate("cannot fail to drop sources");
    }

    fn drop_storage_sinks(&mut self, sinks: Vec<GlobalId>) {
        self.controller
            .storage
            .drop_sinks(sinks)
            .unwrap_or_terminate("cannot fail to drop sinks");
    }

    /// A convenience method for dropping materialized views.
    fn drop_materialized_views(&mut self, mviews: Vec<(ClusterId, GlobalId)>) {
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        let mut source_ids = Vec::new();
        for (cluster_id, id) in mviews {
            by_cluster.entry(cluster_id).or_default().push(id);
            source_ids.push(id);
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

    fn drop_vpc_endpoints_in_background(&mut self, vpc_endpoints: Vec<GlobalId>) {
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
            .instrument(info_span!("coord::controller_apply::drop_vpc_endpoints")),
        );
    }
}
