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

use std::collections::HashMap;
use std::time::Duration;

use itertools::Itertools;
use serde_json::json;
use timely::progress::Antichain;
use tracing::Level;
use tracing::{event, warn};

use mz_audit_log::VersionedEvent;
use mz_compute_client::controller::ComputeInstanceId;
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::names::ResolvedDatabaseSpecifier;
use mz_stash::Append;
use mz_storage::controller::{CreateExportToken, ExportDescription};
use mz_storage::types::sinks::{SinkAsOf, StorageSinkConnection};
use mz_storage::types::sources::{PostgresSourceConnection, SourceConnection, Timeline};

use crate::catalog::{
    CatalogItem, CatalogState, DataSourceDesc, Op, Sink, StorageSinkConnectionState,
    TransactionResult, SYSTEM_CONN_ID,
};
use crate::client::ConnectionId;
use crate::coord::appends::BuiltinTableUpdateSource;
use crate::coord::Coordinator;
use crate::session::vars::SystemVars;
use crate::session::Session;
use crate::util::ComputeSinkId;
use crate::{catalog, AdapterError};

/// State provided to a catalog transaction closure.
pub struct CatalogTxn<'a, T> {
    pub(crate) dataflow_client: &'a mz_controller::Controller<T>,
    pub(crate) catalog: &'a CatalogState,
}

impl<S: Append + 'static> Coordinator<S> {
    /// Perform a catalog transaction. The closure is passed a [`CatalogTxn`]
    /// made from the prospective [`CatalogState`] (i.e., the `Catalog` with `ops`
    /// applied but before the transaction is committed). The closure can return
    /// an error to abort the transaction, or otherwise return a value that is
    /// returned by this function. This allows callers to error while building
    /// [`DataflowDesc`]s. [`Coordinator::ship_dataflow`] must be called after this
    /// function successfully returns on any built `DataflowDesc`.
    ///
    /// [`CatalogState`]: crate::catalog::CatalogState
    /// [`DataflowDesc`]: mz_compute_client::command::DataflowDesc
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn catalog_transact<F, R>(
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

        for op in &ops {
            if let catalog::Op::DropItem(id) = op {
                match self.catalog.get_entry(id).item() {
                    CatalogItem::Table(_) => {
                        tables_to_drop.push(*id);
                    }
                    CatalogItem::Source(source) => {
                        sources_to_drop.push(*id);
                        if let DataSourceDesc::Ingest(ingestion) = &source.data_source {
                            match &ingestion.desc.connection {
                                SourceConnection::Postgres(PostgresSourceConnection {
                                    connection,
                                    details,
                                    ..
                                }) => {
                                    let config = connection
                                        .config(&*self.connection_context.secrets_reader)
                                        .await
                                        .unwrap_or_else(|e| {
                                            panic!("Postgres source {id} missing secrets: {e}")
                                        });
                                    replication_slots_to_drop.push((config, details.slot.clone()));
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
                    CatalogItem::Index(catalog::Index {
                        compute_instance, ..
                    }) => {
                        indexes_to_drop.push((*compute_instance, *id));
                    }
                    CatalogItem::MaterializedView(catalog::MaterializedView {
                        compute_instance,
                        ..
                    }) => {
                        materialized_views_to_drop.push((*compute_instance, *id));
                    }
                    CatalogItem::Secret(_) => {
                        secrets_to_drop.push(*id);
                    }
                    CatalogItem::Connection(catalog::Connection { connection, .. }) => {
                        // SSH connections have an associated secret that should be dropped
                        match connection {
                            mz_storage::types::connections::Connection::Ssh(_) => {
                                secrets_to_drop.push(*id);
                            }
                            _ => {}
                        }
                    }
                    _ => (),
                }
            } else if let catalog::Op::DropComputeInstance { name } = op {
                let instance = self.catalog.resolve_compute_instance(name)?;
                let id = instance.id;

                // Drop the introspection sources
                for replica in instance.replicas_by_id.values() {
                    sources_to_drop.extend(replica.config.logging.source_ids());
                }

                // Drop timelines
                timelines_to_drop.extend(self.remove_compute_instance_from_timeline(id));
            } else if let catalog::Op::DropComputeReplica { name, compute_name } = op {
                let compute_instance = self.catalog.resolve_compute_instance(compute_name)?;
                let replica_id = &compute_instance.replica_id_by_name[name];
                let replica = &compute_instance.replicas_by_id[replica_id];

                // Drop the introspection sources
                sources_to_drop.extend(replica.config.logging.source_ids());
            }
        }

        timelines_to_drop = self.remove_storage_ids_from_timeline(
            sources_to_drop
                .iter()
                .chain(storage_sinks_to_drop.iter())
                .chain(tables_to_drop.iter())
                .chain(materialized_views_to_drop.iter().map(|(_, id)| id))
                .cloned(),
        );
        timelines_to_drop.extend(
            self.remove_compute_ids_from_timeline(
                indexes_to_drop
                    .iter()
                    .chain(materialized_views_to_drop.iter())
                    .cloned(),
            ),
        );
        ops.extend(timelines_to_drop.into_iter().map(catalog::Op::DropTimeline));

        self.validate_resource_limits(
            &ops,
            session
                .map(|session| session.conn_id())
                .unwrap_or(SYSTEM_CONN_ID),
        )?;

        let TransactionResult {
            builtin_table_updates,
            audit_events,
            collections,
            result,
        } = self
            .catalog
            .transact(session, ops, |catalog| {
                f(CatalogTxn {
                    dataflow_client: &self.controller,
                    catalog,
                })
            })
            .await?;

        // No error returns are allowed after this point. Enforce this at compile time
        // by using this odd structure so we don't accidentally add a stray `?`.
        let _: () = async {
            self.send_builtin_table_updates(builtin_table_updates, BuiltinTableUpdateSource::DDL)
                .await;

            if !sources_to_drop.is_empty() {
                self.drop_sources(sources_to_drop).await;
            }
            if !tables_to_drop.is_empty() {
                self.drop_sources(tables_to_drop).await;
            }
            if !storage_sinks_to_drop.is_empty() {
                self.drop_storage_sinks(storage_sinks_to_drop).await;
            }
            if !indexes_to_drop.is_empty() {
                self.drop_indexes(indexes_to_drop).await;
            }
            if !materialized_views_to_drop.is_empty() {
                self.drop_materialized_views(materialized_views_to_drop)
                    .await;
            }
            if !secrets_to_drop.is_empty() {
                self.drop_secrets(secrets_to_drop).await;
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
        }
        .await;

        self.consolidations_tx
            .send(collections)
            .expect("sending on consolidations_tx must succeed");

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
                segment_client.track(
                    user_metadata.user_id,
                    event_type,
                    json!({
                        "event_source": "environmentd",
                        "details": event.details.as_json(),
                    }),
                    Some(json!({
                        "groupId": user_metadata.group_id,
                    })),
                );
            }
        }

        Ok(result)
    }

    async fn drop_sources(&mut self, sources: Vec<GlobalId>) {
        for id in &sources {
            self.drop_read_policy(id);
        }
        self.controller.storage.drop_sources(sources).await.unwrap();
    }

    pub(crate) async fn drop_compute_sinks(&mut self, sinks: Vec<ComputeSinkId>) {
        let by_compute_instance = sinks
            .into_iter()
            .map(
                |ComputeSinkId {
                     compute_instance,
                     global_id,
                 }| (compute_instance, global_id),
            )
            .into_group_map();
        let mut compute = self.controller.active_compute();
        for (compute_instance, ids) in by_compute_instance {
            // A cluster could have been dropped, so verify it exists.
            if compute.instance_exists(compute_instance) {
                compute
                    .drop_collections(compute_instance, ids)
                    .await
                    .unwrap();
            }
        }
    }

    pub(crate) async fn drop_storage_sinks(&mut self, sinks: Vec<GlobalId>) {
        for id in &sinks {
            self.drop_read_policy(id);
        }
        self.controller.storage.drop_sinks(sinks).await.unwrap();
    }

    pub(crate) async fn drop_indexes(&mut self, indexes: Vec<(ComputeInstanceId, GlobalId)>) {
        let mut by_compute_instance = HashMap::new();
        for (compute_instance, id) in indexes {
            if self.drop_read_policy(&id) {
                by_compute_instance
                    .entry(compute_instance)
                    .or_insert(vec![])
                    .push(id);
            } else {
                tracing::error!("Instructed to drop a non-index index");
            }
        }
        for (compute_instance, ids) in by_compute_instance {
            self.controller
                .active_compute()
                .drop_collections(compute_instance, ids)
                .await
                .unwrap();
        }
    }

    async fn drop_materialized_views(&mut self, mviews: Vec<(ComputeInstanceId, GlobalId)>) {
        let mut by_compute_instance = HashMap::new();
        let mut source_ids = Vec::new();
        for (compute_instance, id) in mviews {
            if self.drop_read_policy(&id) {
                by_compute_instance
                    .entry(compute_instance)
                    .or_insert(vec![])
                    .push(id);
                source_ids.push(id);
            } else {
                tracing::error!("Instructed to drop a materialized view that isn't one");
            }
        }

        // Drop compute sinks.
        // TODO(chae): Drop storage sinks when they're moved over
        let mut compute = self.controller.active_compute();
        for (compute_instance, ids) in by_compute_instance {
            // A cluster could have been dropped, so verify it exists.
            if compute.instance_exists(compute_instance) {
                compute
                    .drop_collections(compute_instance, ids)
                    .await
                    .unwrap();
            }
        }

        // Drop storage sources.
        self.controller
            .storage
            .drop_sources(source_ids)
            .await
            .unwrap();
    }

    async fn drop_secrets(&mut self, secrets: Vec<GlobalId>) {
        for secret in secrets {
            if let Err(e) = self.secrets_controller.delete(secret).await {
                warn!("Dropping secrets has encountered an error: {}", e);
            }
        }
    }

    /// Removes all temporary items created by the specified connection, though
    /// not the temporary schema itself.
    pub(crate) async fn drop_temp_items(&mut self, session: &Session) {
        let ops = self.catalog.drop_temp_item_ops(session.conn_id());
        if ops.is_empty() {
            return;
        }
        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await
            .expect("unable to drop temporary items for conn_id");
    }

    async fn create_storage_export(
        &mut self,
        create_export_token: CreateExportToken,
        sink: &Sink,
        connection: StorageSinkConnection,
    ) -> Result<(), AdapterError> {
        // Validate `sink.from` is in fact a storage collection
        self.controller.storage.collection(sink.from)?;

        // The AsOf is used to determine at what time to snapshot reading from the persist collection.  This is
        // primarily relevant when we do _not_ want to include the snapshot in the sink.  Choosing now will mean
        // that only things going forward are exported.
        let timeline = self
            .get_timeline(sink.from)
            .unwrap_or(Timeline::EpochMilliseconds);
        let now = self.ensure_timeline_state(timeline).await.oracle.read_ts();
        let frontier = Antichain::from_elem(now);
        let as_of = SinkAsOf {
            frontier,
            strict: !sink.with_snapshot,
        };

        let storage_sink_from_entry = self.catalog.get_entry(&sink.from);
        let storage_sink_desc = mz_storage::types::sinks::StorageSinkDesc {
            from: sink.from,
            from_desc: storage_sink_from_entry
                .desc(&self.catalog.resolve_full_name(
                    storage_sink_from_entry.name(),
                    storage_sink_from_entry.conn_id(),
                ))
                .unwrap()
                .into_owned(),
            connection,
            envelope: Some(sink.envelope),
            as_of,
            from_storage_metadata: (),
        };

        Ok(self
            .controller
            .storage
            .create_exports(vec![(
                create_export_token,
                ExportDescription {
                    sink: storage_sink_desc,
                    host_config: sink.host_config.clone(),
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
        let entry = self.catalog.get_entry(&id);
        let name = entry.name().clone();
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
                });
                match self.catalog_transact(session, ops, move |_| Ok(())).await {
                    Ok(()) => (),
                    catalog_err @ Err(_) => {
                        let () = self.drop_storage_sinks(vec![id]).await;
                        catalog_err?
                    }
                }
            }
            storage_err @ Err(_) => {
                match self.catalog_transact(session, ops, move |_| Ok(())).await {
                    Ok(()) => storage_err?,
                    catalog_err @ Err(_) => catalog_err?,
                }
            }
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
        let mut new_tables = 0;
        let mut new_sources = 0;
        let mut new_sinks = 0;
        let mut new_materialized_views = 0;
        let mut new_clusters = 0;
        let mut new_replicas_per_cluster = HashMap::new();
        let mut new_databases = 0;
        let mut new_schemas_per_database = HashMap::new();
        let mut new_objects_per_schema = HashMap::new();
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
                Op::CreateComputeInstance { .. } => {
                    new_clusters += 1;
                }
                Op::CreateComputeReplica {
                    on_cluster_name, ..
                } => {
                    *new_replicas_per_cluster.entry(on_cluster_name).or_insert(0) += 1;
                }
                Op::CreateItem { name, item, .. } => {
                    *new_objects_per_schema
                        .entry((
                            name.qualifiers.database_spec.clone(),
                            name.qualifiers.schema_spec.clone(),
                        ))
                        .or_insert(0) += 1;
                    match item {
                        CatalogItem::Table(_) => {
                            new_tables += 1;
                        }
                        CatalogItem::Source(_) => {
                            new_sources += 1;
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
                        | CatalogItem::Func(_)
                        | CatalogItem::Connection(_)
                        | CatalogItem::StorageManagedTable(_) => {}
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
                Op::DropComputeInstance { .. } => {
                    new_clusters -= 1;
                }
                Op::DropComputeReplica { compute_name, .. } => {
                    *new_replicas_per_cluster.entry(compute_name).or_insert(0) -= 1;
                }
                Op::DropItem(id) => {
                    let entry = self.catalog.get_entry(id);
                    *new_objects_per_schema
                        .entry((
                            entry.name().qualifiers.database_spec.clone(),
                            entry.name().qualifiers.schema_spec.clone(),
                        ))
                        .or_insert(0) -= 1;
                    match entry.item() {
                        CatalogItem::Table(_) => {
                            new_tables -= 1;
                        }
                        CatalogItem::Source(_) => {
                            new_sources -= 1;
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
                        | CatalogItem::Func(_)
                        | CatalogItem::Connection(_)
                        | CatalogItem::StorageManagedTable(_) => {}
                    }
                }
                Op::AlterSink { .. }
                | Op::AlterSource { .. }
                | Op::DropTimeline(_)
                | Op::RenameItem { .. }
                | Op::UpdateComputeInstanceStatus { .. }
                | Op::UpdateStorageUsage { .. }
                | Op::UpdateSystemConfiguration { .. }
                | Op::ResetSystemConfiguration { .. }
                | Op::ResetAllSystemConfiguration { .. }
                | Op::UpdateItem { .. }
                | Op::UpdateRotatedKeys { .. } => {}
            }
        }

        self.validate_resource_limit(
            self.catalog.user_tables().count(),
            new_tables,
            SystemVars::max_tables,
            "Table",
        )?;
        self.validate_resource_limit(
            self.catalog.user_sources().count(),
            new_sources,
            SystemVars::max_sources,
            "Source",
        )?;
        self.validate_resource_limit(
            self.catalog.user_sinks().count(),
            new_sinks,
            SystemVars::max_sinks,
            "Sink",
        )?;
        self.validate_resource_limit(
            self.catalog.user_materialized_views().count(),
            new_materialized_views,
            SystemVars::max_materialized_views,
            "Materialized view",
        )?;
        self.validate_resource_limit(
            self.catalog.user_compute_instances().count(),
            new_clusters,
            SystemVars::max_clusters,
            "Cluster",
        )?;
        for (cluster_name, new_replicas) in new_replicas_per_cluster {
            // It's possible that the compute instance hasn't been created yet.
            let current_amount = self
                .catalog
                .resolve_compute_instance(cluster_name)
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
            self.catalog.databases().count(),
            new_databases,
            SystemVars::max_databases,
            "Database",
        )?;
        for (database_id, new_schemas) in new_schemas_per_database {
            self.validate_resource_limit(
                self.catalog.get_database(database_id).schemas_by_id.len(),
                new_schemas,
                SystemVars::max_schemas_per_database,
                "Schemas per database",
            )?;
        }
        for ((database_spec, schema_spec), new_objects) in new_objects_per_schema {
            self.validate_resource_limit(
                self.catalog
                    .get_schema(&database_spec, &schema_spec, conn_id)
                    .items
                    .len(),
                new_objects,
                SystemVars::max_objects_per_schema,
                "Objects per schema",
            )?;
        }
        self.validate_resource_limit(
            self.catalog.user_secrets().count(),
            new_secrets,
            SystemVars::max_secrets,
            "Secret",
        )?;
        self.validate_resource_limit(
            self.catalog.user_roles().count(),
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
        let limit = resource_limit(self.catalog.system_config());
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
            })
        } else {
            Ok(())
        }
    }
}
