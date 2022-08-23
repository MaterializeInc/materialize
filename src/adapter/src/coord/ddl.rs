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
use timely::progress::Antichain;
use tracing::Level;
use tracing::{event, warn};

use mz_compute_client::controller::{ComputeInstanceId, ComputeSinkId};
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::names::ResolvedDatabaseSpecifier;
use mz_stash::Append;
use mz_storage::controller::ExportDescription;
use mz_storage::types::sinks::{SinkAsOf, StorageSinkConnection};
use mz_storage::types::sources::{PostgresSourceConnection, SourceConnection, Timeline};

use crate::catalog::{
    CatalogItem, CatalogState, Op, Sink, StorageSinkConnectionState, SYSTEM_CONN_ID,
};
use crate::client::ConnectionId;
use crate::coord::appends::BuiltinTableUpdateSource;
use crate::coord::Coordinator;
use crate::session::vars::SystemVars;
use crate::session::Session;
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
        let mut replication_slots_to_drop: Vec<(tokio_postgres::Config, String)> = vec![];
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
                        match &source.source_desc.connection {
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
                    _ => (),
                }
            } else if let catalog::Op::DropComputeInstance { name } = op {
                let instance = self.catalog.resolve_compute_instance(name)?;
                let id = instance.id;

                // Drop the introspection sources
                for replica in instance.replicas_by_id.values() {
                    sources_to_drop.extend(replica.config.persisted_logs.get_source_ids());
                }

                // Drop timelines
                timelines_to_drop.extend(self.remove_compute_instance_from_timeline(id));
            } else if let catalog::Op::DropComputeInstanceReplica { name, compute_name } = op {
                let compute_instance = self.catalog.resolve_compute_instance(compute_name)?;
                let replica_id = &compute_instance.replica_id_by_name[name];
                let replica = &compute_instance.replicas_by_id[&replica_id];

                // Drop the introspection sources
                sources_to_drop.extend(replica.config.persisted_logs.get_source_ids());
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

        let (builtin_table_updates, collections, result) = self
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
        Ok(result)
    }

    async fn drop_sources(&mut self, sources: Vec<GlobalId>) {
        for id in &sources {
            self.drop_read_policy(id);
        }
        self.controller
            .storage_mut()
            .drop_sources(sources)
            .await
            .unwrap();
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
        for (compute_instance, ids) in by_compute_instance {
            // A cluster could have been dropped, so verify it exists.
            if let Some(mut compute) = self.controller.compute_mut(compute_instance) {
                compute.drop_sinks(ids).await.unwrap();
            }
        }
    }

    pub(crate) async fn drop_storage_sinks(&mut self, sinks: Vec<GlobalId>) {
        for id in &sinks {
            self.drop_read_policy(id);
        }
        self.controller
            .storage_mut()
            .drop_sinks(sinks)
            .await
            .unwrap();
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
                .compute_mut(compute_instance)
                .unwrap()
                .drop_indexes(ids)
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
        for (compute_instance, ids) in by_compute_instance {
            // A cluster could have been dropped, so verify it exists.
            if let Some(mut compute) = self.controller.compute_mut(compute_instance) {
                compute.drop_sinks(ids).await.unwrap();
            }
        }

        // Drop storage sources.
        self.controller
            .storage_mut()
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
        id: GlobalId,
        sink: &Sink,
        connection: StorageSinkConnection,
    ) -> Result<(), AdapterError> {
        // Validate `sink.from` is in fact a storage collection
        self.controller.storage().collection(sink.from)?;

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
            .storage_mut()
            .create_exports(vec![(
                id,
                ExportDescription {
                    sink: storage_sink_desc,
                    remote_addr: None,
                },
            )])
            .await?)
    }

    pub(crate) async fn handle_sink_connection_ready(
        &mut self,
        id: GlobalId,
        oid: u32,
        connection: StorageSinkConnection,
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

        let ops = vec![
            catalog::Op::DropItem(id),
            catalog::Op::CreateItem {
                id,
                oid,
                name,
                item: CatalogItem::Sink(sink.clone()),
            },
        ];

        let () = self.catalog_transact(session, ops, move |_| Ok(())).await?;
        // TODO(#14220): should this happen in the same task where we build the connection?  Or should it need to happen
        // after we update the catalog?
        match self.create_storage_export(id, &sink, connection).await {
            Ok(()) => Ok(()),
            Err(storage_error) =>
            // TODO: catalog_transact that can take async function to actually make the `CreateItem` above transactional.
            {
                match self
                    .catalog_transact(session, vec![catalog::Op::DropItem(id)], move |_| Ok(()))
                    .await
                {
                    Ok(()) => Err(storage_error),
                    Err(e) => Err(e),
                }
            }
        }
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
                Op::CreateComputeInstanceReplica {
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
                        | CatalogItem::StorageCollection(_) => {}
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
                Op::DropComputeInstanceReplica { compute_name, .. } => {
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
                        | CatalogItem::StorageCollection(_) => {}
                    }
                }
                Op::DropTimeline(_)
                | Op::RenameItem { .. }
                | Op::UpdateComputeInstanceStatus { .. }
                | Op::UpdateStorageUsage { .. }
                | Op::UpdateSystemConfiguration { .. }
                | Op::ResetSystemConfiguration { .. }
                | Op::ResetAllSystemConfiguration { .. } => {}
            }
        }

        self.validate_resource_limit(
            self.catalog
                .user_tables()
                .count()
                .try_into()
                .expect("number of tables should fit into i32"),
            new_tables,
            SystemVars::max_tables,
            "Table",
        )?;
        self.validate_resource_limit(
            self.catalog
                .user_sources()
                .count()
                .try_into()
                .expect("number of sources should fit into i32"),
            new_sources,
            SystemVars::max_sources,
            "Source",
        )?;
        self.validate_resource_limit(
            self.catalog
                .user_sinks()
                .count()
                .try_into()
                .expect("number of sinks should fit into i32"),
            new_sinks,
            SystemVars::max_sinks,
            "Sink",
        )?;
        self.validate_resource_limit(
            self.catalog
                .user_materialized_views()
                .count()
                .try_into()
                .expect("number of materialized views should fit into i32"),
            new_materialized_views,
            SystemVars::max_materialized_views,
            "Materialized view",
        )?;
        self.validate_resource_limit(
            self.catalog
                .compute_instances()
                .count()
                .try_into()
                .expect("number of compute instances should fit into i32"),
            new_clusters,
            SystemVars::max_clusters,
            "Cluster",
        )?;
        for (cluster_name, new_replicas) in new_replicas_per_cluster {
            // It's possible that the compute instance hasn't been created yet.
            let current_amount = self
                .catalog
                .resolve_compute_instance(cluster_name)
                .map(|instance| {
                    instance
                        .replicas_by_id
                        .len()
                        .try_into()
                        .expect("number of replicas should fit into i32")
                })
                .unwrap_or(0);
            self.validate_resource_limit(
                current_amount,
                new_replicas,
                SystemVars::max_replicas_per_cluster,
                "Replicas per cluster",
            )?;
        }
        self.validate_resource_limit(
            self.catalog
                .databases()
                .count()
                .try_into()
                .expect("number of databases should fit into i32"),
            new_databases,
            SystemVars::max_databases,
            "Database",
        )?;
        for (database_id, new_schemas) in new_schemas_per_database {
            self.validate_resource_limit(
                self.catalog
                    .get_database(database_id)
                    .schemas_by_id
                    .len()
                    .try_into()
                    .expect("number of schemas should fit into i32"),
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
                    .len()
                    .try_into()
                    .expect("number of items should fit into i32"),
                new_objects,
                SystemVars::max_objects_per_schema,
                "Objects per schema",
            )?;
        }
        self.validate_resource_limit(
            self.catalog
                .user_secrets()
                .count()
                .try_into()
                .expect("number of secrets should fit into i32"),
            new_secrets,
            SystemVars::max_secrets,
            "Secret",
        )?;
        self.validate_resource_limit(
            self.catalog
                .user_roles()
                .count()
                .try_into()
                .expect("number of secrets should fit into i32"),
            new_roles,
            SystemVars::max_roles,
            "Role",
        )?;
        Ok(())
    }

    /// Validate a specific type of resource limit and return an error if that limit is exceeded.
    fn validate_resource_limit<F>(
        &self,
        current_amount: i32,
        new_instances: i32,
        resource_limit: F,
        resource_type: &str,
    ) -> Result<(), AdapterError>
    where
        F: Fn(&SystemVars) -> i32,
    {
        let limit = resource_limit(self.catalog.state().system_config());
        if new_instances > 0 && current_amount + new_instances > limit {
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
