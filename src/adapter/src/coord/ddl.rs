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
use tracing::Level;
use tracing::{event, warn};

use mz_compute_client::controller::ComputeInstanceId;
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_repr::{GlobalId, Timestamp};
use mz_stash::Append;
use mz_storage::types::sinks::{SinkAsOf, SinkConnection};
use mz_storage::types::sources::{PostgresSourceConnection, SourceConnection};

use crate::catalog::{CatalogItem, CatalogState, SinkConnectionState};
use crate::coord::Coordinator;
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
        let mut sinks_to_drop = vec![];
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
                    CatalogItem::Sink(catalog::Sink {
                        connection: SinkConnectionState::Ready(_),
                        compute_instance,
                        ..
                    }) => {
                        sinks_to_drop.push((*compute_instance, *id));
                    }
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
                    sources_to_drop.extend(replica.config.persisted_logs.get_log_ids());
                }

                // Drop timelines
                timelines_to_drop.extend(self.remove_compute_instance_from_timeline(id));
            } else if let catalog::Op::DropComputeInstanceReplica { name, compute_name } = op {
                let compute_instance = self.catalog.resolve_compute_instance(compute_name)?;
                let replica_id = &compute_instance.replica_id_by_name[name];
                let replica = &compute_instance.replicas_by_id[&replica_id];

                // Drop the introspection sources
                sources_to_drop.extend(replica.config.persisted_logs.get_log_ids());
            }
        }

        timelines_to_drop = self.remove_storage_ids_from_timeline(
            sources_to_drop
                .iter()
                .chain(tables_to_drop.iter())
                .chain(materialized_views_to_drop.iter().map(|(_, id)| id))
                .cloned(),
        );
        timelines_to_drop.extend(
            self.remove_compute_ids_from_timeline(
                sinks_to_drop
                    .iter()
                    .chain(indexes_to_drop.iter())
                    .chain(materialized_views_to_drop.iter())
                    .cloned(),
            ),
        );
        ops.extend(timelines_to_drop.into_iter().map(catalog::Op::DropTimeline));

        let (builtin_table_updates, result) = self
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
            self.send_builtin_table_updates(builtin_table_updates).await;

            if !sources_to_drop.is_empty() {
                self.drop_sources(sources_to_drop).await;
            }
            if !tables_to_drop.is_empty() {
                self.drop_sources(tables_to_drop).await;
            }
            if !sinks_to_drop.is_empty() {
                self.drop_sinks(sinks_to_drop).await;
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

    pub(crate) async fn drop_sinks(&mut self, sinks: Vec<(ComputeInstanceId, GlobalId)>) {
        // TODO(chae): Drop storage sinks when they're moved over
        let by_compute_instance = sinks.into_iter().into_group_map();
        for (compute_instance, ids) in by_compute_instance {
            // A cluster could have been dropped, so verify it exists.
            if let Some(mut compute) = self.controller.compute_mut(compute_instance) {
                compute.drop_sinks(ids).await.unwrap();
            }
        }
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

    pub(crate) async fn handle_sink_connection_ready(
        &mut self,
        id: GlobalId,
        oid: u32,
        connection: SinkConnection<()>,
        compute_instance: ComputeInstanceId,
        session: Option<&Session>,
    ) -> Result<(), AdapterError> {
        // Update catalog entry with sink connection.
        let entry = self.catalog.get_entry(&id);
        let name = entry.name().clone();
        let mut sink = match entry.item() {
            CatalogItem::Sink(sink) => sink.clone(),
            _ => unreachable!(),
        };
        sink.connection = catalog::SinkConnectionState::Ready(connection.clone());
        // We don't try to linearize the as of for the sink; we just pick the
        // least valid read timestamp. If users want linearizability across
        // Materialize and their sink, they'll need to reason about the
        // timestamps we emit anyway, so might as emit as much historical detail
        // as we possibly can.
        let id_bundle = self
            .index_oracle(compute_instance)
            .sufficient_collections(&[sink.from]);
        let frontier = self.least_valid_read(&id_bundle);
        let as_of = SinkAsOf {
            frontier,
            strict: !sink.with_snapshot,
        };
        let ops = vec![
            catalog::Op::DropItem(id),
            catalog::Op::CreateItem {
                id,
                oid,
                name: name.clone(),
                item: CatalogItem::Sink(sink.clone()),
            },
        ];
        let df = self
            .catalog_transact(session, ops, |txn| {
                let mut builder = txn.dataflow_builder(compute_instance);
                let from_entry = builder.catalog.get_entry(&sink.from);
                let sink_description = mz_storage::types::sinks::SinkDesc {
                    from: sink.from,
                    from_desc: from_entry
                        .desc(
                            &builder
                                .catalog
                                .resolve_full_name(from_entry.name(), from_entry.conn_id()),
                        )
                        .unwrap()
                        .into_owned(),
                    connection: connection.clone(),
                    envelope: Some(sink.envelope),
                    as_of,
                };
                Ok(builder.build_sink_dataflow(name.to_string(), id, sink_description)?)
            })
            .await?;

        Ok(self.ship_dataflow(df, compute_instance).await)
    }
}
