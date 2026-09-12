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
use std::time::{Duration, Instant};

use differential_dataflow::lattice::Lattice;
use fail::fail_point;
use maplit::{btreemap, btreeset};
use mz_adapter_types::connection::ConnectionId;
use mz_audit_log::VersionedEvent;
use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::memory::objects::{CatalogItem, DataSourceDesc, Sink};
use mz_cluster_client::ReplicaId;
use mz_controller::clusters::ReplicaLocation;
use mz_controller_types::ClusterId;
use mz_ore::instrument;
use mz_ore::metrics::MetricsFutureExt;
use mz_ore::now::to_datetime;
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::catalog::{CatalogClusterReplica, CatalogSchema};
use mz_sql::names::ResolvedDatabaseSpecifier;
use mz_sql::plan::ConnectionDetails;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::{
    MAX_AWS_PRIVATELINK_CONNECTIONS, MAX_CLUSTERS, MAX_CREDIT_CONSUMPTION_RATE, MAX_DATABASES,
    MAX_KAFKA_CONNECTIONS, MAX_MATERIALIZED_VIEWS, MAX_MYSQL_CONNECTIONS, MAX_NETWORK_POLICIES,
    MAX_OBJECTS_PER_SCHEMA, MAX_POSTGRES_CONNECTIONS, MAX_REPLICAS_PER_CLUSTER, MAX_ROLES,
    MAX_SCHEMAS_PER_DATABASE, MAX_SECRETS, MAX_SINKS, MAX_SOURCES, MAX_SQL_SERVER_CONNECTIONS,
    MAX_TABLES, SystemVars, Var,
};
use mz_storage_client::controller::{CollectionDescription, DataSource, ExportDescription};
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::sources::kafka::KAFKA_PROGRESS_DESC;
use serde_json::json;
use tracing::{Instrument, Level, event, info_span, warn};

use crate::active_compute_sink::{ActiveComputeSink, ActiveComputeSinkRetireReason};
use crate::catalog::{DropObjectInfo, Op, TransactionResult};
use crate::coord::Coordinator;
use crate::coord::appends::{BuiltinTableAppendCompletion, BuiltinTableAppendNotify};
use crate::coord::catalog_implications::parsed_state_updates::ParsedStateUpdate;
use crate::session::{Session, Transaction, TransactionOps};
use crate::telemetry::{EventDetails, SegmentClientExt};
use crate::util::ResultExt;
use crate::{AdapterError, ExecuteContext, catalog};

impl Coordinator {
    /// Same as [`Self::catalog_transact_with_context`] but takes a [`Session`].
    #[instrument(name = "coord::catalog_transact")]
    pub(crate) async fn catalog_transact(
        &mut self,
        session: Option<&Session>,
        ops: Vec<catalog::Op>,
    ) -> Result<(), AdapterError> {
        let start = Instant::now();
        let result = self
            .catalog_transact_with_context(session.map(|session| session.conn_id()), None, ops)
            .await;
        self.metrics
            .catalog_transact_seconds
            .with_label_values(&["catalog_transact"])
            .observe(start.elapsed().as_secs_f64());
        result
    }

    /// Same as [`Self::catalog_transact_with_context`] but takes a [`Session`]
    /// and runs builtin table updates concurrently with any side effects (e.g.
    /// creating collections).
    // TODO(aljoscha): Remove this method once all call-sites have been migrated
    // to the newer catalog_transact_with_context. The latter is what allows us
    // to apply catalog implications that we derive from catalog chanages either
    // when initially applying the ops to the catalog _or_ when following
    // catalog changes from another process.
    #[instrument(name = "coord::catalog_transact_with_side_effects")]
    pub(crate) async fn catalog_transact_with_side_effects<F>(
        &mut self,
        mut ctx: Option<&mut ExecuteContext>,
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
        let start = Instant::now();

        let (table_updates, catalog_updates, _created_clients) = self
            .catalog_transact_inner(ctx.as_ref().map(|ctx| ctx.session().conn_id()), ops)
            .await?;

        // We can't run this concurrently with the explicit side effects,
        // because both want to borrow self mutably.
        let apply_implications_res = self
            .apply_catalog_implications(ctx.as_deref_mut(), catalog_updates)
            .await;

        // We would get into an inconsistent state if we updated the catalog but
        // then failed to apply commands/updates to the controller. Easiest
        // thing to do is panic and let restart/bootstrap handle it.
        apply_implications_res.expect("cannot fail to apply catalog update implications");

        // NOTE: `check_consistency` only runs with soft assertions enabled, so
        // this phase reads about zero in production. We time it because a local
        // rig debugging a transact stall commonly has them on, where the check is
        // O(catalog size) and would otherwise appear as an unexplained remainder
        // against the wrapper metric. The observation stays outside the macro,
        // anything inside it compiles out exactly where soft assertions are off.
        let consistency_start = Instant::now();

        // Note: It's important that we keep the function call inside macro, this way we only run
        // the consistency checks if soft assertions are enabled.
        mz_ore::soft_assert_eq_no_log!(
            self.check_consistency(),
            Ok(()),
            "coordinator inconsistency detected"
        );

        self.metrics
            .catalog_transact_phase_seconds
            .with_label_values(&["consistency_check"])
            .observe(consistency_start.elapsed().as_secs_f64());

        let side_effects_seconds = self
            .metrics
            .catalog_transact_phase_seconds
            .with_label_values(&["side_effects"]);
        // Distinct from `table_updates_wait` in `catalog_transact_with_context`.
        // Here the group commit has already been running concurrently with
        // `apply_catalog_implications` above, so this wrapper is first polled
        // late and only records the residual wait.
        let table_updates_wait = self
            .metrics
            .catalog_transact_phase_seconds
            .with_label_values(&["table_updates_residual_wait"]);
        let side_effects_fut = side_effect(self, ctx);

        // Run our side effects concurrently with the table updates.
        let ((), ()) = futures::future::join(
            side_effects_fut
                .wall_time()
                .observe(side_effects_seconds)
                .instrument(info_span!(
                    "coord::catalog_transact_with_side_effects::side_effects_fut"
                )),
            table_updates
                .wall_time()
                .observe(table_updates_wait)
                .instrument(info_span!(
                    "coord::catalog_transact_with_side_effects::table_updates"
                )),
        )
        .await;

        self.metrics
            .catalog_transact_seconds
            .with_label_values(&["catalog_transact_with_side_effects"])
            .observe(start.elapsed().as_secs_f64());

        Ok(())
    }

    /// Same as [`Self::catalog_transact_inner`] but takes an execution context
    /// or connection ID and runs builtin table updates concurrently with any
    /// catalog implications that are generated as part of applying the given
    /// `ops` (e.g. creating collections).
    ///
    /// This will use a connection ID if provided and otherwise fall back to
    /// getting a connection ID from the execution context.
    #[instrument(name = "coord::catalog_transact_with_context")]
    pub(crate) async fn catalog_transact_with_context(
        &mut self,
        conn_id: Option<&ConnectionId>,
        ctx: Option<&mut ExecuteContext>,
        ops: Vec<catalog::Op>,
    ) -> Result<(), AdapterError> {
        self.catalog_transact_with_results(conn_id, ctx, ops)
            .await
            .map(|_| ())
    }

    /// Commits and applies catalog effects, returning client identities allocated by the batch.
    pub(crate) async fn catalog_transact_with_results(
        &mut self,
        conn_id: Option<&ConnectionId>,
        ctx: Option<&mut ExecuteContext>,
        ops: Vec<catalog::Op>,
    ) -> Result<Vec<u64>, AdapterError> {
        let start = Instant::now();

        let conn_id = conn_id.or_else(|| ctx.as_ref().map(|ctx| ctx.session().conn_id()));

        let (table_updates, catalog_updates, created_clients) =
            self.catalog_transact_inner(conn_id, ops).await?;

        let table_updates_wait = self
            .metrics
            .catalog_transact_phase_seconds
            .with_label_values(&["table_updates_wait"]);
        let apply_catalog_implications_fut = self.apply_catalog_implications(ctx, catalog_updates);

        // Apply catalog implications concurrently with the table updates.
        let (combined_apply_res, ()) = futures::future::join(
            apply_catalog_implications_fut.instrument(info_span!(
                "coord::catalog_transact_with_context::side_effects_fut"
            )),
            table_updates
                .wall_time()
                .observe(table_updates_wait)
                .instrument(info_span!(
                    "coord::catalog_transact_with_context::table_updates"
                )),
        )
        .await;

        // We would get into an inconsistent state if we updated the catalog but
        // then failed to apply implications. Easiest thing to do is panic and
        // let restart/bootstrap handle it.
        combined_apply_res.expect("cannot fail to apply catalog implications");

        // See the note in `catalog_transact_with_side_effects` on why this is
        // timed outside the macro and reads about zero in production.
        let consistency_start = Instant::now();

        // Note: It's important that we keep the function call inside macro, this way we only run
        // the consistency checks if soft assertions are enabled.
        mz_ore::soft_assert_eq_no_log!(
            self.check_consistency(),
            Ok(()),
            "coordinator inconsistency detected"
        );

        self.metrics
            .catalog_transact_phase_seconds
            .with_label_values(&["consistency_check"])
            .observe(consistency_start.elapsed().as_secs_f64());

        self.metrics
            .catalog_transact_seconds
            .with_label_values(&["catalog_transact_with_context"])
            .observe(start.elapsed().as_secs_f64());

        Ok(created_clients)
    }

    /// Executes a Catalog transaction with handling if the provided [`Session`]
    /// is in a SQL transaction that is executing DDL.
    #[instrument(name = "coord::catalog_transact_with_ddl_transaction")]
    pub(crate) async fn catalog_transact_with_ddl_transaction<F>(
        &mut self,
        ctx: &mut ExecuteContext,
        mut ops: Vec<catalog::Op>,
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
        let start = Instant::now();

        let Some(Transaction {
            ops:
                TransactionOps::DDL {
                    ops: txn_ops,
                    transient_revision: txn_revision,
                    state: txn_state,
                    snapshot: txn_snapshot,
                    side_effects: _,
                },
            ..
        }) = ctx.session().transaction().inner()
        else {
            let result = self
                .catalog_transact_with_side_effects(Some(ctx), ops, side_effect)
                .await;
            self.metrics
                .catalog_transact_seconds
                .with_label_values(&["catalog_transact_with_ddl_transaction"])
                .observe(start.elapsed().as_secs_f64());
            return result;
        };

        if self.catalog().transient_revision() != *txn_revision {
            self.metrics
                .catalog_transact_seconds
                .with_label_values(&["catalog_transact_with_ddl_transaction"])
                .observe(start.elapsed().as_secs_f64());
            return Err(AdapterError::DDLTransactionRace);
        }

        // The per-statement phases of a DDL transaction carry their own labels.
        // The work differs from a real transaction's phases, and it is billed
        // once per statement rather than once per transaction, so pooling the two
        // populations under one label would blur both.
        let phase_seconds = self.metrics.catalog_transact_phase_seconds.clone();

        // Clone what we need from the session before taking &mut below.
        let clone_start = Instant::now();
        let txn_ops_clone = txn_ops.clone();
        let txn_state_clone = txn_state.clone();
        // NOTE: `txn_snapshot` is a deep clone of the durable `Snapshot`, which is
        // O(catalog size) in allocations, once per statement. `txn_state` next to
        // it is cheap, `CatalogState` holds its large collections in `imbl` maps.
        let prev_snapshot = txn_snapshot.clone();
        phase_seconds
            .with_label_values(&["ddl_txn_snapshot_clone"])
            .observe(clone_start.elapsed().as_secs_f64());

        // Validate resource limits with all accumulated + new ops (cheap O(N) counting).
        let prep_start = Instant::now();
        let mut combined_ops = txn_ops_clone;
        combined_ops.extend(ops.iter().cloned());
        let creates_scoped_object = ops.iter().any(|op| {
            matches!(
                op,
                catalog::Op::CreateCluster { .. } | catalog::Op::CreateClusterReplica { .. }
            )
        });
        if creates_scoped_object {
            // Include accumulated creates when deriving contexts. A replica can
            // be created in a later DDL statement than its still-uncommitted
            // cluster, which is absent from the coordinator's live catalog.
            if let Some(scoped_op) = self.scoped_overrides_create_op(&combined_ops) {
                ops.push(scoped_op.clone());
                combined_ops.push(scoped_op);
            }
        }
        let conn_id = ctx.session().conn_id().clone();
        let validate_res = self.validate_resource_limits(&combined_ops, &conn_id);
        phase_seconds
            .with_label_values(&["ddl_txn_prep"])
            .observe(prep_start.elapsed().as_secs_f64());
        validate_res?;

        // Get oracle timestamp for audit log entries.
        let oracle_write_ts = self
            .get_local_write_ts()
            .wall_time()
            .observe(phase_seconds.with_label_values(&["ddl_txn_write_ts"]))
            .await
            .timestamp;

        // Get ConnMeta for the session.
        let conn = self.active_conns.get(ctx.session().conn_id());

        // Incremental dry run: process only NEW ops against accumulated state.
        // If we have a saved snapshot from a previous dry run, use it to
        // initialize the transaction so it starts in sync with the accumulated
        // state. Otherwise (first statement), the fresh durable transaction is
        // already in sync with the real catalog state.
        let (new_state, new_snapshot) = self
            .catalog()
            .transact_incremental_dry_run(
                &txn_state_clone,
                ops.clone(),
                conn,
                prev_snapshot,
                oracle_write_ts,
            )
            .wall_time()
            .observe(phase_seconds.with_label_values(&["ddl_txn_dry_run"]))
            .await?;

        // Accumulate ops for eventual COMMIT.
        let result = ctx
            .session_mut()
            .transaction_mut()
            .add_ops(TransactionOps::DDL {
                ops: combined_ops,
                state: new_state,
                side_effects: vec![Box::new(side_effect)],
                transient_revision: self.catalog().transient_revision(),
                snapshot: Some(new_snapshot),
            });

        self.metrics
            .catalog_transact_seconds
            .with_label_values(&["catalog_transact_with_ddl_transaction"])
            .observe(start.elapsed().as_secs_f64());

        result
    }

    /// Perform a catalog transaction. [`Coordinator::ship_dataflow`] must be
    /// called after this function successfully returns on any built
    /// [`DataflowDesc`](mz_compute_types::dataflows::DataflowDesc).
    #[instrument(name = "coord::catalog_transact_inner")]
    pub(crate) async fn catalog_transact_inner(
        &mut self,
        conn_id: Option<&ConnectionId>,
        ops: Vec<catalog::Op>,
    ) -> Result<(BuiltinTableAppendNotify, Vec<ParsedStateUpdate>, Vec<u64>), AdapterError> {
        let metadata_only = ops.iter().all(|op| {
            matches!(
                op,
                catalog::Op::SetReadProtection { .. }
                    | catalog::Op::CreateClientIncarnation
                    | catalog::Op::PublishClientReadRequirements { .. }
                    | catalog::Op::ReclaimClientIncarnation { .. }
            )
        });
        loop {
            let revision = self.catalog().transient_revision();
            match self.catalog_transact_attempt(conn_id, ops.clone()).await {
                Err(AdapterError::Catalog(error))
                    if matches!(
                        &error.kind,
                        mz_catalog::memory::error::ErrorKind::Durable(
                            mz_catalog::durable::DurableCatalogError::CatalogOutOfSync { .. }
                        )
                    ) =>
                {
                    let (builtin, updates) = self.catalog_mut().sync_to_current_updates().await?;
                    let builtin = self
                        .catalog()
                        .state()
                        .resolve_builtin_table_updates(builtin);
                    let notify = self.builtin_table_update().execute(builtin);
                    match mz_ore::future::OreFutureExt::ore_catch_unwind(
                        std::panic::AssertUnwindSafe(Box::pin(
                            self.apply_catalog_implications(None, updates),
                        )),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => mz_ore::halt!(
                            "cannot enact committed catalog changes, restart required: {error}"
                        ),
                        Err(payload) => {
                            let cause = mz_ore::panic::downcast_panic_message(&*payload);
                            mz_ore::halt!(
                                "cannot enact committed catalog changes, restart required: {cause}"
                            )
                        }
                    }
                    notify.await;
                    if !metadata_only && self.catalog().transient_revision() != revision {
                        return Err(AdapterError::DDLTransactionRace);
                    }
                    // Rebuild the transaction, including admission checks, against
                    // the refreshed state. Never replay a stale durable batch.
                }
                result => return result,
            }
        }
    }

    async fn catalog_transact_attempt(
        &mut self,
        conn_id: Option<&ConnectionId>,
        mut ops: Vec<catalog::Op>,
    ) -> Result<(BuiltinTableAppendNotify, Vec<ParsedStateUpdate>, Vec<u64>), AdapterError> {
        if self.controller.read_only() {
            return Err(AdapterError::ReadOnly);
        }

        if let Some(scoped_op) = self.scoped_overrides_create_op(&ops) {
            ops.push(scoped_op);
        }

        event!(Level::TRACE, ops = format!("{:?}", ops));

        let phase_seconds = self.metrics.catalog_transact_phase_seconds.clone();
        let phase_start = Instant::now();

        let mut webhook_sources_to_restart = BTreeSet::new();
        let mut clusters_to_drop = vec![];
        let mut cluster_replicas_to_drop = vec![];
        let mut clusters_to_create = vec![];
        let mut cluster_replicas_to_create = vec![];

        for op in &ops {
            match op {
                catalog::Op::DropObjects(drop_object_infos) => {
                    for drop_object_info in drop_object_infos {
                        match &drop_object_info {
                            catalog::DropObjectInfo::Item(_) => {
                                // Nothing to do, these will be handled by
                                // applying the side effects that we return.
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

        // Observe before propagating, so a transaction rejected on resource
        // limits still accounts for the op scan it burned on the loop.
        let validate_res = self.validate_resource_limits(&ops, conn_id.unwrap_or(&SYSTEM_CONN_ID));
        phase_seconds
            .with_label_values(&["prep"])
            .observe(phase_start.elapsed().as_secs_f64());
        validate_res?;

        // This will produce timestamps that are guaranteed to increase on each
        // call, and also never be behind the system clock. If the system clock
        // hasn't advanced (or has gone backward), it will increment by 1. For
        // the audit log, we need to balance "close (within 10s or so) to the
        // system clock" and "always goes up". We've chosen here to prioritize
        // always going up, and believe we will always be close to the system
        // clock because it is well configured (chrony) and so may only rarely
        // regress or pause for 10s.
        let oracle_write_ts = self
            .get_catalog_write_ts()
            .wall_time()
            .observe(phase_seconds.with_label_values(&["write_ts"]))
            .await;

        let Coordinator {
            catalog,
            active_conns,
            controller,
            cluster_replica_statuses,
            ..
        } = self;
        let catalog = Arc::make_mut(catalog);
        let conn = conn_id.map(|id| active_conns.get(id).expect("connection must exist"));

        // Register the session as an ephemeral owner (its uuid <-> connection
        // mapping) at its first temporary-item creation.
        if let Some(conn) = conn {
            let creates_temp_item = ops.iter().any(
                |op| matches!(op, catalog::Op::CreateItem { item, .. } if item.is_temporary()),
            );
            if creates_temp_item && !catalog.state().has_temporary_namespace(conn.conn_id()) {
                catalog.register_temporary_namespace(conn.conn_id(), conn.uuid());
            }
        }

        // NOTE: This phase contains every durable `sync` and `commit` a catalog
        // transaction performs, which is what makes `transact` minus those two
        // histograms an estimate of the in-memory work. Two caveats. More than
        // one sync happens per transaction, so the subtraction is only valid on
        // rates of `_sum`, never on per-observation means. And durable
        // `allocate_id` (user ID pool refills, storage usage batch IDs) observes
        // into the same histograms from outside any catalog transaction, so the
        // estimate is biased low while allocation is active.
        let TransactionResult {
            builtin_table_updates,
            catalog_updates,
            audit_events,
            created_client_incarnations,
        } = catalog
            .transact(
                Some(&mut controller.storage_collections),
                oracle_write_ts,
                conn,
                ops,
            )
            .wall_time()
            .observe(phase_seconds.with_label_values(&["transact"]))
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
        let stage_start = Instant::now();
        let builtin_update_notify = self.builtin_table_update().execute(builtin_table_updates);
        phase_seconds
            .with_label_values(&["stage_builtin"])
            .observe(stage_start.elapsed().as_secs_f64());

        let finalize_start = Instant::now();

        // No error returns are allowed after this point. Enforce this at compile time
        // by using this odd structure so we don't accidentally add a stray `?`.
        let _: () = async {
            if !webhook_sources_to_restart.is_empty() {
                self.restart_webhook_sources(webhook_sources_to_restart);
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

        phase_seconds
            .with_label_values(&["finalize"])
            .observe(finalize_start.elapsed().as_secs_f64());

        Ok((
            builtin_update_notify,
            catalog_updates,
            created_client_incarnations,
        ))
    }

    pub(crate) fn drop_replica(&mut self, cluster_id: ClusterId, replica_id: ReplicaId) {
        self.drop_introspection_subscribes(replica_id);
        self.drop_metric_sinks(replica_id);

        self.controller
            .drop_replica(cluster_id, replica_id)
            .expect("dropping replica must not fail");
    }

    /// A convenience method for dropping sources.
    pub(crate) fn drop_sources(&mut self, sources: Vec<(CatalogItemId, GlobalId)>) {
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

    /// A convenience method for dropping tables.
    /// Txn-wal membership must already be forgotten through the group committer.
    pub(crate) fn drop_tables(&mut self, tables: Vec<(CatalogItemId, GlobalId)>) {
        for (item_id, _gid) in &tables {
            self.active_webhooks.remove(item_id);
        }

        let table_gids: Vec<_> = tables.into_iter().map(|(_id, gid)| gid).collect();

        let storage_metadata = self.catalog.state().storage_metadata();
        self.controller
            .storage
            .drop_tables(storage_metadata, table_gids)
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
    pub async fn drop_compute_sink(
        &mut self,
        sink_id: GlobalId,
    ) -> Option<(ActiveComputeSink, BuiltinTableAppendNotify)> {
        self.drop_compute_sinks([sink_id]).await.remove(&sink_id)
    }

    /// Drops a batch of compute sinks.
    ///
    /// For each sink that exists, the coordinator and controller's state
    /// associated with the sink is removed.
    ///
    /// Returns a map from sink id to the controller's state for the sink and a notify that
    /// resolves once the sink's `mz_subscriptions` retraction is durable (see
    /// `remove_active_compute_sink`). It is the caller's responsibility to await the notify
    /// off the coordinator loop and then retire the returned sinks. Consider using
    /// `retire_compute_sinks` instead.
    #[must_use]
    pub async fn drop_compute_sinks(
        &mut self,
        sink_ids: impl IntoIterator<Item = GlobalId>,
    ) -> BTreeMap<GlobalId, (ActiveComputeSink, BuiltinTableAppendNotify)> {
        let mut by_id = BTreeMap::new();
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for sink_id in sink_ids {
            let query_execution = self
                .active_compute_sinks
                .get_mut(&sink_id)
                .and_then(|sink| sink.query_execution_mut().take());
            let query_owned = query_execution.is_some();
            // Release installed and pending query work before waiting on catalog
            // bookkeeping. Only its owning connection may compact these exports.
            drop(query_execution);
            let (sink, write_notify) = match self.remove_active_compute_sink(sink_id).await {
                None => {
                    // This can happen due to a race condition: an internal
                    // subscribe may be cleaned up via its own message while
                    // session disconnect cleanup is in progress. This is
                    // benign.
                    tracing::debug!(%sink_id, "drop_compute_sinks: sink already removed");
                    continue;
                }
                Some(entry) => entry,
            };

            if !query_owned {
                by_cluster
                    .entry(sink.cluster_id())
                    .or_default()
                    .push(sink_id);
            }
            by_id.insert(sink_id, (sink, write_notify));
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
    /// then retired with its corresponding reason. Returns a notify that resolves
    /// once all `mz_subscriptions` retractions are durable and the sinks are retired.
    pub async fn retire_compute_sinks(
        &mut self,
        mut reasons: BTreeMap<GlobalId, ActiveComputeSinkRetireReason>,
    ) -> BuiltinTableAppendCompletion {
        let sink_ids = reasons.keys().cloned();
        let to_retire: Vec<_> = self
            .drop_compute_sinks(sink_ids)
            .await
            .into_iter()
            .map(|(id, (sink, write_notify))| {
                let reason = reasons
                    .remove(&id)
                    .expect("all returned IDs are in `reasons`");
                (sink, write_notify, reason)
            })
            .collect();

        // Retire off the coordinator loop. We wait for each `mz_subscriptions` retraction
        // before telling the subscribing client that the sink is gone. The returned notify
        // lets statements that caused the retirement also wait before sending their response.
        // The wait must not happen on the coordinator loop, since that would block every
        // other session on the group-commit oracle round trip.
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        task::spawn(|| "retire_compute_sinks", async move {
            for (sink, write_notify, reason) in to_retire {
                write_notify.await;
                sink.retire(reason);
            }
            let _ = done_tx.send(());
        });
        BuiltinTableAppendCompletion::new(Box::pin(async move {
            let _ = done_rx.await;
        }))
    }

    /// Cancels all active compute sinks for the identified connection.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn cancel_compute_sinks_for_conn(
        &mut self,
        conn_id: &ConnectionId,
    ) -> BuiltinTableAppendCompletion {
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
    ) -> BuiltinTableAppendCompletion {
        let drop_sinks = self
            .active_conns
            .get_mut(conn_id)
            .expect("must exist for active session")
            .drop_sinks
            .iter()
            .map(|sink_id| (*sink_id, reason.clone()))
            .collect();
        self.retire_compute_sinks(drop_sinks).await
    }

    pub(crate) fn drop_storage_sinks(&mut self, sink_gids: Vec<GlobalId>) {
        let storage_metadata = self.catalog.state().storage_metadata();
        self.controller
            .storage
            .drop_sinks(storage_metadata, sink_gids)
            .unwrap_or_terminate("cannot fail to drop sinks");
    }

    pub(crate) fn drop_compute_collections(&mut self, collections: Vec<(ClusterId, GlobalId)>) {
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for (cluster_id, gid) in collections {
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

    pub(crate) fn drop_vpc_endpoints_in_background(&self, vpc_endpoints: Vec<CatalogItemId>) {
        // Match the create path (catalog_implications.rs) which gracefully
        // logs an error when cloud_resource_controller is None, rather than
        // panicking.
        let Some(cloud_resource_controller) = self.cloud_resource_controller.as_ref() else {
            warn!("dropping VPC endpoints without cloud_resource_controller; skipping cleanup");
            return;
        };
        let cloud_resource_controller = Arc::clone(cloud_resource_controller);
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

        self.catalog_transact_with_context(Some(conn_id), None, vec![op])
            .await
            .expect("unable to drop temporary items for conn_id");
    }

    pub(crate) async fn create_storage_export(
        &mut self,
        id: GlobalId,
        sink: &Sink,
    ) -> Result<(), AdapterError> {
        let (desc, read_holds) = self.storage_export_description(id, sink)?;
        let collection_desc = CollectionDescription {
            // TODO(sinks): make generic once we have more than one sink type.
            desc: KAFKA_PROGRESS_DESC.clone(),
            data_source: DataSource::Sink { desc },
            since: None,
            timeline: None,
            primary: None,
        };
        let storage_metadata = self.catalog.state().storage_metadata();
        let res = self
            .controller
            .storage
            .create_collections(storage_metadata, None, vec![(id, collection_desc)])
            .await;
        // The controller owns dependency protection after installation.
        drop(read_holds);
        Ok(res?)
    }

    pub(crate) async fn alter_storage_export(&mut self, sink: &Sink) -> Result<(), AdapterError> {
        let (desc, read_holds) = self.storage_export_description(sink.global_id(), sink)?;
        let res = self
            .controller
            .storage
            .alter_export(sink.global_id(), desc)
            .await;
        drop(read_holds);
        Ok(res?)
    }

    /// Reconstructs an export from committed state and protects its inputs until
    /// the controller takes over their protection during installation.
    fn storage_export_description(
        &self,
        id: GlobalId,
        sink: &Sink,
    ) -> Result<(ExportDescription, crate::ReadHolds), AdapterError> {
        // Validate `sink.from` is in fact a storage collection
        self.controller.storage.check_exists(sink.from)?;

        // The AsOf is used to determine at what time to snapshot reading from
        // the persist collection.  This is primarily relevant when we do _not_
        // want to include the snapshot in the sink.
        let id_bundle = crate::CollectionIdBundle {
            storage_ids: btreeset! {sink.from},
            compute_ids: btreemap! {},
        };

        // Keep dependencies readable while constructing the description and
        // until the controller acquires its own dependency holds.
        let read_holds = self.acquire_read_holds(&id_bundle);
        let mut as_of = read_holds.least_valid_read();
        if self.catalog().state().catalog_read_protection_enabled() {
            let requirement = &self.catalog().state().maintained_read_requirements()[&id];
            let committed = requirement.frontier.into_iter().collect();
            // A read-only catalog snapshot can lag the leased since. Actual holds,
            // not the snapshot's permission, determine what remains executable.
            as_of.join_assign(&committed);
        }

        let storage_sink_from_entry = self.catalog().get_entry_by_global_id(&sink.from);
        let storage_sink_desc = mz_storage_types::sinks::StorageSinkDesc {
            from: sink.from,
            from_desc: storage_sink_from_entry
                .relation_desc()
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
            commit_interval: sink.commit_interval,
        };

        Ok((
            ExportDescription {
                sink: storage_sink_desc,
                instance_id: sink.cluster_id,
            },
            read_holds,
        ))
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
                            new_credit_consumption_rate += self.replica_credits_per_hour(location);
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
                            | ConnectionDetails::GlueSchemaRegistry(_)
                            | ConnectionDetails::Ssh { .. }
                            | ConnectionDetails::Aws(_)
                            | ConnectionDetails::Gcp(_)
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
                        CatalogItem::Log(_)
                        | CatalogItem::View(_)
                        | CatalogItem::Index(_)
                        | CatalogItem::Type(_)
                        | CatalogItem::Func(_)
                        | CatalogItem::MetricSink(_) => {}
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
                                        new_credit_consumption_rate -=
                                            self.replica_credits_per_hour(location);
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
                                    CatalogItem::Log(_)
                                    | CatalogItem::View(_)
                                    | CatalogItem::Index(_)
                                    | CatalogItem::Type(_)
                                    | CatalogItem::Func(_)
                                    | CatalogItem::MetricSink(_) => {}
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
                    | CatalogItem::MetricSink(_) => {}
                },
                Op::AlterRole { .. }
                | Op::AlterRetainHistory { .. }
                | Op::AlterSourceTimestampInterval { .. }
                | Op::AlterNetworkPolicy { .. }
                | Op::AlterAddColumn { .. }
                | Op::AlterMaterializedViewApplyReplacement { .. }
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
                | Op::UpdateSourceReferences { .. }
                | Op::UpdateSystemConfiguration { .. }
                | Op::ResetSystemConfiguration { .. }
                | Op::ResetAllSystemConfiguration { .. }
                | Op::UpdateScopedSystemParameters { .. }
                | Op::SetReadProtection { .. }
                | Op::CreateClientIncarnation
                | Op::PublishClientReadRequirements { .. }
                | Op::ReclaimClientIncarnation { .. }
                | Op::Comment { .. }
                | Op::CheckClusterState { .. }
                | Op::InjectAuditEvents { .. } => {}
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
                | ConnectionDetails::GlueSchemaRegistry(_)
                | ConnectionDetails::Ssh { .. }
                | ConnectionDetails::Aws(_)
                | ConnectionDetails::Gcp(_)
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
            self.current_credit_consumption_rate(None),
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
            // For temporary schemas that don't exist yet (lazy creation),
            // treat them as having 0 items.
            let current_items = self
                .catalog()
                .try_get_schema(&database_spec, &schema_spec, conn_id)
                .map(|schema| schema.items.len())
                .unwrap_or(0);
            self.validate_resource_limit(
                current_items,
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
            self.catalog().user_network_policies().count(),
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
