// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for processing [`Coordinator`] messages. The [`Coordinator`] receives
//! messages from various sources (ex: controller, clients, background tasks, etc).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::DurationRound;
use mz_persist_client::usage::ShardsUsage;
use rand::{rngs, Rng, SeedableRng};
use tracing::{event, warn, Level};

use mz_controller::clusters::ClusterEvent;
use mz_controller::ControllerResponse;
use mz_ore::now::EpochMillis;
use mz_ore::task;
use mz_sql::ast::Statement;
use mz_sql::plan::{CreateSourcePlans, Plan};
use mz_storage_client::controller::CollectionMetadata;

use crate::client::ConnectionId;
use crate::command::{Command, ExecuteResponse};
use crate::coord::appends::Deferred;
use crate::coord::timestamp_selection::TimestampContext;
use crate::coord::{
    Coordinator, CreateSourceStatementReady, Message, PendingReadTxn, RealTimeRecencyContext,
    SinkConnectionReady,
};
use crate::util::ResultExt;
use crate::{catalog, AdapterNotice};

use super::{PeekStage, PeekStageFinish, TransientPlan};

impl Coordinator {
    pub(crate) async fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::Command(cmd) => self.message_command(cmd).await,
            Message::ControllerReady => {
                if let Some(m) = self
                    .controller
                    .process()
                    .await
                    .expect("`process` never returns an error")
                {
                    self.message_controller(m).await
                }
            }
            Message::CreateSourceStatementReady(ready) => {
                self.message_create_source_statement_ready(ready).await
            }
            Message::SinkConnectionReady(ready) => self.message_sink_connection_ready(ready).await,
            Message::WriteLockGrant(write_lock_guard) => {
                self.message_write_lock_grant(write_lock_guard).await;
            }
            Message::GroupCommitInitiate => {
                self.try_group_commit().await;
            }
            Message::GroupCommitApply(timestamp, responses, write_lock_guard) => {
                self.group_commit_apply(timestamp, responses, write_lock_guard)
                    .await;
            }
            Message::AdvanceTimelines => {
                self.advance_timelines().await;
            }
            Message::ClusterEvent(event) => self.message_cluster_event(event).await,
            // Processing this message DOES NOT send a response to the client;
            // in any situation where you use it, you must also have a code
            // path that responds to the client (e.g. reporting an error).
            Message::RemovePendingPeeks { conn_id } => {
                tracing::info!("remove pending peeks");
                self.cancel_pending_peeks(&conn_id);
            }
            Message::LinearizeReads(pending_read_txns) => {
                self.message_linearize_reads(pending_read_txns).await;
            }
            Message::StorageUsageFetch => {
                self.storage_usage_fetch().await;
            }
            Message::StorageUsageUpdate(sizes) => {
                self.storage_usage_update(sizes).await;
            }
            Message::RealTimeRecencyTimestamp {
                conn_id,
                real_time_recency_ts,
                replan,
            } => {
                self.message_real_time_recency_timestamp(conn_id, real_time_recency_ts, replan)
                    .await;
            }
            Message::Replan {
                tx,
                session,
                replan,
            } => {
                self.sequence_plan(tx, session, replan.plan, replan.depends_on)
                    .await;
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn storage_usage_fetch(&mut self) {
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let client = self.storage_usage_client.clone();

        // Record the currently live shards.
        let live_shards: BTreeSet<_> = self
            .controller
            .storage
            .collections()
            // A collection is dropped if its read capability has been advanced
            // to the empty antichain.
            .filter(|(_id, collection)| !collection.read_capabilities.is_empty())
            .flat_map(|(_id, collection)| {
                let CollectionMetadata {
                    data_shard,
                    remap_shard,
                    status_shard,
                    // No wildcards, to improve the odds that the addition of a
                    // new shard type results in a compiler error here.
                    //
                    // ATTENTION: If you add a new type of shard that is
                    // associated with a collection, almost surely you should
                    // return it below, so that its usage is recorded in the
                    // `mz_storage_usage_by_shard` table.
                    persist_location: _,
                    relation_desc: _,
                } = &collection.collection_metadata;
                [*remap_shard, *status_shard, Some(*data_shard)].into_iter()
            })
            .filter_map(|shard| shard)
            .collect();

        let collection_metric = self
            .metrics
            .storage_usage_collection_time_seconds
            .with_label_values(&[]);

        // Spawn an asynchronous task to compute the storage usage, which
        // requires a slow scan of the underlying storage engine.
        task::spawn(|| "storage_usage_fetch", async move {
            let collection_metric_timer = collection_metric.start_timer();
            let mut shard_sizes = client.shards_usage().await;

            // Don't record usage for shards that are no longer live.
            // Technically the storage is in use, but we never free it, and
            // we don't want to bill the customer for it.
            //
            // See: https://github.com/MaterializeInc/materialize/issues/8185
            shard_sizes
                .by_shard
                .retain(|shard_id, _| live_shards.contains(shard_id));
            collection_metric_timer.observe_duration();

            // It is not an error for shard sizes to become ready after
            // `internal_cmd_rx` is dropped.
            if let Err(e) = internal_cmd_tx.send(Message::StorageUsageUpdate(shard_sizes)) {
                warn!("internal_cmd_rx dropped before we could send: {:?}", e);
            }
        });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn storage_usage_update(&mut self, shards_usage: ShardsUsage) {
        // Similar to audit events, use the oracle ts so this is guaranteed to
        // increase. This is intentionally the timestamp of when collection
        // finished, not when it started, so that we don't write data with a
        // timestamp in the past.
        let collection_timestamp: EpochMillis = self.get_local_write_ts().await.timestamp.into();

        let mut ops = vec![];
        for (shard_id, shard_usage) in shards_usage.by_shard {
            ops.push(catalog::Op::UpdateStorageUsage {
                shard_id: Some(shard_id.to_string()),
                size_bytes: shard_usage.referenced_bytes(),
                collection_timestamp,
            });
        }
        if shards_usage.unattributable_bytes > 0 {
            ops.push(catalog::Op::UpdateStorageUsage {
                shard_id: None,
                size_bytes: shards_usage.unattributable_bytes,
                collection_timestamp,
            });
        }

        if let Err(err) = self.catalog_transact(None, ops).await {
            tracing::warn!("Failed to update storage metrics: {:?}", err);
        }
        self.schedule_storage_usage_collection();
    }

    pub fn schedule_storage_usage_collection(&self) {
        // Instead of using an `tokio::timer::Interval`, we calculate the time until the next
        // usage collection and wait for that amount of time. This is so we can keep the intervals
        // consistent even across restarts. If collection takes too long, it is possible that
        // we miss an interval.

        // 1) Deterministically pick some offset within the collection interval to prevent
        // thundering herds across environments.
        const SEED_LEN: usize = 32;
        let mut seed = [0; SEED_LEN];
        for (i, byte) in self
            .catalog()
            .state()
            .config()
            .environment_id
            .organization_id()
            .as_bytes()
            .into_iter()
            .take(SEED_LEN)
            .enumerate()
        {
            seed[i] = *byte;
        }
        let storage_usage_collection_interval_ms: EpochMillis =
            EpochMillis::try_from(self.storage_usage_collection_interval.as_millis())
                .expect("storage usage collection interval must fit into u64");
        let offset =
            rngs::SmallRng::from_seed(seed).gen_range(0..storage_usage_collection_interval_ms);
        let now_ts: EpochMillis = self.peek_local_write_ts().into();

        // 2) Determine the amount of ms between now and the next collection time.
        let previous_collection_ts =
            (now_ts - (now_ts % storage_usage_collection_interval_ms)) + offset;
        let next_collection_ts = if previous_collection_ts > now_ts {
            previous_collection_ts
        } else {
            previous_collection_ts + storage_usage_collection_interval_ms
        };
        let next_collection_interval = Duration::from_millis(next_collection_ts - now_ts);

        // 3) Sleep for that amount of time, then initiate another storage usage collection.
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(|| "storage_usage_collection", async move {
            tokio::time::sleep(next_collection_interval).await;
            if internal_cmd_tx.send(Message::StorageUsageFetch).is_err() {
                // If sending fails, the main thread has shutdown.
            }
        });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn message_command(&mut self, cmd: Command) {
        event!(Level::TRACE, cmd = format!("{:?}", cmd));
        self.handle_command(cmd).await;
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn message_controller(&mut self, message: ControllerResponse) {
        event!(Level::TRACE, message = format!("{:?}", message));
        match message {
            ControllerResponse::PeekResponse(uuid, response, otel_ctx) => {
                self.send_peek_response(uuid, response, otel_ctx);
            }
            ControllerResponse::SubscribeResponse(sink_id, response) => {
                // We use an `if let` here because the peek could have been canceled already.
                // We can also potentially receive multiple `Complete` responses, followed by
                // a `Dropped` response.
                if let Some(active_subscribe) = self.active_subscribes.get_mut(&sink_id) {
                    let remove = active_subscribe.process_response(response);
                    if remove {
                        self.remove_active_subscribe(sink_id).await;
                    }
                }
            }
            ControllerResponse::ComputeReplicaHeartbeat(replica_id, when) => {
                let replica_status_interval = chrono::Duration::seconds(60);
                let new = when
                    .duration_trunc(replica_status_interval)
                    .expect("Time coarsening should not fail");
                let hb = match self
                    .transient_replica_metadata
                    .entry(replica_id)
                    .or_insert_with(|| Some(Default::default()))
                {
                    // `None` is the tombstone for a removed replica
                    None => return,
                    Some(md) => &mut md.last_heartbeat,
                };
                let old = std::mem::replace(hb, Some(new));

                if old.as_ref() != Some(&new) {
                    let retraction = old.map(|old| {
                        self.catalog()
                            .state()
                            .pack_replica_heartbeat_update(replica_id, old, -1)
                    });
                    let insertion = self
                        .catalog()
                        .state()
                        .pack_replica_heartbeat_update(replica_id, new, 1);
                    let updates = if let Some(retraction) = retraction {
                        vec![retraction, insertion]
                    } else {
                        vec![insertion]
                    };
                    self.buffer_builtin_table_updates(updates);
                }
            }
            ControllerResponse::ComputeReplicaMetrics(replica_id, new) => {
                let m = match self
                    .transient_replica_metadata
                    .entry(replica_id)
                    .or_insert_with(|| Some(Default::default()))
                {
                    // `None` is the tombstone for a removed replica
                    None => return,
                    Some(md) => &mut md.metrics,
                };
                let old = std::mem::replace(m, Some(new.clone()));
                if old.as_ref() != Some(&new) {
                    let retractions = old.map(|old| {
                        self.catalog()
                            .state()
                            .pack_replica_metric_updates(replica_id, &old, -1)
                    });
                    let insertions = self
                        .catalog()
                        .state()
                        .pack_replica_metric_updates(replica_id, &new, 1);
                    let updates = if let Some(retractions) = retractions {
                        retractions
                            .into_iter()
                            .chain(insertions.into_iter())
                            .collect()
                    } else {
                        insertions
                    };
                    self.buffer_builtin_table_updates(updates);
                }
            }
            ControllerResponse::ComputeReplicaWriteFrontiers(updates) => {
                let mut builtin_updates = vec![];
                for (replica_id, new) in updates {
                    let m = match self
                        .transient_replica_metadata
                        .entry(replica_id)
                        .or_insert_with(|| Some(Default::default()))
                    {
                        // `None` is the tombstone for a removed replica
                        None => continue,
                        Some(md) => &mut md.write_frontiers,
                    };
                    let old = std::mem::replace(m, new.clone());
                    if old != new {
                        let retractions = self
                            .catalog()
                            .state()
                            .pack_replica_write_frontiers_updates(replica_id, &old, -1);
                        builtin_updates.extend(retractions.into_iter());

                        let insertions = self
                            .catalog()
                            .state()
                            .pack_replica_write_frontiers_updates(replica_id, &new, 1);
                        builtin_updates.extend(insertions.into_iter());
                    }
                }

                self.buffer_builtin_table_updates(builtin_updates);
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self, tx, session))]
    async fn message_create_source_statement_ready(
        &mut self,
        CreateSourceStatementReady {
            mut session,
            tx,
            result,
            params,
            depends_on,
            original_stmt,
            otel_ctx,
        }: CreateSourceStatementReady,
    ) {
        otel_ctx.attach_as_parent();

        // Ensure that all dependencies still exist after purification, as a
        // `DROP CONNECTION` may have sneaked in. If any have gone missing, we
        // repurify the original statement. This will either produce a nice
        // "unknown connector" error, or pick up a new connector that has
        // replaced the dropped connector.
        //
        // WARNING: If we support `ALTER CONNECTION`, we'll need to also check
        // for connectors that were altered while we were purifying.
        if !depends_on
            .iter()
            .all(|id| self.catalog().try_get_entry(id).is_some())
        {
            self.handle_execute_inner(original_stmt, params, session, tx)
                .await;
            return;
        }

        let (subsource_stmts, stmt) = match result {
            Ok(ok) => ok,
            Err(e) => return tx.send(Err(e), session),
        };

        let mut plans: Vec<CreateSourcePlans> = vec![];
        let mut id_allocation = BTreeMap::new();

        // First we'll allocate global ids for each subsource and plan them
        for (transient_id, subsource_stmt) in subsource_stmts {
            let depends_on = Vec::from_iter(mz_sql::names::visit_dependencies(&subsource_stmt));
            let source_id = match self.catalog_mut().allocate_user_id().await {
                Ok(id) => id,
                Err(e) => return tx.send(Err(e.into()), session),
            };
            let plan = match self.plan_statement(
                &mut session,
                Statement::CreateSubsource(subsource_stmt),
                &params,
            ) {
                Ok(Plan::CreateSource(plan)) => plan,
                Ok(_) => {
                    unreachable!("planning CREATE SUBSOURCE must result in a Plan::CreateSource")
                }
                Err(e) => return tx.send(Err(e), session),
            };
            id_allocation.insert(transient_id, source_id);
            plans.push((source_id, plan, depends_on).into());
        }

        // Then, we'll rewrite the source statement to point to the newly minted global ids and
        // plan it too
        let stmt = match mz_sql::names::resolve_transient_ids(&id_allocation, stmt) {
            Ok(ok) => ok,
            Err(e) => return tx.send(Err(e.into()), session),
        };
        let depends_on = Vec::from_iter(mz_sql::names::visit_dependencies(&stmt));
        let source_id = match self.catalog_mut().allocate_user_id().await {
            Ok(id) => id,
            Err(e) => return tx.send(Err(e.into()), session),
        };
        let plan = match self.plan_statement(&mut session, Statement::CreateSource(stmt), &params) {
            Ok(Plan::CreateSource(plan)) => plan,
            Ok(_) => {
                unreachable!("planning CREATE SOURCE must result in a Plan::CreateSource")
            }
            Err(e) => return tx.send(Err(e), session),
        };
        plans.push((source_id, plan, depends_on).into());

        // Finally, sequence all plans in one go
        self.sequence_plan(tx, session, Plan::CreateSources(plans), Vec::new())
            .await;
    }

    #[tracing::instrument(level = "debug", skip(self, session_and_tx))]
    async fn message_sink_connection_ready(
        &mut self,
        SinkConnectionReady {
            session_and_tx,
            id,
            oid,
            create_export_token,
            result,
        }: SinkConnectionReady,
    ) {
        match result {
            Ok(connection) => {
                // NOTE: we must not fail from here on out. We have a
                // connection, which means there is external state (like
                // a Kafka topic) that's been created on our behalf. If
                // we fail now, we'll leak that external state.
                if self.catalog().try_get_entry(&id).is_some() {
                    // TODO(benesch): this `expect` here is possibly scary, but
                    // no better solution presents itself. Possibly sinks should
                    // have an error bit, and an error here would set the error
                    // bit on the sink.
                    self.handle_sink_connection_ready(
                        id,
                        oid,
                        connection,
                        create_export_token,
                        session_and_tx.as_ref().map(|(ref session, _tx)| session),
                    )
                    .await
                    // XXX(chae): I really don't like this -- especially as we're now doing cross
                    // process calls to start a sink.
                    .expect("sinks should be validated by sequence_create_sink");
                } else {
                    // Another session dropped the sink while we were
                    // creating the connection. Report to the client that
                    // we created the sink, because from their
                    // perspective we did, as there is state (e.g. a
                    // Kafka topic) they need to clean up.
                }
                if let Some((session, tx)) = session_and_tx {
                    tx.send(Ok(ExecuteResponse::CreatedSink), session);
                }
            }
            Err(e) => {
                // Drop the placeholder sink if still present.
                if self.catalog().try_get_entry(&id).is_some() {
                    let ops = self
                        .catalog()
                        .item_dependents(id)
                        .into_iter()
                        .map(catalog::Op::DropObject)
                        .collect();
                    self.catalog_transact(
                        session_and_tx.as_ref().map(|(ref session, _tx)| session),
                        ops,
                    )
                    .await
                    .expect("deleting placeholder sink cannot fail");
                } else {
                    // Another session may have dropped the placeholder sink while we were
                    // attempting to create the connection, in which case we don't need to do
                    // anything.
                }
                // Drop the placeholder sink in the storage controller
                let () = self
                    .controller
                    .storage
                    .cancel_prepare_export(create_export_token);
                if let Some((session, tx)) = session_and_tx {
                    tx.send(Err(e), session);
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn message_write_lock_grant(
        &mut self,
        write_lock_guard: tokio::sync::OwnedMutexGuard<()>,
    ) {
        // It's possible to have more incoming write lock grants
        // than pending writes because of cancellations.
        if let Some(ready) = self.write_lock_wait_group.pop_front() {
            match ready {
                Deferred::Plan(mut ready) => {
                    ready.session.grant_write_lock(write_lock_guard);
                    // Write statements never need to track catalog
                    // dependencies.
                    let depends_on = vec![];
                    self.sequence_plan(ready.tx, ready.session, ready.plan, depends_on)
                        .await;
                }
                Deferred::GroupCommit => self.group_commit_initiate(Some(write_lock_guard)).await,
            }
        }
        // N.B. if no deferred plans, write lock is released by drop
        // here.
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn message_cluster_event(&mut self, event: ClusterEvent) {
        event!(Level::TRACE, event = format!("{:?}", event));

        // It is possible that we receive a status update for a replica that has
        // already been dropped from the catalog. Just ignore these events.
        let Some(cluster) = self.catalog().try_get_cluster(event.cluster_id) else {
            return;
        };
        let Some(replica) = cluster.replicas_by_id.get(&event.replica_id) else {
            return;
        };

        if event.status != replica.process_status[&event.process_id].status {
            let old_status = replica.status();

            self.catalog_transact(
                None,
                vec![catalog::Op::UpdateClusterReplicaStatus {
                    event: event.clone(),
                }],
            )
            .await
            .unwrap_or_terminate("updating cluster status cannot fail");

            let cluster = self.catalog().get_cluster(event.cluster_id);
            let replica = &cluster.replicas_by_id[&event.replica_id];
            let new_status = replica.status();

            if old_status != new_status {
                self.broadcast_notice(AdapterNotice::ClusterReplicaStatusChanged {
                    cluster: cluster.name.clone(),
                    replica: replica.name.clone(),
                    status: new_status,
                    time: event.time,
                });
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    /// Linearizes sending the results of a read transaction by,
    ///   1. Holding back any results that were executed at some point in the future, until the
    ///   containing timeline has advanced to that point in the future.
    ///   2. Confirming that we are still the current leader before sending results to the client.
    async fn message_linearize_reads(&mut self, pending_read_txns: Vec<PendingReadTxn>) {
        let mut shortest_wait = Duration::from_millis(0);
        let mut ready_txns = Vec::new();
        let mut deferred_txns = Vec::new();

        for read_txn in pending_read_txns {
            if let TimestampContext::TimelineTimestamp(timeline, timestamp) =
                read_txn.timestamp_context()
            {
                let timestamp_oracle = self.get_timestamp_oracle_mut(&timeline);
                let read_ts = timestamp_oracle.read_ts();
                if timestamp <= read_ts {
                    ready_txns.push(read_txn);
                } else {
                    let wait = Duration::from_millis(timestamp.saturating_sub(read_ts).into());
                    if wait < shortest_wait {
                        shortest_wait = wait;
                    }
                    deferred_txns.push(read_txn);
                }
            } else {
                ready_txns.push(read_txn);
            }
        }

        if !ready_txns.is_empty() {
            self.catalog_mut()
                .confirm_leadership()
                .await
                .unwrap_or_terminate("unable to confirm leadership");
            for ready_txn in ready_txns {
                ready_txn.finish();
            }
        }

        if !deferred_txns.is_empty() {
            // Cap wait time to 1s.
            let remaining_ms = std::cmp::min(shortest_wait, Duration::from_millis(1_000));
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "deferred_read_txns", async move {
                tokio::time::sleep(remaining_ms).await;
                // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
                let result = internal_cmd_tx.send(Message::LinearizeReads(deferred_txns));
                if let Err(e) = result {
                    warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                }
            });
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    /// Finishes sequencing a command that was waiting on a real time recency timestamp.
    async fn message_real_time_recency_timestamp(
        &mut self,
        conn_id: ConnectionId,
        real_time_recency_ts: mz_repr::Timestamp,
        replan: TransientPlan,
    ) {
        let real_time_recency_context =
            match self.pending_real_time_recency_timestamp.remove(&conn_id) {
                Some(real_time_recency_context) => real_time_recency_context,
                // Query was cancelled while waiting.
                None => return,
            };

        if replan.transient_revision != self.catalog().transient_revision() {
            // Revision change; re plan from the beginning.
            let (tx, session) = real_time_recency_context.take_tx_and_session();
            self.internal_cmd_tx
                .send(Message::Replan {
                    tx,
                    session,
                    replan,
                })
                .expect("coordinator must exist");
            return;
        }

        match real_time_recency_context {
            RealTimeRecencyContext::ExplainTimestamp {
                tx,
                session,
                format,
                cluster_id,
                optimized_plan,
                id_bundle,
            } => tx.send(
                self.sequence_explain_timestamp_finish(
                    &session,
                    format,
                    cluster_id,
                    optimized_plan,
                    id_bundle,
                    Some(real_time_recency_ts),
                ),
                session,
            ),
            RealTimeRecencyContext::Peek {
                tx,
                finishing,
                copy_to,
                dataflow,
                session,
                cluster_id,
                when,
                target_replica,
                view_id,
                index_id,
                timeline_context,
                source_ids,
                in_immediate_multi_stmt_txn,
                key,
                typ,
            } => {
                self.sequence_peek_stage(
                    tx,
                    session,
                    PeekStage::Finish(PeekStageFinish {
                        replan,
                        finishing,
                        copy_to,
                        dataflow,
                        cluster_id,
                        when,
                        target_replica,
                        view_id,
                        index_id,
                        timeline_context,
                        source_ids,
                        in_immediate_multi_stmt_txn,
                        real_time_recency_ts: Some(real_time_recency_ts),
                        key,
                        typ,
                    }),
                )
                .await;
            }
        }
    }
}
