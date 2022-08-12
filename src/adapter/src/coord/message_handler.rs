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

use std::collections::HashMap;

use chrono::DurationRound;
use tracing::{event, warn, Level};

use mz_controller::{ComputeInstanceEvent, ControllerResponse};
use mz_ore::task;
use mz_persist_client::ShardId;
use mz_sql::ast::Statement;
use mz_sql::plan::{Plan, SendDiffsPlan};
use mz_stash::Append;

use crate::catalog::{self};
use crate::command::{Command, ExecuteResponse};
use crate::coord::appends::{BuiltinTableUpdateSource, Deferred};

use crate::coord::{
    Coordinator, CreateSourceStatementReady, Message, PendingTxn, ReplicaMetadata, SendDiffs,
    SinkConnectionReady,
};

impl<S: Append + 'static> Coordinator<S> {
    pub(crate) async fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::Command(cmd) => self.message_command(cmd).await,
            Message::ControllerReady => {
                if let Some(m) = self.controller.process().await.unwrap() {
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
            Message::SendDiffs(diffs) => self.message_send_diffs(diffs),
            Message::GroupCommitInitiate => {
                self.try_group_commit().await;
            }
            Message::GroupCommitApply(timestamp, responses, write_lock_guard) => {
                self.group_commit_apply(timestamp, responses, false, write_lock_guard)
                    .await;
            }
            Message::ComputeInstanceStatus(status) => {
                self.message_compute_instance_status(status).await
            }
            // Processing this message DOES NOT send a response to the client;
            // in any situation where you use it, you must also have a code
            // path that responds to the client (e.g. reporting an error).
            Message::RemovePendingPeeks { conn_id } => {
                self.cancel_pending_peeks(conn_id).await;
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
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn storage_usage_fetch(&self) {
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let client = self.storage_usage_client.clone();
        task::spawn(|| "storage_usage_fetch", async move {
            let shard_sizes = client.shard_sizes().await;
            // It is not an error for shard sizes to become ready after `internal_cmd_rx`
            // is dropped.
            let result = internal_cmd_tx.send(Message::StorageUsageUpdate(shard_sizes));
            if let Err(e) = result {
                warn!("internal_cmd_rx dropped before we could send: {:?}", e);
            }
        });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn storage_usage_update(&mut self, shard_sizes: HashMap<Option<ShardId>, u64>) {
        let object_id = None;

        let mut unk_storage = 0;
        let mut known_storage = 0;
        for (key, val) in shard_sizes {
            match key {
                Some(_) => known_storage += val,
                None => unk_storage += val,
            }
        }
        // TODO(jpepin): What, if anything, do we want to do with orphaned storage?
        if unk_storage > 0 {
            tracing::debug!("Found {} bytes of orphaned storage", unk_storage);
        }
        if let Err(err) = self
            .catalog_transact(
                None,
                vec![catalog::Op::UpdateStorageMetrics {
                    object_id,
                    size_bytes: known_storage,
                }],
                |_| Ok(()),
            )
            .await
        {
            tracing::warn!("Failed to update storage metrics: {:?}", err);
        }
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
            ControllerResponse::TailResponse(sink_id, response) => {
                // We use an `if let` here because the peek could have been canceled already.
                // We can also potentially receive multiple `Complete` responses, followed by
                // a `Dropped` response.
                if let Some(pending_tail) = self.pending_tails.get_mut(&sink_id) {
                    let remove = pending_tail.process_response(response);
                    if remove {
                        self.pending_tails.remove(&sink_id);
                    }
                }
            }
            ControllerResponse::ComputeReplicaHeartbeat(replica_id, when) => {
                let replica_status_granularity = chrono::Duration::seconds(60);
                let when_coarsened = when
                    .duration_trunc(replica_status_granularity)
                    .expect("Time coarsening should not fail");
                let new = ReplicaMetadata {
                    last_heartbeat: when_coarsened,
                };
                let old = match self
                    .transient_replica_metadata
                    .insert(replica_id, Some(new.clone()))
                {
                    None => None,
                    // `None` is the tombstone for a removed replica
                    Some(None) => return,
                    Some(Some(md)) => Some(md),
                };

                if old.as_ref() != Some(&new) {
                    let retraction = old.map(|old| {
                        self.catalog
                            .state()
                            .pack_replica_heartbeat_update(replica_id, old, -1)
                    });
                    let insertion = self
                        .catalog
                        .state()
                        .pack_replica_heartbeat_update(replica_id, new, 1);
                    let updates = if let Some(retraction) = retraction {
                        vec![retraction, insertion]
                    } else {
                        vec![insertion]
                    };
                    self.send_builtin_table_updates(updates, BuiltinTableUpdateSource::Background)
                        .await;
                }
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

        let stmt = match result {
            Ok(stmt) => stmt,
            Err(e) => return tx.send(Err(e), session),
        };

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
            .all(|id| self.catalog.try_get_entry(id).is_some())
        {
            self.handle_execute_inner(original_stmt, params, session, tx)
                .await;
            return;
        }

        let plan = match self
            .plan_statement(&mut session, Statement::CreateSource(stmt), &params)
            .await
        {
            Ok(Plan::CreateSource(plan)) => plan,
            Ok(_) => unreachable!("planning CREATE SOURCE must result in a Plan::CreateSource"),
            Err(e) => return tx.send(Err(e), session),
        };

        let result = self
            .sequence_create_source(&mut session, plan, depends_on)
            .await;
        tx.send(result, session);
    }

    #[tracing::instrument(level = "debug", skip(self, tx, session))]
    async fn message_sink_connection_ready(
        &mut self,
        SinkConnectionReady {
            session,
            tx,
            id,
            oid,
            result,
        }: SinkConnectionReady,
    ) {
        match result {
            Ok(connection) => {
                // NOTE: we must not fail from here on out. We have a
                // connection, which means there is external state (like
                // a Kafka topic) that's been created on our behalf. If
                // we fail now, we'll leak that external state.
                if self.catalog.try_get_entry(&id).is_some() {
                    // TODO(benesch): this `expect` here is possibly scary, but
                    // no better solution presents itself. Possibly sinks should
                    // have an error bit, and an error here would set the error
                    // bit on the sink.
                    self.handle_sink_connection_ready(id, oid, connection, Some(&session))
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
                tx.send(Ok(ExecuteResponse::CreatedSink { existed: false }), session);
            }
            Err(e) => {
                // Drop the placeholder sink if still present.
                if self.catalog.try_get_entry(&id).is_some() {
                    self.catalog_transact(Some(&session), vec![catalog::Op::DropItem(id)], |_| {
                        Ok(())
                    })
                    .await
                    .expect("deleting placeholder sink cannot fail");
                } else {
                    // Another session may have dropped the placeholder sink while we were
                    // attempting to create the connection, in which case we don't need to do
                    // anything.
                }
                tx.send(Err(e), session);
            }
        }
    }

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
                Deferred::GroupCommit => self.group_commit_initiate().await,
            }
        }
        // N.B. if no deferred plans, write lock is released by drop
        // here.
    }

    #[tracing::instrument(level = "debug", skip_all, fields(id, kind))]
    fn message_send_diffs(
        &mut self,
        SendDiffs {
            mut session,
            tx,
            id,
            diffs,
            kind,
            returning,
        }: SendDiffs,
    ) {
        event!(Level::TRACE, diffs = format!("{:?}", diffs));
        match diffs {
            Ok(diffs) => {
                tx.send(
                    self.sequence_send_diffs(
                        &mut session,
                        SendDiffsPlan {
                            id,
                            updates: diffs,
                            kind,
                            returning,
                        },
                    ),
                    session,
                );
            }
            Err(e) => {
                tx.send(Err(e), session);
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn message_compute_instance_status(&mut self, event: ComputeInstanceEvent) {
        event!(Level::TRACE, event = format!("{:?}", event));
        self.catalog_transact(
            None,
            vec![catalog::Op::UpdateComputeInstanceStatus { event }],
            |_| Ok(()),
        )
        .await
        .expect("updating compute instance status cannot fail");
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn message_linearize_reads(&mut self, pending_read_txns: Vec<PendingTxn>) {
        self.catalog
            .confirm_leadership()
            .await
            .expect("unable to confirm leadership");
        for PendingTxn {
            client_transmitter,
            response,
            mut session,
            action,
        } in pending_read_txns
        {
            session.vars_mut().end_transaction(action);
            client_transmitter.send(response, session);
        }
    }
}
