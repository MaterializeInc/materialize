// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic and types for all appends executed by the [`Coordinator`].
//!
//! Runtime table appends, registrations, and forgets are serialized by the
//! [`GroupCommitter`]. FIFO order is required so appends cannot overtake registration or
//! forgetting.
//!
//! For each command, the committer:
//!
//! 1. allocates a write timestamp from the shared oracle,
//! 2. advances the catalog upper to [`WriteTimestamp::advance_to`],
//! 3. writes the txns shard, retrying `InvalidUppers` from step 1, and
//! 4. applies the successful write to the oracle.
//!
//! Step 2 keeps the catalog readable at the oracle read timestamp. It also enforces fencing for
//! a post-fence retry: the fresh oracle timestamp's advance frontier is above the stale process's
//! cached catalog upper, so the advance reaches Persist and observes the fence before another txns
//! write.
//!
//! On `environmentd` bootstrap in read/write mode, system-table snapshots cannot complete until a
//! txns-shard write has advanced the table uppers. A stale write either linearizes before this
//! barrier and is observed by the snapshot, or conflicts and follows the fenced retry path above.
//! This relies on generations sharing the oracle.
//!
//! Work that requires coordinator state is returned via [`Message::GroupCommitApplied`].

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use derivative::Derivative;
use futures::future::{BoxFuture, FutureExt};
use mz_adapter_types::connection::ConnectionId;
use mz_adapter_types::dyncfgs::GROUP_COMMIT_MAX_ATTEMPTS;
use mz_catalog::builtin::{BuiltinTable, MZ_SESSIONS};
use mz_dyncfg::{ConfigSet, ConfigValHandle};
use mz_expr::CollectionPlan;
use mz_ore::assert_none;
use mz_ore::halt;
use mz_ore::instrument;
use mz_ore::now::NowFn;
use mz_ore::task;
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{ExplainPlanPlan, ExplainTimestampPlan, Explainee, ExplaineeStatement, Plan};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_client::client::TableData;
use mz_storage_client::controller::{TableRegistration, TableWriteHandle};
use mz_storage_types::controller::StorageError;
use mz_timestamp_oracle::{TimestampOracle, WriteTimestamp};
use smallvec::SmallVec;
use tokio::sync::{Notify, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tracing::{Instrument, Span, info, warn};

use crate::catalog::{BuiltinTableUpdate, Catalog, CatalogUpperHandle};
use crate::coord::{Coordinator, Message, PendingTxn, PlanValidity};
use crate::metrics::Metrics;
use crate::session::{GroupCommitWriteLocks, Session, WriteLocks};
use crate::statement_logging::StatementLoggingId;
use crate::util::{CompletedClientTransmitter, ResultExt};
use crate::{AdapterError, ExecuteContext};

/// Tables that we emit updates for when starting a new session.
pub(crate) static REQUIRED_BUILTIN_TABLES: &[&LazyLock<BuiltinTable>] = &[&MZ_SESSIONS];

/// An operation that was deferred waiting on a resource to be available.
///
/// For example when inserting into a table we defer on acquiring [`WriteLocks`].
#[derive(Debug)]
pub enum DeferredOp {
    /// A plan, e.g. ReadThenWrite, that needs locks before sequencing.
    Plan(DeferredPlan),
    /// Inserts into a collection.
    Write(DeferredWrite),
}

impl DeferredOp {
    /// Certain operations, e.g. "blind writes"/`INSERT` statements, can be optimistically retried
    /// because we can share a write lock between multiple operations. In this case we wait to
    /// acquire the locks until [`stage_group_commit`], where writes are grouped by collection and
    /// committed at a single timestamp.
    ///
    /// Other operations, e.g. read-then-write plans/`UPDATE` statements, must uniquely hold their
    /// write locks and thus we should acquire the locks in [`try_deferred`] to prevent multiple
    /// queued plans attempting to get retried at the same time, when we know only one can proceed.
    ///
    /// [`try_deferred`]: crate::coord::Coordinator::try_deferred
    /// [`stage_group_commit`]: crate::coord::Coordinator::stage_group_commit
    pub(crate) fn can_be_optimistically_retried(&self) -> bool {
        match self {
            DeferredOp::Plan(_) => false,
            DeferredOp::Write(_) => true,
        }
    }

    /// Returns an Iterator of all the required locks for current operation.
    pub fn required_locks(&self) -> impl Iterator<Item = CatalogItemId> + '_ {
        match self {
            DeferredOp::Plan(plan) => {
                let iter = plan.requires_locks.iter().copied();
                itertools::Either::Left(iter)
            }
            DeferredOp::Write(write) => {
                let iter = write.writes.keys().copied();
                itertools::Either::Right(iter)
            }
        }
    }

    /// Returns the [`ConnectionId`] associated with this deferred op.
    pub fn conn_id(&self) -> &ConnectionId {
        match self {
            DeferredOp::Plan(plan) => plan.ctx.session().conn_id(),
            DeferredOp::Write(write) => write.pending_txn.ctx.session().conn_id(),
        }
    }

    /// Consumes the [`DeferredOp`], returning the inner [`ExecuteContext`].
    pub fn into_ctx(self) -> ExecuteContext {
        match self {
            DeferredOp::Plan(plan) => plan.ctx,
            DeferredOp::Write(write) => write.pending_txn.ctx,
        }
    }
}

/// Describes a plan that is awaiting [`WriteLocks`].
#[derive(Derivative)]
#[derivative(Debug)]
pub struct DeferredPlan {
    #[derivative(Debug = "ignore")]
    pub ctx: ExecuteContext,
    pub plan: Plan,
    pub validity: PlanValidity,
    pub requires_locks: BTreeSet<CatalogItemId>,
    pub resolved_ids: ResolvedIds,
    pub sql_impl_resolved_ids: ResolvedIds,
}

#[derive(Debug)]
pub struct DeferredWrite {
    pub span: Span,
    pub writes: BTreeMap<CatalogItemId, SmallVec<[TableData; 1]>>,
    pub pending_txn: PendingTxn,
}

/// Describes what action triggered an update to a builtin table.
#[derive(Debug)]
pub(crate) enum BuiltinTableUpdateSource {
    /// Internal update, notify the caller when it's complete.
    Internal(oneshot::Sender<()>),
    /// Update was triggered by some background process, such as periodic heartbeats from COMPUTE.
    Background(oneshot::Sender<()>),
}

/// Result of a write submitted by frontend sequencing.
#[derive(Debug, Clone)]
pub enum WriteResult {
    /// The write committed at this timestamp.
    Success { timestamp: Timestamp },
    /// The requested timestamp was no longer eligible.
    TimestampPassed {
        target_timestamp: Timestamp,
        next_eligible_timestamp: Timestamp,
    },
    /// The write was canceled before it entered the committer.
    Canceled,
    /// The coordinator cannot accept writes.
    ReadOnly,
    /// The target table was dropped or changed after planning.
    TargetChanged,
    /// The committer shut down with the write's outcome unknown.
    Indeterminate,
}

/// Delivers an internal write result, including on task shutdown.
#[derive(Debug)]
pub struct InternalWriteResponder {
    tx: Option<oneshot::Sender<WriteResult>>,
    expected_target_global_id: GlobalId,
}

impl InternalWriteResponder {
    pub(crate) fn new(
        tx: oneshot::Sender<WriteResult>,
        expected_target_global_id: GlobalId,
    ) -> Self {
        Self {
            tx: Some(tx),
            expected_target_global_id,
        }
    }

    fn expected_target_global_id(&self) -> GlobalId {
        self.expected_target_global_id
    }

    pub(crate) fn send(mut self, result: WriteResult) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(result);
        }
    }
}

impl Drop for InternalWriteResponder {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(WriteResult::Indeterminate);
        }
    }
}

/// Where to deliver the result of a [`PendingWriteTxn::User`] write.
#[derive(Debug)]
pub(crate) enum UserWriteResponder {
    /// Session-bound write. The coordinator retires the session's
    /// `ExecuteContext` once the write commits.
    Session(PendingTxn),
    /// Frontend-sequenced blind write.
    Internal {
        conn_id: ConnectionId,
        result: InternalWriteResponder,
    },
}

impl UserWriteResponder {
    pub(crate) fn conn_id(&self) -> &ConnectionId {
        match self {
            UserWriteResponder::Session(pending) => pending.ctx.session().conn_id(),
            UserWriteResponder::Internal { conn_id, .. } => conn_id,
        }
    }
}

/// A pending write transaction that will be committing during the next group commit.
#[derive(Debug)]
pub(crate) enum PendingWriteTxn {
    /// Write to a user table. The write timestamp is picked by the oracle
    /// during group commit. The write lock is either handed off from the
    /// submitting session (via `write_locks: Some(..)`) or acquired during
    /// group commit (`write_locks: None`).
    User {
        span: Span,
        /// List of all write operations within the transaction.
        writes: BTreeMap<CatalogItemId, SmallVec<[TableData; 1]>>,
        /// If they exist, should contain locks for each [`CatalogItemId`] in `writes`.
        write_locks: Option<WriteLocks>,
        /// Where to deliver the result once the write commits.
        responder: UserWriteResponder,
    },
    /// Write to a system table.
    System {
        updates: Vec<BuiltinTableUpdate>,
        source: BuiltinTableUpdateSource,
    },
}

impl PendingWriteTxn {
    fn is_internal_system(&self) -> bool {
        match self {
            PendingWriteTxn::System {
                source: BuiltinTableUpdateSource::Internal(_),
                ..
            } => true,
            _ => false,
        }
    }
}

pub(crate) enum TableWriteCmd {
    GroupCommit(GroupCommitRequest),
    TimestampedWrite(TimestampedWriteRequest),
    Register {
        tables: Vec<TableRegistration>,
        result: oneshot::Sender<Timestamp>,
    },
    Forget {
        ids: Vec<GlobalId>,
        result: oneshot::Sender<Timestamp>,
    },
}

/// An OCC write whose diffs are valid only at `target_timestamp`.
pub(crate) struct TimestampedWriteRequest {
    pub(crate) appends: Vec<(GlobalId, Vec<TableData>)>,
    pub(crate) target_timestamp: Timestamp,
    pub(crate) result: InternalWriteResponder,
    pub(crate) span: Span,
}

/// A group commit staged on the coordinator loop for the [`GroupCommitter`].
pub(crate) struct GroupCommitRequest {
    /// Appends resolved to their latest [`GlobalId`]. Empty for a keepalive.
    appends: Vec<(GlobalId, Vec<TableData>)>,
    responses: Vec<CompletedClientTransmitter>,
    statement_logging_ids: Vec<StatementLoggingId>,
    notifies: Vec<oneshot::Sender<()>>,
    internal_results: Vec<InternalWriteResponder>,
    write_locks: GroupCommitWriteLocks,
    /// In-progress permits held until the commit is applied.
    permits: Vec<GroupCommitPermit>,
    contains_internal_system_write: bool,
    span: Span,
}

impl GroupCommitRequest {
    fn merge(&mut self, other: GroupCommitRequest) {
        let GroupCommitRequest {
            appends,
            responses,
            statement_logging_ids,
            notifies,
            internal_results,
            write_locks,
            permits,
            contains_internal_system_write,
            span: _,
        } = other;
        self.appends.extend(appends);
        self.responses.extend(responses);
        self.statement_logging_ids.extend(statement_logging_ids);
        self.notifies.extend(notifies);
        self.internal_results.extend(internal_results);
        self.write_locks.extend(write_locks);
        self.permits.extend(permits);
        self.contains_internal_system_write |= contains_internal_system_write;
    }
}

/// Serializes runtime txns-shard writes off the coordinator loop.
///
/// Dropped group-commit requests retire their clients through [`ExecuteContext`]. Dropped
/// registration or forget replies cause their coordinator waiters to halt.
pub(crate) struct GroupCommitter {
    rx: mpsc::UnboundedReceiver<TableWriteCmd>,
    oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync>,
    table_write_handle: Arc<dyn TableWriteHandle>,
    catalog_upper: CatalogUpperHandle,
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    now: NowFn,
    metrics: Metrics,
    max_attempts: ConfigValHandle<usize>,
}

impl GroupCommitter {
    async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            // `commit` may pull a non-mergeable command off the queue while it waits in the
            // throttle. Process it before receiving anew, to preserve queue order.
            let mut next = Some(cmd);
            while let Some(cmd) = next.take() {
                match cmd {
                    TableWriteCmd::GroupCommit(request) => {
                        let span = request.span.clone();
                        match self.commit(request).instrument(span).await {
                            ControlFlow::Continue(deferred) => next = deferred,
                            ControlFlow::Break(()) => return,
                        }
                    }
                    TableWriteCmd::TimestampedWrite(request) => {
                        let span = request.span.clone();
                        if !self.commit_timestamped(request).instrument(span).await {
                            return;
                        }
                    }
                    TableWriteCmd::Register { tables, result } => {
                        let Some(write_ts) = self
                            .write_to_txns(None, |ts, _advance_to| {
                                self.table_write_handle.register(ts, tables.clone())
                            })
                            .await
                        else {
                            return;
                        };
                        let _ = result.send(write_ts.timestamp);
                    }
                    TableWriteCmd::Forget { ids, result } => {
                        let Some(write_ts) = self
                            .write_to_txns(None, |ts, _advance_to| {
                                self.table_write_handle.forget(ts, ids.clone())
                            })
                            .await
                        else {
                            return;
                        };
                        let _ = result.send(write_ts.timestamp);
                    }
                }
            }
        }
    }

    /// Attempts an OCC write exactly once at its requested timestamp.
    ///
    /// Unlike blind writes, an `InvalidUppers` conflict must return to the
    /// subscribe loop. Retrying the same diffs at a fresh timestamp would apply
    /// a mutation to state it was not computed from.
    async fn commit_timestamped(&self, request: TimestampedWriteRequest) -> bool {
        let TimestampedWriteRequest {
            appends,
            target_timestamp,
            result,
            span: _,
        } = request;

        let oracle_write_ts = self.oracle.peek_write_ts().await;
        if target_timestamp <= oracle_write_ts {
            result.send(WriteResult::TimestampPassed {
                target_timestamp,
                next_eligible_timestamp: oracle_write_ts.step_forward(),
            });
            return true;
        }

        let advance_to = target_timestamp.step_forward();
        let catalog_upper_start = Instant::now();
        self.catalog_upper
            .advance_upper(advance_to)
            .await
            .unwrap_or_terminate("unable to advance catalog upper");
        self.metrics
            .group_commit_catalog_upper_seconds
            .observe(catalog_upper_start.elapsed().as_secs_f64());

        let append_start = Instant::now();
        let append_result = self
            .table_write_handle
            .append(target_timestamp, advance_to, appends)
            .await;
        self.metrics
            .append_table_duration_seconds
            .observe(append_start.elapsed().as_secs_f64());

        match append_result {
            Ok(Ok(())) => {}
            Ok(Err(StorageError::InvalidUppers(_))) => {
                result.send(WriteResult::TimestampPassed {
                    target_timestamp,
                    next_eligible_timestamp: advance_to,
                });
                return true;
            }
            Ok(Err(other)) => {
                Err::<(), _>(other).unwrap_or_terminate("cannot fail to write to txns shard");
                unreachable!("unwrap_or_terminate does not return on Err");
            }
            Err(_recv) => {
                warn!("table write worker gone with a timestamped write outstanding");
                return false;
            }
        }

        let now: Timestamp = (self.now)().into();
        crate::coord::timeline::check_runaway_write_ts(&now, target_timestamp);
        self.oracle.apply_write(target_timestamp).await;

        if self
            .internal_cmd_tx
            .send(Message::GroupCommitApplied {
                responses: Vec::new(),
                statement_logging_ids: Vec::new(),
                internal_results: vec![result],
                write_ts: target_timestamp,
            })
            .is_err()
        {
            warn!("coordinator shut down before a timestamped write could be finalized");
        }
        true
    }

    /// Writes at a fresh oracle timestamp and applies a successful write to the oracle.
    ///
    /// Returns `None` when the table write worker shuts down.
    async fn write_to_txns(
        &self,
        op_duration_metric: Option<&prometheus::Histogram>,
        mut op: impl FnMut(Timestamp, Timestamp) -> oneshot::Receiver<Result<(), StorageError>>,
    ) -> Option<WriteTimestamp> {
        // Persistent conflicts indicate an unexpected writer. Halt instead of spinning forever.
        let mut attempt = 0;
        let write_ts = loop {
            let max_attempts = self.max_attempts.get().max(1);
            if attempt >= max_attempts {
                halt!(
                    "txns-shard write reached attempt limit {max_attempts} after {attempt} conflicts, rebuilding"
                );
            }
            attempt += 1;
            let write_ts = self.oracle.write_ts().await;

            // A post-fence retry has an advance frontier above this handle's stale upper, so this
            // reaches Persist and observes the fence.
            let catalog_upper_start = Instant::now();
            self.catalog_upper
                .advance_upper(write_ts.advance_to)
                .await
                .unwrap_or_terminate("unable to advance catalog upper");
            self.metrics
                .group_commit_catalog_upper_seconds
                .observe(catalog_upper_start.elapsed().as_secs_f64());

            let op_start = Instant::now();
            let op_res = op(write_ts.timestamp, write_ts.advance_to).await;
            if let Some(metric) = op_duration_metric {
                metric.observe(op_start.elapsed().as_secs_f64());
            }

            match op_res {
                Ok(Ok(())) => break write_ts,
                Ok(Err(StorageError::InvalidUppers(_))) => {
                    warn!(
                        write_ts = %write_ts.timestamp,
                        attempt,
                        "txns-shard write conflicted with another writer, retrying at a fresh timestamp"
                    );
                    continue;
                }
                Ok(Err(other)) => {
                    Err::<(), _>(other).unwrap_or_terminate("cannot fail to write to txns shard");
                    unreachable!("unwrap_or_terminate does not return on Err");
                }
                Err(_recv) => {
                    // The outcome is indeterminate. Stop before processing more writes.
                    warn!("table write worker gone (process shutting down), winding down");
                    return None;
                }
            }
        };

        let now: Timestamp = (self.now)().into();
        crate::coord::timeline::check_runaway_write_ts(&now, write_ts.timestamp);

        self.oracle.apply_write(write_ts.timestamp).await;

        Some(write_ts)
    }

    /// Applies a staged group commit.
    ///
    /// Group commits queued during throttling are merged until a registration or forget command
    /// preserves the queue boundary. `Break` means the table worker shut down.
    async fn commit(
        &mut self,
        mut request: GroupCommitRequest,
    ) -> ControlFlow<(), Option<TableWriteCmd>> {
        let mut deferred_cmd = None;
        // Once the channel is closed, `recv` resolves immediately with `None`. Disable that
        // select branch then, so the throttle sleep still runs instead of busy-looping.
        let mut rx_closed = false;

        // Throttle: keep the global write timeline from running ahead of the wall clock. The
        // peek and the sleep happen here in the committer task, so a slow oracle backend does
        // not stall the coordinator loop.
        loop {
            while deferred_cmd.is_none() {
                match self.rx.try_recv() {
                    Ok(TableWriteCmd::GroupCommit(other)) => request.merge(other),
                    Ok(other) => deferred_cmd = Some(other),
                    Err(_) => break,
                }
            }

            // Internal writes bypass the throttle for mocked clocks. A queued DDL registration or
            // forget bypasses it because DDL was not previously throttled and blocks the loop.
            if request.contains_internal_system_write || deferred_cmd.is_some() {
                break;
            }
            let ts = self.oracle.peek_write_ts().await;
            let now: Timestamp = (self.now)().into();
            if ts <= now {
                break;
            }
            // A fixed one-second cap bounds clock-regression stalls. Queue wakeups do not extend
            // this deadline.
            let remaining_ms = std::cmp::min(ts.saturating_sub(now), Timestamp::from(1_000u64));
            let sleep = tokio::time::sleep(Duration::from_millis(remaining_ms.into()));
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    _ = &mut sleep => break,
                    cmd = self.rx.recv(), if deferred_cmd.is_none() && !rx_closed => match cmd {
                        Some(TableWriteCmd::GroupCommit(other)) => request.merge(other),
                        Some(other) => deferred_cmd = Some(other),
                        None => rx_closed = true,
                    },
                }
                if request.contains_internal_system_write || deferred_cmd.is_some() {
                    break;
                }
            }
        }

        let GroupCommitRequest {
            appends,
            responses,
            statement_logging_ids,
            notifies,
            internal_results,
            write_locks,
            permits,
            contains_internal_system_write: _,
            span: _,
        } = request;

        let append_metric = self.metrics.append_table_duration_seconds.clone();
        let Some(write_ts) = self
            .write_to_txns(Some(&append_metric), |ts, advance_to| {
                self.table_write_handle
                    .append(ts, advance_to, appends.clone())
            })
            .await
        else {
            // Dropping the batch retires its clients through `ExecuteContext`.
            return ControlFlow::Break(());
        };
        let timestamp = write_ts.timestamp;

        let modified_tables: Vec<_> = appends
            .iter()
            .filter_map(|(id, updates)| {
                (id.is_user() && !updates.iter().all(|u| u.is_empty())).then_some(id)
            })
            .collect();
        if !modified_tables.is_empty() {
            info!(
                "Appending to tables, {modified_tables:?}, at {timestamp}, advancing to {}",
                write_ts.advance_to
            );
        }

        // Hold permits and locks until `apply_write` completes. Otherwise another write could
        // proceed while this timestamp is not yet readable.
        drop(permits);
        drop(write_locks);

        for notify in notifies {
            let _ = notify.send(());
        }

        // The coordinator records timestamps before retiring responses. The applied write
        // timestamp is also a valid frontier for local read holds.
        if self
            .internal_cmd_tx
            .send(Message::GroupCommitApplied {
                responses,
                statement_logging_ids,
                internal_results,
                write_ts: timestamp,
            })
            .is_err()
        {
            warn!("coordinator shut down before a group commit could be finalized");
        }

        ControlFlow::Continue(deferred_cmd)
    }
}

pub(crate) fn spawn_group_committer(
    rx: mpsc::UnboundedReceiver<TableWriteCmd>,
    oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync>,
    table_write_handle: Arc<dyn TableWriteHandle>,
    catalog_upper: CatalogUpperHandle,
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    now: NowFn,
    metrics: Metrics,
    dyncfgs: &ConfigSet,
) {
    let committer = GroupCommitter {
        rx,
        oracle,
        table_write_handle,
        catalog_upper,
        internal_cmd_tx,
        now,
        metrics,
        max_attempts: GROUP_COMMIT_MAX_ATTEMPTS.handle(dyncfgs),
    };
    task::spawn(|| "group_committer", committer.run());
}

impl Coordinator {
    /// Send a message to the Coordinate to start a group commit.
    pub(crate) fn trigger_group_commit(&mut self) {
        self.group_commit_tx.notify();
        // Avoid excessive `Message::GroupCommitInitiate` by resetting the periodic table
        // advancement. The group commit triggered by the message above will already advance all
        // tables.
        self.advance_timelines_interval.reset();
    }

    /// Tries to execute a previously [`DeferredOp`] that requires write locks.
    ///
    /// If we can't acquire all of the write locks then we'll defer the plan again and wait for
    /// the necessary locks to become available.
    pub(crate) async fn try_deferred(
        &mut self,
        conn_id: ConnectionId,
        acquired_lock: Option<(CatalogItemId, tokio::sync::OwnedMutexGuard<()>)>,
    ) {
        // Try getting the deferred op, it may have already been canceled.
        let Some(op) = self.deferred_write_ops.remove(&conn_id) else {
            tracing::warn!(%conn_id, "no deferred op found, it must have been canceled?");
            return;
        };
        tracing::info!(%conn_id, "trying deferred plan");

        // If we pre-acquired a lock, try to acquire the rest.
        let write_locks = match acquired_lock {
            Some((acquired_gid, acquired_lock)) => {
                let mut write_locks = WriteLocks::builder(op.required_locks());

                // Insert the one lock we already acquired into the our builder.
                write_locks.insert_lock(acquired_gid, acquired_lock);

                // Acquire the rest of our locks, filtering out the one we already have.
                for gid in op.required_locks().filter(|gid| *gid != acquired_gid) {
                    if let Some(lock) = self.try_grant_object_write_lock(gid) {
                        write_locks.insert_lock(gid, lock);
                    }
                }

                // If we failed to acquire any locks, spawn a task that waits for them to become available.
                let locks = match write_locks.all_or_nothing(op.conn_id()) {
                    Ok(locks) => locks,
                    Err(failed_to_acquire) => {
                        let acquire_future = self
                            .grant_object_write_lock(failed_to_acquire)
                            .map(Option::Some);
                        self.defer_op(acquire_future, op);
                        return;
                    }
                };

                Some(locks)
            }
            None => None,
        };

        match op {
            DeferredOp::Plan(mut deferred) => {
                if let Err(e) = deferred.validity.check(self.catalog()) {
                    deferred.ctx.retire(Err(e))
                } else {
                    // If we pre-acquired our locks, grant them to the session.
                    if let Some(locks) = write_locks {
                        let conn_id = deferred.ctx.session().conn_id().clone();
                        if let Err(existing) =
                            deferred.ctx.session_mut().try_grant_write_locks(locks)
                        {
                            tracing::error!(
                                %conn_id,
                                ?existing,
                                "session already write locks granted?",
                            );
                            return deferred.ctx.retire(Err(AdapterError::WrongSetOfLocks));
                        }
                    };

                    // Note: This plan is not guaranteed to run, it may get deferred again.
                    self.sequence_plan(
                        deferred.ctx,
                        deferred.plan,
                        deferred.resolved_ids,
                        deferred.sql_impl_resolved_ids,
                    )
                    .await;
                }
            }
            DeferredOp::Write(DeferredWrite {
                span,
                writes,
                pending_txn,
            }) => {
                self.submit_write(PendingWriteTxn::User {
                    span,
                    writes,
                    write_locks,
                    responder: UserWriteResponder::Session(pending_txn),
                });
            }
        }
    }

    /// Stages pending writes for the group committer.
    ///
    /// Writes blocked on locks are deferred. Included writes share one timestamp.
    #[instrument(name = "coord::stage_group_commit")]
    pub(crate) fn stage_group_commit(&mut self, permit: Option<GroupCommitPermit>) {
        let mut validated_writes = Vec::new();
        let mut deferred_writes = Vec::new();
        let mut group_write_locks = GroupCommitWriteLocks::default();

        // TODO(parkmycar): Refactor away this allocation. Currently `drain(..)` requires holding
        // a mutable borrow on the Coordinator and so does trying to grant a write lock.
        let pending_writes: Vec<_> = self.pending_writes.drain(..).collect();

        // Validate, merge, and possibly acquire write locks for as many pending writes as possible.
        for pending_write in pending_writes {
            match pending_write {
                PendingWriteTxn::System { .. } => validated_writes.push(pending_write),
                PendingWriteTxn::User {
                    span,
                    write_locks: Some(write_locks),
                    writes,
                    responder,
                } => match write_locks.validate(writes.keys().copied()) {
                    Ok(validated_locks) => {
                        group_write_locks.merge(validated_locks);
                        validated_writes.push(PendingWriteTxn::User {
                            span,
                            writes,
                            write_locks: None,
                            responder,
                        });
                    }
                    Err(missing) => {
                        let writes: Vec<_> = writes.keys().collect();
                        panic!(
                            "got to group commit with partial set of locks!\nmissing: {:?}, writes: {:?}, conn_id: {}",
                            missing,
                            writes,
                            responder.conn_id(),
                        );
                    }
                },
                PendingWriteTxn::User {
                    span,
                    writes,
                    write_locks: None,
                    responder,
                } => {
                    let missing = group_write_locks.missing_locks(writes.keys().copied());
                    if missing.is_empty() {
                        validated_writes.push(PendingWriteTxn::User {
                            span,
                            writes,
                            write_locks: None,
                            responder,
                        });
                        continue;
                    }

                    match responder {
                        UserWriteResponder::Session(pending_txn) => {
                            let mut just_in_time_locks = WriteLocks::builder(missing.clone());
                            for collection in missing {
                                if let Some(lock) = self.try_grant_object_write_lock(collection) {
                                    just_in_time_locks.insert_lock(collection, lock);
                                }
                            }
                            match just_in_time_locks
                                .all_or_nothing(pending_txn.ctx.session().conn_id())
                            {
                                Ok(locks) => {
                                    group_write_locks.merge(locks);
                                    validated_writes.push(PendingWriteTxn::User {
                                        span,
                                        writes,
                                        write_locks: None,
                                        responder: UserWriteResponder::Session(pending_txn),
                                    });
                                }
                                Err(missing) => {
                                    let acquire_future =
                                        self.grant_object_write_lock(missing).map(Option::Some);
                                    deferred_writes.push((
                                        acquire_future,
                                        DeferredWrite {
                                            span,
                                            writes,
                                            pending_txn,
                                        },
                                    ));
                                }
                            }
                        }
                        UserWriteResponder::Internal { conn_id, result } => {
                            let acquired = missing
                                .into_iter()
                                .map(|id| {
                                    self.try_grant_object_write_lock(id).map(|lock| (id, lock))
                                })
                                .collect::<Option<Vec<_>>>();
                            if let Some(acquired) = acquired {
                                for (id, lock) in acquired {
                                    group_write_locks.insert_lock(id, lock);
                                }
                                validated_writes.push(PendingWriteTxn::User {
                                    span,
                                    writes,
                                    write_locks: None,
                                    responder: UserWriteResponder::Internal { conn_id, result },
                                });
                            } else {
                                self.pending_writes.push(PendingWriteTxn::User {
                                    span,
                                    writes,
                                    write_locks: None,
                                    responder: UserWriteResponder::Internal { conn_id, result },
                                });
                            }
                        }
                    }
                }
            }
        }

        // Queue all of our deferred ops.
        for (acquire_future, write) in deferred_writes {
            self.defer_op(acquire_future, DeferredOp::Write(write));
        }

        let contains_internal_system_write = validated_writes
            .iter()
            .any(|write| write.is_internal_system());

        let mut appends: BTreeMap<CatalogItemId, SmallVec<[TableData; 1]>> = BTreeMap::new();
        let mut responses = Vec::with_capacity(validated_writes.len());
        let mut statement_logging_ids = Vec::new();
        let mut notifies = Vec::new();
        let mut internal_results = Vec::new();

        for validated_write_txn in validated_writes {
            match validated_write_txn {
                PendingWriteTxn::User {
                    span: _,
                    writes,
                    write_locks,
                    responder:
                        UserWriteResponder::Session(PendingTxn {
                            ctx,
                            response,
                            action,
                        }),
                } => {
                    assert_none!(write_locks, "should have merged together all locks above");
                    for (id, table_data) in writes {
                        // If the table that some write was targeting has been deleted while the
                        // write was waiting, then the write will be ignored and we respond to the
                        // client that the write was successful. This is only possible if the write
                        // and the delete were concurrent. Therefore, we are free to order the
                        // write before the delete without violating any consistency guarantees.
                        if self.catalog().try_get_entry(&id).is_some() {
                            appends.entry(id).or_default().extend(table_data);
                        }
                    }
                    if let Some(id) = ctx.extra().contents() {
                        statement_logging_ids.push(id);
                    }

                    responses.push(CompletedClientTransmitter::new(ctx, response, action));
                }
                PendingWriteTxn::User {
                    span: _,
                    writes,
                    write_locks,
                    responder: UserWriteResponder::Internal { result, .. },
                } => {
                    assert_none!(write_locks, "should have merged together all locks above");
                    let current_global_id = if writes.len() == 1 {
                        writes.keys().next().and_then(|id| {
                            self.catalog()
                                .try_get_entry(id)
                                .map(|entry| entry.latest_global_id())
                        })
                    } else {
                        None
                    };
                    if current_global_id != Some(result.expected_target_global_id()) {
                        result.send(WriteResult::TargetChanged);
                        continue;
                    }
                    for (id, table_data) in writes {
                        appends.entry(id).or_default().extend(table_data);
                    }
                    internal_results.push(result);
                }
                PendingWriteTxn::System { updates, source } => {
                    for update in updates {
                        appends.entry(update.id).or_default().push(update.data);
                    }
                    // Once the write completes we notify any waiters.
                    match source {
                        BuiltinTableUpdateSource::Internal(tx)
                        | BuiltinTableUpdateSource::Background(tx) => notifies.push(tx),
                    }
                }
            }
        }

        // Consolidate all Rows for a given table. We do not consolidate the
        // staged batches, that's up to whoever staged them.
        let mut all_appends = Vec::with_capacity(appends.len());
        for (item_id, table_data) in appends.into_iter() {
            let mut all_rows = Vec::new();
            let mut all_data = Vec::new();
            for data in table_data {
                match data {
                    TableData::Rows(rows) => all_rows.extend(rows),
                    TableData::Batches(_) => all_data.push(data),
                }
            }
            differential_dataflow::consolidation::consolidate(&mut all_rows);
            all_data.push(TableData::Rows(all_rows));

            // TODO(parkmycar): Use SmallVec throughout.
            all_appends.push((item_id, all_data));
        }

        let appends: Vec<_> = all_appends
            .into_iter()
            .map(|(id, updates)| {
                let gid = self.catalog().get_entry(&id).latest_global_id();
                (gid, updates)
            })
            .collect();

        // Always enqueue keepalives so registered tables remain readable at the oracle read ts.
        let request = GroupCommitRequest {
            appends,
            responses,
            statement_logging_ids,
            notifies,
            internal_results,
            write_locks: group_write_locks,
            permits: permit.into_iter().collect(),
            contains_internal_system_write,
            span: Span::current(),
        };
        if self
            .group_committer_tx
            .send(TableWriteCmd::GroupCommit(request))
            .is_err()
        {
            // Dropping the request retires its clients and notifies its waiters.
            warn!("group committer task gone, dropping staged group commit");
        }
    }

    /// Registers `tables` in FIFO order and returns the applied timestamp.
    pub(crate) async fn register_tables_via_committer(
        &self,
        tables: Vec<TableRegistration>,
    ) -> Timestamp {
        let (tx, rx) = oneshot::channel();
        if self
            .group_committer_tx
            .send(TableWriteCmd::Register { tables, result: tx })
            .is_err()
        {
            halt!("group committer terminated before a table registration could be submitted");
        }
        match rx.await {
            Ok(ts) => ts,
            Err(_) => halt!("group committer terminated with a table registration outstanding"),
        }
    }

    /// Forgets `ids` in FIFO order and returns the applied timestamp.
    pub(crate) async fn forget_tables_via_committer(&self, ids: Vec<GlobalId>) -> Timestamp {
        let (tx, rx) = oneshot::channel();
        if self
            .group_committer_tx
            .send(TableWriteCmd::Forget { ids, result: tx })
            .is_err()
        {
            halt!("group committer terminated before a table forget could be submitted");
        }
        match rx.await {
            Ok(ts) => ts,
            Err(_) => halt!("group committer terminated with a table forget outstanding"),
        }
    }

    /// Submit a write to be executed during the next group commit and trigger a group commit.
    pub(crate) fn submit_write(&mut self, pending_write_txn: PendingWriteTxn) {
        if self.controller.read_only() {
            panic!(
                "attempting table write in read-only mode: {:?}",
                pending_write_txn
            );
        }
        self.pending_writes.push(pending_write_txn);
        self.trigger_group_commit();
    }

    /// Append some [`BuiltinTableUpdate`]s, with various degrees of waiting and blocking.
    pub(crate) fn builtin_table_update<'a>(&'a mut self) -> BuiltinTableAppend<'a> {
        BuiltinTableAppend { coord: self }
    }

    pub(crate) fn defer_op<F>(&mut self, acquire_future: F, op: DeferredOp)
    where
        F: Future<Output = Option<(CatalogItemId, tokio::sync::OwnedMutexGuard<()>)>>
            + Send
            + 'static,
    {
        let conn_id = op.conn_id().clone();

        // Track all of our deferred ops.
        let is_optimistic = op.can_be_optimistically_retried();
        self.deferred_write_ops.insert(conn_id.clone(), op);

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let conn_id_ = conn_id.clone();
        mz_ore::task::spawn(|| format!("defer op {conn_id_}"), async move {
            tracing::info!(%conn_id, "deferring plan");
            // Once we can acquire the first failed lock, try running the deferred plan.
            //
            // Note: This does not guarantee the plan will be able to run, there might be
            // other locks that we later fail to get.
            let acquired_lock = acquire_future.await;

            // Some operations, e.g. blind INSERTs, can be optimistically retried, meaning we
            // can run multiple at once. In those cases we don't hold the lock so we retry all
            // blind writes for a single object.
            let acquired_lock = match (acquired_lock, is_optimistic) {
                (Some(_lock), true) => None,
                (Some(lock), false) => Some(lock),
                (None, _) => None,
            };

            // If this send fails then the Coordinator is shutting down.
            let _ = internal_cmd_tx.send(Message::TryDeferred {
                conn_id,
                acquired_lock,
            });
        });
    }

    /// Returns a future that waits until it can get an exclusive lock on the specified collection.
    pub(crate) fn grant_object_write_lock(
        &mut self,
        object_id: CatalogItemId,
    ) -> impl Future<Output = (CatalogItemId, OwnedMutexGuard<()>)> + 'static {
        let write_lock_handle = self
            .write_locks
            .entry(object_id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
        let write_lock_handle = Arc::clone(write_lock_handle);

        write_lock_handle
            .lock_owned()
            .map(move |guard| (object_id, guard))
    }

    /// Lazily creates the lock for the provided `object_id`, and grants it if possible, returns
    /// `None` if the lock is already held.
    pub(crate) fn try_grant_object_write_lock(
        &mut self,
        object_id: CatalogItemId,
    ) -> Option<OwnedMutexGuard<()>> {
        let write_lock_handle = self
            .write_locks
            .entry(object_id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
        let write_lock_handle = Arc::clone(write_lock_handle);

        write_lock_handle.try_lock_owned().ok()
    }
}

/// Helper struct to run a builtin table append.
pub struct BuiltinTableAppend<'a> {
    coord: &'a mut Coordinator,
}

/// `Future` that notifies when a builtin table write has completed.
///
/// Callers that expose completion of an operation whose builtin-table write is
/// user-observable should await this future before sending that completion. It
/// is safe to drop the future only when the caller does not provide such an
/// ordering guarantee, or when the future is known to resolve immediately.
///
/// Note: builtin table writes need to talk to persist, which can take 100s of milliseconds. This
/// type allows you to execute a builtin table write, e.g. via [`BuiltinTableAppend::execute`], and
/// wait for it to complete, while other long running tasks are concurrently executing.
pub type BuiltinTableAppendNotify = Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>;

/// Completion handle for a builtin-table append response barrier.
pub struct BuiltinTableAppendCompletion {
    notify: BuiltinTableAppendNotify,
}

impl BuiltinTableAppendCompletion {
    pub fn new(notify: BuiltinTableAppendNotify) -> Self {
        Self { notify }
    }

    pub fn into_notify(self) -> BuiltinTableAppendNotify {
        self.notify
    }
}

impl<'a> BuiltinTableAppend<'a> {
    /// Submit a write to a system table to be executed during the next group commit. This method
    /// __does not__ trigger a group commit.
    ///
    /// This is useful for non-critical writes like metric updates because it allows us to piggy
    /// back off the next group commit instead of triggering a potentially expensive group commit.
    ///
    /// Note: __do not__ call this for DDL which needs the system tables updated immediately.
    ///
    /// Note: When in read-only mode, this will buffer the update and return
    /// immediately.
    pub fn background(self, mut updates: Vec<BuiltinTableUpdate>) -> BuiltinTableAppendNotify {
        if self.coord.controller.read_only() {
            self.coord
                .buffered_builtin_table_updates
                .as_mut()
                .expect("in read-only mode")
                .append(&mut updates);

            return Box::pin(futures::future::ready(()));
        }

        let (tx, rx) = oneshot::channel();
        self.coord.pending_writes.push(PendingWriteTxn::System {
            updates,
            source: BuiltinTableUpdateSource::Background(tx),
        });

        Box::pin(rx.map(|_| ()))
    }

    /// Submits a write to be executed during the next group commit __and__ triggers a group commit.
    ///
    /// Returns a `Future` that resolves when the write has completed, does not block the
    /// Coordinator.
    ///
    /// Note: When in read-only mode, this will buffer the update and the
    /// returned future will resolve immediately, without the update actually
    /// having been written.
    pub fn defer(self, mut updates: Vec<BuiltinTableUpdate>) -> BuiltinTableAppendNotify {
        if self.coord.controller.read_only() {
            self.coord
                .buffered_builtin_table_updates
                .as_mut()
                .expect("in read-only mode")
                .append(&mut updates);

            return Box::pin(futures::future::ready(()));
        }

        let (tx, rx) = oneshot::channel();
        self.coord.pending_writes.push(PendingWriteTxn::System {
            updates,
            source: BuiltinTableUpdateSource::Internal(tx),
        });
        self.coord.trigger_group_commit();

        Box::pin(rx.map(|_| ()))
    }

    /// Submits a system-table write immediately and returns its completion future.
    ///
    /// In read-only mode, buffers the update and returns a ready future.
    pub fn execute(self, mut updates: Vec<BuiltinTableUpdate>) -> BuiltinTableAppendNotify {
        if self.coord.controller.read_only() {
            self.coord
                .buffered_builtin_table_updates
                .as_mut()
                .expect("in read-only mode")
                .append(&mut updates);

            return Box::pin(futures::future::ready(()));
        }

        let (tx, rx) = oneshot::channel();

        // DDL system writes bypass the periodic wait. Extremely fast DDL can advance the global
        // timeline ahead of the wall clock, delaying later queries without affecting correctness.
        self.coord.pending_writes.push(PendingWriteTxn::System {
            updates,
            source: BuiltinTableUpdateSource::Internal(tx),
        });
        self.coord.stage_group_commit(None);

        // The staged commit already advances every table.
        self.coord.advance_timelines_interval.reset();

        Box::pin(rx.map(|_| ()))
    }
}

/// Returns two sides of a "channel" that can be used to notify the coordinator when we want a
/// group commit to be run.
pub fn notifier() -> (GroupCommitNotifier, GroupCommitWaiter) {
    let notify = Arc::new(Notify::new());
    let in_progress = Arc::new(Semaphore::new(1));

    let notifier = GroupCommitNotifier {
        notify: Arc::clone(&notify),
    };
    let waiter = GroupCommitWaiter {
        notify,
        in_progress,
    };

    (notifier, waiter)
}

/// A handle that allows us to notify the coordinator that a group commit should be run at some
/// point in the future.
#[derive(Debug, Clone)]
pub struct GroupCommitNotifier {
    /// Tracks if there are any outstanding group commits.
    notify: Arc<Notify>,
}

impl GroupCommitNotifier {
    /// Notifies the [`GroupCommitWaiter`] that we'd like a group commit to be run.
    pub fn notify(&self) {
        self.notify.notify_one()
    }
}

/// A handle that returns a future when a group commit needs to be run, and one is not currently
/// being run.
#[derive(Debug)]
pub struct GroupCommitWaiter {
    /// Tracks if there are any outstanding group commits.
    notify: Arc<Notify>,
    /// Distributes permits which tracks in progress group commits.
    in_progress: Arc<Semaphore>,
}
static_assertions::assert_not_impl_all!(GroupCommitWaiter: Clone);

impl GroupCommitWaiter {
    /// Returns a permit for a group commit, once a permit is available _and_ there someone
    /// requested a group commit to be run.
    ///
    /// # Cancel Safety
    ///
    /// * Waiting on the returned Future is cancel safe because we acquire an in-progress permit
    ///   before waiting for notifications. If the Future gets dropped after acquiring a permit but
    ///   before a group commit is queued, we'll release the permit which can be acquired by the
    ///   next caller.
    ///
    pub async fn ready(&self) -> GroupCommitPermit {
        let permit = Semaphore::acquire_owned(Arc::clone(&self.in_progress))
            .await
            .expect("semaphore should not close");

        // Note: We must wait for notifies _after_ waiting for a permit to be acquired for cancel
        // safety.
        self.notify.notified().await;

        GroupCommitPermit {
            _permit: Some(permit),
        }
    }
}

/// A permit to run a group commit, this must be kept alive for the entire duration of the commit.
///
/// Note: We sometimes want to throttle how many group commits are running at once, which this
/// permit allows us to do.
#[derive(Debug)]
pub struct GroupCommitPermit {
    /// Permit that is preventing other group commits from running.
    ///
    /// Only `None` if the permit has been moved into a tokio task for waiting.
    _permit: Option<OwnedSemaphorePermit>,
}

/// When we start a [`Session`] we need to update some builtin tables, but we don't want to wait for
/// these writes to complete for two reasons:
///
/// 1. Doing a write can take a relatively long time.
/// 2. Decoupling the write from the session start allows us to batch multiple writes together, if
///    sessions are being created with a high frequency.
///
/// So, as an optimization we do not wait for these writes to complete. But if a [`Session`] tries
/// to query any of these builtin objects, we need to block that query on the writes completing to
/// maintain linearizability.
///
/// Warning: this already clears the wait flag (i.e., it calls `clear_builtin_table_updates`).
///
/// TODO(peek-seq): After we delete the old peek sequencing, we can remove the first component of
/// the return tuple.
pub(crate) fn waiting_on_startup_appends(
    catalog: &Catalog,
    session: &mut Session,
    plan: &Plan,
) -> Option<(BTreeSet<CatalogItemId>, BoxFuture<'static, ()>)> {
    // TODO(parkmycar): We need to check transitive uses here too if we ever move the
    // referenced builtin tables out of mz_internal, or we allow creating views on
    // mz_internal objects.
    let depends_on = match plan {
        Plan::Select(plan) => plan.source.depends_on(),
        Plan::ReadThenWrite(plan) => plan.selection.depends_on(),
        Plan::ShowColumns(plan) => plan.select_plan.source.depends_on(),
        Plan::Subscribe(plan) => plan.from.depends_on(),
        Plan::ExplainPlan(ExplainPlanPlan {
            explainee: Explainee::Statement(ExplaineeStatement::Select { plan, .. }),
            ..
        }) => plan.source.depends_on(),
        Plan::ExplainTimestamp(ExplainTimestampPlan { raw_plan, .. }) => raw_plan.depends_on(),
        Plan::CreateConnection(_)
        | Plan::CreateDatabase(_)
        | Plan::CreateSchema(_)
        | Plan::CreateRole(_)
        | Plan::CreateNetworkPolicy(_)
        | Plan::CreateCluster(_)
        | Plan::CreateClusterReplica(_)
        | Plan::CreateSource(_)
        | Plan::CreateSources(_)
        | Plan::CreateSecret(_)
        | Plan::CreateSink(_)
        | Plan::CreateTable(_)
        | Plan::CreateView(_)
        | Plan::CreateMaterializedView(_)
        | Plan::CreateIndex(_)
        | Plan::CreateType(_)
        | Plan::Comment(_)
        | Plan::DiscardTemp
        | Plan::DiscardAll
        | Plan::DropObjects(_)
        | Plan::DropOwned(_)
        | Plan::EmptyQuery
        | Plan::ShowAllVariables
        | Plan::ShowCreate(_)
        | Plan::ShowVariable(_)
        | Plan::InspectShard(_)
        | Plan::SetVariable(_)
        | Plan::ResetVariable(_)
        | Plan::SetTransaction(_)
        | Plan::StartTransaction(_)
        | Plan::CommitTransaction(_)
        | Plan::AbortTransaction(_)
        | Plan::CopyFrom(_)
        | Plan::CopyTo(_)
        | Plan::ExplainPlan(_)
        | Plan::ExplainPushdown(_)
        | Plan::ExplainSinkSchema(_)
        | Plan::Insert(_)
        | Plan::AlterNetworkPolicy(_)
        | Plan::AlterNoop(_)
        | Plan::AlterClusterRename(_)
        | Plan::AlterClusterSwap(_)
        | Plan::AlterClusterReplicaRename(_)
        | Plan::AlterCluster(_)
        | Plan::AlterConnection(_)
        | Plan::AlterSource(_)
        | Plan::AlterSetCluster(_)
        | Plan::AlterItemRename(_)
        | Plan::AlterRetainHistory(_)
        | Plan::AlterSourceTimestampInterval(_)
        | Plan::AlterSchemaRename(_)
        | Plan::AlterSchemaSwap(_)
        | Plan::AlterSecret(_)
        | Plan::AlterSink(_)
        | Plan::AlterSystemSet(_)
        | Plan::AlterSystemReset(_)
        | Plan::AlterSystemResetAll(_)
        | Plan::AlterRole(_)
        | Plan::AlterOwner(_)
        | Plan::AlterTableAddColumn(_)
        | Plan::AlterMaterializedViewApplyReplacement(_)
        | Plan::Declare(_)
        | Plan::Fetch(_)
        | Plan::Close(_)
        | Plan::Prepare(_)
        | Plan::Execute(_)
        | Plan::Deallocate(_)
        | Plan::Raise(_)
        | Plan::GrantRole(_)
        | Plan::RevokeRole(_)
        | Plan::GrantPrivileges(_)
        | Plan::RevokePrivileges(_)
        | Plan::AlterDefaultPrivileges(_)
        | Plan::ReassignOwned(_)
        | Plan::ValidateConnection(_)
        | Plan::SideEffectingFunc(_) => BTreeSet::default(),
    };
    let depends_on_required_id = REQUIRED_BUILTIN_TABLES
        .iter()
        .map(|table| catalog.resolve_builtin_table(&**table))
        .any(|id| {
            catalog
                .get_global_ids(&id)
                .any(|gid| depends_on.contains(&gid))
        });

    // If our plan does not depend on any required ID, then we don't need to
    // wait for any builtin writes to occur.
    if !depends_on_required_id {
        return None;
    }

    // Even if we depend on a builtin table, there's no need to wait if the
    // writes have already completed.
    //
    // TODO(parkmycar): As an optimization we should add a `Notify` type to
    // `mz_ore` that allows peeking. If the builtin table writes have already
    // completed then there is no need to defer this plan.
    match session.clear_builtin_table_updates() {
        Some(wait_future) => {
            let depends_on = depends_on
                .into_iter()
                .map(|gid| catalog.get_entry_by_global_id(&gid).id())
                .collect();
            Some((depends_on, wait_future.boxed()))
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use timely::progress::Antichain;

    use super::*;
    use crate::catalog::Catalog;

    #[derive(Debug, Default)]
    struct MemTimestampOracle {
        read_write_ts: Mutex<(Timestamp, Timestamp)>,
        apply_writes: AtomicUsize,
    }

    impl MemTimestampOracle {
        fn starting_at(ts: Timestamp) -> Self {
            Self {
                read_write_ts: Mutex::new((ts, ts)),
                apply_writes: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl TimestampOracle<Timestamp> for MemTimestampOracle {
        async fn write_ts(&self) -> WriteTimestamp {
            let (read_ts, write_ts) = &mut *self.read_write_ts.lock().expect("lock poisoned");
            let new_write_ts = std::cmp::max(*read_ts, *write_ts).step_forward();
            *write_ts = new_write_ts;
            WriteTimestamp {
                timestamp: new_write_ts,
                advance_to: new_write_ts.step_forward(),
            }
        }

        async fn peek_write_ts(&self) -> Timestamp {
            let (_, write_ts) = &*self.read_write_ts.lock().expect("lock poisoned");
            *write_ts
        }

        async fn read_ts(&self) -> Timestamp {
            let (read_ts, _) = &*self.read_write_ts.lock().expect("lock poisoned");
            *read_ts
        }

        async fn apply_write(&self, lower_bound: Timestamp) {
            self.apply_writes.fetch_add(1, Ordering::SeqCst);
            let (read_ts, write_ts) = &mut *self.read_write_ts.lock().expect("lock poisoned");
            *read_ts = std::cmp::max(*read_ts, lower_bound);
            *write_ts = std::cmp::max(*read_ts, *write_ts);
        }
    }

    #[derive(Debug)]
    struct ConflictingTableWriteHandle {
        conflicts: usize,
        calls: AtomicUsize,
        write_timestamps: Mutex<Vec<Timestamp>>,
    }

    impl ConflictingTableWriteHandle {
        fn new(conflicts: usize) -> Self {
            Self {
                conflicts,
                calls: AtomicUsize::new(0),
                write_timestamps: Mutex::new(Vec::new()),
            }
        }

        fn respond(&self, write_ts: Timestamp) -> oneshot::Receiver<Result<(), StorageError>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.write_timestamps
                .lock()
                .expect("lock poisoned")
                .push(write_ts);
            let (tx, rx) = oneshot::channel();
            let result = if call < self.conflicts {
                Err(StorageError::InvalidUppers(vec![
                    mz_storage_types::controller::InvalidUpper {
                        id: GlobalId::User(1),
                        current_upper: Antichain::from_elem(write_ts.step_forward()),
                    },
                ]))
            } else {
                Ok(())
            };
            tx.send(result).expect("receiver still in scope");
            rx
        }
    }

    impl TableWriteHandle for ConflictingTableWriteHandle {
        fn append(
            &self,
            write_ts: Timestamp,
            _advance_to: Timestamp,
            _commands: Vec<(GlobalId, Vec<TableData>)>,
        ) -> oneshot::Receiver<Result<(), StorageError>> {
            self.respond(write_ts)
        }

        fn register(
            &self,
            register_ts: Timestamp,
            _tables: Vec<TableRegistration>,
        ) -> oneshot::Receiver<Result<(), StorageError>> {
            self.respond(register_ts)
        }

        fn forget(
            &self,
            forget_ts: Timestamp,
            _ids: Vec<GlobalId>,
        ) -> oneshot::Receiver<Result<(), StorageError>> {
            self.respond(forget_ts)
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn test_write_to_txns_conflict_retry() {
        Catalog::with_debug(|catalog| async move {
            // Start beyond the catalog upper to exercise the durable advance path.
            let initial_upper = catalog.current_upper().await;
            let oracle = Arc::new(MemTimestampOracle::starting_at(initial_upper));
            let handle = Arc::new(ConflictingTableWriteHandle::new(1));
            let oracle_dyn: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                Arc::<MemTimestampOracle>::clone(&oracle);
            let handle_dyn: Arc<dyn TableWriteHandle> =
                Arc::<ConflictingTableWriteHandle>::clone(&handle);
            let (_tx, rx) = mpsc::unbounded_channel();
            let (internal_cmd_tx, _internal_cmd_rx) = mpsc::unbounded_channel();
            let committer = GroupCommitter {
                rx,
                oracle: oracle_dyn,
                table_write_handle: handle_dyn,
                catalog_upper: catalog.upper_handle(),
                internal_cmd_tx,
                now: SYSTEM_TIME.clone(),
                metrics: Metrics::register_into(&MetricsRegistry::new()),
                max_attempts: ConfigValHandle::disconnected(2),
            };

            let write_ts = committer
                .write_to_txns(None, |ts, advance_to| {
                    assert_eq!(advance_to, ts.step_forward());
                    handle.append(ts, advance_to, Vec::new())
                })
                .await
                .expect("worker stays alive in this test");

            let attempts = handle
                .write_timestamps
                .lock()
                .expect("lock poisoned")
                .clone();
            assert_eq!(attempts.len(), 2, "one conflict, one success");
            assert!(
                attempts[0] > initial_upper,
                "attempts start past the initial catalog upper: {attempts:?} vs {initial_upper}"
            );
            assert!(
                attempts[0] < attempts[1],
                "retry must use a fresh, larger timestamp: {attempts:?}"
            );
            assert_eq!(write_ts.timestamp, attempts[1]);

            assert_eq!(oracle.apply_writes.load(Ordering::SeqCst), 1);
            assert_eq!(oracle.read_ts().await, write_ts.timestamp);

            let catalog_upper = catalog.current_upper().await;
            assert!(
                catalog_upper >= write_ts.advance_to,
                "catalog upper {catalog_upper} must cover the write's advance_to {}",
                write_ts.advance_to
            );

            catalog.expire().await;
        })
        .await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn test_write_to_txns_zero_limit_allows_one_attempt() {
        Catalog::with_debug(|catalog| async move {
            let initial_upper = catalog.current_upper().await;
            let oracle = Arc::new(MemTimestampOracle::starting_at(initial_upper));
            let handle = Arc::new(ConflictingTableWriteHandle::new(0));
            let oracle_dyn: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                Arc::<MemTimestampOracle>::clone(&oracle);
            let handle_dyn: Arc<dyn TableWriteHandle> =
                Arc::<ConflictingTableWriteHandle>::clone(&handle);
            let (_tx, rx) = mpsc::unbounded_channel();
            let (internal_cmd_tx, _internal_cmd_rx) = mpsc::unbounded_channel();
            let committer = GroupCommitter {
                rx,
                oracle: oracle_dyn,
                table_write_handle: handle_dyn,
                catalog_upper: catalog.upper_handle(),
                internal_cmd_tx,
                now: SYSTEM_TIME.clone(),
                metrics: Metrics::register_into(&MetricsRegistry::new()),
                max_attempts: ConfigValHandle::disconnected(0),
            };

            let write_ts = committer
                .write_to_txns(None, |ts, advance_to| {
                    handle.append(ts, advance_to, Vec::new())
                })
                .await
                .expect("zero limit permits one successful attempt");

            let attempts = handle
                .write_timestamps
                .lock()
                .expect("lock poisoned")
                .clone();
            assert_eq!(attempts, vec![write_ts.timestamp]);
            assert_eq!(oracle.apply_writes.load(Ordering::SeqCst), 1);

            catalog.expire().await;
        })
        .await;
    }
}
