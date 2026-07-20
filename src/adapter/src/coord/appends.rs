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
//! # The group committer
//!
//! All txns-shard writes (group-commit table appends, and table registration and forgetting for
//! DDL) flow through one long-lived task, the [`GroupCommitter`], as [`TableWriteCmd`]s. The
//! coordinator loop only does cheap, state-touching staging (draining pending writes, taking
//! write locks, building append batches). The committer does everything that involves a
//! timestamp, per command:
//!
//! 1. allocate a write timestamp from the oracle,
//! 2. advance the catalog shard upper to the timestamp's [`WriteTimestamp::advance_to`], which
//!    keeps the catalog readable at the read timestamp the oracle moves to in step 4 and, when
//!    the advance actually writes, re-checks leadership before user data is written (see
//!    materialize#28216; an advance that finds the upper already past the target short-circuits
//!    without a durable check, so this is defense in depth, not a per-attempt guarantee),
//! 3. issue the txns-shard write. An `InvalidUppers` conflict means another process advanced
//!    the txns shard past our timestamp, so retry from step 1 at a fresh timestamp (a wasted
//!    timestamp is harmless, `apply_write` is a high-water mark),
//! 4. apply the write to the oracle, so no read ever observes a timestamp at which the write is
//!    not yet durable.
//!
//! Processing one command at a time makes the committer the single in-process writer to the
//! txns shard, and the queue order is load-bearing: an append staged before a table's forget
//! reaches the table-write worker before it, and appends staged after a registration cannot
//! overtake it. See the corresponding section in `doc/developer/guide-adapter.md` for why, and
//! for the cross-process story.
//!
//! # Cross-generation safety
//!
//! The txns shard can have concurrent writers: another generation during a 0dt handover, or a
//! fenced-out process whose committer stays alive for a moment. A stale generation's txns write
//! must never land above the new generation's system-table snapshot timestamp, where it would go
//! unobserved. Two mechanisms keep this safe, and neither is the catalog-upper advance of step 2.
//!
//! Conflict-freedom comes from the shared oracle and the txns-shard compare-and-append. The
//! oracle is strictly increasing across generations, so every write takes a unique, larger
//! timestamp, and the compare-and-append linearizes concurrent writers: a loser sees
//! `InvalidUppers` and retries from step 1 at a fresh timestamp. This orders writes but does not
//! decide who may write, so on its own it does not stop a fresh write at a higher timestamp.
//!
//! Authorization comes from fencing. A new generation fences the old one at the catalog shard,
//! and the fenced generation halts: its coordinator exits the process on the next catalog
//! operation that observes the fence, so it stops allocating timestamps. Any txns write it still
//! has in flight was therefore allocated before the fence, below the new generation's snapshot
//! timestamp, so it lands below the snapshot-and-reset and is observed there (retracted for
//! system tables, visible to reads for user tables). A write above the snapshot would need a
//! post-fence timestamp, which the halted generation does not allocate. This rests on the fenced
//! generation winding down promptly and on the oracle being shared and strictly increasing across
//! generations. A per-generation oracle, or a committer that kept running long past the fence,
//! would reopen the hole.
//!
//! The catalog-upper advance is defense in depth on top of that contract, not a separate
//! guarantee. Because it is a catalog write, a fenced committer that reaches its durable
//! compare-and-append notices the fence there and halts before the txns write, one more place the
//! outgoing generation stops. It is not a per-attempt check: the advance short-circuits when the
//! upper is already past its target, and the coordinator's own catalog writes are the primary
//! fence trigger.
//!
//! Finalization that needs coordinator state (statement-logging timestamps, retiring client
//! responses, downgrading read holds) is handed back to the coordinator loop via
//! [`Message::GroupCommitApplied`].

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use derivative::Derivative;
use futures::future::{BoxFuture, FutureExt};
use mz_adapter_types::connection::ConnectionId;
use mz_catalog::builtin::{BuiltinTable, MZ_SESSIONS};
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

/// Where to deliver the result of a [`PendingWriteTxn::User`] write.
#[derive(Debug)]
pub(crate) enum UserWriteResponder {
    /// Session-bound write. The coordinator retires the session's
    /// `ExecuteContext` once the write commits.
    Session(PendingTxn),
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

/// A command for the [`GroupCommitter`]. See the module docs for the protocol and the ordering
/// contract carried by the command queue.
pub(crate) enum TableWriteCmd {
    /// A staged group commit, see [`GroupCommitRequest`].
    GroupCommit(GroupCommitRequest),
    /// Register tables in the txns shard, making them available for writes. Replies with the
    /// registration timestamp, which is also the timestamp at which the tables become readable
    /// (the committer applies it to the oracle before replying).
    Register {
        tables: Vec<TableRegistration>,
        result: oneshot::Sender<Timestamp>,
    },
    /// Forget tables in the txns shard. Replies with the forget timestamp.
    Forget {
        ids: Vec<GlobalId>,
        result: oneshot::Sender<Timestamp>,
    },
}

/// A batch of writes staged on the coordinator loop, to be committed by the [`GroupCommitter`].
///
/// The loop does the cheap, state-touching work (draining pending writes, acquiring write locks,
/// building the append batch) and hands the rest to the committer. Timestamp allocation, the table
/// append, advancing the catalog upper, and applying the write to the oracle all happen off the
/// loop.
pub(crate) struct GroupCommitRequest {
    /// Table appends, resolved to their latest [`GlobalId`] and consolidated. Empty for a keepalive.
    appends: Vec<(GlobalId, Vec<TableData>)>,
    /// Client transmitters for user writes, retired once the commit is durable.
    responses: Vec<CompletedClientTransmitter>,
    /// Statement-logging ids for user writes, paired with the commit timestamp once we know it.
    ///
    /// Invariant: no execution-ended event may be emitted for these ids while they sit here. The
    /// contexts that could end them are in `responses`, and the coordinator records the
    /// timestamps before retiring those contexts when handling [`Message::GroupCommitApplied`].
    /// A path that ends one of these executions early would turn the timestamp recording into a
    /// write to an ended execution, see `set_statement_execution_timestamp`.
    statement_logging_ids: Vec<StatementLoggingId>,
    /// Notifiers for system-table writes (DDL, background heartbeats).
    notifies: Vec<oneshot::Sender<()>>,
    /// Write locks held for the duration of the commit.
    write_locks: GroupCommitWriteLocks,
    /// In-progress permits, released once the commit is applied. Merging batches can accumulate
    /// more than one.
    permits: Vec<GroupCommitPermit>,
    /// Whether the batch has an internal system write (e.g. `mz_sessions`), which bypasses the
    /// throttle so that tests with a mocked wall clock can still make progress.
    contains_internal_system_write: bool,
    span: Span,
}

impl GroupCommitRequest {
    /// Absorbs another staged commit into this one, so both apply at a single timestamp.
    ///
    /// Lock sets of separately staged commits are disjoint (staging defers writes whose locks are
    /// already held), so extending the lock map cannot double-lock. The other request's span is
    /// dropped, which loses its trace linkage. Not great, but merged batches are the exception.
    fn merge(&mut self, other: GroupCommitRequest) {
        let GroupCommitRequest {
            appends,
            responses,
            statement_logging_ids,
            notifies,
            write_locks,
            permits,
            contains_internal_system_write,
            span: _,
        } = other;
        // The worker applies every (id, updates) entry, so duplicate ids across the merged
        // batches are fine.
        self.appends.extend(appends);
        self.responses.extend(responses);
        self.statement_logging_ids.extend(statement_logging_ids);
        self.notifies.extend(notifies);
        self.write_locks.extend(write_locks);
        // Hold all merged permits until the combined batch is applied, so the permit keeps
        // bounding in-flight batches as documented.
        self.permits.extend(permits);
        self.contains_internal_system_write |= contains_internal_system_write;
    }
}

/// A single, long-lived task that owns all txns-shard writes, applied off the coordinator loop.
/// See the module docs for the protocol.
///
/// It holds only shareable handles, captured once at startup. The oracle is never replaced
/// in-process, and the table write handle stays valid for the controller's lifetime. The catalog
/// upper handle shares the durable-storage mutex with catalog transactions, so upper advancement
/// stays serialized with DDL commits.
///
/// No graceful shutdown: when the process shuts down, in-flight and queued responses are
/// dropped, which retires the clients with a shutdown error, see the drop backstop on
/// [`ExecuteContext`].
pub(crate) struct GroupCommitter {
    rx: mpsc::UnboundedReceiver<TableWriteCmd>,
    oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync>,
    table_write_handle: Arc<dyn TableWriteHandle>,
    catalog_upper: CatalogUpperHandle,
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    now: NowFn,
    metrics: Metrics,
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
                    TableWriteCmd::Register { tables, result } => {
                        let Some(write_ts) = self
                            .write_to_txns(None, |ts, _advance_to| {
                                self.table_write_handle.register(ts, tables.clone())
                            })
                            .await
                        else {
                            return;
                        };
                        // We don't care if the waiter has gone away.
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
                        // We don't care if the waiter has gone away.
                        let _ = result.send(write_ts.timestamp);
                    }
                }
            }
        }
    }

    /// Writes to the txns shard at a fresh oracle timestamp and applies the write to the oracle,
    /// implementing steps 1-4 of the protocol in the module docs.
    ///
    /// Returns `None` when the table write worker is gone, which only happens at process
    /// shutdown. The caller must wind the committer down then, see the `Err(_recv)` arm.
    async fn write_to_txns(
        &self,
        op_duration_metric: Option<&prometheus::Histogram>,
        mut op: impl FnMut(Timestamp, Timestamp) -> oneshot::Receiver<Result<(), StorageError>>,
    ) -> Option<WriteTimestamp> {
        // In-process conflicts are impossible (single writer), and the only conflicting writer
        // we understand today is another generation, whose fence stops us via `advance_upper`
        // within a retry or two. A conflict that persists past this cap means a writer we don't
        // understand, so we convert it into the halt-and-rebuild path instead of spinning
        // forever. The cap is generous on purpose: halting is disruptive, and each retry warns,
        // so a misbehaving writer is visible in the logs long before we give up.
        const MAX_ATTEMPTS: usize = 100;
        let mut attempt = 0;
        let write_ts = loop {
            attempt += 1;
            if attempt > MAX_ATTEMPTS {
                halt!("txns-shard write conflicted {MAX_ATTEMPTS} times, rebuilding");
            }
            let write_ts = self.oracle.write_ts().await;

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
                    // Another process advanced the txns shard past us; retry at a fresh
                    // timestamp.
                    warn!(
                        write_ts = %write_ts.timestamp,
                        attempt,
                        "txns-shard write conflicted with another writer, retrying at a fresh timestamp"
                    );
                    continue;
                }
                Ok(Err(other)) => {
                    // Only another writer's upper conflict is retryable, anything else is fatal.
                    Err::<(), _>(other).unwrap_or_terminate("cannot fail to write to txns shard");
                    unreachable!("unwrap_or_terminate does not return on Err");
                }
                Err(_recv) => {
                    // The worker replies to every command, so a dropped reply means its task
                    // was cancelled, which only happens at process shutdown (a worker panic
                    // aborts the process through the panic hook). The write is in an indefinite
                    // state, but so is everything else at this point. Wind the committer down
                    // instead of processing more writes during teardown: dropped responses
                    // retire their clients via the `ExecuteContext` drop backstop, dropped
                    // register/forget waiters halt, matching the storage controller's shutdown
                    // posture on the bootstrap register path.
                    warn!("table write worker gone (process shutting down), winding down");
                    return None;
                }
            }
        };

        // The committer bypasses `apply_local_write`, so run the runaway-timeline check here:
        // a write timestamp far ahead of the wall clock is the signal that the timeline has run
        // away (e.g. after a clock regression).
        let now: Timestamp = (self.now)().into();
        crate::coord::timeline::check_runaway_write_ts(&now, write_ts.timestamp);

        // Mark the write complete on the oracle so reads can advance to it.
        self.oracle.apply_write(write_ts.timestamp).await;

        Some(write_ts)
    }

    /// Applies one staged group commit: allocate a timestamp, append, and apply the write.
    ///
    /// While waiting in the wall-clock throttle, later queued group commits are merged into this
    /// one. That bounds queue growth under a slow oracle, and it lets an arriving internal system
    /// write flip the throttle bypass, so system writes are not stuck behind a throttled
    /// user-only batch. Merging stops at the first register or forget command, which is returned
    /// to the caller to process next, preserving queue order (an append staged after a register
    /// may target the newly registered table and must not commit before it).
    ///
    /// `Break` means the table write worker is gone and the committer must wind down.
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

            // Check the bypasses before peeking the oracle, so bypassed batches (every DDL
            // builtin-table commit, for one) skip that round trip. An internal system write
            // bypasses the throttle so tests with a mocked wall clock still make progress. A
            // deferred register or forget means a DDL statement is blocked on the coordinator
            // loop behind this batch. DDL was never subject to the wall-clock throttle (its
            // timestamp allocations advanced the timeline directly when they ran on the
            // coordinator loop), so rather than stall the whole control plane until the clock
            // catches up, we let the batch through.
            if request.contains_internal_system_write || deferred_cmd.is_some() {
                break;
            }
            let ts = self.oracle.peek_write_ts().await;
            let now: Timestamp = (self.now)().into();
            if ts <= now {
                break;
            }
            // Cap the wait at 1s so a wall clock that jumped far ahead and then back does not
            // stall us for a long time. The deadline is fixed per peek: merge wakeups below
            // re-check the bypasses but do not re-peek the oracle.
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
            // Winding down at shutdown. Dropping the batch retires the clients with a shutdown
            // error via the `ExecuteContext` drop backstop.
            return ControlFlow::Break(());
        };
        let timestamp = write_ts.timestamp;

        // Log non-empty user appends.
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

        // IMPORTANT: Release the in-progress permits and write locks now, after the write is
        // applied at the oracle, so no other write goes through at a timestamp we have not yet
        // applied. Retiring the client responses does not need these locks, so we hand that back
        // to the coordinator loop.
        drop(permits);
        drop(write_locks);

        // Notify system-table write waiters (DDL, background heartbeats).
        for notify in notifies {
            // We don't care if the listeners have gone away.
            let _ = notify.send(());
        }

        // Hand the parts that need coordinator state back to the coordinator loop: recording
        // statement execution timestamps, retiring client responses, and downgrading read holds.
        // Retiring must happen there after the timestamps are recorded, because it ends the
        // statement execution.
        //
        // NOTE: After `apply_write(timestamp)` the oracle read ts is at least `timestamp`, and
        // reads at `timestamp` observe this commit, so the coordinator can downgrade the
        // EpochMilliseconds read holds to `timestamp` without an oracle round trip.
        if self
            .internal_cmd_tx
            .send(Message::GroupCommitApplied {
                responses,
                statement_logging_ids,
                write_ts: timestamp,
            })
            .is_err()
        {
            // The coordinator loop is gone, which only happens at process shutdown. Dropping
            // the responses retires the clients with a shutdown error, see the drop backstop on
            // `ExecuteContext`.
            warn!("coordinator shut down before a group commit could be finalized");
        }

        ControlFlow::Continue(deferred_cmd)
    }
}

/// Spawns the [`GroupCommitter`] task, consuming `rx` for staged commits.
///
/// Called once at coordinator startup with handles that stay valid for the process lifetime.
pub(crate) fn spawn_group_committer(
    rx: mpsc::UnboundedReceiver<TableWriteCmd>,
    oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync>,
    table_write_handle: Arc<dyn TableWriteHandle>,
    catalog_upper: CatalogUpperHandle,
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    now: NowFn,
    metrics: Metrics,
) {
    let committer = GroupCommitter {
        rx,
        oracle,
        table_write_handle,
        catalog_upper,
        internal_cmd_tx,
        now,
        metrics,
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

    /// Stages all pending write transactions into a [`GroupCommitRequest`] and hands it to the
    /// group committer, which allocates the write timestamp and applies the commit off the
    /// coordinator loop.
    ///
    /// If the caller holds the write lock they can pass it in. Writes whose locks are held by
    /// another operation are deferred (only system writes and table advancement proceed). Writes
    /// whose locks are free are acquired here and included.
    ///
    /// All included pending writes are combined into a single append committed at one timestamp,
    /// and all involved tables advance to a timestamp larger than the write's. This does no
    /// timestamp-oracle round trips itself: the committer does the throttle, allocation, and apply
    /// off the coordinator loop.
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
                // We always allow system writes to proceed.
                PendingWriteTxn::System { .. } => validated_writes.push(pending_write),
                // We have a set of locks! Validate they're correct (expected).
                PendingWriteTxn::User {
                    span,
                    write_locks: Some(write_locks),
                    writes,
                    responder: UserWriteResponder::Session(pending_txn),
                } => match write_locks.validate(writes.keys().copied()) {
                    Ok(validated_locks) => {
                        // Merge all of our write locks together since we can allow concurrent
                        // writes at the same timestamp.
                        group_write_locks.merge(validated_locks);

                        let validated_write = PendingWriteTxn::User {
                            span,
                            writes,
                            write_locks: None,
                            responder: UserWriteResponder::Session(pending_txn),
                        };
                        validated_writes.push(validated_write);
                    }
                    // This is very unexpected since callers of this method should be validating.
                    //
                    // We cannot allow these write to occur since if the correct set of locks was
                    // not taken we could violate serializability.
                    Err(missing) => {
                        let writes: Vec<_> = writes.keys().collect();
                        panic!(
                            "got to group commit with partial set of locks!\nmissing: {:?}, writes: {:?}, txn: {:?}",
                            missing, writes, pending_txn,
                        );
                    }
                },
                // If we don't have any locks, try to acquire them, otherwise defer the write.
                PendingWriteTxn::User {
                    span,
                    writes,
                    write_locks: None,
                    responder: UserWriteResponder::Session(pending_txn),
                } => {
                    let missing = group_write_locks.missing_locks(writes.keys().copied());

                    if missing.is_empty() {
                        // We have all the locks! Queue the pending write.
                        let validated_write = PendingWriteTxn::User {
                            span,
                            writes,
                            write_locks: None,
                            responder: UserWriteResponder::Session(pending_txn),
                        };
                        validated_writes.push(validated_write);
                    } else {
                        // Try to acquire the locks we're missing.
                        let mut just_in_time_locks = WriteLocks::builder(missing.clone());
                        for collection in missing {
                            if let Some(lock) = self.try_grant_object_write_lock(collection) {
                                just_in_time_locks.insert_lock(collection, lock);
                            }
                        }

                        match just_in_time_locks.all_or_nothing(pending_txn.ctx.session().conn_id())
                        {
                            // We acquired all of the locks! Proceed with the write.
                            Ok(locks) => {
                                group_write_locks.merge(locks);
                                let validated_write = PendingWriteTxn::User {
                                    span,
                                    writes,
                                    write_locks: None,
                                    responder: UserWriteResponder::Session(pending_txn),
                                };
                                validated_writes.push(validated_write);
                            }
                            // Darn. We couldn't acquire the locks, defer the write.
                            Err(missing) => {
                                let acquire_future =
                                    self.grant_object_write_lock(missing).map(Option::Some);
                                let write = DeferredWrite {
                                    span,
                                    writes,
                                    pending_txn,
                                };
                                deferred_writes.push((acquire_future, write));
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
                    // The commit timestamp is allocated by the committer, so we record it for
                    // statement logging once the committer reports it back.
                    if let Some(id) = ctx.extra().contents() {
                        statement_logging_ids.push(id);
                    }

                    responses.push(CompletedClientTransmitter::new(ctx, response, action));
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

        // Hand the staged commit to the committer, which allocates the timestamp and applies it
        // off the coordinator loop.
        //
        // NOTE: We always send, even with no appends, so the committer periodically bumps the upper
        // of all tables, which is required to keep them readable at the latest oracle read ts.
        let request = GroupCommitRequest {
            appends,
            responses,
            statement_logging_ids,
            notifies,
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
            // The committer is gone, which only happens at process shutdown. Dropping the
            // request retires the clients with a shutdown error, see the drop backstop on
            // `ExecuteContext`. The notify senders are dropped too, which their
            // waiters observe as completion. We cannot signal failure through them, and the
            // process is shutting down regardless.
            warn!("group committer task gone, dropping staged group commit");
        }
    }

    /// Registers `tables` in the txns shard through the group committer, returning the timestamp
    /// at which they became writable (and readable, once the oracle read ts passes it). Going
    /// through the committer orders the registration against staged appends, see the module docs.
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
            // The command never entered the queue, so the txns shard was not touched, but the
            // committer only disappears at process shutdown and we cannot make progress.
            halt!("group committer terminated before a table registration could be submitted");
        }
        match rx.await {
            Ok(ts) => ts,
            // The committer only stops replying when the table write worker shut down, i.e. at
            // process shutdown. The registration is in an indeterminate state, so we cannot
            // continue.
            Err(_) => halt!("group committer terminated with a table registration outstanding"),
        }
    }

    /// Forgets `ids` in the txns shard through the group committer, returning the forget
    /// timestamp. Going through the committer orders the forget after all previously staged
    /// appends, which is what makes dropping a table with in-flight writes safe, see the module
    /// docs.
    pub(crate) async fn forget_tables_via_committer(&self, ids: Vec<GlobalId>) -> Timestamp {
        let (tx, rx) = oneshot::channel();
        if self
            .group_committer_tx
            .send(TableWriteCmd::Forget { ids, result: tx })
            .is_err()
        {
            // See `register_tables_via_committer`.
            halt!("group committer terminated before a table forget could be submitted");
        }
        match rx.await {
            Ok(ts) => ts,
            // See `register_tables_via_committer` for why halting is the only option.
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

    /// Submit a write to a system table and trigger a group commit.
    ///
    /// Returns a `Future` that completes once the write has been applied. Does not block the
    /// coordinator on the timestamp oracle: the write timestamp is allocated by the group
    /// committer off the coordinator loop.
    ///
    /// Note: When in read-only mode, this buffers the update and the returned future resolves
    /// immediately, without the update actually having been written.
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

        // Most DDL queries write to system tables. Unlike user-table writes, system table writes
        // explicitly trigger a group commit rather than waiting for the next one. If DDL runs faster
        // than one query per millisecond the global timeline can advance past the system clock,
        // which can make future queries block but does not affect correctness. That rate of DDL is
        // unlikely, so we let DDL trigger a commit directly.
        self.coord.pending_writes.push(PendingWriteTxn::System {
            updates,
            source: BuiltinTableUpdateSource::Internal(tx),
        });
        self.coord.stage_group_commit(None);

        // Avoid excessive group commits by resetting the periodic table advancement timer. The
        // commit staged above already advances all tables.
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

    /// A minimal in-memory [`TimestampOracle`] for driving the committer in tests. Counts
    /// `apply_write` calls so tests can assert how often the committer applies.
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

    /// A [`TableWriteHandle`] that reports an `InvalidUppers` conflict for the first
    /// `conflicts` calls and succeeds afterwards, recording the write timestamp of every call.
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

    /// Tests the committer's txns-shard conflict retry protocol: on `InvalidUppers` it retries
    /// at a fresh, strictly larger oracle timestamp, durably advances the catalog upper, and
    /// applies the write to the oracle exactly once, for the attempt that succeeds.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn test_write_to_txns_conflict_retry() {
        Catalog::with_debug(|catalog| async move {
            // Seed the oracle from the catalog upper, so the committer's `advance_upper` calls
            // target timestamps past the freshly bootstrapped catalog and must take the durable
            // compare-and-append path rather than the already-covered short-circuit.
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

            // The write was applied to the oracle exactly once, and the successful timestamp is
            // readable.
            assert_eq!(oracle.apply_writes.load(Ordering::SeqCst), 1);
            assert_eq!(oracle.read_ts().await, write_ts.timestamp);

            // The catalog upper was durably advanced to (at least) the successful attempt's
            // `advance_to`, which started beyond the bootstrap upper, so this fails if
            // `write_to_txns` stops advancing the catalog upper.
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
}
