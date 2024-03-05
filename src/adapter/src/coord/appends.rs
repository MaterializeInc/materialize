// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic and types for all appends executed by the [`Coordinator`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use futures::future::{BoxFuture, FutureExt};
use mz_ore::instrument;
use mz_ore::metrics::MetricsFutureExt;
use mz_ore::task;
use mz_ore::vec::VecExt;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_sql::plan::Plan;
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_client::client::TimestamplessUpdate;
use mz_timestamp_oracle::WriteTimestamp;
use tokio::sync::{oneshot, Notify, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};
use tracing::{warn, Instrument, Span};

use crate::catalog::BuiltinTableUpdate;
use crate::coord::{Coordinator, Message, PendingTxn, PlanValidity};
use crate::session::{Session, WriteOp};
use crate::util::{CompletedClientTransmitter, ResultExt};
use crate::ExecuteContext;

/// An operation that is deferred while waiting for a lock.
#[derive(Debug)]
pub(crate) enum Deferred {
    Plan(DeferredPlan),
    GroupCommit,
}

/// This is the struct meant to be paired with [`Message::WriteLockGrant`], but
/// could theoretically be used to queue any deferred plan.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct DeferredPlan {
    #[derivative(Debug = "ignore")]
    pub ctx: ExecuteContext,
    pub plan: Plan,
    pub validity: PlanValidity,
}

/// Describes what action triggered an update to a builtin table.
#[derive(Debug)]
pub(crate) enum BuiltinTableUpdateSource {
    /// Internal update, notify the caller when it's complete.
    Internal(oneshot::Sender<()>),
    /// Update was triggered by some background process, such as periodic heartbeats from COMPUTE.
    Background,
}

/// A pending write transaction that will be committing during the next group commit.
#[derive(Debug)]
pub(crate) enum PendingWriteTxn {
    /// Write to a user table.
    User {
        span: Span,
        /// List of all write operations within the transaction.
        writes: Vec<WriteOp>,
        /// Holds the coordinator's write lock.
        write_lock_guard: Option<OwnedMutexGuard<()>>,
        /// Inner transaction.
        pending_txn: PendingTxn,
    },
    /// Write to a system table.
    System {
        updates: Vec<BuiltinTableUpdate>,
        source: BuiltinTableUpdateSource,
    },
}

impl PendingWriteTxn {
    fn take_write_lock(&mut self) -> Option<OwnedMutexGuard<()>> {
        match self {
            PendingWriteTxn::User {
                write_lock_guard, ..
            } => std::mem::take(write_lock_guard),
            PendingWriteTxn::System { .. } => None,
        }
    }

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

/// Enforces critical section invariants for functions that perform writes to
/// tables, e.g. `INSERT`, `UPDATE`.
///
/// If the provided session doesn't currently hold the write lock, attempts to
/// grant it. If the coord cannot immediately grant the write lock, defers
/// executing the provided plan until the write lock is available, and exits the
/// function.
///
/// # Parameters
/// - `$coord: &mut Coord`
/// - `$tx: ClientTransmitter<ExecuteResponse>`
/// - `mut $session: Session`
/// - `$plan_to_defer: Plan`
///
/// Note that making this a macro rather than a function lets us avoid taking
/// ownership of e.g. session and lets us unilaterally enforce the return when
/// deferring work.
#[macro_export]
macro_rules! guard_write_critical_section {
    ($coord:expr, $ctx:expr, $plan_to_defer:expr, $dependency_ids:expr) => {
        if !$ctx.session().has_write_lock() {
            if $coord
                .try_grant_session_write_lock($ctx.session_mut())
                .is_err()
            {
                let role_metadata = $ctx.session().role_metadata().clone();
                $coord.defer_write(Deferred::Plan(DeferredPlan {
                    ctx: $ctx,
                    plan: $plan_to_defer,
                    validity: PlanValidity {
                        transient_revision: $coord.catalog().transient_revision(),
                        dependency_ids: $dependency_ids,
                        cluster_id: None,
                        replica_id: None,
                        role_metadata,
                    },
                }));
                return;
            }
        }
    };
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

    /// Attempts to commit all pending write transactions in a group commit. If the timestamp
    /// chosen for the writes is not ahead of `now()`, then we can execute and commit the writes
    /// immediately. Otherwise we must wait for `now()` to advance past the timestamp chosen for the
    /// writes.
    #[instrument(level = "debug")]
    pub(crate) async fn try_group_commit(&mut self, permit: Option<GroupCommitPermit>) {
        let timestamp = self.peek_local_write_ts().await;
        let now = Timestamp::from((self.catalog().config().now)());

        // HACK: This is a special case to allow writes to the mz_sessions table to proceed even
        // if the timestamp oracle is ahead of the current walltime. We do this because there are
        // some tests that mock the walltime, so it doesn't automatically advance, and updating
        // those tests to advance the walltime while creating a connection is too much.
        //
        // TODO(parkmycar): Get rid of the check below when refactoring group commits.
        let contains_internal_system_write = self
            .pending_writes
            .iter()
            .any(|write| write.is_internal_system());

        if timestamp > now && !contains_internal_system_write {
            // Cap retry time to 1s. In cases where the system clock has retreated by
            // some large amount of time, this prevents against then waiting for that
            // large amount of time in case the system clock then advances back to near
            // what it was.
            let remaining_ms = std::cmp::min(timestamp.saturating_sub(now), 1_000.into());
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(
                || "group_commit_initiate",
                async move {
                    tokio::time::sleep(Duration::from_millis(remaining_ms.into())).await;
                    // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
                    let result =
                        internal_cmd_tx.send(Message::GroupCommitInitiate(Span::current(), permit));
                    if let Err(e) = result {
                        warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                    }
                }
                .instrument(Span::current()),
            );
        } else {
            self.group_commit_initiate(None, permit).await;
        }
    }

    /// Tries to commit all pending writes transactions at the same timestamp.
    ///
    /// If the caller of this function has the `write_lock` acquired, then they can optionally pass
    /// it in to this method. If the caller does not have the `write_lock` acquired and the
    /// `write_lock` is currently locked by another operation, then only writes to system tables
    /// and table advancements will be applied. If the caller does not have the `write_lock`
    /// acquired and the `write_lock` is not currently locked by another operation, then group
    /// commit will acquire it and all writes will be applied.
    ///
    /// All applicable pending writes will be combined into a single Append command and sent to
    /// STORAGE as a single batch. All applicable writes will happen at the same timestamp and all
    /// involved tables will be advanced to some timestamp larger than the timestamp of the write.
    #[instrument(name = "coord::group_commit_initiate")]
    pub(crate) async fn group_commit_initiate(
        &mut self,
        write_lock_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
        permit: Option<GroupCommitPermit>,
    ) {
        let (write_lock_guard, pending_writes): (_, Vec<_>) = if let Some(guard) = write_lock_guard
        {
            // If the caller passed in the write lock, then we can execute a group commit.
            (Some(guard), self.pending_writes.drain(..).collect())
        } else if self
            .pending_writes
            .iter()
            .all(|write| matches!(write, PendingWriteTxn::System { .. }))
            || self.pending_writes.is_empty()
        {
            // If none of the pending transactions are for user tables, then we don't need the
            // write lock.
            (None, self.pending_writes.drain(..).collect())
        } else if let Some(guard) = self
            .pending_writes
            .iter_mut()
            .find_map(|write| write.take_write_lock())
        {
            // If some pending transaction already holds the write lock, then we can execute a group
            // commit.
            (Some(guard), self.pending_writes.drain(..).collect())
        } else if let Ok(guard) = Arc::clone(&self.write_lock).try_lock_owned() {
            // If no pending transaction holds the write lock, then we need to acquire it.
            (Some(guard), self.pending_writes.drain(..).collect())
        } else {
            // If some running transaction already holds the write lock, then one of the
            // following things will happen:
            //   1. The transaction will submit a write which will transfer the
            //      ownership of the lock to group commit and trigger another group
            //      group commit.
            //   2. The transaction will complete without submitting a write (abort,
            //      empty writes, etc) which will drop the lock. The deferred group
            //      commit will then acquire the lock and execute a group commit.
            self.defer_write(Deferred::GroupCommit);

            // Without the write lock we can only apply writes to system tables.
            let pending_writes = self
                .pending_writes
                .drain_filter_swapping(|w| matches!(w, PendingWriteTxn::System { .. }))
                .collect();
            (None, pending_writes)
        };

        // The value returned here still might be ahead of `now()` if `now()` has gone backwards at
        // any point during this method or if this was triggered from DDL. We will still commit the
        // write without waiting for `now()` to advance. This is ok because the next batch of writes
        // will trigger the wait loop in `try_group_commit()` if `now()` hasn't advanced past the
        // global timeline, preventing an unbounded advancing of the global timeline ahead of
        // `now()`. Additionally DDL is infrequent enough and takes long enough that we don't think
        // it's practical for continuous DDL to advance the global timestamp in an unbounded manner.
        let WriteTimestamp {
            timestamp,
            advance_to,
        } = self.get_local_write_ts().await;

        // While we're flipping on the feature flags for persist-txn tables and
        // the separated Postgres timestamp oracle, we also need to confirm
        // leadership on writes _after_ getting the timestamp and _before_
        // writing anything to table shards. See the big comment on `init_txns`
        // in the Storage controller for details.
        //
        // TODO: Remove this after both (either?) of the above features are on
        // for good and no possibility of running the old code.
        let () = self
            .catalog
            .confirm_leadership()
            .await
            .unwrap_or_terminate("unable to confirm leadership");

        let mut appends: BTreeMap<GlobalId, Vec<(Row, Diff)>> = BTreeMap::new();
        let mut responses = Vec::with_capacity(self.pending_writes.len());
        let mut notifies = Vec::new();

        for pending_write_txn in pending_writes {
            match pending_write_txn {
                PendingWriteTxn::User {
                    span: _,
                    writes,
                    write_lock_guard: _,
                    pending_txn:
                        PendingTxn {
                            ctx,
                            response,
                            action,
                        },
                } => {
                    for WriteOp { id, rows } in writes {
                        // If the table that some write was targeting has been deleted while the
                        // write was waiting, then the write will be ignored and we respond to the
                        // client that the write was successful. This is only possible if the write
                        // and the delete were concurrent. Therefore, we are free to order the
                        // write before the delete without violating any consistency guarantees.
                        if self.catalog().try_get_entry(&id).is_some() {
                            appends.entry(id).or_default().extend(rows);
                        }
                    }
                    if let Some(id) = ctx.extra().contents() {
                        self.set_statement_execution_timestamp(id, timestamp);
                    }

                    responses.push(CompletedClientTransmitter::new(ctx, response, action));
                }
                PendingWriteTxn::System { updates, source } => {
                    for update in updates {
                        appends
                            .entry(update.id)
                            .or_default()
                            .push((update.row, update.diff));
                    }
                    // Once the write completes we notify any waiters.
                    if let BuiltinTableUpdateSource::Internal(tx) = source {
                        notifies.push(tx);
                    }
                }
            }
        }

        for (_, updates) in &mut appends {
            differential_dataflow::consolidation::consolidate(updates);
        }
        // Add table advancements for all tables.
        for table in self.catalog().entries().filter(|entry| entry.is_table()) {
            appends.entry(table.id()).or_default();
        }
        let appends = appends
            .into_iter()
            .map(|(id, updates)| {
                let updates = updates
                    .into_iter()
                    .map(|(row, diff)| TimestamplessUpdate { row, diff })
                    .collect();
                (id, updates)
            })
            .collect();

        // Instrument our table writes since they can block the coordinator.
        let histogram = self
            .metrics
            .append_table_duration_seconds
            .with_label_values(&[]);
        let append_fut = self
            .controller
            .storage
            .append_table(timestamp, advance_to, appends)
            .expect("invalid updates")
            .wall_time()
            .observe(histogram);

        // Spawn a task to do the table writes.
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let apply_write_fut = self.apply_local_write(timestamp);

        task::spawn(
            || "group_commit_apply",
            async move {
                // Wait for the writes to complete.
                match append_fut.await {
                    Ok(append_result) => {
                        append_result.unwrap_or_terminate("cannot fail to apply appends")
                    }
                    Err(_) => warn!("Writer terminated with writes in indefinite state"),
                };

                // Apply the write by marking the timestamp as complete on the timeline.
                apply_write_fut.await;

                // Notify the external clients of the result.
                for response in responses {
                    let (mut ctx, result) = response.finalize();
                    ctx.session_mut().apply_write(timestamp);
                    ctx.retire(result);
                }

                // IMPORTANT: Make sure we hold the permit and write lock until
                // here, to prevent other writes from going through while we
                // haven't yet applied the write at the timestamp oracle.
                drop(permit);
                drop(write_lock_guard);

                // Advance other timelines.
                if let Err(e) = internal_cmd_tx.send(Message::AdvanceTimelines) {
                    warn!("Server closed with non-advanced timelines, {e}");
                }

                for notify in notifies {
                    // We don't care if the listeners have gone away.
                    let _ = notify.send(());
                }
            }
            .instrument(Span::current()),
        );
    }

    /// Applies the results of a completed group commit. The read timestamp of the timeline
    /// containing user tables will be advanced to the timestamp of the completed write, the read
    /// hold on the timeline containing user tables is advanced to the new time, and responses are
    /// sent to all waiting clients.
    ///
    /// It's important that the timeline is advanced before responses are sent so that the client
    /// is guaranteed to see the write.
    ///
    /// We also advance all other timelines and update the read holds of non-realtime
    /// timelines.
    #[instrument(level = "debug")]
    pub(crate) async fn group_commit_apply(
        &mut self,
        timestamp: Timestamp,
        responses: Vec<CompletedClientTransmitter>,
        _write_lock_guard: Option<OwnedMutexGuard<()>>,
        _permit: Option<GroupCommitPermit>,
    ) {
        self.apply_local_write(timestamp).await;
        for response in responses {
            let (mut ctx, result) = response.finalize();
            ctx.session_mut().apply_write(timestamp);
            ctx.retire(result);
        }

        // Advancing timelines will update all timeline read holds, and update the read timestamps
        // of non-realtime timelines. There are no guarantees that we need to provide with the
        // ordering of advancing timelines and user transactions. Updating read holds are only to
        // allow compaction and free some memory. Non-realtime timelines can only be written to by
        // upstream sources, which we don't provide ordering guarantees for with respect to user
        // transactions. We send the `AdvanceTimelines` message here out of convenience, because we
        // know at least the real-time timeline will have a read hold that can be updated.
        self.internal_cmd_tx
            .send(Message::AdvanceTimelines)
            .expect("sending to self.internal_cmd_tx cannot fail");
    }

    /// Submit a write to be executed during the next group commit and trigger a group commit.
    pub(crate) fn submit_write(&mut self, pending_write_txn: PendingWriteTxn) {
        self.pending_writes.push(pending_write_txn);
        self.trigger_group_commit();
    }

    /// Append some [`BuiltinTableUpdate`]s, with various degrees of waiting and blocking.
    pub(crate) fn builtin_table_update<'a>(&'a mut self) -> BuiltinTableAppend<'a> {
        BuiltinTableAppend { coord: self }
    }

    /// Defers executing `deferred` until the write lock becomes available; waiting
    /// occurs in a green-thread, so callers of this function likely want to
    /// return after calling it.
    pub(crate) fn defer_write(&mut self, deferred: Deferred) {
        let id = match &deferred {
            Deferred::Plan(plan) => plan.ctx.session().conn_id().to_string(),
            Deferred::GroupCommit => "group_commit".to_string(),
        };
        self.write_lock_wait_group.push_back(deferred);

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let write_lock = Arc::clone(&self.write_lock);
        // TODO(guswynn): see if there is more relevant info to add to this name
        task::spawn(|| format!("defer_write:{id}"), async move {
            let guard = write_lock.lock_owned().await;
            // It is not an error for this lock to be released after `internal_cmd_rx` to be dropped.
            let result = internal_cmd_tx.send(Message::WriteLockGrant(guard));
            if let Err(e) = result {
                warn!("internal_cmd_rx dropped before we could send: {:?}", e);
            }
        });
    }

    /// Attempts to immediately grant `session` access to the write lock or
    /// errors if the lock is currently held.
    pub(crate) fn try_grant_session_write_lock(
        &self,
        session: &mut Session,
    ) -> Result<(), tokio::sync::TryLockError> {
        Arc::clone(&self.write_lock).try_lock_owned().map(|p| {
            session.grant_write_lock(p);
        })
    }
}

/// Helper struct to run a builtin table append.
pub struct BuiltinTableAppend<'a> {
    coord: &'a mut Coordinator,
}

/// `Future` that notifies when a builtin table write has completed.
///
/// Note: builtin table writes need to talk to persist, which can take 100s of milliseconds. This
/// type allows you to execute a builtin table write, e.g. via [`BuiltinTableAppend::execute`], and
/// wait for it to complete, while other long running tasks are concurrently executing.
pub type BuiltinTableAppendNotify = BoxFuture<'static, ()>;

impl<'a> BuiltinTableAppend<'a> {
    /// Submit a write to a system table to be executed during the next group commit. This method
    /// __does not__ trigger a group commit.
    ///
    /// This is useful for non-critical writes like metric updates because it allows us to piggy
    /// back off the next group commit instead of triggering a potentially expensive group commit.
    ///
    /// Note: __do not__ call this for DDL which needs the system tables updated immediately.
    pub fn background(self, updates: Vec<BuiltinTableUpdate>) {
        self.coord.pending_writes.push(PendingWriteTxn::System {
            updates,
            source: BuiltinTableUpdateSource::Background,
        });
    }

    /// Submits a write to be executed during the next group commit __and__ triggers a group commit.
    ///
    /// Returns a `Future` that resolves when the write has completed, does not block the
    /// Coordinator.
    pub fn defer(self, updates: Vec<BuiltinTableUpdate>) -> BuiltinTableAppendNotify {
        let (tx, rx) = oneshot::channel();
        self.coord.pending_writes.push(PendingWriteTxn::System {
            updates,
            source: BuiltinTableUpdateSource::Internal(tx),
        });
        self.coord.trigger_group_commit();

        Box::pin(rx.map(|_| ()))
    }

    /// Submit a write to a system table.
    ///
    /// This method will block the Coordinator on acquiring a write timestamp from the timestamp
    /// oracle, and then returns a `Future` that will complete once the write has been applied.
    pub async fn execute(self, updates: Vec<BuiltinTableUpdate>) -> BuiltinTableAppendNotify {
        let (tx, rx) = oneshot::channel();

        // Most DDL queries cause writes to system tables. Unlike writes to user tables, system
        // table writes do not wait for a group commit, they explicitly trigger one. There is a
        // possibility that if a user is executing DDL at a rate faster than 1 query per
        // millisecond, then the global timeline will unboundedly advance past the system clock.
        // This can cause future queries to block, but will not affect correctness. Since this
        // rate of DDL is unlikely, we allow DDL to explicitly trigger group commit.
        self.coord.pending_writes.push(PendingWriteTxn::System {
            updates,
            source: BuiltinTableUpdateSource::Internal(tx),
        });
        self.coord.group_commit_initiate(None, None).await;

        // Avoid excessive group commits by resetting the periodic table advancement timer. The
        // group commit triggered by above will already advance all tables.
        self.coord.advance_timelines_interval.reset();

        Box::pin(rx.map(|_| ()))
    }

    /// Submit a write to a system table, blocking until complete.
    ///
    /// Note: if possible you should use the `execute(...)` method, which returns a `Future` that
    /// can be `await`-ed concurrently with other tasks.
    pub async fn blocking(self, updates: Vec<BuiltinTableUpdate>) {
        let notify = self.execute(updates).await;
        notify.await;
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

        GroupCommitPermit(permit)
    }
}

/// A permit to run a group commit, this must be kept alive for the entire duration of the commit.
///
/// Note: We sometimes want to throttle how many group commits are running at once, which this
/// permit allows us to do.
#[derive(Debug)]
pub struct GroupCommitPermit(OwnedSemaphorePermit);
