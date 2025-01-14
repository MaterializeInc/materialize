// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic and types for all appends executed by the [`Coordinator`].

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use futures::future::{BoxFuture, FutureExt};
use mz_adapter_types::connection::ConnectionId;
use mz_ore::metrics::MetricsFutureExt;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{assert_none, instrument};
use mz_repr::{CatalogItemId, Timestamp};
use mz_sql::names::ResolvedIds;
use mz_sql::plan::Plan;
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_client::client::TableData;
use mz_timestamp_oracle::WriteTimestamp;
use smallvec::SmallVec;
use tokio::sync::{oneshot, Notify, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};
use tracing::{debug_span, info, warn, Instrument, Span};

use crate::catalog::BuiltinTableUpdate;
use crate::coord::{Coordinator, Message, PendingTxn, PlanValidity};
use crate::session::{GroupCommitWriteLocks, WriteLocks};
use crate::util::{CompletedClientTransmitter, ResultExt};
use crate::{AdapterError, ExecuteContext};

/// An operation that was deferred waiting for [`WriteLocks`].
#[derive(Debug)]
pub enum DeferredWriteOp {
    /// A plan, e.g. ReadThenWrite, that needs locks before sequencing.
    Plan(DeferredPlan),
    /// Inserts into a collection.
    Write(DeferredWrite),
}

impl DeferredWriteOp {
    /// Certain operations, e.g. "blind writes"/`INSERT` statements, can be optimistically retried
    /// because we can share a write lock between multiple operations. In this case we wait to
    /// acquire the locks until [`group_commit`], where writes are groupped by collection and
    /// comitted at a single timestamp.
    ///
    /// Other operations, e.g. read-then-write plans/`UPDATE` statements, must uniquely hold their
    /// write locks and thus we should acquire the locks in [`try_deferred`] to prevent multiple
    /// queued plans attempting to get retried at the same time, when we know only one can proceed.
    ///
    /// [`try_deferred`]: crate::coord::Coordinator::try_deferred
    /// [`group_commit`]: crate::coord::Coordinator::group_commit
    pub(crate) fn can_be_optimistically_retried(&self) -> bool {
        match self {
            DeferredWriteOp::Plan(_) => false,
            DeferredWriteOp::Write(_) => true,
        }
    }

    /// Returns an Iterator of all the required locks for current operation.
    pub fn required_locks(&self) -> impl Iterator<Item = CatalogItemId> + '_ {
        match self {
            DeferredWriteOp::Plan(plan) => {
                let iter = plan.requires_locks.iter().copied();
                itertools::Either::Left(iter)
            }
            DeferredWriteOp::Write(write) => {
                let iter = write.writes.keys().copied();
                itertools::Either::Right(iter)
            }
        }
    }

    /// Returns the [`ConnectionId`] associated with this deferred op.
    pub fn conn_id(&self) -> &ConnectionId {
        match self {
            DeferredWriteOp::Plan(plan) => plan.ctx.session().conn_id(),
            DeferredWriteOp::Write(write) => write.pending_txn.ctx.session().conn_id(),
        }
    }

    /// Consumes the [`DeferredWriteOp`], returning the inner [`ExecuteContext`].
    pub fn into_ctx(self) -> ExecuteContext {
        match self {
            DeferredWriteOp::Plan(plan) => plan.ctx,
            DeferredWriteOp::Write(write) => write.pending_txn.ctx,
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
    Background,
}

/// A pending write transaction that will be committing during the next group commit.
#[derive(Debug)]
pub(crate) enum PendingWriteTxn {
    /// Write to a user table.
    User {
        span: Span,
        /// List of all write operations within the transaction.
        writes: BTreeMap<CatalogItemId, SmallVec<[TableData; 1]>>,
        /// If they exist, should contain locks for each [`CatalogItemId`] in `writes`.
        write_locks: Option<WriteLocks>,
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

impl Coordinator {
    /// Send a message to the Coordinate to start a group commit.
    pub(crate) fn trigger_group_commit(&mut self) {
        self.group_commit_tx.notify();
        // Avoid excessive `Message::GroupCommitInitiate` by resetting the periodic table
        // advancement. The group commit triggered by the message above will already advance all
        // tables.
        self.advance_timelines_interval.reset();
    }

    /// Tries to execute a previously [`DeferredWriteOp`] that requires write locks.
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
                        let acquire_future = self.grant_object_write_lock(failed_to_acquire);
                        self.defer_op(acquire_future, op);
                        return;
                    }
                };

                Some(locks)
            }
            None => None,
        };

        match op {
            DeferredWriteOp::Plan(mut deferred) => {
                if let Err(e) = deferred.validity.check(self.catalog()) {
                    deferred.ctx.retire(Err(e))
                } else {
                    // Write statements never need to track resolved IDs (NOTE: This is not the
                    // same thing as plan dependencies, which we do need to re-validate).
                    let resolved_ids = ResolvedIds::empty();

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
                    self.sequence_plan(deferred.ctx, deferred.plan, resolved_ids)
                        .await;
                }
            }
            DeferredWriteOp::Write(DeferredWrite {
                span,
                writes,
                pending_txn,
            }) => {
                self.submit_write(PendingWriteTxn::User {
                    span,
                    writes,
                    write_locks,
                    pending_txn,
                });
            }
        }
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
            self.group_commit(permit).await;
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
    ///
    /// Returns the timestamp of the write.
    #[instrument(name = "coord::group_commit")]
    pub(crate) async fn group_commit(&mut self, permit: Option<GroupCommitPermit>) -> Timestamp {
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
                    pending_txn,
                } => match write_locks.validate(writes.keys().copied()) {
                    Ok(validated_locks) => {
                        // Merge all of our write locks together since we can allow concurrent
                        // writes at the same timestamp.
                        group_write_locks.merge(validated_locks);

                        let validated_write = PendingWriteTxn::User {
                            span,
                            writes,
                            write_locks: None,
                            pending_txn,
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
                            missing,
                            writes,
                            pending_txn,
                        );
                    }
                },
                // If we don't have any locks, try to acquire them, otherwise defer the write.
                PendingWriteTxn::User {
                    span,
                    writes,
                    write_locks: None,
                    pending_txn,
                } => {
                    let missing = group_write_locks.missing_locks(writes.keys().copied());

                    if missing.is_empty() {
                        // We have all the locks! Queue the pending write.
                        let validated_write = PendingWriteTxn::User {
                            span,
                            writes,
                            write_locks: None,
                            pending_txn,
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
                                    pending_txn,
                                };
                                validated_writes.push(validated_write);
                            }
                            // Darn. We couldn't acquire the locks, defer the write.
                            Err(missing) => {
                                let acquire_future = self.grant_object_write_lock(missing);
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
            self.defer_op(acquire_future, DeferredWriteOp::Write(write));
        }

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

        // While we're flipping on the feature flags for txn-wal tables and
        // the separated Postgres timestamp oracle, we also need to confirm
        // leadership on writes _after_ getting the timestamp and _before_
        // writing anything to table shards.
        //
        // TODO: Remove this after both (either?) of the above features are on
        // for good and no possibility of running the old code.
        let () = self
            .catalog
            .confirm_leadership()
            .await
            .unwrap_or_terminate("unable to confirm leadership");

        let mut appends: BTreeMap<CatalogItemId, SmallVec<[TableData; 1]>> = BTreeMap::new();
        let mut responses = Vec::with_capacity(validated_writes.len());
        let mut notifies = Vec::new();

        for validated_write_txn in validated_writes {
            match validated_write_txn {
                PendingWriteTxn::User {
                    span: _,
                    writes,
                    write_locks,
                    pending_txn:
                        PendingTxn {
                            ctx,
                            response,
                            action,
                        },
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
                        self.set_statement_execution_timestamp(id, timestamp);
                    }

                    responses.push(CompletedClientTransmitter::new(ctx, response, action));
                }
                PendingWriteTxn::System { updates, source } => {
                    for update in updates {
                        let data = TableData::Rows(vec![(update.row, update.diff)]);
                        appends.entry(update.id).or_default().push(data);
                    }
                    // Once the write completes we notify any waiters.
                    if let BuiltinTableUpdateSource::Internal(tx) = source {
                        notifies.push(tx);
                    }
                }
            }
        }

        // Add table advancements for all tables.
        for table in self.catalog().entries().filter(|entry| entry.is_table()) {
            appends.entry(table.id()).or_default();
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

        // Log non-empty user appends.
        let modified_tables: Vec<_> = appends
            .iter()
            .filter_map(|(id, updates)| {
                if id.is_user() && !updates.iter().all(|u| u.is_empty()) {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();
        if !modified_tables.is_empty() {
            info!("Appending to tables, {modified_tables:?}, at {timestamp}, advancing to {advance_to}");
        }
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

        let span = debug_span!(parent: None, "group_commit_apply");
        OpenTelemetryContext::obtain().attach_as_parent_to(&span);
        task::spawn(
            || "group_commit_apply",
            async move {
                // Wait for the writes to complete.
                match append_fut
                    .instrument(debug_span!("group_commit_apply::append_fut"))
                    .await
                {
                    Ok(append_result) => {
                        append_result.unwrap_or_terminate("cannot fail to apply appends")
                    }
                    Err(_) => warn!("Writer terminated with writes in indefinite state"),
                };

                // Apply the write by marking the timestamp as complete on the timeline.
                apply_write_fut
                    .instrument(debug_span!("group_commit_apply::append_write_fut"))
                    .await;

                // Notify the external clients of the result.
                for response in responses {
                    let (mut ctx, result) = response.finalize();
                    ctx.session_mut().apply_write(timestamp);
                    ctx.retire(result);
                }

                // IMPORTANT: Make sure we hold the permit and write locks
                // until here, to prevent other writes from going through while
                // we haven't yet applied the write at the timestamp oracle.
                drop(permit);
                drop(group_write_locks);

                // Advance other timelines.
                if let Err(e) = internal_cmd_tx.send(Message::AdvanceTimelines) {
                    warn!("Server closed with non-advanced timelines, {e}");
                }

                for notify in notifies {
                    // We don't care if the listeners have gone away.
                    let _ = notify.send(());
                }
            }
            .instrument(span),
        );

        timestamp
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

    pub(crate) fn defer_op<F>(&mut self, acquire_future: F, op: DeferredWriteOp)
    where
        F: Future<Output = (CatalogItemId, tokio::sync::OwnedMutexGuard<()>)> + Send + 'static,
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

            // If the op can be optimistically retried, don't hold onto the lock so other similar
            // ops might be queued at the same time.
            let acquired_lock = if is_optimistic {
                None
            } else {
                Some(acquired_lock)
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
    ///
    /// Note: When in read-only mode, this will buffer the update and return
    /// immediately.
    pub fn background(self, mut updates: Vec<BuiltinTableUpdate>) {
        if self.coord.controller.read_only() {
            self.coord
                .buffered_builtin_table_updates
                .as_mut()
                .expect("in read-only mode")
                .append(&mut updates);

            return;
        }

        self.coord.pending_writes.push(PendingWriteTxn::System {
            updates,
            source: BuiltinTableUpdateSource::Background,
        });
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

            return futures::future::ready(()).boxed();
        }

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
    /// oracle, and then returns a `Future` that will complete once the write has been applied and
    /// the write timestamp.
    ///
    /// Note: When in read-only mode, this will buffer the update, the
    /// returned future will resolve immediately, without the update actually
    /// having been written, and no timestamp is returned.
    pub async fn execute(
        self,
        mut updates: Vec<BuiltinTableUpdate>,
    ) -> (BuiltinTableAppendNotify, Option<Timestamp>) {
        if self.coord.controller.read_only() {
            self.coord
                .buffered_builtin_table_updates
                .as_mut()
                .expect("in read-only mode")
                .append(&mut updates);

            return (futures::future::ready(()).boxed(), None);
        }

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
        let write_ts = self.coord.group_commit(None).await;

        // Avoid excessive group commits by resetting the periodic table advancement timer. The
        // group commit triggered by above will already advance all tables.
        self.coord.advance_timelines_interval.reset();

        (Box::pin(rx.map(|_| ())), Some(write_ts))
    }

    /// Submit a write to a system table, blocking until complete.
    ///
    /// Note: if possible you should use the `execute(...)` method, which returns a `Future` that
    /// can be `await`-ed concurrently with other tasks.
    ///
    /// Note: When in read-only mode, this will buffer the update and the
    /// returned future will resolve immediately, without the update actually
    /// having been written.
    pub async fn blocking(self, updates: Vec<BuiltinTableUpdate>) {
        let (notify, _) = self.execute(updates).await;
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
pub struct GroupCommitPermit(#[allow(dead_code)] OwnedSemaphorePermit);
