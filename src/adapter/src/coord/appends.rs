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
use mz_ore::task;
use mz_ore::vec::VecExt;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_sql::plan::Plan;
use mz_storage_client::client::Update;
use tokio::sync::OwnedMutexGuard;
use tracing::warn;

use crate::catalog::BuiltinTableUpdate;
use crate::coord::timeline::WriteTimestamp;
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
#[derive(Clone, Debug)]
pub(crate) enum BuiltinTableUpdateSource {
    /// Update was triggered by some DDL.
    DDL,
    /// Update was triggered by some background process, such as periodic heartbeats from COMPUTE.
    Background,
}

/// A pending write transaction that will be committing during the next group commit.
#[derive(Debug)]
pub(crate) enum PendingWriteTxn {
    /// Write to a user table.
    User {
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

    /// Returns true if this transaction should cause group commit to block and not be executed
    /// asynchronously.
    fn should_block(&self) -> bool {
        match self {
            PendingWriteTxn::User { .. } => false,
            PendingWriteTxn::System { source, .. } => match source {
                BuiltinTableUpdateSource::DDL => true,
                BuiltinTableUpdateSource::Background => false,
            },
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
    ($coord:expr, $ctx:expr, $plan_to_defer:expr, $source_ids:expr) => {
        if !$ctx.session().has_write_lock() {
            if $coord
                .try_grant_session_write_lock($ctx.session_mut())
                .is_err()
            {
                $coord.defer_write(Deferred::Plan(DeferredPlan {
                    ctx: $ctx,
                    plan: $plan_to_defer,
                    validity: PlanValidity {
                        transient_revision: $coord.catalog().transient_revision(),
                        source_ids: $source_ids,
                        cluster_id: None,
                        replica_id: None,
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
        self.internal_cmd_tx
            .send(Message::GroupCommitInitiate)
            .expect("sending to self.internal_cmd_tx cannot fail");
        // Avoid excessive `Message::GroupCommitInitiate` by resetting the periodic table
        // advancement. The group commit triggered by the message above will already advance all
        // tables.
        self.advance_timelines_interval.reset();
    }

    /// Attempts to commit all pending write transactions in a group commit. If the timestamp
    /// chosen for the writes is not ahead of `now()`, then we can execute and commit the writes
    /// immediately. Otherwise we must wait for `now()` to advance past the timestamp chosen for the
    /// writes.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn try_group_commit(&mut self) {
        let timestamp = self.peek_local_write_ts();
        let now = Timestamp::from((self.catalog().config().now)());
        if timestamp > now {
            // Cap retry time to 1s. In cases where the system clock has retreated by
            // some large amount of time, this prevents against then waiting for that
            // large amount of time in case the system clock then advances back to near
            // what it was.
            let remaining_ms = std::cmp::min(timestamp.saturating_sub(now), 1_000.into());
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "group_commit_initiate", async move {
                tokio::time::sleep(Duration::from_millis(remaining_ms.into())).await;
                // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
                let result = internal_cmd_tx.send(Message::GroupCommitInitiate);
                if let Err(e) = result {
                    warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                }
            });
        } else {
            self.group_commit_initiate(None).await;
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
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn group_commit_initiate(
        &mut self,
        write_lock_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
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
        let mut appends: BTreeMap<GlobalId, Vec<(Row, Diff)>> = BTreeMap::new();
        let mut responses = Vec::with_capacity(self.pending_writes.len());
        let should_block = pending_writes.iter().any(|write| write.should_block());
        for pending_write_txn in pending_writes {
            match pending_write_txn {
                PendingWriteTxn::User {
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
                    responses.push(CompletedClientTransmitter::new(ctx, response, action));
                }
                PendingWriteTxn::System { updates, .. } => {
                    for update in updates {
                        appends
                            .entry(update.id)
                            .or_default()
                            .push((update.row, update.diff));
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
                    .map(|(row, diff)| Update {
                        row,
                        diff,
                        timestamp,
                    })
                    .collect();
                (id, updates, advance_to)
            })
            .collect();

        let append_fut = self
            .controller
            .storage
            .append(appends)
            .expect("invalid updates");
        if should_block {
            // We may panic here if the storage controller has shut down, because we cannot
            // correctly return control, nor can we simply hang here.
            // TODO: Clean shutdown.
            append_fut
                .await
                .expect("One-shot dropped while waiting synchronously")
                .unwrap_or_terminate("cannot fail to apply appends");
            self.group_commit_apply(timestamp, responses, write_lock_guard)
                .await;
        } else {
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "group_commit_apply", async move {
                if let Ok(response) = append_fut.await {
                    response.unwrap_or_terminate("cannot fail to apply appends");
                    if let Err(e) = internal_cmd_tx.send(Message::GroupCommitApply(
                        timestamp,
                        responses,
                        write_lock_guard,
                    )) {
                        warn!("Server closed with non-responded writes, {e}");
                    }
                } else {
                    warn!("Writer terminated with writes in indefinite state");
                }
            });
        }
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
    #[tracing::instrument(level = "debug", skip(self, responses))]
    pub(crate) async fn group_commit_apply(
        &mut self,
        timestamp: Timestamp,
        responses: Vec<CompletedClientTransmitter>,
        _write_lock_guard: Option<OwnedMutexGuard<()>>,
    ) {
        self.apply_local_write(timestamp).await;
        for response in responses {
            let (ctx, result) = response.finalize();
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

    /// Submit a write to a system table be executed during the next group commit. This method does
    /// not trigger a group commit.
    ///
    /// This is useful for non-critical writes like metric updates because it allows us to piggy
    /// back off the next group commit instead of triggering a potentially expensive group commit.
    ///
    /// DO NOT call this for DDL which needs the system tables updated immediately.
    #[tracing::instrument(level = "debug", skip_all, fields(updates = updates.len()))]
    pub(crate) fn buffer_builtin_table_updates(&mut self, updates: Vec<BuiltinTableUpdate>) {
        self.pending_writes.push(PendingWriteTxn::System {
            updates,
            source: BuiltinTableUpdateSource::Background,
        });
    }

    /// Submit a write to a system table. This method will block until the write has been applied.
    #[tracing::instrument(level = "debug", skip_all, fields(updates = updates.len()))]
    pub(crate) async fn send_builtin_table_updates(&mut self, updates: Vec<BuiltinTableUpdate>) {
        // Most DDL queries cause writes to system tables. Unlike writes to user tables, system
        // table writes do not wait for a group commit, they explicitly trigger one. There is a
        // possibility that if a user is executing DDL at a rate faster than 1 query per
        // millisecond, then the global timeline will unboundedly advance past the system clock.
        // This can cause future queries to block, but will not affect correctness. Since this
        // rate of DDL is unlikely, we allow DDL to explicitly trigger group commit.
        self.pending_writes.push(PendingWriteTxn::System {
            updates,
            source: BuiltinTableUpdateSource::DDL,
        });
        self.group_commit_initiate(None).await;
        // Avoid excessive group commits by resetting the periodic table advancement timer. The
        // group commit triggered by above will already advance all tables.
        self.advance_timelines_interval.reset();
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
