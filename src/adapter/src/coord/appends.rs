// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic and types for all appends executed by the [`Coordinator`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use timely::PartialOrder;
use tokio::sync::OwnedMutexGuard;
use tracing::warn;

use mz_ore::task;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_sql::plan::Plan;
use mz_stash::Append;
use mz_storage::protocol::client::Update;
use mz_storage::types::sources::Timeline;

use crate::catalog::BuiltinTableUpdate;
use crate::coord::timeline::{TimelineState, WriteTimestamp};
use crate::coord::{Coordinator, Message, PendingTxn};
use crate::session::{Session, WriteOp};
use crate::util::{ClientTransmitter, CompletedClientTransmitter};
use crate::ExecuteResponse;

/// An operation that is deferred while waiting for a lock.
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
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub session: Session,
    pub plan: Plan,
}

/// Describes what action triggered an update to a builtin table.
#[derive(Clone)]
pub(crate) enum BuiltinTableUpdateSource {
    /// Update was triggered by some DDL.
    DDL,
    /// Update was triggered by some background process, such as periodic heartbeats from COMPUTE.
    Background,
}

/// A pending write transaction that will be committing during the next group commit.
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
        update: BuiltinTableUpdate,
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
    ($coord:expr, $tx:expr, $session:expr, $plan_to_defer: expr) => {
        if !$session.has_write_lock() {
            if $coord.try_grant_session_write_lock(&mut $session).is_err() {
                $coord.defer_write(Deferred::Plan(DeferredPlan {
                    tx: $tx,
                    session: $session,
                    plan: $plan_to_defer,
                }));
                return;
            }
        }
    };
}

impl<S: Append + 'static> Coordinator<S> {
    /// Attempts to commit all pending write transactions in a group commit. If the timestamp
    /// chosen for the writes is not ahead of `now()`, then we can execute and commit the writes
    /// immediately. Otherwise we must wait for `now()` to advance past the timestamp chosen for the
    /// writes.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn try_group_commit(&mut self) {
        let timestamp = self.peek_local_write_ts();
        let now = (self.catalog.config().now)();
        if timestamp > now {
            // Cap retry time to 1s. In cases where the system clock has retreated by
            // some large amount of time, this prevents against then waiting for that
            // large amount of time in case the system clock then advances back to near
            // what it was.
            let remaining_ms = std::cmp::min(timestamp.saturating_sub(now), 1_000);
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "group_commit_initiate", async move {
                tokio::time::sleep(Duration::from_millis(remaining_ms)).await;
                // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
                let result = internal_cmd_tx.send(Message::GroupCommitInitiate);
                if let Err(e) = result {
                    warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                }
            });
        } else {
            self.group_commit_initiate().await;
        }
    }

    /// Tries to commit all pending writes transactions at the same timestamp. If the `write_lock`
    /// is currently locked, then only writes to system tables and table advancements will be
    /// applied. If the `write_lock` is not currently locked, then group commit will acquire it and
    /// all writes will be applied.
    ///
    /// All applicable pending writes will be combined into a single Append command and sent to
    /// STORAGE as a single batch. All applicable writes will happen at the same timestamp and all
    /// involved tables will be advanced to some timestamp larger than the timestamp of the write.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn group_commit_initiate(&mut self) {
        let (write_lock_guard, pending_writes): (_, Vec<_>) = if self
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
            // TODO(jkosh44) replace with drain_filter when it's stable.
            let mut pending_writes = Vec::new();
            let mut i = 0;
            while i < self.pending_writes.len() {
                if matches!(&self.pending_writes[i], PendingWriteTxn::System { .. }) {
                    pending_writes.push(self.pending_writes.swap_remove(i));
                } else {
                    i += 1;
                }
            }
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
        let mut appends: HashMap<GlobalId, Vec<(Row, Diff)>> =
            HashMap::with_capacity(self.pending_writes.len());
        let mut responses = Vec::with_capacity(self.pending_writes.len());
        let should_block = pending_writes.iter().any(|write| write.should_block());
        for pending_write_txn in pending_writes {
            match pending_write_txn {
                PendingWriteTxn::User {
                    writes,
                    write_lock_guard: _,
                    pending_txn:
                        PendingTxn {
                            client_transmitter,
                            response,
                            session,
                            action,
                        },
                } => {
                    for WriteOp { id, rows } in writes {
                        // If the table that some write was targeting has been deleted while the
                        // write was waiting, then the write will be ignored and we respond to the
                        // client that the write was successful. This is only possible if the write
                        // and the delete were concurrent. Therefore, we are free to order the
                        // write before the delete without violating any consistency guarantees.
                        if self.catalog.try_get_entry(&id).is_some() {
                            appends.entry(id).or_default().extend(rows);
                        }
                    }
                    responses.push(CompletedClientTransmitter::new(
                        client_transmitter,
                        response,
                        session,
                        action,
                    ));
                }
                PendingWriteTxn::System { update, .. } => {
                    appends
                        .entry(update.id)
                        .or_default()
                        .push((update.row, update.diff));
                }
            }
        }

        for (_, updates) in &mut appends {
            differential_dataflow::consolidation::consolidate(updates);
        }
        // Add table advancements for all tables.
        for table in self.catalog.entries().filter(|entry| entry.is_table()) {
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
            .storage_mut()
            .append(appends)
            .expect("invalid updates");
        if should_block {
            // We may panic here if the storage controller has shut down, because we cannot
            // correctly return control, nor can we simply hang here.
            // TODO: Clean shutdown.
            append_fut
                .await
                .expect("One-shot dropped while waiting synchronously")
                .unwrap();
            self.group_commit_apply(timestamp, responses, true, write_lock_guard)
                .await;
        } else {
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "group_commit_apply", async move {
                if let Ok(response) = append_fut.await {
                    response.unwrap();
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
        responses: Vec<CompletedClientTransmitter<ExecuteResponse>>,
        in_ddl: bool,
        _write_lock_guard: Option<OwnedMutexGuard<()>>,
    ) {
        self.apply_local_write(timestamp).await;

        // If we're in the middle of DDL then the catalog and COMPUTE/STORAGE may be out of sync,
        // so we don't advance other timelines and we don't update read holds.
        if !in_ddl {
            let global_timelines = std::mem::take(&mut self.global_timelines);
            for (
                timeline,
                TimelineState {
                    mut oracle,
                    mut read_holds,
                },
            ) in global_timelines
            {
                let now = if timeline == Timeline::EpochMilliseconds {
                    timestamp
                } else {
                    // For non realtime sources, we define now as the largest timestamp, not in
                    // advance of any object's upper. This is the largest timestamp that is closed
                    // to writes.
                    let id_bundle = self.ids_in_timeline(&timeline);
                    self.largest_not_in_advance_of_upper(&id_bundle)
                };
                oracle
                    .apply_write(now, |ts| self.catalog.persist_timestamp(&timeline, ts))
                    .await;
                let read_ts = oracle.read_ts();
                if read_holds.time.less_than(&read_ts) {
                    read_holds = self.update_read_hold(read_holds, read_ts).await;
                }
                self.global_timelines
                    .insert(timeline, TimelineState { oracle, read_holds });
            }
        }

        for response in responses {
            response.send();
        }
    }

    /// Submit a write to be executed during the next group commit.
    pub(crate) fn submit_write(&mut self, pending_write_txn: PendingWriteTxn) {
        self.internal_cmd_tx
            .send(Message::GroupCommitInitiate)
            .expect("sending to self.internal_cmd_tx cannot fail");
        self.pending_writes.push(pending_write_txn);
    }

    #[tracing::instrument(level = "debug", skip_all, fields(updates = updates.len()))]
    pub(crate) async fn send_builtin_table_updates(
        &mut self,
        updates: Vec<BuiltinTableUpdate>,
        source: BuiltinTableUpdateSource,
    ) {
        // Most DDL queries cause writes to system tables. Unlike writes to user tables, system
        // table writes do not wait for a group commit, they explicitly trigger one. There is a
        // possibility that if a user is executing DDL at a rate faster than 1 query per
        // millisecond, then the global timeline will unboundedly advance past the system clock.
        // This can cause future queries to block, but will not affect correctness. Since this
        // rate of DDL is unlikely, we allow DDL to explicitly trigger group commit.
        self.pending_writes
            .extend(updates.into_iter().map(|update| PendingWriteTxn::System {
                update,
                source: source.clone(),
            }));
        self.group_commit_initiate().await;
    }

    /// Defers executing `deferred` until the write lock becomes available; waiting
    /// occurs in a green-thread, so callers of this function likely want to
    /// return after calling it.
    pub(crate) fn defer_write(&mut self, deferred: Deferred) {
        let id = match &deferred {
            Deferred::Plan(plan) => plan.session.conn_id().to_string(),
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
