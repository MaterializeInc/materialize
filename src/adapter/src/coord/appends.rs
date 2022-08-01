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
use tokio::sync::OwnedMutexGuard;

use mz_ore::task;
use mz_repr::{Diff, GlobalId, Row};
use mz_sql::plan::Plan;
use mz_stash::Append;
use mz_storage::protocol::client::Update;

use crate::catalog::BuiltinTableUpdate;
use crate::coord::timeline::WriteTimestamp;
use crate::coord::{Coordinator, Message, PendingTxn};
use crate::session::{Session, WriteOp};
use crate::util::ClientTransmitter;
use crate::ExecuteResponse;

#[derive(Debug)]
pub struct AdvanceLocalInput<T> {
    advance_to: T,
    ids: Vec<GlobalId>,
}

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
    System(BuiltinTableUpdate),
}

impl PendingWriteTxn {
    fn take_write_lock(&mut self) -> Option<OwnedMutexGuard<()>> {
        match self {
            PendingWriteTxn::User {
                write_lock_guard, ..
            } => std::mem::take(write_lock_guard),
            PendingWriteTxn::System(_) => None,
        }
    }
}

impl From<BuiltinTableUpdate> for PendingWriteTxn {
    fn from(update: BuiltinTableUpdate) -> Self {
        Self::System(update)
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
        if self.pending_writes.is_empty() {
            return;
        }

        // If we need to sleep below, then it's possible that some DDL or table advancements may
        // execute while we sleep. In that case, the DDL or table advancement will use some
        // timestamp greater than or equal to the time that we peeked, closing the peeked time and
        // making it invalid for future writes. Therefore, we must get a new valid timestamp
        // everytime this method is called.
        let timestamp = self.peek_local_ts();
        let now = (self.catalog.config().now)();
        if timestamp > now {
            // Cap retry time to 1s. In cases where the system clock has retreated by
            // some large amount of time, this prevents against then waiting for that
            // large amount of time in case the system clock then advances back to near
            // what it was.
            let remaining_ms = std::cmp::min(timestamp.saturating_sub(now), 1_000);
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "group_commit", async move {
                tokio::time::sleep(Duration::from_millis(remaining_ms)).await;
                internal_cmd_tx
                    .send(Message::GroupCommit)
                    .expect("sending to internal_cmd_tx cannot fail");
            });
        } else {
            self.group_commit().await;
        }
    }

    /// Tries to commit all pending writes transactions at the same timestamp. If the `write_lock`
    /// is currently locked, then only writes to system tables will be applied. If the `write_lock`
    /// is not currently locked, then group commit will acquire it and all writes will be applied.
    ///
    /// All applicable pending writes will be combined into a single Append command and sent to
    /// STORAGE as a single batch. All applicable writes will happen at the same timestamp and all
    /// involved tables will be advanced to some timestamp larger than the timestamp of the write.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn group_commit(&mut self) {
        if self.pending_writes.is_empty() {
            return;
        }

        let (_write_lock_guard, pending_writes): (_, Vec<_>) = if self
            .pending_writes
            .iter()
            .all(|write| matches!(write, PendingWriteTxn::System { .. }))
        {
            // If all pending transactions are for system tables, then we don't need the write lock.
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
                if matches!(&self.pending_writes[i], PendingWriteTxn::System(_)) {
                    pending_writes.push(self.pending_writes.swap_remove(i));
                } else {
                    i += 1;
                }
            }
            if pending_writes.is_empty() {
                return;
            }
            (None, pending_writes)
        };

        // The value returned here still might be ahead of `now()` if `now()` has gone backwards at
        // any point during this method or if this was triggered from DDL. We will still commit the
        // write without waiting for `now()`to advance. This is ok because the next batch of writes
        // will trigger the wait loop in `try_group_commit()` if `now()` hasn't advanced past the
        // global timeline, preventing an unbounded advancing of the global timeline ahead of
        // `now()`. Additionally DDL is infrequent enough and takes long enough that we don't think
        // it's practical for continuous DDL to advance the global timestamp in an unbounded manner.
        let WriteTimestamp {
            timestamp,
            advance_to,
        } = self.get_and_step_local_write_ts().await;
        let mut appends: HashMap<GlobalId, Vec<(Row, Diff)>> =
            HashMap::with_capacity(self.pending_writes.len());
        let mut responses = Vec::with_capacity(self.pending_writes.len());
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
                        // If the table that some write was targeting has been deleted while the write was
                        // waiting, then the write will be ignored and we respond to the client that the
                        // write was successful. This is only possible if the write and the delete were
                        // concurrent. Therefore, we are free to order the write before the delete without
                        // violating any consistency guarantees.
                        if self.catalog.try_get_entry(&id).is_some() {
                            appends.entry(id).or_default().extend(rows);
                        }
                    }
                    responses.push((client_transmitter, response, session, action));
                }
                PendingWriteTxn::System(update) => {
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
        appends.retain(|_key, updates| !updates.is_empty());
        let appends: Vec<_> = appends
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
        if !appends.is_empty() {
            self.controller
                .storage_mut()
                .append(appends)
                .expect("invalid updates")
                .await
                .expect("One-shot shouldn't fail")
                .unwrap();
        }
        for (client_transmitter, response, mut session, action) in responses {
            session.vars_mut().end_transaction(action);
            client_transmitter.send(response, session);
        }
    }

    /// Submit a write to be executed during the next group commit.
    pub(crate) fn submit_write(&mut self, pending_write_txn: PendingWriteTxn) {
        self.internal_cmd_tx
            .send(Message::GroupCommit)
            .expect("sending to internal_cmd_tx cannot fail");
        self.pending_writes.push(pending_write_txn);
    }

    #[tracing::instrument(level = "debug", skip_all, fields(updates = updates.len()))]
    pub(crate) async fn send_builtin_table_updates(&mut self, updates: Vec<BuiltinTableUpdate>) {
        // Most DDL queries cause writes to system tables. Unlike writes to user tables, system
        // table writes do not wait for a group commit, they explicitly trigger one. There is a
        // possibility that if a user is executing DDL at a rate faster than 1 query per
        // millisecond, then the global timeline will unboundedly advance past the system clock.
        // This can cause future queries to block, but will not affect correctness. Since this
        // rate of DDL is unlikely, we allow DDL to explicitly trigger group commit.
        self.pending_writes
            .extend(updates.into_iter().map(|update| update.into()));
        self.group_commit().await;
    }

    /// Enqueue requests to advance all local inputs (tables) to the current wall
    /// clock or at least a time greater than any previous table read (if wall
    /// clock has gone backward).
    pub(crate) async fn queue_local_input_advances(&mut self, advance_to: mz_repr::Timestamp) {
        self.internal_cmd_tx
            .send(Message::AdvanceLocalInput(AdvanceLocalInput {
                advance_to,
                ids: self
                    .catalog
                    .entries()
                    .filter_map(|e| {
                        if e.is_table() || e.is_storage_collection() {
                            Some(e.id())
                        } else {
                            None
                        }
                    })
                    .collect(),
            }))
            .expect("sending to internal_cmd_tx cannot fail");
    }

    /// Advance a local input (table). This downgrades the capability of a table,
    /// which means that it can no longer produce new data before this timestamp.
    #[tracing::instrument(level = "debug", skip_all, fields(num_tables = inputs.ids.len()))]
    pub(crate) async fn advance_local_inputs(
        &mut self,
        inputs: AdvanceLocalInput<mz_repr::Timestamp>,
    ) {
        let storage = self.controller.storage();
        let appends = inputs
            .ids
            .into_iter()
            .filter_map(|id| {
                if self.catalog.try_get_entry(&id).is_none()
                    || !storage
                        .collection(id)
                        .unwrap()
                        .write_frontier
                        .less_than(&inputs.advance_to)
                {
                    // Filter out tables that were dropped while waiting for advancement.
                    // Filter out tables whose upper is already advanced. This is not needed for
                    // correctness (advance_to and write_frontier should be equal here), just
                    // performance, as it's a no-op.
                    None
                } else {
                    Some((id, vec![], inputs.advance_to))
                }
            })
            .collect::<Vec<_>>();
        // Note: Do not await the result; let it drop (as we don't need to block on completion)
        // The error that could be return
        self.controller
            .storage_mut()
            .append(appends)
            .expect("Empty updates cannot be invalid");
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
            internal_cmd_tx
                .send(Message::WriteLockGrant(guard))
                .expect("sending to internal_cmd_tx cannot fail");
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
