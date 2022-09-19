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
use tracing::warn;

use mz_ore::task;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_sql::plan::Plan;
use mz_stash::Append;
use mz_storage::protocol::client::Update;

use crate::catalog::BuiltinTableUpdate;
use crate::coord::timeline::WriteTimestamp;
use crate::coord::{Coordinator, Message, PendingTxn};
use crate::session::{Session, WriteOp};
use crate::util::{ClientTransmitter, CompletedClientTransmitter};
use crate::ExecuteResponse;

/// An operation that is deferred while waiting for a lock.
pub(crate) enum Deferred {
    Plan(DeferredPlan),
    GroupCommit,
}

pub(crate) enum GroupCommitState {
    Idle,
    WaitForSystemClock,
    Initiated,
    WaitForLock,
    Append,
    WaitForAppends,
    Apply,
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
    /// TODO(jkosh44)
    pub(crate) async fn initiate_group_commit(&mut self) {
        match self.group_commit_state {
            GroupCommitState::Idle => {
                self.group_commit_state = GroupCommitState::WaitForSystemClock;
                self.wait_for_system_clock().await;
            }
            _ => {}
        }
    }

    /// TODO(jkosh44)
    pub(crate) async fn wait_for_system_clock(&mut self) {
        let timestamp = self.peek_local_write_ts();
        let now = Timestamp::from((self.catalog.config().now)());
        if timestamp > now {
            // Cap retry time to 1s. In cases where the system clock has retreated by
            // some large amount of time, this prevents against then waiting for that
            // large amount of time in case the system clock then advances back to near
            // what it was.
            let remaining_ms = std::cmp::min(timestamp.saturating_sub(now), 1_000.into());
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "group_commit_wait_for_system_clock", async move {
                tokio::time::sleep(Duration::from_millis(remaining_ms.into())).await;
                // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
                let result = internal_cmd_tx.send(Message::GroupCommitWaitForSystemClock);
                if let Err(e) = result {
                    warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                }
            });
        } else {
            self.start_group_commit().await;
        }
    }

    // TODO(jkosh44) Bad name and docs
    async fn start_group_commit(&mut self) {
        self.group_commit_state = GroupCommitState::Initiated;
        let write_lock_guard = if let Some(guard) = self
            .pending_writes
            .iter_mut()
            .find_map(|write| write.take_write_lock())
        {
            Some(guard)
        } else {
            Arc::clone(&self.write_lock).try_lock_owned().ok()
        };

        match write_lock_guard {
            Some(write_lock_guard) => {
                self.append_group_commit(Some(write_lock_guard)).await;
            }
            None if self.pending_writes.is_empty()
                || self
                    .pending_writes
                    .iter()
                    .all(|write| matches!(write, PendingWriteTxn::System { .. })) =>
            {
                self.append_group_commit(None).await;
            }
            None => {
                self.group_commit_state = GroupCommitState::WaitForLock;
                self.defer_write(Deferred::GroupCommit);
            }
        }
    }

    pub(crate) async fn append_group_commit(
        &mut self,
        write_lock_guard: Option<OwnedMutexGuard<()>>,
    ) {
        self.group_commit_state = GroupCommitState::Append;
        let WriteTimestamp {
            timestamp,
            advance_to,
        } = self.get_local_write_ts().await;
        let pending_writes: Vec<_> = self.pending_writes.drain(..).collect();
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
                .unwrap();
            self.apply_group_commit(timestamp, responses).await;
        } else {
            self.group_commit_state = GroupCommitState::WaitForAppends;
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "group_commit_wait_for_appends", async move {
                let _write_lock_guard = write_lock_guard;
                if let Ok(response) = append_fut.await {
                    response.unwrap();
                    if let Err(e) =
                        internal_cmd_tx.send(Message::GroupCommitApply(timestamp, responses))
                    {
                        warn!("Server closed with non-responded writes, {e}");
                    }
                } else {
                    warn!("Writer terminated with writes in indefinite state");
                }
            });
        }
    }

    pub(crate) async fn apply_group_commit(
        &mut self,
        timestamp: mz_repr::Timestamp,
        responses: Vec<CompletedClientTransmitter<ExecuteResponse>>,
    ) {
        self.group_commit_state = GroupCommitState::Apply;
        self.apply_local_write(timestamp).await;
        // TODO(jkosh44) Handle read holds and non-realtime timelines.
        for response in responses {
            response.send();
        }

        self.internal_cmd_tx
            .send(Message::AdvanceTimelines)
            .expect("sending to self.internal_cmd_tx cannot fail");

        self.group_commit_state = GroupCommitState::Idle;
        if !self.pending_writes.is_empty() {
            self.internal_cmd_tx
                .send(Message::GroupCommitInitiate)
                .expect("sending to self.internal_cmd_tx cannot fail")
        }
    }

    /// Submit a write to be executed during the next group commit.
    pub(crate) fn submit_write(&mut self, pending_write_txn: PendingWriteTxn) {
        self.pending_writes.push(pending_write_txn);
        self.internal_cmd_tx
            .send(Message::GroupCommitInitiate)
            .expect("sending to self.internal_cmd_tx cannot fail");
    }

    #[tracing::instrument(level = "debug", skip_all, fields(updates = updates.len()))]
    pub(crate) async fn send_builtin_table_updates(
        &mut self,
        updates: Vec<BuiltinTableUpdate>,
        source: BuiltinTableUpdateSource,
    ) {
        self.pending_writes
            .extend(updates.into_iter().map(|update| PendingWriteTxn::System {
                update,
                source: source.clone(),
            }));
        self.internal_cmd_tx
            .send(Message::GroupCommitInitiate)
            .expect("sending to self.internal_cmd_tx cannot fail");
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
