// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::client::ConnectionId;
use crate::command::Command;
use crate::coord::{ConnMeta, Coordinator, Message};
use crate::session::{Session, TransactionId};
use crate::AdapterError;
use mz_ore::task;
use mz_ore::task::JoinHandleExt;
use mz_stash::Append;
use std::fmt::{Display, Formatter};
use std::time::{Duration, Instant};
use tracing::warn;

#[derive(Debug)]
pub enum TimeoutOperation {
    Add((Timeout, Duration, Instant)),
    Remove(Timeout),
    Handle(Timeout),
}

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub enum Timeout {
    IdleInTransactionSession(ConnectionId, TransactionId),
}

impl Timeout {
    fn conn_id(&self) -> &ConnectionId {
        match self {
            Timeout::IdleInTransactionSession(conn_id, _) => conn_id,
        }
    }
}

impl Display for Timeout {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Timeout::IdleInTransactionSession(conn_id, txn_id) => {
                writeln!(f, "idle in transaction session timeout for connection: {conn_id}, transaction: {txn_id}")
            }
        }
    }
}

pub(crate) fn send_add_timeout(
    internal_cmd_tx: &tokio::sync::mpsc::UnboundedSender<Message>,
    timeout: Timeout,
    duration: Duration,
) {
    // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
    let result = internal_cmd_tx.send(Message::Timeout(TimeoutOperation::Add((
        timeout,
        duration,
        Instant::now(),
    ))));
    if let Err(e) = result {
        warn!("internal_cmd_rx dropped before we could send: {:?}", e);
    }
}

pub(crate) fn send_remove_timeout(
    internal_cmd_tx: &tokio::sync::mpsc::UnboundedSender<Message>,
    timeout: Timeout,
) {
    // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
    let result = internal_cmd_tx.send(Message::Timeout(TimeoutOperation::Remove(timeout)));
    if let Err(e) = result {
        warn!("internal_cmd_rx dropped before we could send: {:?}", e);
    }
}

pub(crate) fn add_idle_in_transaction_session_timeout(
    internal_cmd_tx: &tokio::sync::mpsc::UnboundedSender<Message>,
    session: &Session,
) {
    let timeout_dur = session.vars().idle_in_transaction_session_timeout();
    if !timeout_dur.is_zero() {
        if let Some(txn) = session.transaction().inner() {
            let timeout = Timeout::IdleInTransactionSession(session.conn_id(), txn.id.clone());
            send_add_timeout(internal_cmd_tx, timeout, timeout_dur.clone());
        }
    }
}

pub(crate) fn remove_idle_in_transaction_session_timeout(
    internal_cmd_tx: &tokio::sync::mpsc::UnboundedSender<Message>,
    cmd: &Command,
) {
    if let Some(session) = cmd.session() {
        if let Some(txn) = session.transaction().inner() {
            let timeout = Timeout::IdleInTransactionSession(session.conn_id(), txn.id.clone());
            send_remove_timeout(internal_cmd_tx, timeout);
        }
    }
}

impl<S: Append + 'static> Coordinator<S> {
    pub(crate) fn add_timeout(&mut self, timeout: Timeout, duration: Duration, start: Instant) {
        if let Some(ConnMeta::Active {
            active_timeouts, ..
        }) = self.active_conns.get_mut(timeout.conn_id())
        {
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            let timeout_key = timeout.clone();
            let handle = task::spawn(|| format!("{timeout_key}"), async move {
                let duration = duration.saturating_sub(start.elapsed());
                tokio::time::sleep(duration).await;
                // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
                let result =
                    internal_cmd_tx.send(Message::Timeout(TimeoutOperation::Handle(timeout)));
                if let Err(e) = result {
                    warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                }
            })
            .abort_on_drop();
            active_timeouts.insert(timeout_key, handle);
        }
    }

    pub(crate) fn remove_timeout(&mut self, timeout: &Timeout) -> bool {
        if let Some(ConnMeta::Active {
            active_timeouts, ..
        }) = self.active_conns.get_mut(timeout.conn_id())
        {
            active_timeouts.remove(timeout).is_some()
        } else {
            false
        }
    }

    pub(crate) async fn handle_timeout(&mut self, timeout: Timeout) {
        if self.remove_timeout(&timeout) {
            match timeout {
                Timeout::IdleInTransactionSession(conn_id, _transaction_id) => {
                    self.terminate_connection_error(
                        AdapterError::IdleInTransactionSessionTimeout,
                        &conn_id,
                    )
                    .await;
                }
            }
        }
    }
}
