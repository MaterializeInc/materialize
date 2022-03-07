// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::command::{Command, Response};
use crate::coord::Message;
use crate::error::CoordError;
use crate::session::Session;

/// Handles responding to clients.
pub struct ClientTransmitter<T> {
    tx: Option<oneshot::Sender<Response<T>>>,
    internal_cmd_tx: UnboundedSender<Message>,
}

impl<T> ClientTransmitter<T> {
    /// Creates a new client transmitter.
    pub fn new(
        tx: oneshot::Sender<Response<T>>,
        internal_cmd_tx: UnboundedSender<Message>,
    ) -> ClientTransmitter<T> {
        ClientTransmitter {
            tx: Some(tx),
            internal_cmd_tx,
        }
    }

    /// Transmits `result` to the client, returning ownership of the session
    /// `session` as well.
    pub fn send(mut self, result: Result<T, CoordError>, session: Session) {
        // If we were not able to send a message, we must clean up the session
        // ourselves. Return it to the caller for disposal.
        if let Err(res) = self.tx.take().unwrap().send(Response { result, session }) {
            self.internal_cmd_tx
                .send(Message::Command(Command::Terminate {
                    session: res.session,
                }))
                .expect("coordinator unexpectedly gone");
        }
    }

    pub fn take(mut self) -> oneshot::Sender<Response<T>> {
        self.tx.take().unwrap()
    }
}

impl<T> Drop for ClientTransmitter<T> {
    fn drop(&mut self) {
        if self.tx.is_some() {
            panic!("client transmitter dropped without send")
        }
    }
}
