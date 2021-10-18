// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::sync::oneshot;

use crate::command::Response;
use crate::error::CoordError;
use crate::session::Session;

/// Handles responding to clients.
pub struct ClientTransmitter<T> {
    tx: Option<oneshot::Sender<Response<T>>>,
}

impl<T> ClientTransmitter<T> {
    /// Creates a new client transmitter.
    pub fn new(tx: oneshot::Sender<Response<T>>) -> ClientTransmitter<T> {
        ClientTransmitter { tx: Some(tx) }
    }

    /// Transmits `result` to the client, returning ownership of the session
    /// `session` as well.
    pub fn send(mut self, result: Result<T, CoordError>, session: Session) {
        // We can safely ignore failure to send the message, as that simply
        // indicates that the client has disconnected and is no longer
        // interested in the response.
        let _ = self.tx.take().unwrap().send(Response { result, session });
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
