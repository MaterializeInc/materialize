// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use sql::Session;

use crate::command::Response;

/// Handles responding to clients.
pub struct ClientTransmitter<T> {
    tx: futures::channel::oneshot::Sender<Response<T>>,
}

impl<T> ClientTransmitter<T> {
    /// Creates a new client transmitter.
    pub fn new(tx: futures::channel::oneshot::Sender<Response<T>>) -> ClientTransmitter<T> {
        ClientTransmitter { tx }
    }

    /// Transmits `result` to the client, returning ownership of the session
    /// `session` as well.
    pub fn send(self, result: Result<T, failure::Error>, session: Session) {
        // We can safely ignore failure to send the message, as that simply
        // indicates that the client has disconnected and is no longer
        // interested in the response.
        let _ = self.tx.send(Response { result, session });
    }
}
