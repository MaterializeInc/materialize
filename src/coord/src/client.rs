// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::SinkExt;

use sql::ast::{Raw, Statement};
use sql::plan::Params;

use crate::command::{
    Command, ExecuteResponse, NoSessionExecuteResponse, Response, StartupMessage,
};
use crate::error::CoordError;
use crate::session::{EndTransactionAction, Session};

/// A client for a [`Coordinator`](crate::Coordinator).
///
/// A client is a simple handle to a communication channel with the coordinator.
/// They can be cheaply cloned.
#[derive(Debug, Clone)]
pub struct Client {
    cmd_tx: futures::channel::mpsc::UnboundedSender<Command>,
}

impl Client {
    /// Constructs a new client.
    pub fn new(cmd_tx: futures::channel::mpsc::UnboundedSender<Command>) -> Client {
        Client { cmd_tx }
    }

    /// Binds this client to a session.
    pub fn for_session(&self, session: Session) -> SessionClient {
        SessionClient {
            inner: self.clone(),
            session: Some(session),
            started_up: false,
        }
    }

    /// Dumps the catalog to a JSON string.
    pub async fn dump_catalog(&mut self) -> String {
        self.send(|tx| Command::DumpCatalog { tx }).await
    }

    /// Executes a statement as the system user that is not tied to a session.
    ///
    /// This will execute in a pseudo session that is not able to create any
    /// temporary resources that would normally need to be cleaned up by Terminate.
    pub async fn execute(
        &mut self,
        stmt: Statement<Raw>,
        params: Params,
    ) -> Result<NoSessionExecuteResponse, CoordError> {
        self.send(|tx| Command::NoSessionExecute { stmt, params, tx })
            .await
    }

    /// Cancel the query currently running on another connection.
    pub async fn cancel_request(&mut self, conn_id: u32) {
        self.cmd_tx
            .send(Command::CancelRequest { conn_id })
            .await
            .expect("coordinator unexpectedly canceled request")
    }

    async fn send<T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(futures::channel::oneshot::Sender<T>) -> Command,
    {
        let (tx, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(f(tx))
            .await
            .expect("coordinator unexpectedly gone");
        rx.await.expect("coordinator unexpectedly canceled request")
    }
}

/// A [`Client`] that is bound to a session.
pub struct SessionClient {
    inner: Client,
    // Invariant: session may only be `None` during a method call. Every public
    // method must ensure that `Session` is `Some` before it returns.
    session: Option<Session>,
    /// Whether the coordinator has been notified of this `SessionClient` via
    /// a call to `startup`.
    started_up: bool,
}

impl SessionClient {
    /// Notifies the coordinator of a new client session.
    ///
    /// Returns a list of messages that are intended to be displayed to the
    /// user.
    ///
    /// Once you observe a successful response to this method, you must not call
    /// it again. You must observe a successful response to this method before
    /// calling any other method on the client, besides
    /// [`SessionClient::terminate`].
    pub async fn startup(&mut self) -> Result<Vec<StartupMessage>, CoordError> {
        assert!(!self.started_up);
        match self
            .send(|tx, session| Command::Startup { session, tx })
            .await
        {
            Ok(messages) => {
                self.started_up = true;
                Ok(messages)
            }
            Err(e) => Err(e),
        }
    }

    /// Saves the specified statement as a prepared statement.
    ///
    /// The prepared statement is saved in the connection's [`sql::Session`]
    /// under the specified name.
    ///
    /// You must have observed a successful response to
    /// [`SessionClient::startup`] before calling this method.
    pub async fn describe(
        &mut self,
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<pgrepr::Type>>,
    ) -> Result<(), CoordError> {
        self.send(|tx, session| Command::Describe {
            name,
            stmt,
            param_types,
            session,
            tx,
        })
        .await
    }

    /// Binds a statement to a portal.
    ///
    /// You must have observed a successful response to
    /// [`SessionClient::startup`] before calling this method.
    pub async fn declare(
        &mut self,
        name: String,
        stmt: Statement<Raw>,
        param_types: Vec<Option<pgrepr::Type>>,
    ) -> Result<(), CoordError> {
        self.send(|tx, session| Command::Declare {
            name,
            stmt,
            param_types,
            session,
            tx,
        })
        .await
    }

    /// Executes a previously-bound portal.
    ///
    /// You must have observed a successful response to
    /// [`SessionClient::startup`] before calling this method.
    pub async fn execute(&mut self, portal_name: String) -> Result<ExecuteResponse, CoordError> {
        self.send(|tx, session| Command::Execute {
            portal_name,
            session,
            tx,
        })
        .await
    }

    /// Ends a transaction.
    ///
    /// You must have observed a successful response to
    /// [`SessionClient::startup`] before calling this method.
    pub async fn end_transaction(
        &mut self,
        action: EndTransactionAction,
    ) -> Result<ExecuteResponse, CoordError> {
        self.send(|tx, session| Command::Commit {
            action,
            session,
            tx,
        })
        .await
    }

    /// Terminates this client session.
    ///
    /// This consumes this `SessionClient`. If the coordinator was notified of
    /// this client session by `startup`, then this method will clean up any
    /// state on the coordinator about this session.
    pub async fn terminate(mut self) {
        let session = self.session.take().expect("session invariant violated");
        if self.started_up {
            self.inner
                .cmd_tx
                .send(Command::Terminate { session })
                .await
                .expect("coordinator unexpectedly gone");
        }
    }

    /// Returns a mutable reference to the session bound to this client.
    pub fn session(&mut self) -> &mut Session {
        self.session.as_mut().unwrap()
    }

    async fn send<T, F>(&mut self, f: F) -> Result<T, CoordError>
    where
        F: FnOnce(futures::channel::oneshot::Sender<Response<T>>, Session) -> Command,
    {
        let session = self.session.take().expect("session invariant violated");
        let res = self.inner.send(|tx| f(tx, session)).await;
        self.session = Some(res.session);
        res.result
    }
}
