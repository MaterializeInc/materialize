// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::SinkExt;

use sql::ast::Statement;
use sql::plan::Params;

use crate::command::{
    Command, ExecuteResponse, NoSessionExecuteResponse, Response, StartupMessage,
};
use crate::session::Session;

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
        stmt: Statement,
        params: Params,
    ) -> Result<NoSessionExecuteResponse, anyhow::Error> {
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
}

impl SessionClient {
    /// Notifies the coordinator of a new client session.
    ///
    /// Returns a list of messages that are intended to be displayed to the
    /// user.
    pub async fn startup(&mut self) -> Result<Vec<StartupMessage>, anyhow::Error> {
        self.send(|tx, session| Command::Startup { session, tx })
            .await
    }

    /// Saves the specified statement as a prepared statement.
    ///
    /// The prepared statement is saved in the connection's [`sql::Session`]
    /// under the specified name.
    pub async fn describe(
        &mut self,
        name: String,
        stmt: Option<Statement>,
        param_types: Vec<Option<pgrepr::Type>>,
    ) -> Result<(), anyhow::Error> {
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
    pub async fn declare(
        &mut self,
        name: String,
        stmt: Statement,
        param_types: Vec<Option<pgrepr::Type>>,
    ) -> Result<(), anyhow::Error> {
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
    pub async fn execute(&mut self, portal_name: String) -> Result<ExecuteResponse, anyhow::Error> {
        self.send(|tx, session| Command::Execute {
            portal_name,
            session,
            tx,
        })
        .await
    }

    /// Terminates this client session.
    ///
    /// This both consumes this `SessionClient` and cleans up any state
    /// associated with the session on stored by the coordinator.
    pub async fn terminate(mut self) {
        let session = self.session.take().expect("session invariant violated");
        self.inner
            .cmd_tx
            .send(Command::Terminate { session })
            .await
            .expect("coordinator unexpectedly gone");
    }

    /// Returns a mutable reference to the session bound to this client.
    pub fn session(&mut self) -> &mut Session {
        self.session.as_mut().unwrap()
    }

    async fn send<T, F>(&mut self, f: F) -> Result<T, anyhow::Error>
    where
        F: FnOnce(futures::channel::oneshot::Sender<Response<T>>, Session) -> Command,
    {
        let session = self.session.take().expect("session invariant violated");
        let res = self.inner.send(|tx| f(tx, session)).await;
        self.session = Some(res.session);
        res.result
    }
}
