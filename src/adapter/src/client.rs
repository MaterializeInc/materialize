// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::From;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

use mz_ore::collections::CollectionExt;
use mz_ore::id_gen::IdAllocator;
use mz_ore::thread::JoinOnDropHandle;
use mz_repr::{GlobalId, Row, ScalarType};
use mz_sql::ast::{Raw, Statement};

use crate::catalog::SYSTEM_USER;
use crate::command::{
    Canceled, Command, ExecuteResponse, Response, SimpleExecuteResponse, SimpleResult,
    StartupResponse,
};
use crate::coord::peek::PeekResponseUnary;
use crate::error::AdapterError;
use crate::session::{EndTransactionAction, PreparedStatement, Session};

/// An abstraction allowing us to name different connections.
pub type ConnectionId = u32;

/// A handle to a running coordinator.
///
/// The coordinator runs on its own thread. Dropping the handle will wait for
/// the coordinator's thread to exit, which will only occur after all
/// outstanding [`Client`]s for the coordinator have dropped.
pub struct Handle {
    pub(crate) cluster_id: Uuid,
    pub(crate) session_id: Uuid,
    pub(crate) start_instant: Instant,
    pub(crate) _thread: JoinOnDropHandle<()>,
}

impl Handle {
    /// Returns the cluster ID associated with this coordinator.
    ///
    /// The cluster ID is recorded in the data directory when it is first
    /// created and persists until the data directory is deleted.
    pub fn cluster_id(&self) -> Uuid {
        self.cluster_id
    }

    /// Returns the session ID associated with this coordinator.
    ///
    /// The session ID is generated on coordinator boot. It lasts for the
    /// lifetime of the coordinator. Restarting the coordinator will result
    /// in a new session ID.
    pub fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Returns the instant at which the coordinator booted.
    pub fn start_instant(&self) -> Instant {
        self.start_instant
    }
}

/// A coordinator client.
///
/// A coordinator client is a simple handle to a communication channel with the
/// coordinator. It can be cheaply cloned.
///
/// Clients keep the coordinator alive. The coordinator will not exit until all
/// outstanding clients have dropped.
#[derive(Debug, Clone)]
pub struct Client {
    cmd_tx: mpsc::UnboundedSender<Command>,
    id_alloc: Arc<IdAllocator<ConnectionId>>,
}

impl Client {
    pub(crate) fn new(cmd_tx: mpsc::UnboundedSender<Command>) -> Client {
        Client {
            cmd_tx,
            id_alloc: Arc::new(IdAllocator::new(1, 1 << 16)),
        }
    }

    /// Allocates a client for an incoming connection.
    pub fn new_conn(&self) -> Result<ConnClient, AdapterError> {
        Ok(ConnClient {
            conn_id: self
                .id_alloc
                .alloc()
                .ok_or(AdapterError::IdExhaustionError)?,
            inner: self.clone(),
        })
    }

    /// Executes SQL statements, as if by [`SessionClient::simple_execute`], as
    /// a system user.
    pub async fn system_execute(&self, stmts: &str) -> Result<SimpleExecuteResponse, AdapterError> {
        let conn_client = self.new_conn()?;
        let session = Session::new(conn_client.conn_id(), SYSTEM_USER.into());
        let (mut session_client, _) = conn_client.startup(session, false).await?;
        session_client.simple_execute(stmts).await
    }

    /// Like [`Client::system_execute`], but for cases when `stmt` is known to
    /// contain just one statement.
    ///
    /// # Panics
    ///
    /// Panics if `stmt` parses to more than one SQL statement.
    pub async fn system_execute_one(&self, stmt: &str) -> Result<SimpleResult, AdapterError> {
        let response = self.system_execute(stmt).await?;
        Ok(response.results.into_element())
    }
}

/// A coordinator client that is bound to a connection.
///
/// The `ConnClient` automatically allocates an ID for the connection when
/// it is created, and frees that ID for potential reuse when it is dropped.
///
/// See also [`Client`].
#[derive(Debug)]
pub struct ConnClient {
    conn_id: ConnectionId,
    inner: Client,
}

impl ConnClient {
    /// Returns the ID of the connection associated with this client.
    pub fn conn_id(&self) -> ConnectionId {
        self.conn_id
    }

    /// Upgrades this connection client to a session client.
    ///
    /// A session is a connection that has successfully negotiated parameters,
    /// like the user. Most coordinator operations are available only after
    /// upgrading a connection to a session.
    ///
    /// Returns a new client that is bound to the session and a response
    /// containing various details about the startup.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn startup(
        self,
        session: Session,
        create_user_if_not_exists: bool,
    ) -> Result<(SessionClient, StartupResponse), AdapterError> {
        // Cancellation works by creating a watch channel (which remembers only
        // the last value sent to it) and sharing it between the coordinator and
        // connection. The coordinator will send a canceled message on it if a
        // cancellation request comes. The connection will reset that on every message
        // it receives and then check for it where we want to add the ability to cancel
        // an in-progress statement.
        let (cancel_tx, cancel_rx) = watch::channel(Canceled::NotCanceled);
        let cancel_tx = Arc::new(cancel_tx);
        let mut client = SessionClient {
            inner: self,
            session: Some(session),
            cancel_tx: Arc::clone(&cancel_tx),
            cancel_rx,
        };
        let response = client
            .send(|tx, session| Command::Startup {
                session,
                create_user_if_not_exists,
                cancel_tx,
                tx,
            })
            .await;
        match response {
            Ok(response) => Ok((client, response)),
            Err(e) => {
                // When startup fails, no need to call terminate. Remove the
                // session from the client to sidestep the panic in the `Drop`
                // implementation.
                client.session.take();
                Err(e)
            }
        }
    }

    /// Cancels the query currently running on another connection.
    pub async fn cancel_request(&mut self, conn_id: ConnectionId, secret_key: u32) {
        self.inner
            .cmd_tx
            .send(Command::CancelRequest {
                conn_id,
                secret_key,
            })
            .expect("coordinator unexpectedly gone");
    }

    async fn send<T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(oneshot::Sender<T>) -> Command,
    {
        let (tx, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(f(tx))
            .expect("coordinator unexpectedly gone");
        rx.await.expect("coordinator unexpectedly canceled request")
    }
}

impl Drop for ConnClient {
    fn drop(&mut self) {
        self.inner.id_alloc.free(self.conn_id);
    }
}

/// A coordinator client that is bound to a connection.
///
/// See also [`Client`].
pub struct SessionClient {
    inner: ConnClient,
    // Invariant: session may only be `None` during a method call. Every public
    // method must ensure that `Session` is `Some` before it returns.
    session: Option<Session>,
    cancel_tx: Arc<watch::Sender<Canceled>>,
    cancel_rx: watch::Receiver<Canceled>,
}

impl SessionClient {
    pub fn canceled(&self) -> impl Future<Output = ()> + Send {
        let mut cancel_rx = self.cancel_rx.clone();
        async move {
            loop {
                let _ = cancel_rx.changed().await;
                if let Canceled::Canceled = *cancel_rx.borrow() {
                    return;
                }
            }
        }
    }

    pub fn reset_canceled(&mut self) {
        // Clear any cancellation message.
        // TODO(mjibson): This makes the use of .changed annoying since it will
        // generally always have a NotCanceled message first that needs to be ignored,
        // and thus run in a loop. Figure out a way to have the future only resolve on
        // a Canceled message.
        let _ = self.cancel_tx.send(Canceled::NotCanceled);
    }

    // Verify and return the named prepared statement. We need to verify each use
    // to make sure the prepared statement is still safe to use.
    pub async fn get_prepared_statement(
        &mut self,
        name: &str,
    ) -> Result<&PreparedStatement, AdapterError> {
        self.send(|tx, session| Command::VerifyPreparedStatement {
            name: name.to_string(),
            session,
            tx,
        })
        .await?;
        Ok(self
            .session()
            .get_prepared_statement_unverified(&name)
            .expect("must exist"))
    }

    /// Saves the specified statement as a prepared statement.
    ///
    /// The prepared statement is saved in the connection's [`crate::session::Session`]
    /// under the specified name.
    pub async fn describe(
        &mut self,
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
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
        stmt: Statement<Raw>,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
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
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn execute(&mut self, portal_name: String) -> Result<ExecuteResponse, AdapterError> {
        self.send(|tx, session| Command::Execute {
            portal_name,
            session,
            tx,
            span: tracing::Span::current(),
        })
        .await
    }

    /// Starts a transaction based on implicit:
    /// - `None`: InTransaction
    /// - `Some(1)`: Started
    /// - `Some(n > 1)`: InTransactionImplicit
    /// - `Some(0)`: no change
    pub async fn start_transaction(&mut self, implicit: Option<usize>) -> Result<(), AdapterError> {
        self.send(|tx, session| Command::StartTransaction {
            implicit,
            session,
            tx,
        })
        .await
    }

    /// Cancels the query currently running on another connection.
    pub async fn cancel_request(&mut self, conn_id: ConnectionId, secret_key: u32) {
        self.inner.cancel_request(conn_id, secret_key).await
    }

    /// Ends a transaction.
    pub async fn end_transaction(
        &mut self,
        action: EndTransactionAction,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.send(|tx, session| Command::Commit {
            action,
            session,
            tx,
        })
        .await
    }

    /// Fails a transaction.
    pub fn fail_transaction(&mut self) {
        let session = self.session.take().expect("session invariant violated");
        let session = session.fail_transaction();
        self.session = Some(session);
    }

    /// Dumps the catalog to a JSON string.
    pub async fn dump_catalog(&mut self) -> Result<String, AdapterError> {
        self.send(|tx, session| Command::DumpCatalog { session, tx })
            .await
    }

    /// Inserts a set of rows into the given table.
    ///
    /// The rows only contain the columns positions in `columns`, so they
    /// must be re-encoded for adding the default values for the remaining
    /// ones.
    pub async fn insert_rows(
        &mut self,
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.send(|tx, session| Command::CopyRows {
            id,
            columns,
            rows,
            session,
            tx,
        })
        .await
    }

    /// Executes SQL statements using a simple protocol that does not involve
    /// portals.
    ///
    /// The standard flow for executing a SQL statement requires parsing the
    /// statement, binding it into a portal, and then executing that portal.
    /// This function is a wrapper around that complexity with a simpler
    /// interface. The provided `stmts` are executed directly, and their results
    /// are returned as a vector of rows, where each row is a vector of JSON
    /// objects.
    pub async fn simple_execute(
        &mut self,
        stmts: &str,
    ) -> Result<SimpleExecuteResponse, AdapterError> {
        let stmts =
            mz_sql::parse::parse(&stmts).map_err(|e| AdapterError::Unstructured(e.into()))?;
        let num_stmts = stmts.len();
        const EMPTY_PORTAL: &str = "";
        let mut results = vec![];
        for stmt in stmts {
            // Mirror the behavior of the PostgreSQL simple query protocol.
            // See the pgwire::protocol::StateMachine::query method for details.
            self.start_transaction(Some(num_stmts)).await?;

            if let Err(e) = self.declare(EMPTY_PORTAL.into(), stmt, vec![]).await {
                results.push(SimpleResult::err(e));
                continue;
            }

            let desc = self
                .session()
                // We do not need to verify here because `self.execute` verifies below.
                .get_portal_unverified(EMPTY_PORTAL)
                .map(|portal| portal.desc.clone())
                .expect("unnamed portal should be present");
            if !desc.param_types.is_empty() {
                results.push(SimpleResult::err("query parameters are not supported"));
                continue;
            }

            let res = match self.execute(EMPTY_PORTAL.into()).await {
                Ok(res) => res,
                Err(e) => {
                    results.push(SimpleResult::err(e));
                    continue;
                }
            };

            match res {
                ExecuteResponse::Canceled => {
                    results.push(SimpleResult::err("statement canceled due to user request"));
                }
                ExecuteResponse::CreatedConnection { existed: _ }
                | ExecuteResponse::CreatedDatabase { existed: _ }
                | ExecuteResponse::CreatedSchema { existed: _ }
                | ExecuteResponse::CreatedRole
                | ExecuteResponse::CreatedComputeInstance { existed: _ }
                | ExecuteResponse::CreatedComputeInstanceReplica { existed: _ }
                | ExecuteResponse::CreatedTable { existed: _ }
                | ExecuteResponse::CreatedIndex { existed: _ }
                | ExecuteResponse::CreatedSecret { existed: _ }
                | ExecuteResponse::CreatedSource { existed: _ }
                | ExecuteResponse::CreatedSources
                | ExecuteResponse::CreatedSink { existed: _ }
                | ExecuteResponse::CreatedView { existed: _ }
                | ExecuteResponse::CreatedMaterializedView { existed: _ }
                | ExecuteResponse::CreatedType
                | ExecuteResponse::Deleted(_)
                | ExecuteResponse::DiscardedTemp
                | ExecuteResponse::DiscardedAll
                | ExecuteResponse::DroppedDatabase
                | ExecuteResponse::DroppedSchema
                | ExecuteResponse::DroppedRole
                | ExecuteResponse::DroppedComputeInstance
                | ExecuteResponse::DroppedComputeInstanceReplicas
                | ExecuteResponse::DroppedSource
                | ExecuteResponse::DroppedIndex
                | ExecuteResponse::DroppedSink
                | ExecuteResponse::DroppedTable
                | ExecuteResponse::DroppedView
                | ExecuteResponse::DroppedMaterializedView
                | ExecuteResponse::DroppedType
                | ExecuteResponse::DroppedSecret
                | ExecuteResponse::DroppedConnection
                | ExecuteResponse::EmptyQuery
                | ExecuteResponse::Inserted(_)
                | ExecuteResponse::StartedTransaction { duplicated: _ }
                | ExecuteResponse::TransactionExited {
                    tag: _,
                    was_implicit: _,
                }
                | ExecuteResponse::Updated(_)
                | ExecuteResponse::AlteredObject(_)
                | ExecuteResponse::AlteredIndexLogicalCompaction
                | ExecuteResponse::AlteredSystemConfiguraion
                | ExecuteResponse::Deallocate { all: _ }
                | ExecuteResponse::Prepare => {
                    results.push(SimpleResult::Ok);
                }
                ExecuteResponse::SendingRows {
                    future: rows,
                    span: _,
                } => {
                    let rows = match rows.await {
                        PeekResponseUnary::Rows(rows) => rows,
                        PeekResponseUnary::Error(e) => {
                            results.push(SimpleResult::err(e.to_string()));
                            continue;
                        }
                        PeekResponseUnary::Canceled => {
                            results
                                .push(SimpleResult::err("statement canceled due to user request"));
                            continue;
                        }
                    };
                    let mut sql_rows: Vec<Vec<serde_json::Value>> = vec![];
                    let col_names = match desc.relation_desc {
                        Some(desc) => desc.iter_names().map(|name| name.to_string()).collect(),
                        None => vec![],
                    };
                    let mut datum_vec = mz_repr::DatumVec::new();
                    for row in rows {
                        let datums = datum_vec.borrow_with(&row);
                        sql_rows.push(datums.iter().map(From::from).collect());
                    }
                    results.push(SimpleResult::Rows {
                        rows: sql_rows,
                        col_names,
                    });
                }
                ExecuteResponse::Fetch { .. }
                | ExecuteResponse::SetVariable { .. }
                | ExecuteResponse::Tailing { .. }
                | ExecuteResponse::CopyTo { .. }
                | ExecuteResponse::CopyFrom { .. }
                | ExecuteResponse::Raise { .. }
                | ExecuteResponse::DeclaredCursor
                | ExecuteResponse::ClosedCursor => {
                    // NOTE(benesch): it is a bit scary to ignore the response
                    // to these types of planning *after* they have been
                    // planned, as they may have mutated state. On a quick
                    // glance, though, ignoring the execute response in all of
                    // the above situations seems safe enough, and it's a more
                    // target allowlist than the code that was here before.
                    results.push(SimpleResult::err(
                        "executing statements of this type is unsupported via this API",
                    ));
                }
            };
        }
        Ok(SimpleExecuteResponse { results })
    }

    /// Returns a mutable reference to the session bound to this client.
    pub fn session(&mut self) -> &mut Session {
        self.session.as_mut().unwrap()
    }

    async fn send<T, F>(&mut self, f: F) -> Result<T, AdapterError>
    where
        F: FnOnce(oneshot::Sender<Response<T>>, Session) -> Command,
    {
        let session = self.session.take().expect("session invariant violated");
        let res = self.inner.send(|tx| f(tx, session)).await;
        self.session = Some(res.session);
        res.result
    }
}

impl Drop for SessionClient {
    fn drop(&mut self) {
        // We may not have a session if this client was dropped while awaiting
        // a response. In this case, it is the coordinator's responsibility to
        // terminate the session.
        if let Some(session) = self.session.take() {
            self.inner
                .inner
                .cmd_tx
                .send(Command::Terminate { session })
                .expect("coordinator unexpectedly gone");
        }
    }
}
