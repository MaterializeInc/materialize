// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

use mz_dataflow_types::PeekResponseUnary;
use mz_expr::GlobalId;
use mz_ore::collections::CollectionExt;
use mz_ore::thread::JoinOnDropHandle;
use mz_repr::{Datum, Row, ScalarType};
use mz_sql::ast::{Raw, Statement};

use crate::command::{
    Canceled, Command, ExecuteResponse, Response, SimpleExecuteResponse, SimpleResult,
    StartupResponse,
};
use crate::error::CoordError;
use crate::id_alloc::IdAllocator;
use crate::session::{EndTransactionAction, PreparedStatement, Session};

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
    id_alloc: Arc<IdAllocator>,
}

impl Client {
    pub(crate) fn new(cmd_tx: mpsc::UnboundedSender<Command>) -> Client {
        Client {
            cmd_tx,
            id_alloc: Arc::new(IdAllocator::new(1, 1 << 16)),
        }
    }

    /// Allocates a client for an incoming connection.
    pub fn new_conn(&self) -> Result<ConnClient, CoordError> {
        Ok(ConnClient {
            conn_id: self.id_alloc.alloc()?,
            inner: self.clone(),
        })
    }

    /// Executes SQL statements, as if by [`SessionClient::simple_execute`], as
    /// a system user.
    pub async fn system_execute(&self, stmts: &str) -> Result<SimpleExecuteResponse, CoordError> {
        let conn_client = self.new_conn()?;
        let session = Session::new(conn_client.conn_id(), "mz_system".into());
        let (mut session_client, _) = conn_client.startup(session, false).await?;
        let res = session_client.simple_execute(stmts).await;
        session_client.terminate().await;
        res
    }

    /// Like [`Client::system_execute`], but for cases when `stmt` is known to
    /// contain just one statement.
    ///
    /// # Panics
    ///
    /// Panics if `stmt` parses to more than one SQL statement.
    pub async fn system_execute_one(&self, stmt: &str) -> Result<SimpleResult, CoordError> {
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
#[derive(Debug, Clone)]
pub struct ConnClient {
    conn_id: u32,
    inner: Client,
}

impl ConnClient {
    /// Returns the ID of the connection associated with this client.
    pub fn conn_id(&self) -> u32 {
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
    pub async fn startup(
        self,
        session: Session,
        create_user_if_not_exists: bool,
    ) -> Result<(SessionClient, StartupResponse), CoordError> {
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
    pub async fn cancel_request(&mut self, conn_id: u32, secret_key: u32) {
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
/// You must call [`SessionClient::terminate`] rather than dropping a session
/// client directly. Dropping an unterminated `SessionClient` will panic.
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
    ) -> Result<&PreparedStatement, CoordError> {
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
    pub async fn declare(
        &mut self,
        name: String,
        stmt: Statement<Raw>,
        param_types: Vec<Option<ScalarType>>,
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
    pub async fn execute(&mut self, portal_name: String) -> Result<ExecuteResponse, CoordError> {
        self.send(|tx, session| Command::Execute {
            portal_name,
            session,
            tx,
        })
        .await
    }

    /// Starts a transaction based on implicit:
    /// - `None`: InTransaction
    /// - `Some(1)`: Started
    /// - `Some(n > 1)`: InTransactionImplicit
    /// - `Some(0)`: no change
    pub async fn start_transaction(&mut self, implicit: Option<usize>) -> Result<(), CoordError> {
        self.send(|tx, session| Command::StartTransaction {
            implicit,
            session,
            tx,
        })
        .await
    }

    /// Cancels the query currently running on another connection.
    pub async fn cancel_request(&mut self, conn_id: u32, secret_key: u32) {
        self.inner.cancel_request(conn_id, secret_key).await
    }

    /// Ends a transaction.
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

    /// Fails a transaction.
    pub fn fail_transaction(&mut self) {
        let session = self.session.take().expect("session invariant violated");
        let session = session.fail_transaction();
        self.session = Some(session);
    }

    /// Dumps the catalog to a JSON string.
    pub async fn dump_catalog(&mut self) -> Result<String, CoordError> {
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
    ) -> Result<ExecuteResponse, CoordError> {
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
    ) -> Result<SimpleExecuteResponse, CoordError> {
        // Convert most floats to a JSON Number. JSON Numbers don't support NaN or
        // Infinity, so those will still be rendered as strings.
        fn float_to_json(f: f64) -> serde_json::Value {
            match serde_json::Number::from_f64(f) {
                Some(n) => serde_json::Value::Number(n),
                None => serde_json::Value::String(f.to_string()),
            }
        }

        fn datum_to_json(datum: &Datum) -> serde_json::Value {
            match datum {
                // Convert some common things to a native JSON value. This doesn't need to be
                // too exhaustive because the SQL-over-HTTP interface is currently not hooked
                // up to arbitrary external user queries.
                Datum::Null | Datum::JsonNull => serde_json::Value::Null,
                Datum::False => serde_json::Value::Bool(false),
                Datum::True => serde_json::Value::Bool(true),
                Datum::Int16(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                Datum::Int32(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                Datum::Int64(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                Datum::Float32(n) => float_to_json(n.into_inner() as f64),
                Datum::Float64(n) => float_to_json(n.into_inner()),
                Datum::Numeric(d) => {
                    // serde_json requires floats to be finite
                    if d.0.is_infinite() {
                        serde_json::Value::String(d.0.to_string())
                    } else {
                        serde_json::Value::Number(
                            serde_json::Number::from_f64(f64::try_from(d.0).unwrap()).unwrap(),
                        )
                    }
                }
                Datum::String(s) => serde_json::Value::String(s.to_string()),
                Datum::List(list) => serde_json::Value::Array(
                    list.iter().map(|entry| datum_to_json(&entry)).collect(),
                ),
                Datum::Map(map) => serde_json::Value::Object(
                    map.iter()
                        .map(|(k, v)| (k.to_owned(), datum_to_json(&v)))
                        .collect(),
                ),
                _ => serde_json::Value::String(datum.to_string()),
            }
        }

        let stmts = mz_sql::parse::parse(&stmts).map_err(|e| CoordError::Unstructured(e.into()))?;
        self.start_transaction(None).await?;
        const EMPTY_PORTAL: &str = "";
        let mut results = vec![];
        for stmt in stmts {
            self.declare(EMPTY_PORTAL.into(), stmt, vec![]).await?;
            let desc = self
                .session()
                // We do not need to verify here because `self.execute` verifies below.
                .get_portal_unverified(EMPTY_PORTAL)
                .map(|portal| portal.desc.clone())
                .expect("unnamed portal should be present");
            if !desc.param_types.is_empty() {
                return Err(CoordError::Unsupported("parameters"));
            }

            let res = self.execute(EMPTY_PORTAL.into()).await?;

            let rows = match res {
                ExecuteResponse::SendingRows(rows) => {
                    let response = rows.await;
                    response
                }
                _ => return Err(CoordError::Unsupported("statements of the executed type")),
            };
            let rows = match rows {
                PeekResponseUnary::Rows(rows) => rows,
                PeekResponseUnary::Error(e) => coord_bail!("{}", e),
                PeekResponseUnary::Canceled => coord_bail!("execution canceled"),
            };
            let mut sql_rows: Vec<Vec<serde_json::Value>> = vec![];
            let col_names = match desc.relation_desc {
                Some(desc) => desc.iter_names().map(|name| name.to_string()).collect(),
                None => vec![],
            };
            let mut datum_vec = mz_repr::DatumVec::new();
            for row in rows {
                let datums = datum_vec.borrow_with(&row);
                sql_rows.push(datums.iter().map(datum_to_json).collect());
            }
            results.push(SimpleResult {
                rows: sql_rows,
                col_names,
            })
        }
        Ok(SimpleExecuteResponse { results })
    }

    /// Terminates this client session.
    ///
    /// This method cleans up any coordinator state associated with the session
    /// before consuming the `SessionClient. Call this method instead of
    /// dropping the object directly.
    pub async fn terminate(mut self) {
        let session = self.session.take().expect("session invariant violated");
        self.inner
            .inner
            .cmd_tx
            .send(Command::Terminate { session })
            .expect("coordinator unexpectedly gone");
    }

    // Like `terminate`, but permits the session to not be present, assuming it was
    // terminated by the coordinator.
    pub async fn terminate_allow_no_session(self) {
        if self.session.is_none() {
            return;
        }
        self.terminate().await
    }

    /// Returns a mutable reference to the session bound to this client.
    pub fn session(&mut self) -> &mut Session {
        self.session.as_mut().unwrap()
    }

    async fn send<T, F>(&mut self, f: F) -> Result<T, CoordError>
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
        if self.session.is_some() {
            panic!("unterminated SessionClient dropped")
        }
    }
}
