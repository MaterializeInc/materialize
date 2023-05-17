// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use chrono::{DateTime, Utc};
use mz_sql::session::hint::ApplicationNameHint;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::error;
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_ore::collections::CollectionExt;
use mz_ore::id_gen::IdAllocator;
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_ore::task::{AbortOnDropHandle, JoinHandleExt};
use mz_ore::thread::JoinOnDropHandle;
use mz_repr::{GlobalId, Row, ScalarType};
use mz_sql::ast::{Raw, Statement};
use mz_sql::session::user::{User, INTROSPECTION_USER};

use crate::command::{
    Canceled, CatalogDump, Command, ExecuteResponse, GetVariablesResponse, Response,
    StartupResponse,
};
use crate::error::AdapterError;
use crate::metrics::Metrics;
use crate::session::{EndTransactionAction, PreparedStatement, Session, TransactionId};
use crate::PeekResponseUnary;

/// An abstraction allowing us to name different connections.
pub type ConnectionId = u32;

/// A handle to a running coordinator.
///
/// The coordinator runs on its own thread. Dropping the handle will wait for
/// the coordinator's thread to exit, which will only occur after all
/// outstanding [`Client`]s for the coordinator have dropped.
pub struct Handle {
    pub(crate) session_id: Uuid,
    pub(crate) start_instant: Instant,
    pub(crate) _thread: JoinOnDropHandle<()>,
}

impl Handle {
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
    build_info: &'static BuildInfo,
    inner_cmd_tx: mpsc::UnboundedSender<Command>,
    id_alloc: Arc<IdAllocator<ConnectionId>>,
    now: NowFn,
    metrics: Metrics,
}

impl Client {
    pub(crate) fn new(
        build_info: &'static BuildInfo,
        cmd_tx: mpsc::UnboundedSender<Command>,
        metrics: Metrics,
        now: NowFn,
    ) -> Client {
        Client {
            build_info,
            inner_cmd_tx: cmd_tx,
            id_alloc: Arc::new(IdAllocator::new(1, 1 << 16)),
            now,
            metrics,
        }
    }

    /// Allocates a client for an incoming connection.
    pub fn new_conn(&self) -> Result<ConnClient, AdapterError> {
        Ok(ConnClient {
            build_info: self.build_info,
            conn_id: self
                .id_alloc
                .alloc()
                .ok_or(AdapterError::IdExhaustionError)?,
            inner: self.clone(),
        })
    }

    /// Executes a single SQL statement that returns rows as the
    /// `mz_introspection` user.
    pub async fn introspection_execute_one(&self, sql: &str) -> Result<Vec<Row>, anyhow::Error> {
        // Connect to the coordinator.
        let conn_client = self.new_conn()?;
        let session = conn_client.new_session(INTROSPECTION_USER.clone());
        let (mut session_client, _) = conn_client.startup(session).await?;

        // Parse the SQL statement.
        let stmts = mz_sql::parse::parse(sql)?;
        if stmts.len() != 1 {
            bail!("must supply exactly one query");
        }
        let stmt = stmts.into_element();

        const EMPTY_PORTAL: &str = "";
        session_client.start_transaction(Some(1))?;
        session_client
            .declare(EMPTY_PORTAL.into(), stmt, vec![])
            .await?;
        match session_client
            .execute(EMPTY_PORTAL.into(), futures::future::pending())
            .await?
        {
            ExecuteResponse::SendingRows { future, span: _ } => match future.await {
                PeekResponseUnary::Rows(rows) => Ok(rows),
                PeekResponseUnary::Canceled => bail!("query canceled"),
                PeekResponseUnary::Error(e) => bail!(e),
            },
            r => bail!("unsupported response type: {r:?}"),
        }
    }

    /// Returns the metrics associated with the adapter layer.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    fn send(&self, cmd: Command) {
        self.inner_cmd_tx
            .send(cmd)
            .expect("coordinator unexpectedly gone");
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
    build_info: &'static BuildInfo,
    conn_id: ConnectionId,
    inner: Client,
}

impl ConnClient {
    /// Creates a new session associated with this connection for the given
    /// user.
    ///
    /// It is the caller's responsibility to have authenticated the user.
    pub fn new_session(&self, user: User) -> Session {
        // We use the system clock to determine when a session connected to Materialize. This is not
        // intended to be 100% accurate and correct, so we don't burden the timestamp oracle with
        // generating a more correct timestamp.
        Session::new(self.build_info, self.conn_id, user, (self.inner.now)())
    }

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
            inner: Some(self),
            session: Some(session),
            cancel_tx: Arc::clone(&cancel_tx),
            cancel_rx,
            timeouts: Timeout::new(),
        };
        let response = client
            .send(|tx, session| Command::Startup {
                session,
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
    pub fn cancel_request(&mut self, conn_id: ConnectionId, secret_key: u32) {
        self.inner.send(Command::CancelRequest {
            conn_id,
            secret_key,
        });
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
    // Invariant: inner may only be `None` after the session has been terminated.
    // Once the session is terminated, no communication to the Coordinator
    // should be attempted.
    inner: Option<ConnClient>,
    // Invariant: session may only be `None` during a method call. Every public
    // method must ensure that `Session` is `Some` before it returns.
    session: Option<Session>,
    cancel_tx: Arc<watch::Sender<Canceled>>,
    cancel_rx: watch::Receiver<Canceled>,
    timeouts: Timeout,
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
            .get_prepared_statement_unverified(name)
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
        .map(|_| ())
    }

    /// Executes a previously-bound portal.
    #[tracing::instrument(level = "debug", skip(self, cancel_future))]
    pub async fn execute(
        &mut self,
        portal_name: String,
        cancel_future: impl Future<Output = std::io::Error> + Send,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.send_with_cancel(
            |tx, session| Command::Execute {
                portal_name,
                session,
                tx,
                span: tracing::Span::current(),
            },
            cancel_future,
        )
        .await
    }

    fn now(&self) -> EpochMillis {
        (self.inner().inner.now)()
    }

    fn now_datetime(&self) -> DateTime<Utc> {
        to_datetime(self.now())
    }

    /// Starts a transaction based on implicit:
    /// - `None`: InTransaction
    /// - `Some(1)`: Started
    /// - `Some(n > 1)`: InTransactionImplicit
    /// - `Some(0)`: no change
    pub fn start_transaction(&mut self, implicit: Option<usize>) -> Result<(), AdapterError> {
        let session = self.session.take().expect("session invariant violated");
        let now = self.now_datetime();
        let (session, result) = match implicit {
            None => session.start_transaction(now, None, None),
            Some(stmts) => (session.start_transaction_implicit(now, stmts), Ok(())),
        };
        self.session = Some(session);
        result
    }

    /// Cancels the query currently running on another connection.
    pub fn cancel_request(&mut self, conn_id: ConnectionId, secret_key: u32) {
        self.inner_mut().cancel_request(conn_id, secret_key)
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
    pub async fn dump_catalog(&mut self) -> Result<CatalogDump, AdapterError> {
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

    /// Gets the current value of all system variables.
    pub async fn get_system_vars(&mut self) -> Result<GetVariablesResponse, AdapterError> {
        self.send(|tx, session| Command::GetSystemVars { session, tx })
            .await
    }

    /// Updates the specified system variables to the specified values.
    pub async fn set_system_vars(
        &mut self,
        vars: BTreeMap<String, String>,
    ) -> Result<(), AdapterError> {
        self.send(|tx, session| Command::SetSystemVars { vars, session, tx })
            .await
    }

    /// Terminates the client session.
    pub async fn terminate(&mut self) {
        let res = self
            .send(|tx, session| Command::Terminate {
                session,
                tx: Some(tx),
            })
            .await;
        if let Err(e) = res {
            // Nothing we can do to handle a failed terminate so we just log and ignore it.
            error!("Unable to terminate session: {e:?}");
        }
        // Prevent any communication with Coordinator after session is terminated.
        self.inner = None;
    }

    /// Returns a mutable reference to the session bound to this client.
    pub fn session(&mut self) -> &mut Session {
        self.session.as_mut().expect("session invariant violated")
    }

    /// Returns a reference to the inner client.
    pub fn inner(&self) -> &ConnClient {
        self.inner.as_ref().expect("inner invariant violated")
    }

    /// Returns a mutable reference to the inner client.
    pub fn inner_mut(&mut self) -> &mut ConnClient {
        self.inner.as_mut().expect("inner invariant violated")
    }

    async fn send<T, F>(&mut self, f: F) -> Result<T, AdapterError>
    where
        F: FnOnce(oneshot::Sender<Response<T>>, Session) -> Command,
    {
        self.send_with_cancel(f, futures::future::pending()).await
    }

    async fn send_with_cancel<T, F>(
        &mut self,
        f: F,
        cancel_future: impl Future<Output = std::io::Error> + Send,
    ) -> Result<T, AdapterError>
    where
        F: FnOnce(oneshot::Sender<Response<T>>, Session) -> Command,
    {
        let session = self.session.take().expect("session invariant violated");
        let mut typ = None;
        let application_name = session.application_name();
        let name_hint = ApplicationNameHint::from_str(application_name);
        let (tx, mut rx) = oneshot::channel();
        let conn_id = session.conn_id();
        let secret_key = session.secret_key();
        self.inner_mut().inner.send({
            let cmd = f(tx, session);
            // Measure the success and error rate of certain commands:
            // - declare reports success of SQL statement planning
            // - execute reports success of dataflow execution
            match cmd {
                Command::Declare { .. } => typ = Some("declare"),
                Command::Execute { .. } => typ = Some("execute"),
                Command::Startup { .. }
                | Command::Describe { .. }
                | Command::VerifyPreparedStatement { .. }
                | Command::Commit { .. }
                | Command::CancelRequest { .. }
                | Command::DumpCatalog { .. }
                | Command::CopyRows { .. }
                | Command::GetSystemVars { .. }
                | Command::SetSystemVars { .. }
                | Command::Terminate { .. } => {}
            };
            cmd
        });

        let mut cancel_future = pin::pin!(cancel_future);
        let mut cancelled = false;
        loop {
            tokio::select! {
                res = &mut rx => {
                    let res = res.expect("sender dropped");
                    let status = if res.result.is_ok() {
                        "success"
                    } else {
                        "error"
                    };
                    if let Some(typ) = typ {
                        self.inner()
                            .inner
                            .metrics
                            .commands
                            .with_label_values(&[typ, status, name_hint.as_str()])
                            .inc();
                    }
                    self.session = Some(res.session);
                    return res.result
                },
                _err = &mut cancel_future, if !cancelled => {
                    cancelled = true;
                    self.inner_mut().cancel_request(conn_id, secret_key);
                }
            };
        }
    }

    pub fn add_idle_in_transaction_session_timeout(&mut self) {
        let session = self.session();
        let timeout_dur = session.vars().idle_in_transaction_session_timeout();
        if !timeout_dur.is_zero() {
            let timeout_dur = timeout_dur.clone();
            if let Some(txn) = session.transaction().inner() {
                let txn_id = txn.id.clone();
                let timeout = TimeoutType::IdleInTransactionSession(txn_id);
                self.timeouts.add_timeout(timeout, timeout_dur);
            }
        }
    }

    pub fn remove_idle_in_transaction_session_timeout(&mut self) {
        let session = self.session();
        if let Some(txn) = session.transaction().inner() {
            let txn_id = txn.id.clone();
            self.timeouts
                .remove_timeout(&TimeoutType::IdleInTransactionSession(txn_id));
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a
    /// `tokio::select!` statement and some other branch
    /// completes first, it is guaranteed that no messages were received on this
    /// channel.
    pub async fn recv_timeout(&mut self) -> Option<TimeoutType> {
        self.timeouts.recv().await
    }
}

impl Drop for SessionClient {
    fn drop(&mut self) {
        // We may not have a session if this client was dropped while awaiting
        // a response. In this case, it is the coordinator's responsibility to
        // terminate the session.
        if let Some(session) = self.session.take() {
            // We may not have a connection to the Coordinator if the session was
            // prematurely terminated, for example due to a timeout.
            if let Some(inner) = &self.inner {
                inner.inner.send(Command::Terminate { session, tx: None })
            }
        }
    }
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub enum TimeoutType {
    IdleInTransactionSession(TransactionId),
}

impl Display for TimeoutType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeoutType::IdleInTransactionSession(txn_id) => {
                writeln!(f, "Idle in transaction session for transaction '{txn_id}'")
            }
        }
    }
}

impl From<TimeoutType> for AdapterError {
    fn from(timeout: TimeoutType) -> Self {
        match timeout {
            TimeoutType::IdleInTransactionSession(_) => {
                AdapterError::IdleInTransactionSessionTimeout
            }
        }
    }
}

struct Timeout {
    tx: mpsc::UnboundedSender<TimeoutType>,
    rx: mpsc::UnboundedReceiver<TimeoutType>,
    active_timeouts: BTreeMap<TimeoutType, AbortOnDropHandle<()>>,
}

impl Timeout {
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Timeout {
            tx,
            rx,
            active_timeouts: BTreeMap::new(),
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a
    /// `tokio::select!` statement and some other branch
    /// completes first, it is guaranteed that no messages were received on this
    /// channel.
    ///
    /// <https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety>
    async fn recv(&mut self) -> Option<TimeoutType> {
        self.rx.recv().await
    }

    fn add_timeout(&mut self, timeout: TimeoutType, duration: Duration) {
        let tx = self.tx.clone();
        let timeout_key = timeout.clone();
        let handle = mz_ore::task::spawn(|| format!("{timeout_key}"), async move {
            tokio::time::sleep(duration).await;
            let _ = tx.send(timeout);
        })
        .abort_on_drop();
        self.active_timeouts.insert(timeout_key, handle);
    }

    fn remove_timeout(&mut self, timeout: &TimeoutType) {
        self.active_timeouts.remove(timeout);

        // Remove the timeout from the rx queue if it exists.
        let mut timeouts = Vec::new();
        while let Ok(pending_timeout) = self.rx.try_recv() {
            if timeout != &pending_timeout {
                timeouts.push(pending_timeout);
            }
        }
        for pending_timeout in timeouts {
            self.tx.send(pending_timeout).expect("rx is in this struct");
        }
    }
}
