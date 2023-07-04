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
use mz_build_info::BuildInfo;
use mz_ore::collections::CollectionExt;
use mz_ore::id_gen::{IdAllocator, IdHandle};
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_ore::task::{AbortOnDropHandle, JoinHandleExt};
use mz_ore::thread::JoinOnDropHandle;
use mz_repr::{GlobalId, Row, ScalarType};
use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::EnvironmentId;
use mz_sql::session::hint::ApplicationNameHint;
use mz_sql::session::user::{User, INTROSPECTION_USER};
use mz_sql_parser::parser::ParserStatementError;
use serde_json::json;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::error;
use uuid::Uuid;

use crate::command::{
    Canceled, CatalogDump, Command, ExecuteResponse, GetVariablesResponse, Response,
    StartupResponse,
};
use crate::coord::ExecuteContextExtra;
use crate::error::AdapterError;
use crate::metrics::Metrics;
use crate::session::{EndTransactionAction, PreparedStatement, Session, TransactionId};
use crate::telemetry::{self, SegmentClientExt, StatementFailureType};
use crate::PeekResponseUnary;

/// Inner type of a [`ConnectionId`], `u32` for postgres compatibility.
///
/// Note: Generally you should not use this type directly, and instead use [`ConnectionId`].
pub type ConnectionIdType = u32;

/// An abstraction allowing us to name different connections.
pub type ConnectionId = IdHandle<ConnectionIdType>;

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
    id_alloc: IdAllocator<ConnectionIdType>,
    now: NowFn,
    metrics: Metrics,
    environment_id: EnvironmentId,
    segment_client: Option<mz_segment::Client>,
}

impl Client {
    pub(crate) fn new(
        build_info: &'static BuildInfo,
        cmd_tx: mpsc::UnboundedSender<Command>,
        metrics: Metrics,
        now: NowFn,
        environment_id: EnvironmentId,
        segment_client: Option<mz_segment::Client>,
    ) -> Client {
        Client {
            build_info,
            inner_cmd_tx: cmd_tx,
            id_alloc: IdAllocator::new(1, 1 << 16),
            now,
            metrics,
            environment_id,
            segment_client,
        }
    }

    /// Allocates a client for an incoming connection.
    pub fn new_conn_id(&self) -> Result<ConnectionId, AdapterError> {
        self.id_alloc.alloc().ok_or(AdapterError::IdExhaustionError)
    }

    /// Creates a new session associated with this client for the given user.
    ///
    /// It is the caller's responsibility to have authenticated the user.
    pub fn new_session(&self, conn_id: ConnectionId, user: User) -> Session {
        // We use the system clock to determine when a session connected to Materialize. This is not
        // intended to be 100% accurate and correct, so we don't burden the timestamp oracle with
        // generating a more correct timestamp.
        Session::new(self.build_info, conn_id, user, (self.now)())
    }

    /// Upgrades this client to a session client.
    ///
    /// A session is a connection that has successfully negotiated parameters,
    /// like the user. Most coordinator operations are available only after
    /// upgrading a connection to a session.
    ///
    /// Returns a new client that is bound to the session and a response
    /// containing various details about the startup.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn startup(
        &self,
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
            inner: Some(self.clone()),
            session: Some(session),
            cancel_tx: Arc::clone(&cancel_tx),
            cancel_rx,
            timeouts: Timeout::new(),
            environment_id: self.environment_id.clone(),
            segment_client: self.segment_client.clone(),
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

    /// Cancels the query currently running on the specified connection.
    pub fn cancel_request(&mut self, conn_id: ConnectionIdType, secret_key: u32) {
        self.send(Command::CancelRequest {
            conn_id,
            secret_key,
        });
    }

    /// Executes a single SQL statement that returns rows as the
    /// `mz_introspection` user.
    pub async fn introspection_execute_one(&self, sql: &str) -> Result<Vec<Row>, anyhow::Error> {
        // Connect to the coordinator.
        let conn_id = self.new_conn_id()?;
        let session = self.new_session(conn_id, INTROSPECTION_USER.clone());
        let (mut session_client, _) = self.startup(session).await?;

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
/// See also [`Client`].
pub struct SessionClient {
    // Invariant: inner may only be `None` after the session has been terminated.
    // Once the session is terminated, no communication to the Coordinator
    // should be attempted.
    inner: Option<Client>,
    // Invariant: session may only be `None` during a method call. Every public
    // method must ensure that `Session` is `Some` before it returns.
    session: Option<Session>,
    cancel_tx: Arc<watch::Sender<Canceled>>,
    cancel_rx: watch::Receiver<Canceled>,
    timeouts: Timeout,
    segment_client: Option<mz_segment::Client>,
    environment_id: EnvironmentId,
}

impl SessionClient {
    /// Parses a SQL expression, reporting failures as a telemetry event if
    /// possible.
    pub fn parse(
        &self,
        sql: &str,
    ) -> Result<Result<Vec<Statement<Raw>>, ParserStatementError>, String> {
        match mz_sql::parse::parse_with_limit(sql) {
            Ok(Err(e)) => {
                self.track_statement_parse_failure(&e);
                Ok(Err(e))
            }
            r => r,
        }
    }

    fn track_statement_parse_failure(&self, parse_error: &ParserStatementError) {
        let session = self.session.as_ref().expect("session invariant violated");
        let Some(user_id) = session.user().external_metadata.as_ref().map(|m| m.user_id) else {
            return;
        };
        let Some(segment_client) = &self.segment_client else {
            return;
        };
        let Some(statement_kind) = parse_error.statement else {
            return;
        };
        let Some((action, object_type)) = telemetry::analyze_audited_statement(statement_kind) else {
            return;
        };
        let event_type = StatementFailureType::ParseFailure;
        let event_name = format!(
            "{} {} {}",
            object_type.as_title_case(),
            action.as_title_case(),
            event_type.as_title_case(),
        );
        segment_client.environment_track(
            &self.environment_id,
            session.application_name(),
            user_id,
            event_name,
            json!({
                "statement_kind": statement_kind,
                "error": &parse_error.error,
            }),
        );
    }

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

    /// Saves the parsed statement as a prepared statement.
    ///
    /// The prepared statement is saved in the connection's [`crate::session::Session`]
    /// under the specified name.
    pub async fn prepare(
        &mut self,
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
        self.send(|tx, session| Command::Prepare {
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
        (self.inner().now)()
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
        let now = self.now_datetime();
        let session = self.session.as_mut().expect("session invariant violated");
        let result = match implicit {
            None => session.start_transaction(now, None, None),
            Some(stmts) => {
                session.start_transaction_implicit(now, stmts);
                Ok(())
            }
        };
        result
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
        ctx_extra: ExecuteContextExtra,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.send(|tx, session| Command::CopyRows {
            id,
            columns,
            rows,
            session,
            tx,
            ctx_extra,
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
    pub fn inner(&self) -> &Client {
        self.inner.as_ref().expect("inner invariant violated")
    }

    /// Returns a mutable reference to the inner client.
    pub fn inner_mut(&mut self) -> &mut Client {
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
        let conn_id = session.conn_id().clone();
        self.inner_mut().send({
            let cmd = f(tx, session);
            // Measure the success and error rate of certain commands:
            // - declare reports success of SQL statement planning
            // - execute reports success of dataflow execution
            match cmd {
                Command::Declare { .. } => typ = Some("declare"),
                Command::Execute { .. } => typ = Some("execute"),
                Command::Startup { .. }
                | Command::Prepare { .. }
                | Command::VerifyPreparedStatement { .. }
                | Command::Commit { .. }
                | Command::CancelRequest { .. }
                | Command::PrivilegedCancelRequest { .. }
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
                    self.inner().send(Command::PrivilegedCancelRequest {
                        conn_id: conn_id.clone(),
                    });
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
                inner.send(Command::Terminate { session, tx: None })
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
