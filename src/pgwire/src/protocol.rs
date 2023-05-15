// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::future::Future;
use std::iter;
use std::mem;

use byteorder::{ByteOrder, NetworkEndian};
use futures::future::{pending, BoxFuture, FutureExt};
use itertools::izip;
use postgres::error::SqlState;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::select;
use tokio::time::{self, Duration, Instant};
use tracing::{debug, warn, Instrument};

use mz_adapter::session::{
    EndTransactionAction, InProgressRows, Portal, PortalState, RowBatchStream, TransactionStatus,
};
use mz_adapter::{AdapterNotice, ExecuteResponse, PeekResponseUnary, RowsFuture};
use mz_frontegg_auth::{Authentication as FronteggAuthentication, Claims};
use mz_ore::cast::CastFrom;
use mz_ore::netio::AsyncReady;
use mz_ore::str::StrExt;
use mz_pgcopy::CopyFormatParams;
use mz_repr::GlobalId;
use mz_repr::{Datum, RelationDesc, RelationType, Row, RowArena, ScalarType};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{FetchDirection, Ident, Raw, Statement};
use mz_sql::plan::{CopyFormat, ExecuteTimeout, StatementDesc};
use mz_sql::session::user::{ExternalUserMetadata, User, INTERNAL_USER_NAMES};
use mz_sql::session::vars::VarInput;

use crate::codec::FramedConn;
use crate::message::{
    self, BackendMessage, ErrorResponse, FrontendMessage, Severity, VERSIONS, VERSION_3,
};
use crate::server::{Conn, TlsMode};

/// Reports whether the given stream begins with a pgwire handshake.
///
/// To avoid false negatives, there must be at least eight bytes in `buf`.
pub fn match_handshake(buf: &[u8]) -> bool {
    // The pgwire StartupMessage looks like this:
    //
    //     i32 - Length of entire message.
    //     i32 - Protocol version number.
    //     [String] - Arbitrary key-value parameters of any length.
    //
    // Since arbitrary parameters can be included in the StartupMessage, the
    // first Int32 is worthless, since the message could have any length.
    // Instead, we sniff the protocol version number.
    if buf.len() < 8 {
        return false;
    }
    let version = NetworkEndian::read_i32(&buf[4..8]);
    VERSIONS.contains(&version)
}

/// Parameters for the [`run`] function.
pub struct RunParams<'a, A> {
    /// The TLS mode of the pgwire server.
    pub tls_mode: Option<TlsMode>,
    /// A client for the adapter.
    pub adapter_client: mz_adapter::ConnClient,
    /// The connection to the client.
    pub conn: &'a mut FramedConn<A>,
    /// The protocol version that the client provided in the startup message.
    pub version: i32,
    /// The parameters that the client provided in the startup message.
    pub params: BTreeMap<String, String>,
    /// Frontegg authentication.
    pub frontegg: Option<&'a FronteggAuthentication>,
    /// Whether this is an internal server that permits access to restricted
    /// system resources.
    pub internal: bool,
}

/// Runs a pgwire connection to completion.
///
/// This involves responding to `FrontendMessage::StartupMessage` and all future
/// requests until the client terminates the connection or a fatal error occurs.
///
/// Note that this function returns successfully even upon delivering a fatal
/// error to the client. It only returns `Err` if an unexpected I/O error occurs
/// while communicating with the client, e.g., if the connection is severed in
/// the middle of a request.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn run<'a, A>(
    RunParams {
        tls_mode,
        adapter_client,
        conn,
        version,
        mut params,
        frontegg,
        internal,
    }: RunParams<'a, A>,
) -> Result<(), io::Error>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin,
{
    if version != VERSION_3 {
        return conn
            .send(ErrorResponse::fatal(
                SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                "server does not support the client's requested protocol version",
            ))
            .await;
    }

    let user = params.remove("user").unwrap_or_else(String::new);

    if internal {
        // The internal server can only be used to connect to the internal users.
        if !INTERNAL_USER_NAMES.contains(&user) {
            let msg = format!("unauthorized login to user '{user}'");
            return conn
                .send(ErrorResponse::fatal(SqlState::INSUFFICIENT_PRIVILEGE, msg))
                .await;
        }
    } else {
        // The external server cannot be used to connect to any system users.
        if mz_adapter::catalog::is_reserved_role_name(user.as_str()) {
            let msg = format!("unauthorized login to user '{user}'");
            return conn
                .send(ErrorResponse::fatal(SqlState::INSUFFICIENT_PRIVILEGE, msg))
                .await;
        }
    }

    // Validate that the connection is compatible with the TLS mode.
    //
    // The match here explicitly spells out all cases to be resilient to
    // future changes to TlsMode.
    match (tls_mode, conn.inner()) {
        (None, Conn::Unencrypted(_)) => (),
        (None, Conn::Ssl(_)) => unreachable!(),
        (Some(TlsMode::Allow), Conn::Unencrypted(_)) => (),
        (Some(TlsMode::Allow), Conn::Ssl(_)) => (),
        (Some(TlsMode::Require), Conn::Ssl(_)) => (),
        (Some(TlsMode::Require), Conn::Unencrypted(_)) => {
            return conn
                .send(ErrorResponse::fatal(
                    SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                    "TLS encryption is required",
                ))
                .await;
        }
    }

    // Construct session.
    let mut session = adapter_client.new_session(User {
        name: user.clone(),
        external_metadata: None,
    });

    let is_expired = if let Some(frontegg) = frontegg {
        conn.send(BackendMessage::AuthenticationCleartextPassword)
            .await?;
        conn.flush().await?;
        let password = match conn.recv().await? {
            Some(FrontendMessage::Password { password }) => password,
            _ => {
                return conn
                    .send(ErrorResponse::fatal(
                        SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                        "expected Password message",
                    ))
                    .await
            }
        };
        let admin_role = frontegg.admin_role();
        let external_metadata_rx = session.retain_external_metadata_transmitter();
        match frontegg
            .exchange_password_for_token(&password)
            .await
            .and_then(|token| {
                frontegg.continuously_validate_access_token(token, user.clone(), move |claims| {
                    let external_metadata = convert_claims_to_external_metadata(claims, admin_role);
                    // Ignore error if client has hung up.
                    let _ = external_metadata_rx.send(external_metadata);
                })
            }) {
            Ok(is_expired) => {
                // Make sure to apply the initial claims.
                session.apply_external_metadata_updates();
                is_expired.left_future()
            }
            Err(e) => {
                warn!("PGwire connection failed authentication: {}", e);
                return conn
                    .send(ErrorResponse::fatal(
                        SqlState::INVALID_PASSWORD,
                        "invalid password",
                    ))
                    .await;
            }
        }
    } else {
        // No frontegg check, so is_expired never resolves.
        pending().right_future()
    };

    for (name, value) in params {
        let settings = match name.as_str() {
            "options" => match parse_options(&value) {
                Ok(opts) => opts,
                Err(()) => {
                    session.add_notice(AdapterNotice::BadStartupSetting {
                        name,
                        reason: "could not parse".into(),
                    });
                    continue;
                }
            },
            _ => vec![(name, value)],
        };
        for (key, val) in settings {
            const LOCAL: bool = false;
            // TODO: Issuing an error here is better than what we did before
            // (silently ignore errors on set), but erroring the connection
            // might be the better behavior. We maybe need to support more
            // options sent by psql and drivers before we can safely do this.
            if let Err(err) = session.vars_mut().set(&key, VarInput::Flat(&val), LOCAL) {
                session.add_notice(AdapterNotice::BadStartupSetting {
                    name: key,
                    reason: err.to_string(),
                });
            }
        }
    }
    session
        .vars_mut()
        .end_transaction(EndTransactionAction::Commit);

    let mut buf = vec![BackendMessage::AuthenticationOk];
    for var in session.vars().notify_set() {
        buf.push(BackendMessage::ParameterStatus(var.name(), var.value()));
    }
    buf.push(BackendMessage::BackendKeyData {
        conn_id: session.conn_id(),
        secret_key: session.secret_key(),
    });
    // Immediately respond with connection success without waiting on the
    // coordinator. This allows us to better meet SLA goals (like able to
    // connect) at the expense of some specific problems:
    // - Startup notices (like unknown database) won't be sent until the first
    //   query.
    // - An unknown username won't error until the first query. This won't be
    //   noticed by users, though, since with frontegg enabled unknown users are
    //   always created.
    buf.push(BackendMessage::ReadyForQuery(session.transaction().into()));
    conn.send_all(buf).await?;
    conn.flush().await?;

    // Register session with adapter.
    let (adapter_client, startup) = match adapter_client.startup(session).await {
        Ok(startup) => startup,
        Err(e) => {
            return conn
                .send(ErrorResponse::from_adapter_error(Severity::Fatal, e))
                .await
        }
    };

    let mut buf = Vec::new();
    // NoticeResponse messages can be sent at any time
    // (https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-ASYNC),
    // so it is within spec to send them after the initial ReadyForQuery.
    for startup_message in startup.messages {
        buf.push(ErrorResponse::from_startup_message(startup_message).into());
    }
    conn.send_all(buf).await?;
    conn.flush().await?;

    let machine = StateMachine {
        conn,
        adapter_client,
    };

    select! {
        r = machine.run() => r,
        _ = is_expired => {
            conn
                .send(ErrorResponse::fatal(SqlState::INVALID_AUTHORIZATION_SPECIFICATION, "authentication expired"))
                .await?;
            conn.flush().await
        }
    }
}

fn convert_claims_to_external_metadata(claims: Claims, admin_role: &str) -> ExternalUserMetadata {
    ExternalUserMetadata {
        user_id: claims.best_user_id(),
        group_id: claims.tenant_id,
        admin: claims.admin(admin_role),
    }
}

/// Returns (name, value) session settings pairs from an options value.
///
/// From Postgres, see pg_split_opts in postinit.c and process_postgres_switches
/// in postgres.c.
fn parse_options(value: &str) -> Result<Vec<(String, String)>, ()> {
    let opts = split_options(value);
    let mut pairs = Vec::with_capacity(opts.len());
    let mut seen_prefix = false;
    for opt in opts {
        if !seen_prefix {
            if opt == "-c" {
                seen_prefix = true;
            } else {
                let (key, val) = parse_option(&opt)?;
                pairs.push((key.to_owned(), val.to_owned()));
            }
        } else {
            let (key, val) = opt.split_once('=').ok_or(())?;
            pairs.push((key.to_owned(), val.to_owned()));
            seen_prefix = false;
        }
    }
    Ok(pairs)
}

/// Returns the parsed key and value from option of the form `--key=value`, `-c
/// key=value`, or `-ckey=value`. Keys replace `-` with `_`. Returns an error if
/// there was some other prefix.
fn parse_option(option: &str) -> Result<(&str, &str), ()> {
    let (key, value) = option.split_once('=').ok_or(())?;
    for prefix in &["-c", "--"] {
        if let Some(key) = key.strip_prefix(prefix) {
            return Ok((key, value));
        }
    }
    Err(())
}

/// Splits value by any number of spaces except those preceeded by `\`.
fn split_options(value: &str) -> Vec<String> {
    let mut strs = Vec::new();
    // Need to build a string because of the escaping, so we can't simply
    // subslice into value, and this isn't called enough to need to make it
    // smart so it only builds a string if needed.
    let mut current = String::new();
    let mut was_slash = false;
    for c in value.chars() {
        was_slash = match c {
            ' ' => {
                if was_slash {
                    current.push(' ');
                } else if !current.is_empty() {
                    // To ignore multiple spaces in a row, only push if current
                    // is not empty.
                    strs.push(std::mem::take(&mut current));
                }
                false
            }
            '\\' => {
                if was_slash {
                    // Two slashes in a row will add a slash and not escape the
                    // next char.
                    current.push('\\');
                    false
                } else {
                    true
                }
            }
            _ => {
                current.push(c);
                false
            }
        };
    }
    // A `\` at the end will be ignored.
    if !current.is_empty() {
        strs.push(current);
    }
    strs
}

#[derive(Debug)]
enum State {
    Ready,
    Drain,
    Done,
}

struct StateMachine<'a, A> {
    conn: &'a mut FramedConn<A>,
    adapter_client: mz_adapter::SessionClient,
}

impl<'a, A> StateMachine<'a, A>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin + 'a,
{
    // Manually desugar this (don't use `async fn run`) here because a much better
    // error message is produced if there are problems with Send or other traits
    // somewhere within the Future.
    #[allow(clippy::manual_async_fn)]
    #[tracing::instrument(level = "debug", skip_all)]
    fn run(mut self) -> impl Future<Output = Result<(), io::Error>> + Send + 'a {
        async move {
            let mut state = State::Ready;
            loop {
                self.send_pending_notices().await?;
                state = match state {
                    State::Ready => self.advance_ready().await?,
                    State::Drain => self.advance_drain().await?,
                    State::Done => return Ok(()),
                };
                self.adapter_client
                    .add_idle_in_transaction_session_timeout();
            }
        }
    }

    async fn advance_ready(&mut self) -> Result<State, io::Error> {
        // Handle timeouts first so we don't execute any statements when there's a pending timeout.
        let message = select! {
            biased;

            // `recv_timeout()` is cancel-safe as per it's docs.
            Some(timeout) = self.adapter_client.recv_timeout() => {
                let error_response = ErrorResponse::from_adapter_error(Severity::Fatal, timeout.into());
                self.adapter_client.terminate().await;
                // We must wait for the client to send a request before we can send the error response.
                // Due to the PG wire protocol, we can't send an ErrorResponse unless it is in response
                // to a client message.
                let _ = self.conn.recv().await?;
                return self.error(error_response).await;
            },
            // `recv()` is cancel-safe as per it's docs.
            message = self.conn.recv() => message?,
        };

        self.adapter_client
            .remove_idle_in_transaction_session_timeout();
        self.adapter_client.reset_canceled();

        // NOTE(guswynn): we could consider adding spans to all message types. Currently
        // only a few message types seem useful.
        let message_name = message.as_ref().map(|m| m.name()).unwrap_or_default();

        let next_state = match message {
            Some(FrontendMessage::Query { sql }) => {
                let query_root_span =
                    tracing::debug_span!(parent: None, "advance_ready", otel.name = message_name);
                query_root_span.follows_from(tracing::Span::current());
                self.query(sql).instrument(query_root_span).await?
            }
            Some(FrontendMessage::Parse {
                name,
                sql,
                param_types,
            }) => self.parse(name, sql, param_types).await?,
            Some(FrontendMessage::Bind {
                portal_name,
                statement_name,
                param_formats,
                raw_params,
                result_formats,
            }) => {
                self.bind(
                    portal_name,
                    statement_name,
                    param_formats,
                    raw_params,
                    result_formats,
                )
                .await?
            }
            Some(FrontendMessage::Execute {
                portal_name,
                max_rows,
            }) => {
                let max_rows = match usize::try_from(max_rows) {
                    Ok(0) | Err(_) => ExecuteCount::All, // If `max_rows < 0`, no limit.
                    Ok(n) => ExecuteCount::Count(n),
                };
                let execute_root_span =
                    tracing::debug_span!(parent: None, "advance_ready", otel.name = message_name);
                execute_root_span.follows_from(tracing::Span::current());
                let state = self
                    .execute(
                        portal_name,
                        max_rows,
                        portal_exec_message,
                        None,
                        ExecuteTimeout::None,
                    )
                    .instrument(execute_root_span)
                    .await?;
                // Close the current transaction if we are in an implicit transaction.
                // In PostgreSQL, when using the extended query protocol, some statements may
                // trigger an eager commit of the current implicit transaction,
                // see: <https://git.postgresql.org/gitweb/?p=postgresql.git&a=commitdiff&h=f92944137>.
                // In Materialize however, we eagerly commit all statements outside of an explicit
                // transaction when using the extended query protocol. This allows us to remove
                // the ambiguity between multiple and single statement implicit transactions when
                // using the extended query protocol and apply some optimizations to single
                // statement transactions.
                if self.adapter_client.session().transaction().is_implicit() {
                    self.commit_transaction().await?;
                }
                state
            }
            Some(FrontendMessage::DescribeStatement { name }) => {
                self.describe_statement(&name).await?
            }
            Some(FrontendMessage::DescribePortal { name }) => self.describe_portal(&name).await?,
            Some(FrontendMessage::CloseStatement { name }) => self.close_statement(name).await?,
            Some(FrontendMessage::ClosePortal { name }) => self.close_portal(name).await?,
            Some(FrontendMessage::Flush) => self.flush().await?,
            Some(FrontendMessage::Sync) => self.sync().await?,
            Some(FrontendMessage::Terminate) => State::Done,

            Some(FrontendMessage::CopyData(_))
            | Some(FrontendMessage::CopyDone)
            | Some(FrontendMessage::CopyFail(_))
            | Some(FrontendMessage::Password { .. }) => State::Drain,
            None => State::Done,
        };

        Ok(next_state)
    }

    async fn advance_drain(&mut self) -> Result<State, io::Error> {
        let message = self.conn.recv().await?;
        if message.is_some() {
            self.adapter_client
                .remove_idle_in_transaction_session_timeout();
        }
        match message {
            Some(FrontendMessage::Sync) => self.sync().await,
            None => Ok(State::Done),
            _ => Ok(State::Drain),
        }
    }

    async fn one_query(&mut self, stmt: Statement<Raw>) -> Result<State, io::Error> {
        // Bind the portal. Note that this does not set the empty string prepared
        // statement.
        let param_types = vec![];
        const EMPTY_PORTAL: &str = "";
        if let Err(e) = self
            .adapter_client
            .declare(EMPTY_PORTAL.to_string(), stmt, param_types)
            .await
        {
            return self
                .error(ErrorResponse::from_adapter_error(Severity::Error, e))
                .await;
        }

        let stmt_desc = self
            .adapter_client
            .session()
            .get_portal_unverified(EMPTY_PORTAL)
            .map(|portal| portal.desc.clone())
            .expect("unnamed portal should be present");
        if !stmt_desc.param_types.is_empty() {
            return self
                .error(ErrorResponse::error(
                    SqlState::UNDEFINED_PARAMETER,
                    "there is no parameter $1",
                ))
                .await;
        }

        // Maybe send row description.
        if let Some(relation_desc) = &stmt_desc.relation_desc {
            if !stmt_desc.is_copy {
                let formats = vec![mz_pgrepr::Format::Text; stmt_desc.arity()];
                self.send(BackendMessage::RowDescription(
                    message::encode_row_description(relation_desc, &formats),
                ))
                .await?;
            }
        }

        let result = match self.adapter_client.execute(EMPTY_PORTAL.to_string()).await {
            Ok(response) => {
                self.send_pending_notices().await?;
                self.send_execute_response(
                    response,
                    stmt_desc.relation_desc,
                    EMPTY_PORTAL.to_string(),
                    ExecuteCount::All,
                    portal_exec_message,
                    None,
                    ExecuteTimeout::None,
                )
                .await
            }
            Err(e) => {
                self.send_pending_notices().await?;
                self.error(ErrorResponse::from_adapter_error(Severity::Error, e))
                    .await
            }
        };

        // Destroy the portal.
        self.adapter_client.session().remove_portal(EMPTY_PORTAL);

        result
    }

    fn start_transaction(&mut self, stmts: Option<usize>) {
        // start_transaction can't error (but assert that just in case it changes in
        // the future.
        let res = self.adapter_client.start_transaction(stmts);
        assert!(res.is_ok());
    }

    // See "Multiple Statements in a Simple Query" which documents how implicit
    // transactions are handled.
    // From https://www.postgresql.org/docs/current/protocol-flow.html
    async fn query(&mut self, sql: String) -> Result<State, io::Error> {
        // Parse first before doing any transaction checking.
        let stmts = match parse_sql(&sql) {
            Ok(stmts) => stmts,
            Err(err) => {
                self.error(err).await?;
                return self.ready().await;
            }
        };

        let num_stmts = stmts.len();

        // Compare with postgres' backend/tcop/postgres.c exec_simple_query.
        for stmt in stmts {
            // In an aborted transaction, reject all commands except COMMIT/ROLLBACK.
            if self.is_aborted_txn() && !is_txn_exit_stmt(Some(&stmt)) {
                self.aborted_txn_error().await?;
                break;
            }

            // Start an implicit transaction if we aren't in any transaction and there's
            // more than one statement. This mirrors the `use_implicit_block` variable in
            // postgres.
            //
            // This needs to be done in the loop instead of once at the top because
            // a COMMIT/ROLLBACK statement needs to start a new transaction on next
            // statement.
            self.start_transaction(Some(num_stmts));

            match self.one_query(stmt).await? {
                State::Ready => (),
                State::Drain => break,
                State::Done => return Ok(State::Done),
            }
        }

        // Implicit transactions are closed at the end of a Query message.
        {
            if self.adapter_client.session().transaction().is_implicit() {
                self.commit_transaction().await?;
            }
        }

        if num_stmts == 0 {
            self.send(BackendMessage::EmptyQueryResponse).await?;
        }

        self.ready().await
    }

    async fn parse(
        &mut self,
        name: String,
        sql: String,
        param_oids: Vec<u32>,
    ) -> Result<State, io::Error> {
        // Start a transaction if we aren't in one.
        self.start_transaction(Some(1));

        let mut param_types = vec![];
        for oid in param_oids {
            match mz_pgrepr::Type::from_oid(oid) {
                Ok(ty) => match ScalarType::try_from(&ty) {
                    Ok(ty) => param_types.push(Some(ty)),
                    Err(err) => {
                        return self
                            .error(ErrorResponse::error(
                                SqlState::INVALID_PARAMETER_VALUE,
                                err.to_string(),
                            ))
                            .await
                    }
                },
                Err(_) if oid == 0 => param_types.push(None),
                Err(e) => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::PROTOCOL_VIOLATION,
                            e.to_string(),
                        ))
                        .await;
                }
            }
        }

        let stmts = match parse_sql(&sql) {
            Ok(stmts) => stmts,
            Err(err) => {
                return self.error(err).await;
            }
        };
        if stmts.len() > 1 {
            return self
                .error(ErrorResponse::error(
                    SqlState::INTERNAL_ERROR,
                    "cannot insert multiple commands into a prepared statement",
                ))
                .await;
        }
        let maybe_stmt = stmts.into_iter().next();
        if self.is_aborted_txn() && !is_txn_exit_stmt(maybe_stmt.as_ref()) {
            return self.aborted_txn_error().await;
        }
        match self
            .adapter_client
            .describe(name, maybe_stmt, param_types)
            .await
        {
            Ok(()) => {
                self.send(BackendMessage::ParseComplete).await?;
                Ok(State::Ready)
            }
            Err(e) => {
                self.error(ErrorResponse::from_adapter_error(Severity::Error, e))
                    .await
            }
        }
    }

    /// Commits and clears the current transaction.
    async fn commit_transaction(&mut self) -> Result<(), io::Error> {
        self.end_transaction(EndTransactionAction::Commit).await
    }

    /// Rollback and clears the current transaction.
    async fn rollback_transaction(&mut self) -> Result<(), io::Error> {
        self.end_transaction(EndTransactionAction::Rollback).await
    }

    /// End a transaction and report to the user if an error occurred.
    async fn end_transaction(&mut self, action: EndTransactionAction) -> Result<(), io::Error> {
        let resp = self.adapter_client.end_transaction(action).await;
        if let Err(err) = resp {
            self.send(BackendMessage::ErrorResponse(
                ErrorResponse::from_adapter_error(Severity::Error, err),
            ))
            .await?;
        }
        Ok(())
    }

    async fn bind(
        &mut self,
        portal_name: String,
        statement_name: String,
        param_formats: Vec<mz_pgrepr::Format>,
        raw_params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<mz_pgrepr::Format>,
    ) -> Result<State, io::Error> {
        // Start a transaction if we aren't in one.
        self.start_transaction(Some(1));

        let aborted_txn = self.is_aborted_txn();
        let stmt = match self
            .adapter_client
            .get_prepared_statement(&statement_name)
            .await
        {
            Ok(stmt) => stmt,
            Err(err) => {
                return self
                    .error(ErrorResponse::from_adapter_error(Severity::Error, err))
                    .await
            }
        };

        let param_types = &stmt.desc().param_types;
        if param_types.len() != raw_params.len() {
            let message = format!(
                "bind message supplies {actual} parameters, \
                 but prepared statement \"{name}\" requires {expected}",
                name = statement_name,
                actual = raw_params.len(),
                expected = param_types.len()
            );
            return self
                .error(ErrorResponse::error(SqlState::PROTOCOL_VIOLATION, message))
                .await;
        }
        let param_formats = match pad_formats(param_formats, raw_params.len()) {
            Ok(param_formats) => param_formats,
            Err(msg) => {
                return self
                    .error(ErrorResponse::error(SqlState::PROTOCOL_VIOLATION, msg))
                    .await
            }
        };
        if aborted_txn && !is_txn_exit_stmt(stmt.sql()) {
            return self.aborted_txn_error().await;
        }
        let buf = RowArena::new();
        let mut params = vec![];
        for (raw_param, mz_typ, format) in izip!(raw_params, param_types, param_formats) {
            let pg_typ = mz_pgrepr::Type::from(mz_typ);
            let datum = match raw_param {
                None => Datum::Null,
                Some(bytes) => match mz_pgrepr::Value::decode(format, &pg_typ, &bytes) {
                    Ok(param) => param.into_datum(&buf, &pg_typ),
                    Err(err) => {
                        let msg = format!("unable to decode parameter: {}", err);
                        return self
                            .error(ErrorResponse::error(SqlState::INVALID_PARAMETER_VALUE, msg))
                            .await;
                    }
                },
            };
            params.push((datum, mz_typ.clone()))
        }

        let result_formats = match pad_formats(
            result_formats,
            stmt.desc()
                .relation_desc
                .clone()
                .map(|desc| desc.typ().column_types.len())
                .unwrap_or(0),
        ) {
            Ok(result_formats) => result_formats,
            Err(msg) => {
                return self
                    .error(ErrorResponse::error(SqlState::PROTOCOL_VIOLATION, msg))
                    .await
            }
        };

        if let Some(desc) = stmt.desc().relation_desc.clone() {
            for (format, ty) in result_formats.iter().zip(desc.iter_types()) {
                match (format, &ty.scalar_type) {
                    (mz_pgrepr::Format::Binary, mz_repr::ScalarType::List { .. }) => {
                        return self
                            .error(ErrorResponse::error(
                                SqlState::PROTOCOL_VIOLATION,
                                "binary encoding of list types is not implemented",
                            ))
                            .await;
                    }
                    (mz_pgrepr::Format::Binary, mz_repr::ScalarType::Map { .. }) => {
                        return self
                            .error(ErrorResponse::error(
                                SqlState::PROTOCOL_VIOLATION,
                                "binary encoding of map types is not implemented",
                            ))
                            .await;
                    }
                    _ => (),
                }
            }
        }

        let desc = stmt.desc().clone();
        let revision = stmt.catalog_revision;
        let stmt = stmt.sql().cloned();
        if let Err(err) = self.adapter_client.session().set_portal(
            portal_name,
            desc,
            stmt,
            params,
            result_formats,
            revision,
        ) {
            return self
                .error(ErrorResponse::from_adapter_error(Severity::Error, err))
                .await;
        }

        self.send(BackendMessage::BindComplete).await?;
        Ok(State::Ready)
    }

    fn execute(
        &mut self,
        portal_name: String,
        max_rows: ExecuteCount,
        get_response: GetResponse,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
    ) -> BoxFuture<'_, Result<State, io::Error>> {
        async move {
            let aborted_txn = self.is_aborted_txn();

            // Check if the portal has been started and can be continued.
            let portal = match self
                .adapter_client
                .session()
                .get_portal_unverified_mut(&portal_name)
            {
                Some(portal) => portal,
                None => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::INVALID_CURSOR_NAME,
                            format!("portal {} does not exist", portal_name.quoted()),
                        ))
                        .await;
                }
            };

            // In an aborted transaction, reject all commands except COMMIT/ROLLBACK.
            let txn_exit_stmt = is_txn_exit_stmt(portal.stmt.as_ref());
            if aborted_txn && !txn_exit_stmt {
                return self.aborted_txn_error().await;
            }

            let row_desc = portal.desc.relation_desc.clone();

            match &mut portal.state {
                PortalState::NotStarted => {
                    // Start a transaction if we aren't in one. Postgres does this both here and
                    // in bind. We don't do it in bind because I'm not sure what purpose it would
                    // serve us (i.e., I'm not aware of a pgtest that would differ between us and
                    // Postgres).
                    self.start_transaction(Some(1));

                    match self.adapter_client.execute(portal_name.clone()).await {
                        Ok(response) => {
                            self.send_pending_notices().await?;
                            self.send_execute_response(
                                response,
                                row_desc,
                                portal_name,
                                max_rows,
                                get_response,
                                fetch_portal_name,
                                timeout,
                            )
                            .await
                        }
                        Err(e) => {
                            self.send_pending_notices().await?;
                            self.error(ErrorResponse::from_adapter_error(Severity::Error, e))
                                .await
                        }
                    }
                }
                PortalState::InProgress(rows) => {
                    let rows = rows.take().expect("InProgress rows must be populated");
                    self.send_rows(
                        row_desc.expect("portal missing row desc on resumption"),
                        portal_name,
                        rows,
                        max_rows,
                        get_response,
                        fetch_portal_name,
                        timeout,
                    )
                    .await
                }
                // FETCH is an awkward command for our current architecture. In Postgres it
                // will extract <count> rows from the target portal, cache them, and return
                // them to the user as requested. Its command tag is always FETCH <num rows
                // extracted>. In Materialize, since we have chosen to not fully support FETCH,
                // we must remember the number of rows that were returned. Use this tag to
                // remember that information and return it.
                PortalState::Completed(Some(tag)) => {
                    let tag = tag.to_string();
                    self.send(BackendMessage::CommandComplete { tag }).await?;
                    Ok(State::Ready)
                }
                PortalState::Completed(None) => {
                    self.error(ErrorResponse::error(
                        SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE,
                        format!(
                            "portal {} cannot be run",
                            Ident::new(portal_name).to_ast_string_stable()
                        ),
                    ))
                    .await
                }
            }
        }
        .boxed()
    }

    async fn describe_statement(&mut self, name: &str) -> Result<State, io::Error> {
        // Start a transaction if we aren't in one.
        self.start_transaction(Some(1));

        let stmt = match self.adapter_client.get_prepared_statement(name).await {
            Ok(stmt) => stmt,
            Err(err) => {
                return self
                    .error(ErrorResponse::from_adapter_error(Severity::Error, err))
                    .await
            }
        };
        // Cloning to avoid a mutable borrow issue because `send` also uses `adapter_client`
        let parameter_desc = BackendMessage::ParameterDescription(
            stmt.desc()
                .param_types
                .iter()
                .map(mz_pgrepr::Type::from)
                .collect(),
        );
        // Claim that all results will be output in text format, even
        // though the true result formats are not yet known. A bit
        // weird, but this is the behavior that PostgreSQL specifies.
        let formats = vec![mz_pgrepr::Format::Text; stmt.desc().arity()];
        let row_desc = describe_rows(stmt.desc(), &formats);
        self.send_all([parameter_desc, row_desc]).await?;
        Ok(State::Ready)
    }

    async fn describe_portal(&mut self, name: &str) -> Result<State, io::Error> {
        // Start a transaction if we aren't in one.
        self.start_transaction(Some(1));

        let session = self.adapter_client.session();
        let row_desc = session
            .get_portal_unverified(name)
            .map(|portal| describe_rows(&portal.desc, &portal.result_formats));
        match row_desc {
            Some(row_desc) => {
                self.send(row_desc).await?;
                Ok(State::Ready)
            }
            None => {
                self.error(ErrorResponse::error(
                    SqlState::INVALID_CURSOR_NAME,
                    format!("portal {} does not exist", name.quoted()),
                ))
                .await
            }
        }
    }

    async fn close_statement(&mut self, name: String) -> Result<State, io::Error> {
        self.adapter_client
            .session()
            .remove_prepared_statement(&name);
        self.send(BackendMessage::CloseComplete).await?;
        Ok(State::Ready)
    }

    async fn close_portal(&mut self, name: String) -> Result<State, io::Error> {
        self.adapter_client.session().remove_portal(&name);
        self.send(BackendMessage::CloseComplete).await?;
        Ok(State::Ready)
    }

    fn complete_portal(&mut self, name: &str) {
        let portal = self
            .adapter_client
            .session()
            .get_portal_unverified_mut(name)
            .expect("portal should exist");
        portal.state = PortalState::Completed(None);
    }

    async fn fetch(
        &mut self,
        name: String,
        count: Option<FetchDirection>,
        max_rows: ExecuteCount,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
    ) -> Result<State, io::Error> {
        // Unlike Execute, no count specified in FETCH returns 1 row, and 0 means 0
        // instead of All.
        let count = count.unwrap_or(FetchDirection::ForwardCount(1));

        // Figure out how many rows we should send back by looking at the various
        // combinations of the execute and fetch.
        //
        // In Postgres, Fetch will cache <count> rows from the target portal and
        // return those as requested (if, say, an Execute message was sent with a
        // max_rows < the Fetch's count). We expect that case to be incredibly rare and
        // so have chosen to not support it until users request it. This eases
        // implementation difficulty since we don't have to be able to "send" rows to
        // a buffer.
        //
        // TODO(mjibson): Test this somehow? Need to divide up the pgtest files in
        // order to have some that are not Postgres compatible.
        let count = match (max_rows, count) {
            (ExecuteCount::Count(max_rows), FetchDirection::ForwardCount(count)) => {
                let count = usize::cast_from(count);
                if max_rows < count {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::FEATURE_NOT_SUPPORTED,
                            "Execute with max_rows < a FETCH's count is not supported",
                        ))
                        .await;
                }
                ExecuteCount::Count(count)
            }
            (ExecuteCount::Count(_), FetchDirection::ForwardAll) => {
                return self
                    .error(ErrorResponse::error(
                        SqlState::FEATURE_NOT_SUPPORTED,
                        "Execute with max_rows of a FETCH ALL is not supported",
                    ))
                    .await;
            }
            (ExecuteCount::All, FetchDirection::ForwardAll) => ExecuteCount::All,
            (ExecuteCount::All, FetchDirection::ForwardCount(count)) => {
                ExecuteCount::Count(usize::cast_from(count))
            }
        };
        let cursor_name = name.to_string();
        self.execute(
            cursor_name,
            count,
            fetch_message,
            fetch_portal_name,
            timeout,
        )
        .await
    }

    async fn flush(&mut self) -> Result<State, io::Error> {
        self.conn.flush().await?;
        Ok(State::Ready)
    }

    /// Sends a backend message to the client, after applying a severity filter.
    ///
    /// The message is only sent if its severity is above the severity set
    /// in the session, with the default value being NOTICE.
    async fn send<M>(&mut self, message: M) -> Result<(), io::Error>
    where
        M: Into<BackendMessage>,
    {
        let message: BackendMessage = message.into();
        match message {
            BackendMessage::ErrorResponse(ref err) => {
                let minimum_client_severity =
                    self.adapter_client.session().vars().client_min_messages();
                if err
                    .severity
                    .should_output_to_client(minimum_client_severity)
                {
                    self.conn.send(message).await
                } else {
                    Ok(())
                }
            }
            _ => self.conn.send(message).await,
        }
    }

    pub async fn send_all(
        &mut self,
        messages: impl IntoIterator<Item = BackendMessage>,
    ) -> Result<(), io::Error> {
        for m in messages {
            self.send(m).await?;
        }
        Ok(())
    }

    async fn sync(&mut self) -> Result<State, io::Error> {
        // Close the current transaction if we are in an implicit transaction.
        if self.adapter_client.session().transaction().is_implicit() {
            self.commit_transaction().await?;
        }
        self.ready().await
    }

    async fn ready(&mut self) -> Result<State, io::Error> {
        let txn_state = self.adapter_client.session().transaction().into();
        self.send(BackendMessage::ReadyForQuery(txn_state)).await?;
        self.flush().await
    }

    // Converts a RowsFuture to a stream while also checking for connection close.
    async fn row_future_to_stream<'s, 'p>(
        &'s mut self,
        parent: &'p tracing::Span,
        mut rows: RowsFuture,
    ) -> Result<RowBatchStream, io::Error>
    where
        'p: 's,
    {
        // Do not include self.adapter_client.canceled() here because cancel messages
        // will propagate through the PeekResponse. select is safe to use because if
        // close finishes, rows is canceled, which is the intended behavior.
        let span = tracing::debug_span!(parent: parent, "row_future_to_stream");
        async move {
            loop {
                tokio::select! {
                    err = self.conn.wait_closed() => return Err(err),
                    rows = &mut rows => {
                        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                        tx.send(rows).expect("send must succeed");
                        return Ok(rx);
                    }
                    notice = self.adapter_client.session().recv_notice() => {
                        self.send(ErrorResponse::from_adapter_notice(notice))
                            .await?;
                        self.conn.flush().await?;
                    }
                }
            }
        }
        .instrument(span)
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_execute_response(
        &mut self,
        response: ExecuteResponse,
        row_desc: Option<RelationDesc>,
        portal_name: String,
        max_rows: ExecuteCount,
        get_response: GetResponse,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
    ) -> Result<State, io::Error> {
        let mut tag = response.tag();

        macro_rules! command_complete {
            () => {{
                self.send(BackendMessage::CommandComplete {
                    tag: tag
                        .take()
                        .expect("command_complete only called on tag-generating results"),
                })
                .await?;
                Ok(State::Ready)
            }};
        }

        let r = match response {
            ExecuteResponse::Canceled => {
                return self
                    .error(ErrorResponse::error(
                        SqlState::QUERY_CANCELED,
                        "canceling statement due to user request",
                    ))
                    .await;
            }
            ExecuteResponse::ClosedCursor => {
                self.complete_portal(&portal_name);
                command_complete!()
            }
            ExecuteResponse::DeclaredCursor => {
                self.complete_portal(&portal_name);
                command_complete!()
            }
            ExecuteResponse::EmptyQuery => {
                self.send(BackendMessage::EmptyQueryResponse).await?;
                Ok(State::Ready)
            }
            ExecuteResponse::Fetch {
                name,
                count,
                timeout,
            } => {
                self.fetch(
                    name,
                    count,
                    max_rows,
                    Some(portal_name.to_string()),
                    timeout,
                )
                .await
            }
            ExecuteResponse::SendingRows { future: rx, span } => {
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::SendingRows");

                let span = tracing::debug_span!(parent: &span, "send_execute_response");
                let rows = self.row_future_to_stream(&span, rx).await?;
                self.send_rows(
                    row_desc,
                    portal_name,
                    InProgressRows::new(rows),
                    max_rows,
                    get_response,
                    fetch_portal_name,
                    timeout,
                )
                .instrument(span)
                .await
            }
            ExecuteResponse::SetVariable { name, .. } => {
                // This code is somewhat awkwardly structured because we
                // can't hold `var` across an await point.
                let qn = name.to_string();
                let msg = if let Some(var) = self
                    .adapter_client
                    .session()
                    .vars_mut()
                    .notify_set()
                    .find(|v| v.name() == qn)
                {
                    Some(BackendMessage::ParameterStatus(var.name(), var.value()))
                } else {
                    None
                };
                if let Some(msg) = msg {
                    self.send(msg).await?;
                }
                command_complete!()
            }
            ExecuteResponse::Subscribing { rx } => {
                if fetch_portal_name.is_none() {
                    let mut msg = ErrorResponse::notice(
                        SqlState::WARNING,
                        "streaming SUBSCRIBE rows directly requires a client that does not buffer output",
                    );
                    if self.adapter_client.session().vars().application_name() == "psql" {
                        msg.hint = Some(
                            "Wrap your SUBSCRIBE statement in `COPY (SUBSCRIBE ...) TO STDOUT`."
                                .into(),
                        )
                    }
                    self.send(msg).await?;
                    self.conn.flush().await?;
                }
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::Tailing");
                self.send_rows(
                    row_desc,
                    portal_name,
                    InProgressRows::new(rx),
                    max_rows,
                    get_response,
                    fetch_portal_name,
                    timeout,
                )
                .await
            }
            ExecuteResponse::CopyTo { format, resp } => {
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::CopyTo");
                let rows = match *resp {
                    ExecuteResponse::Subscribing { rx } => rx,
                    ExecuteResponse::SendingRows {
                        future: rows_rx,
                        span,
                    } => self.row_future_to_stream(&span, rows_rx).await?,
                    _ => {
                        return self
                            .error(ErrorResponse::error(
                                SqlState::INTERNAL_ERROR,
                                "unsupported COPY response type".to_string(),
                            ))
                            .await;
                    }
                };
                self.copy_rows(format, row_desc, rows).await
            }
            ExecuteResponse::CopyFrom {
                id,
                columns,
                params,
            } => {
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::CopyFrom");
                self.copy_from(id, columns, params, row_desc).await
            }

            ExecuteResponse::AlteredIndexLogicalCompaction
            | ExecuteResponse::AlteredObject(..)
            | ExecuteResponse::AlteredRole
            | ExecuteResponse::AlteredSystemConfiguration
            | ExecuteResponse::CreatedCluster { .. }
            | ExecuteResponse::CreatedClusterReplica { .. }
            | ExecuteResponse::CreatedConnection { .. }
            | ExecuteResponse::CreatedDatabase { .. }
            | ExecuteResponse::CreatedIndex { .. }
            | ExecuteResponse::CreatedMaterializedView { .. }
            | ExecuteResponse::CreatedRole
            | ExecuteResponse::CreatedSchema { .. }
            | ExecuteResponse::CreatedSecret { .. }
            | ExecuteResponse::CreatedSink { .. }
            | ExecuteResponse::CreatedSource { .. }
            | ExecuteResponse::CreatedSources
            | ExecuteResponse::CreatedTable { .. }
            | ExecuteResponse::CreatedType
            | ExecuteResponse::CreatedView { .. }
            | ExecuteResponse::CreatedViews { .. }
            | ExecuteResponse::Deallocate { .. }
            | ExecuteResponse::Deleted(..)
            | ExecuteResponse::DiscardedAll
            | ExecuteResponse::DiscardedTemp
            | ExecuteResponse::DroppedObject(_)
            | ExecuteResponse::DroppedOwned
            | ExecuteResponse::GrantedPrivilege
            | ExecuteResponse::GrantedRole
            | ExecuteResponse::Inserted(..)
            | ExecuteResponse::Prepare
            | ExecuteResponse::Raised
            | ExecuteResponse::ReassignOwned
            | ExecuteResponse::RevokedPrivilege
            | ExecuteResponse::RevokedRole
            | ExecuteResponse::StartedTransaction { .. }
            | ExecuteResponse::TransactionCommitted
            | ExecuteResponse::TransactionRolledBack
            | ExecuteResponse::Updated(..) => {
                command_complete!()
            }
        };

        assert!(tag.is_none(), "tag created but not consumed: {:?}", tag);
        r
    }

    #[allow(clippy::too_many_arguments)]
    // TODO(guswynn): figure out how to get it to compile without skip_all
    #[tracing::instrument(level = "debug", skip_all)]
    async fn send_rows(
        &mut self,
        row_desc: RelationDesc,
        portal_name: String,
        mut rows: InProgressRows,
        max_rows: ExecuteCount,
        get_response: GetResponse,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
    ) -> Result<State, io::Error> {
        // If this portal is being executed from a FETCH then we need to use the result
        // format type of the outer portal.
        let result_format_portal_name: &str = if let Some(ref name) = fetch_portal_name {
            name
        } else {
            &portal_name
        };
        let result_formats = self
            .adapter_client
            .session()
            .get_portal_unverified(result_format_portal_name)
            .expect("valid fetch portal name for send rows")
            .result_formats
            .clone();

        let (mut wait_once, mut deadline) = match timeout {
            ExecuteTimeout::None => (false, None),
            ExecuteTimeout::Seconds(t) => {
                (false, Some(Instant::now() + Duration::from_secs_f64(t)))
            }
            ExecuteTimeout::WaitOnce => (true, None),
        };

        self.conn.set_encode_state(
            row_desc
                .typ()
                .column_types
                .iter()
                .map(|ty| mz_pgrepr::Type::from(&ty.scalar_type))
                .zip(result_formats)
                .collect(),
        );

        let mut total_sent_rows = 0;
        // want_rows is the maximum number of rows the client wants.
        let mut want_rows = match max_rows {
            ExecuteCount::All => usize::MAX,
            ExecuteCount::Count(count) => count,
        };

        // Send rows while the client still wants them and there are still rows to send.
        loop {
            // Fetch next batch of rows, waiting for a possible requested timeout or
            // cancellation.
            let batch = if self.adapter_client.canceled().now_or_never().is_some() {
                FetchResult::Canceled
            } else if rows.current.is_some() {
                FetchResult::Rows(rows.current.take())
            } else if want_rows == 0 {
                FetchResult::Rows(None)
            } else {
                let cancel_fut = self.adapter_client.canceled();
                let notice_fut = self.adapter_client.session().recv_notice();
                tokio::select! {
                    _ = time::sleep_until(deadline.unwrap_or_else(time::Instant::now)), if deadline.is_some() => FetchResult::Rows(None),
                    notice = notice_fut => {
                        FetchResult::Notice(notice)
                    }
                    _ = cancel_fut => FetchResult::Canceled,
                    batch = rows.remaining.recv() => match batch {
                        None => FetchResult::Rows(None),
                        Some(PeekResponseUnary::Rows(rows)) => FetchResult::Rows(Some(rows)),
                        Some(PeekResponseUnary::Error(err)) => FetchResult::Error(err),
                        Some(PeekResponseUnary::Canceled) => FetchResult::Canceled,
                    },
                }
            };

            match batch {
                FetchResult::Rows(None) => break,
                FetchResult::Rows(Some(mut batch_rows)) => {
                    // Verify the first row is of the expected type. This is often good enough to
                    // find problems. Notably it failed to find #6304 when "FETCH 2" was used in a
                    // test, instead we had to use "FETCH 1" twice.
                    if let [row, ..] = batch_rows.as_slice() {
                        let datums = row.unpack();
                        let col_types = &row_desc.typ().column_types;
                        if datums.len() != col_types.len() {
                            return self
                                .error(ErrorResponse::error(
                                    SqlState::INTERNAL_ERROR,
                                    format!(
                                        "internal error: row descriptor has {} columns but row has {} columns",
                                        col_types.len(),
                                        datums.len(),
                                    ),
                                ))
                                .await;
                        }
                        for (i, (d, t)) in datums.iter().zip(col_types).enumerate() {
                            if !d.is_instance_of(t) {
                                return self
                                    .error(ErrorResponse::error(
                                        SqlState::INTERNAL_ERROR,
                                            format!(
                                            "internal error: column {} is not of expected type {:?}: {:?}",
                                            i, t, d
                                        ),
                                    ))
                                    .await;
                            }
                        }
                    }

                    // If wait_once is true: the first time this fn is called it blocks (same as
                    // deadline == None). The second time this fn is called it should behave the
                    // same a 0s timeout.
                    if wait_once && !batch_rows.is_empty() {
                        deadline = Some(Instant::now());
                        wait_once = false;
                    }

                    //  let mut batch_rows = batch_rows;
                    // Drain panics if it's > len, so cap it.
                    let drain_rows = cmp::min(want_rows, batch_rows.len());
                    self.send_all(batch_rows.drain(..drain_rows).map(|row| {
                        BackendMessage::DataRow(mz_pgrepr::values_from_row(row, row_desc.typ()))
                    }))
                    .await?;
                    total_sent_rows += drain_rows;
                    want_rows -= drain_rows;
                    // If we have sent the number of requested rows, put the remainder of the batch
                    // (if any) back and stop sending.
                    if want_rows == 0 {
                        if !batch_rows.is_empty() {
                            rows.current = Some(batch_rows);
                        }
                        break;
                    }
                    self.conn.flush().await?;
                }
                FetchResult::Notice(notice) => {
                    self.send(ErrorResponse::from_adapter_notice(notice))
                        .await?;
                    self.conn.flush().await?;
                }
                FetchResult::Error(text) => {
                    return self
                        .error(ErrorResponse::error(SqlState::INTERNAL_ERROR, text))
                        .await;
                }
                FetchResult::Canceled => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::QUERY_CANCELED,
                            "canceling statement due to user request",
                        ))
                        .await;
                }
            }
        }

        let portal = self
            .adapter_client
            .session()
            .get_portal_unverified_mut(&portal_name)
            .expect("valid portal name for send rows");

        // Always return rows back, even if it's empty. This prevents an unclosed
        // portal from re-executing after it has been emptied.
        portal.state = PortalState::InProgress(Some(rows));

        let fetch_portal = fetch_portal_name.map(|name| {
            self.adapter_client
                .session()
                .get_portal_unverified_mut(&name)
                .expect("valid fetch portal")
        });
        let response_message = get_response(max_rows, total_sent_rows, fetch_portal);
        self.send(response_message).await?;
        Ok(State::Ready)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn copy_rows(
        &mut self,
        format: CopyFormat,
        row_desc: RelationDesc,
        mut stream: RowBatchStream,
    ) -> Result<State, io::Error> {
        let (encode_fn, encode_format): (
            fn(Row, &RelationType, &mut Vec<u8>) -> Result<(), std::io::Error>,
            mz_pgrepr::Format,
        ) = match format {
            CopyFormat::Text => (mz_pgcopy::encode_copy_row_text, mz_pgrepr::Format::Text),
            CopyFormat::Binary => (mz_pgcopy::encode_copy_row_binary, mz_pgrepr::Format::Binary),
            _ => {
                return self
                    .error(ErrorResponse::error(
                        SqlState::FEATURE_NOT_SUPPORTED,
                        format!("COPY TO format {:?} not supported", format),
                    ))
                    .await
            }
        };

        let typ = row_desc.typ();
        let column_formats = iter::repeat(encode_format)
            .take(typ.column_types.len())
            .collect();
        self.send(BackendMessage::CopyOutResponse {
            overall_format: encode_format,
            column_formats,
        })
        .await?;

        // In Postgres, binary copy has a header that is followed (in the same
        // CopyData) by the first row. In order to replicate their behavior, use a
        // common vec that we can extend one time now and then fill up with the encode
        // functions.
        let mut out = Vec::new();

        if let CopyFormat::Binary = format {
            // 11-byte signature.
            out.extend(b"PGCOPY\n\xFF\r\n\0");
            // 32-bit flags field.
            out.extend([0, 0, 0, 0]);
            // 32-bit header extension length field.
            out.extend([0, 0, 0, 0]);
        }

        let mut count = 0;
        loop {
            tokio::select! {
                e = self.conn.wait_closed() => return Err(e),
                _ = self.adapter_client.canceled() => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::QUERY_CANCELED,
                            "canceling statement due to user request",
                        ))
                    .await;
                },
                batch = stream.recv() => match batch {
                    None => break,
                    Some(PeekResponseUnary::Error(text)) => {
                        return self
                            .error(ErrorResponse::error(SqlState::INTERNAL_ERROR, text))
                            .await;
                    }
                    Some(PeekResponseUnary::Canceled) => {
                        return self.error(ErrorResponse::error(
                                SqlState::QUERY_CANCELED,
                                "canceling statement due to user request",
                            ))
                            .await;
                    }
                    Some(PeekResponseUnary::Rows(rows)) => {
                        count += rows.len();
                        for row in rows {
                            encode_fn(row, typ, &mut out)?;
                            self.send(BackendMessage::CopyData(mem::take(&mut out)))
                                .await?;
                        }
                    }
                },
                notice = self.adapter_client.session().recv_notice() => {
                    self.send(ErrorResponse::from_adapter_notice(notice))
                        .await?;
                    self.conn.flush().await?;
                }
            }

            self.conn.flush().await?;
        }
        // Send required trailers.
        if let CopyFormat::Binary = format {
            let trailer: i16 = -1;
            out.extend(trailer.to_be_bytes());
            self.send(BackendMessage::CopyData(mem::take(&mut out)))
                .await?;
        }

        let tag = format!("COPY {}", count);
        self.send(BackendMessage::CopyDone).await?;
        self.send(BackendMessage::CommandComplete { tag }).await?;
        Ok(State::Ready)
    }

    /// Handles the copy-in mode of the postgres protocol from transferring
    /// data to the server.
    async fn copy_from(
        &mut self,
        id: GlobalId,
        columns: Vec<usize>,
        params: CopyFormatParams<'_>,
        row_desc: RelationDesc,
    ) -> Result<State, io::Error> {
        let typ = row_desc.typ();
        let column_formats = vec![mz_pgrepr::Format::Text; typ.column_types.len()];
        self.send(BackendMessage::CopyInResponse {
            overall_format: mz_pgrepr::Format::Text,
            column_formats,
        })
        .await?;
        self.conn.flush().await?;

        let mut data = Vec::new();
        let mut next_state = State::Ready;
        loop {
            let message = self.conn.recv().await?;
            match message {
                Some(FrontendMessage::CopyData(buf)) => data.extend(buf),
                Some(FrontendMessage::CopyDone) => break,
                Some(FrontendMessage::CopyFail(err)) => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::QUERY_CANCELED,
                            format!("COPY from stdin failed: {}", err),
                        ))
                        .await
                }
                Some(FrontendMessage::Flush) | Some(FrontendMessage::Sync) => {}
                Some(_) => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::PROTOCOL_VIOLATION,
                            "unexpected message type during COPY from stdin",
                        ))
                        .await
                }
                _ => {
                    next_state = State::Done;
                    break;
                }
            }
        }

        let column_types = typ
            .column_types
            .iter()
            .map(|x| &x.scalar_type)
            .map(mz_pgrepr::Type::from)
            .collect::<Vec<mz_pgrepr::Type>>();

        if let State::Ready = next_state {
            let rows = match mz_pgcopy::decode_copy_format(&data, &column_types, params) {
                Ok(rows) => rows,
                Err(e) => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::BAD_COPY_FILE_FORMAT,
                            format!("{}", e),
                        ))
                        .await
                }
            };

            let count = rows.len();

            if let Err(e) = self.adapter_client.insert_rows(id, columns, rows).await {
                return self
                    .error(ErrorResponse::from_adapter_error(Severity::Error, e))
                    .await;
            }

            let tag = format!("COPY {}", count);
            self.send(BackendMessage::CommandComplete { tag }).await?;
        }

        Ok(next_state)
    }

    async fn send_pending_notices(&mut self) -> Result<(), io::Error> {
        let notices = self
            .adapter_client
            .session()
            .drain_notices()
            .into_iter()
            .map(|notice| {
                BackendMessage::ErrorResponse(ErrorResponse::from_adapter_notice(notice))
            });
        self.send_all(notices).await?;
        Ok(())
    }

    async fn error(&mut self, err: ErrorResponse) -> Result<State, io::Error> {
        assert!(err.severity.is_error());
        debug!(
            "cid={} error code={} message={}",
            self.adapter_client.session().conn_id(),
            err.code.code(),
            err.message
        );
        let is_fatal = err.severity.is_fatal();
        self.send(BackendMessage::ErrorResponse(err)).await?;
        let txn = self.adapter_client.session().transaction();
        match txn {
            // Error can be called from describe and parse and so might not be in an active
            // transaction.
            TransactionStatus::Default | TransactionStatus::Failed(_) => {}
            // In Started (i.e., a single statement), cleanup ourselves.
            TransactionStatus::Started(_) => {
                self.rollback_transaction().await?;
            }
            // Implicit transactions also clear themselves.
            TransactionStatus::InTransactionImplicit(_) => {
                self.rollback_transaction().await?;
            }
            // Explicit transactions move to failed.
            TransactionStatus::InTransaction(_) => {
                self.adapter_client.fail_transaction();
            }
        };
        if is_fatal {
            Ok(State::Done)
        } else {
            Ok(State::Drain)
        }
    }

    async fn aborted_txn_error(&mut self) -> Result<State, io::Error> {
        self.send(BackendMessage::ErrorResponse(ErrorResponse::error(
            SqlState::IN_FAILED_SQL_TRANSACTION,
            "current transaction is aborted, commands ignored until end of transaction block",
        )))
        .await?;
        Ok(State::Drain)
    }

    fn is_aborted_txn(&mut self) -> bool {
        matches!(
            self.adapter_client.session().transaction(),
            TransactionStatus::Failed(_)
        )
    }
}

fn pad_formats(
    formats: Vec<mz_pgrepr::Format>,
    n: usize,
) -> Result<Vec<mz_pgrepr::Format>, String> {
    match (formats.len(), n) {
        (0, e) => Ok(vec![mz_pgrepr::Format::Text; e]),
        (1, e) => Ok(iter::repeat(formats[0]).take(e).collect()),
        (a, e) if a == e => Ok(formats),
        (a, e) => Err(format!(
            "expected {} field format specifiers, but got {}",
            e, a
        )),
    }
}

fn describe_rows(stmt_desc: &StatementDesc, formats: &[mz_pgrepr::Format]) -> BackendMessage {
    match &stmt_desc.relation_desc {
        Some(desc) if !stmt_desc.is_copy => {
            BackendMessage::RowDescription(message::encode_row_description(desc, formats))
        }
        _ => BackendMessage::NoData,
    }
}

fn parse_sql(sql: &str) -> Result<Vec<Statement<Raw>>, ErrorResponse> {
    match mz_sql::parse::parse_with_limit(sql) {
        Ok(result) => result.map_err(|e| {
            // Convert our 0-based byte position to pgwire's 1-based character
            // position.
            let pos = sql[..e.pos].chars().count() + 1;
            ErrorResponse::error(SqlState::SYNTAX_ERROR, e.message).with_position(pos)
        }),
        Err(e) => Err(ErrorResponse::error(SqlState::PROGRAM_LIMIT_EXCEEDED, e)),
    }
}

type GetResponse = fn(
    max_rows: ExecuteCount,
    total_sent_rows: usize,
    fetch_portal: Option<&mut Portal>,
) -> BackendMessage;

// A GetResponse used by send_rows during execute messages on portals or for
// simple query messages.
fn portal_exec_message(
    max_rows: ExecuteCount,
    total_sent_rows: usize,
    _fetch_portal: Option<&mut Portal>,
) -> BackendMessage {
    // If max_rows is not specified, we will always send back a CommandComplete. If
    // max_rows is specified, we only send CommandComplete if there were more rows
    // requested than were remaining. That is, if max_rows == number of rows that
    // were remaining before sending (not that are remaining after sending), then
    // we still send a PortalSuspended. The number of remaining rows after the rows
    // have been sent doesn't matter. This matches postgres.
    match max_rows {
        ExecuteCount::Count(max_rows) if max_rows <= total_sent_rows => {
            BackendMessage::PortalSuspended
        }
        _ => BackendMessage::CommandComplete {
            tag: format!("SELECT {}", total_sent_rows),
        },
    }
}

// A GetResponse used by send_rows during FETCH queries.
fn fetch_message(
    _max_rows: ExecuteCount,
    total_sent_rows: usize,
    fetch_portal: Option<&mut Portal>,
) -> BackendMessage {
    let tag = format!("FETCH {}", total_sent_rows);
    if let Some(portal) = fetch_portal {
        portal.state = PortalState::Completed(Some(tag.clone()));
    }
    BackendMessage::CommandComplete { tag }
}

#[derive(Debug, Copy, Clone)]
enum ExecuteCount {
    All,
    Count(usize),
}

// See postgres' backend/tcop/postgres.c IsTransactionExitStmt.
fn is_txn_exit_stmt(stmt: Option<&Statement<Raw>>) -> bool {
    match stmt {
        // Add PREPARE to this if we ever support it.
        Some(stmt) => matches!(stmt, Statement::Commit(_) | Statement::Rollback(_)),
        None => false,
    }
}

#[derive(Debug)]
enum FetchResult {
    Rows(Option<Vec<Row>>),
    Canceled,
    Error(String),
    Notice(AdapterNotice),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_options() {
        struct TestCase {
            input: &'static str,
            expect: Result<Vec<(&'static str, &'static str)>, ()>,
        }
        let tests = vec![
            TestCase {
                input: "",
                expect: Ok(vec![]),
            },
            TestCase {
                input: "--key",
                expect: Err(()),
            },
            TestCase {
                input: "--key=val",
                expect: Ok(vec![("key", "val")]),
            },
            TestCase {
                input: r#"--key=val -ckey2=val2 -c key3=val3 -c key4=val4 -ckey5=val5"#,
                expect: Ok(vec![
                    ("key", "val"),
                    ("key2", "val2"),
                    ("key3", "val3"),
                    ("key4", "val4"),
                    ("key5", "val5"),
                ]),
            },
            TestCase {
                input: r#"-c\ key=val"#,
                expect: Ok(vec![(" key", "val")]),
            },
            TestCase {
                input: "--key=val -ckey2 val2",
                expect: Err(()),
            },
            // Unclear what this should do.
            TestCase {
                input: "--key=",
                expect: Ok(vec![("key", "")]),
            },
        ];
        for test in tests {
            let got = parse_options(test.input);
            let expect = test.expect.map(|r| {
                r.into_iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect()
            });
            assert_eq!(got, expect, "input: {}", test.input);
        }
    }

    #[test]
    fn test_parse_option() {
        struct TestCase {
            input: &'static str,
            expect: Result<(&'static str, &'static str), ()>,
        }
        let tests = vec![
            TestCase {
                input: "",
                expect: Err(()),
            },
            TestCase {
                input: "--",
                expect: Err(()),
            },
            TestCase {
                input: "--c",
                expect: Err(()),
            },
            TestCase {
                input: "a=b",
                expect: Err(()),
            },
            TestCase {
                input: "--a=b",
                expect: Ok(("a", "b")),
            },
            TestCase {
                input: "--ca=b",
                expect: Ok(("ca", "b")),
            },
            TestCase {
                input: "-ca=b",
                expect: Ok(("a", "b")),
            },
            // Unclear what this should error, but at least test it.
            TestCase {
                input: "--=",
                expect: Ok(("", "")),
            },
        ];
        for test in tests {
            let got = parse_option(test.input);
            assert_eq!(got, test.expect, "input: {}", test.input);
        }
    }

    #[test]
    fn test_split_options() {
        struct TestCase {
            input: &'static str,
            expect: Vec<&'static str>,
        }
        let tests = vec![
            TestCase {
                input: "",
                expect: vec![],
            },
            TestCase {
                input: "  ",
                expect: vec![],
            },
            TestCase {
                input: " a ",
                expect: vec!["a"],
            },
            TestCase {
                input: "  ab     cd   ",
                expect: vec!["ab", "cd"],
            },
            TestCase {
                input: r#"  ab\     cd   "#,
                expect: vec!["ab ", "cd"],
            },
            TestCase {
                input: r#"  ab\\     cd   "#,
                expect: vec![r#"ab\"#, "cd"],
            },
            TestCase {
                input: r#"  ab\\\     cd   "#,
                expect: vec![r#"ab\ "#, "cd"],
            },
            TestCase {
                input: r#"  ab\\\ cd   "#,
                expect: vec![r#"ab\ cd"#],
            },
            TestCase {
                input: r#"  ab\\\cd   "#,
                expect: vec![r#"ab\cd"#],
            },
            TestCase {
                input: r#"a\"#,
                expect: vec!["a"],
            },
            TestCase {
                input: r#"a\ "#,
                expect: vec!["a "],
            },
            TestCase {
                input: r#"\"#,
                expect: vec![],
            },
            TestCase {
                input: r#"\ "#,
                expect: vec![r#" "#],
            },
            TestCase {
                input: r#" \ "#,
                expect: vec![r#" "#],
            },
            TestCase {
                input: r#"\  "#,
                expect: vec![r#" "#],
            },
        ];
        for test in tests {
            let got = split_options(test.input);
            assert_eq!(got, test.expect, "input: {}", test.input);
        }
    }
}
