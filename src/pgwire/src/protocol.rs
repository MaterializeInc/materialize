// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{iter, mem};

use base64::prelude::*;
use byteorder::{ByteOrder, NetworkEndian};
use csv_core::ReadRecordResult;
use futures::future::{BoxFuture, FutureExt, pending};
use itertools::Itertools;
use mz_adapter::client::RecordFirstRowStream;
use mz_adapter::session::{
    EndTransactionAction, InProgressRows, LifecycleTimestamps, PortalRefMut, PortalState, Session,
    SessionConfig, TransactionStatus,
};
use mz_adapter::statement_logging::{StatementEndedExecutionReason, StatementExecutionStrategy};
use mz_adapter::{
    AdapterError, AdapterNotice, ExecuteContextGuard, ExecuteResponse, PeekResponseUnary, metrics,
    verify_datum_desc,
};
use mz_auth::Authenticated;
use mz_auth::password::Password;
use mz_authenticator::{Authenticator, GenericOidcAuthenticator};
use mz_frontegg_auth::Authenticator as FronteggAuthenticator;
use mz_ore::cast::CastFrom;
use mz_ore::netio::AsyncReady;
use mz_ore::now::{EpochMillis, SYSTEM_TIME};
use mz_ore::str::StrExt;
use mz_ore::{assert_none, assert_ok, instrument, soft_assert_eq_or_log};
use mz_pgcopy::{CopyCsvFormatParams, CopyFormatParams, CopyTextFormatParams};
use mz_pgwire_common::{
    ConnectionCounter, Cursor, ErrorResponse, Format, FrontendMessage, Severity, VERSION_3,
    VERSIONS,
};
use mz_repr::{
    CatalogItemId, ColumnIndex, Datum, RelationDesc, RowArena, RowIterator, RowRef,
    SqlRelationType, SqlScalarType,
};
use mz_server_core::TlsMode;
use mz_server_core::listeners::{AllowedRoles, AuthenticatorKind};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{CopyDirection, CopyStatement, FetchDirection, Ident, Raw, Statement};
use mz_sql::parse::StatementParseResult;
use mz_sql::plan::{CopyFormat, ExecuteTimeout, StatementDesc};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::INTERNAL_USER_NAMES;
use mz_sql::session::vars::VarInput;
use postgres::error::SqlState;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::select;
use tokio::time::{self};
use tokio_metrics::TaskMetrics;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{Instrument, debug, debug_span, warn};
use uuid::Uuid;

use crate::codec::{
    FramedConn, decode_password, decode_sasl_initial_response, decode_sasl_response,
};
use crate::message::{
    self, BackendMessage, SASLServerFinalMessage, SASLServerFinalMessageKinds,
    SASLServerFirstMessage,
};

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
pub struct RunParams<'a, A, I>
where
    I: Iterator<Item = TaskMetrics> + Send,
{
    /// The TLS mode of the pgwire server.
    pub tls_mode: Option<TlsMode>,
    /// A client for the adapter.
    pub adapter_client: mz_adapter::Client,
    /// The connection to the client.
    pub conn: &'a mut FramedConn<A>,
    /// The universally unique identifier for the connection.
    pub conn_uuid: Uuid,
    /// The protocol version that the client provided in the startup message.
    pub version: i32,
    /// The parameters that the client provided in the startup message.
    pub params: BTreeMap<String, String>,
    /// Frontegg JWT authenticator.
    pub frontegg: Option<FronteggAuthenticator>,
    /// OIDC authenticator.
    pub oidc: GenericOidcAuthenticator,
    /// The authentication method defined by the server's listener
    /// configuration.
    pub authenticator_kind: AuthenticatorKind,
    /// Global connection limit and count
    pub active_connection_counter: ConnectionCounter,
    /// Helm chart version
    pub helm_chart_version: Option<String>,
    /// Whether to allow reserved users (ie: mz_system).
    pub allowed_roles: AllowedRoles,
    /// Tokio metrics
    pub tokio_metrics_intervals: I,
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
#[mz_ore::instrument(level = "debug")]
pub async fn run<'a, A, I>(
    RunParams {
        tls_mode,
        adapter_client,
        conn,
        conn_uuid,
        version,
        mut params,
        frontegg,
        oidc,
        authenticator_kind,
        active_connection_counter,
        helm_chart_version,
        allowed_roles,
        tokio_metrics_intervals,
    }: RunParams<'a, A, I>,
) -> Result<(), io::Error>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin,
    I: Iterator<Item = TaskMetrics> + Send,
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
    let options = parse_options(params.get("options").unwrap_or(&String::new()));

    // If oidc_auth_enabled exists as an option, return its value and filter it from
    // the remaining options.
    let (oidc_auth_enabled, options) = extract_oidc_auth_enabled_from_options(options);
    let authenticator = get_authenticator(
        authenticator_kind,
        frontegg,
        oidc,
        adapter_client.clone(),
        oidc_auth_enabled,
    );
    // TODO move this somewhere it can be shared with HTTP
    let is_internal_user = INTERNAL_USER_NAMES.contains(&user);
    // this is a superset of internal users
    let is_reserved_user = mz_adapter::catalog::is_reserved_role_name(user.as_str());
    let role_allowed = match allowed_roles {
        AllowedRoles::Normal => !is_reserved_user,
        AllowedRoles::Internal => is_internal_user,
        AllowedRoles::NormalAndInternal => !is_reserved_user || is_internal_user,
    };
    if !role_allowed {
        let msg = format!("unauthorized login to user '{user}'");
        return conn
            .send(ErrorResponse::fatal(SqlState::INSUFFICIENT_PRIVILEGE, msg))
            .await;
    }

    if let Err(err) = conn.inner().ensure_tls_compatibility(&tls_mode) {
        return conn.send(err).await;
    }

    let (mut session, expired) = match authenticator {
        Authenticator::Frontegg(frontegg) => {
            let password = match request_cleartext_password(conn).await {
                Ok(password) => password,
                Err(PasswordRequestError::IoError(e)) => return Err(e),
                Err(PasswordRequestError::InvalidPasswordError(e)) => {
                    return conn.send(e).await;
                }
            };

            let auth_response = frontegg.authenticate(&user, &password).await;
            match auth_response {
                // Create a session based on the auth session.
                //
                // In particular, it's important that the username come from the
                // auth session, as Frontegg may return an email address with
                // different casing than the user supplied via the pgwire
                // username fN
                Ok((mut auth_session, authenticated)) => {
                    let session = adapter_client.new_session(
                        SessionConfig {
                            conn_id: conn.conn_id().clone(),
                            uuid: conn_uuid,
                            user: auth_session.user().into(),
                            client_ip: conn.peer_addr().clone(),
                            external_metadata_rx: Some(auth_session.external_metadata_rx()),
                            helm_chart_version,
                        },
                        authenticated,
                    );
                    let expired = async move { auth_session.expired().await };
                    (session, expired.left_future())
                }
                Err(err) => {
                    warn!(?err, "pgwire connection failed authentication");
                    return conn
                        .send(ErrorResponse::fatal(
                            SqlState::INVALID_PASSWORD,
                            "invalid password",
                        ))
                        .await;
                }
            }
        }
        Authenticator::Oidc(oidc) => {
            // OIDC authentication: JWT sent as password in cleartext flow
            let jwt = match request_cleartext_password(conn).await {
                Ok(password) => password,
                Err(PasswordRequestError::IoError(e)) => return Err(e),
                Err(PasswordRequestError::InvalidPasswordError(e)) => {
                    return conn.send(e).await;
                }
            };
            let auth_response = oidc.authenticate(&jwt, Some(&user)).await;
            match auth_response {
                Ok((claims, authenticated)) => {
                    let session = adapter_client.new_session(
                        SessionConfig {
                            conn_id: conn.conn_id().clone(),
                            uuid: conn_uuid,
                            user: claims.user,
                            client_ip: conn.peer_addr().clone(),
                            external_metadata_rx: None,
                            helm_chart_version,
                        },
                        authenticated,
                    );
                    // No invalidation of the auth session once authenticated,
                    // so auth session lasts indefinitely.
                    (session, pending().right_future())
                }
                Err(err) => {
                    warn!(?err, "pgwire connection failed authentication");
                    return conn.send(err.into_response()).await;
                }
            }
        }
        Authenticator::Password(adapter_client) => {
            let session = match authenticate_with_password(
                conn,
                &adapter_client,
                user,
                conn_uuid,
                helm_chart_version,
            )
            .await
            {
                Ok(session) => session,
                Err(PasswordRequestError::IoError(e)) => return Err(e),
                Err(PasswordRequestError::InvalidPasswordError(e)) => {
                    return conn.send(e).await;
                }
            };
            // No frontegg check, so auth session lasts indefinitely.
            (session, pending().right_future())
        }
        Authenticator::Sasl(adapter_client) => {
            // Start the handshake
            conn.send(BackendMessage::AuthenticationSASL).await?;
            conn.flush().await?;
            // Get the initial response indicating chosen mechanism
            let (mechanism, initial_response) = match conn.recv().await? {
                Some(FrontendMessage::RawAuthentication(data)) => {
                    match decode_sasl_initial_response(Cursor::new(&data)).ok() {
                        Some(FrontendMessage::SASLInitialResponse {
                            gs2_header,
                            mechanism,
                            initial_response,
                        }) => {
                            // We do not support channel binding
                            if gs2_header.channel_binding_enabled() {
                                return conn
                                    .send(ErrorResponse::fatal(
                                        SqlState::PROTOCOL_VIOLATION,
                                        "channel binding not supported",
                                    ))
                                    .await;
                            }
                            (mechanism, initial_response)
                        }
                        _ => {
                            return conn
                                .send(ErrorResponse::fatal(
                                    SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                                    "expected SASLInitialResponse message",
                                ))
                                .await;
                        }
                    }
                }
                _ => {
                    return conn
                        .send(ErrorResponse::fatal(
                            SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                            "expected SASLInitialResponse message",
                        ))
                        .await;
                }
            };

            if mechanism != "SCRAM-SHA-256" {
                return conn
                    .send(ErrorResponse::fatal(
                        SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                        "unsupported SASL mechanism",
                    ))
                    .await;
            }

            if initial_response.nonce.len() > 256 {
                return conn
                    .send(ErrorResponse::fatal(
                        SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                        "nonce too long",
                    ))
                    .await;
            }

            let (server_first_message_raw, mock_hash) = match adapter_client
                .generate_sasl_challenge(&user, &initial_response.nonce)
                .await
            {
                Ok(response) => {
                    let server_first_message_raw = format!(
                        "r={},s={},i={}",
                        response.nonce, response.salt, response.iteration_count
                    );

                    let client_key = [0u8; 32];
                    let server_key = [1u8; 32];
                    let mock_hash = format!(
                        "SCRAM-SHA-256${}:{}${}:{}",
                        response.iteration_count,
                        response.salt,
                        BASE64_STANDARD.encode(client_key),
                        BASE64_STANDARD.encode(server_key)
                    );

                    conn.send(BackendMessage::AuthenticationSASLContinue(
                        SASLServerFirstMessage {
                            iteration_count: response.iteration_count,
                            nonce: response.nonce,
                            salt: response.salt,
                        },
                    ))
                    .await?;
                    conn.flush().await?;
                    (server_first_message_raw, mock_hash)
                }
                Err(e) => {
                    return conn.send(e.into_response(Severity::Fatal)).await;
                }
            };

            let authenticated = match conn.recv().await? {
                Some(FrontendMessage::RawAuthentication(data)) => {
                    match decode_sasl_response(Cursor::new(&data)).ok() {
                        Some(FrontendMessage::SASLResponse(response)) => {
                            let auth_message = format!(
                                "{},{},{}",
                                initial_response.client_first_message_bare_raw,
                                server_first_message_raw,
                                response.client_final_message_bare_raw
                            );
                            if response.proof.len() > 1024 {
                                return conn
                                    .send(ErrorResponse::fatal(
                                        SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                                        "proof too long",
                                    ))
                                    .await;
                            }
                            match adapter_client
                                .verify_sasl_proof(
                                    &user,
                                    &response.proof,
                                    &auth_message,
                                    &mock_hash,
                                )
                                .await
                            {
                                Ok((proof_response, authenticated)) => {
                                    conn.send(BackendMessage::AuthenticationSASLFinal(
                                        SASLServerFinalMessage {
                                            kind: SASLServerFinalMessageKinds::Verifier(
                                                proof_response.verifier,
                                            ),
                                            extensions: vec![],
                                        },
                                    ))
                                    .await?;
                                    conn.flush().await?;
                                    authenticated
                                }
                                Err(_) => {
                                    return conn
                                        .send(ErrorResponse::fatal(
                                            SqlState::INVALID_PASSWORD,
                                            "invalid password",
                                        ))
                                        .await;
                                }
                            }
                        }
                        _ => {
                            return conn
                                .send(ErrorResponse::fatal(
                                    SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                                    "expected SASLResponse message",
                                ))
                                .await;
                        }
                    }
                }
                _ => {
                    return conn
                        .send(ErrorResponse::fatal(
                            SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                            "expected SASLResponse message",
                        ))
                        .await;
                }
            };

            let session = adapter_client.new_session(
                SessionConfig {
                    conn_id: conn.conn_id().clone(),
                    uuid: conn_uuid,
                    user,
                    client_ip: conn.peer_addr().clone(),
                    external_metadata_rx: None,
                    helm_chart_version,
                },
                authenticated,
            );
            // No frontegg check, so auth session lasts indefinitely.
            let auth_session = pending().right_future();
            (session, auth_session)
        }

        Authenticator::None => {
            let session = adapter_client.new_session(
                SessionConfig {
                    conn_id: conn.conn_id().clone(),
                    uuid: conn_uuid,
                    user,
                    client_ip: conn.peer_addr().clone(),
                    external_metadata_rx: None,
                    helm_chart_version,
                },
                Authenticated,
            );
            // No frontegg check, so auth session lasts indefinitely.
            let auth_session = pending().right_future();
            (session, auth_session)
        }
    };

    let system_vars = adapter_client.get_system_vars().await;
    for (name, value) in params {
        let settings = match name.as_str() {
            "options" => match &options {
                Ok(opts) => opts,
                Err(()) => {
                    session.add_notice(AdapterNotice::BadStartupSetting {
                        name,
                        reason: "could not parse".into(),
                    });
                    continue;
                }
            },
            _ => &vec![(name, value)],
        };
        for (key, val) in settings {
            const LOCAL: bool = false;
            // TODO: Issuing an error here is better than what we did before
            // (silently ignore errors on set), but erroring the connection
            // might be the better behavior. We maybe need to support more
            // options sent by psql and drivers before we can safely do this.
            if let Err(err) = session
                .vars_mut()
                .set(&system_vars, key, VarInput::Flat(val), LOCAL)
            {
                session.add_notice(AdapterNotice::BadStartupSetting {
                    name: key.clone(),
                    reason: err.to_string(),
                });
            }
        }
    }
    session
        .vars_mut()
        .end_transaction(EndTransactionAction::Commit);

    let _guard = match active_connection_counter.allocate_connection(session.user()) {
        Ok(drop_connection) => drop_connection,
        Err(e) => {
            let e: AdapterError = e.into();
            return conn.send(e.into_response(Severity::Fatal)).await;
        }
    };

    // Register session with adapter.
    let mut adapter_client = match adapter_client.startup(session).await {
        Ok(adapter_client) => adapter_client,
        Err(e) => return conn.send(e.into_response(Severity::Fatal)).await,
    };

    let mut buf = vec![BackendMessage::AuthenticationOk];
    for var in adapter_client.session().vars().notify_set() {
        buf.push(BackendMessage::ParameterStatus(var.name(), var.value()));
    }
    buf.push(BackendMessage::BackendKeyData {
        conn_id: adapter_client.session().conn_id().unhandled(),
        secret_key: adapter_client.session().secret_key(),
    });
    buf.extend(
        adapter_client
            .session()
            .drain_notices()
            .into_iter()
            .map(|notice| BackendMessage::ErrorResponse(notice.into_response())),
    );
    buf.push(BackendMessage::ReadyForQuery(
        adapter_client.session().transaction().into(),
    ));
    conn.send_all(buf).await?;
    conn.flush().await?;

    let machine = StateMachine {
        conn,
        adapter_client,
        txn_needs_commit: false,
        tokio_metrics_intervals,
    };

    select! {
        r = machine.run() => {
            // Errors produced internally (like MAX_REQUEST_SIZE being exceeded) should send an
            // error to the client informing them why the connection was closed. We still want to
            // return the original error up the stack, though, so we skip error checking during conn
            // operations.
            if let Err(err) = &r {
                let _ = conn
                    .send(ErrorResponse::fatal(
                        SqlState::CONNECTION_FAILURE,
                        err.to_string(),
                    ))
                    .await;
                let _ = conn.flush().await;
            }
            r
        },
        _ = expired => {
            conn
                .send(ErrorResponse::fatal(SqlState::INVALID_AUTHORIZATION_SPECIFICATION, "authentication expired"))
                .await?;
            conn.flush().await
        }
    }
}

/// Gets `oidc_auth_enabled` from options if it exists.
/// Returns options with oidc_auth_enabled extracted
/// and the oidc_auth_enabled value.
fn extract_oidc_auth_enabled_from_options(
    options: Result<Vec<(String, String)>, ()>,
) -> (bool, Result<Vec<(String, String)>, ()>) {
    let options = match options {
        Ok(opts) => opts,
        Err(_) => return (false, options),
    };

    let mut new_options = Vec::new();
    let mut oidc_auth_enabled = false;

    for (k, v) in options {
        if k == "oidc_auth_enabled" {
            oidc_auth_enabled = v.parse::<bool>().unwrap_or(false);
        } else {
            new_options.push((k, v));
        }
    }

    (oidc_auth_enabled, Ok(new_options))
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

/// Splits value by any number of spaces except those preceded by `\`.
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

enum PasswordRequestError {
    InvalidPasswordError(ErrorResponse),
    IoError(io::Error),
}

impl From<io::Error> for PasswordRequestError {
    fn from(e: io::Error) -> Self {
        PasswordRequestError::IoError(e)
    }
}

/// Requests a cleartext password from a connection and returns it if it is valid.
/// Sends an error response in the connection if the password
/// is not valid.
async fn request_cleartext_password<A>(
    conn: &mut FramedConn<A>,
) -> Result<String, PasswordRequestError>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin,
{
    conn.send(BackendMessage::AuthenticationCleartextPassword)
        .await?;
    conn.flush().await?;

    if let Some(message) = conn.recv().await? {
        if let FrontendMessage::RawAuthentication(data) = message {
            if let Some(FrontendMessage::Password { password }) =
                decode_password(Cursor::new(&data)).ok()
            {
                return Ok(password);
            }
        }
    }

    Err(PasswordRequestError::InvalidPasswordError(
        ErrorResponse::fatal(
            SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
            "expected Password message",
        ),
    ))
}

/// Helper for password-based authentication using AdapterClient
/// and returns an authenticated session.
async fn authenticate_with_password<A>(
    conn: &mut FramedConn<A>,
    adapter_client: &mz_adapter::Client,
    user: String,
    conn_uuid: Uuid,
    helm_chart_version: Option<String>,
) -> Result<Session, PasswordRequestError>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin,
{
    let password = match request_cleartext_password(conn).await {
        Ok(password) => Password(password),
        Err(e) => return Err(e),
    };

    let authenticated = match adapter_client.authenticate(&user, &password).await {
        Ok(authenticated) => authenticated,
        Err(err) => {
            warn!(?err, "pgwire connection failed authentication");
            return Err(PasswordRequestError::InvalidPasswordError(
                ErrorResponse::fatal(SqlState::INVALID_PASSWORD, "invalid password"),
            ));
        }
    };

    let session = adapter_client.new_session(
        SessionConfig {
            conn_id: conn.conn_id().clone(),
            uuid: conn_uuid,
            user,
            client_ip: conn.peer_addr().clone(),
            external_metadata_rx: None,
            helm_chart_version,
        },
        authenticated,
    );

    Ok(session)
}

#[derive(Debug)]
enum State {
    Ready,
    Drain,
    Done,
}

struct StateMachine<'a, A, I>
where
    I: Iterator<Item = TaskMetrics> + Send + 'a,
{
    conn: &'a mut FramedConn<A>,
    adapter_client: mz_adapter::SessionClient,
    txn_needs_commit: bool,
    tokio_metrics_intervals: I,
}

enum SendRowsEndedReason {
    Success {
        result_size: u64,
        rows_returned: u64,
    },
    Errored {
        error: String,
    },
    Canceled,
}

const ABORTED_TXN_MSG: &str =
    "current transaction is aborted, commands ignored until end of transaction block";

impl<'a, A, I> StateMachine<'a, A, I>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin + 'a,
    I: Iterator<Item = TaskMetrics> + Send + 'a,
{
    // Manually desugar this (don't use `async fn run`) here because a much better
    // error message is produced if there are problems with Send or other traits
    // somewhere within the Future.
    #[allow(clippy::manual_async_fn)]
    #[mz_ore::instrument(level = "debug")]
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

    #[instrument(level = "debug")]
    async fn advance_ready(&mut self) -> Result<State, io::Error> {
        // Start a new metrics interval before the `recv()` call.
        self.tokio_metrics_intervals
            .next()
            .expect("infinite iterator");

        // Handle timeouts first so we don't execute any statements when there's a pending timeout.
        let message = select! {
            biased;

            // `recv_timeout()` is cancel-safe as per it's docs.
            Some(timeout) = self.adapter_client.recv_timeout() => {
                let err: AdapterError = timeout.into();
                let conn_id = self.adapter_client.session().conn_id();
                tracing::warn!("session timed out, conn_id {}", conn_id);

                // Process the error, doing any state cleanup.
                let error_response = err.into_response(Severity::Fatal);
                let error_state = self.send_error_and_get_state(error_response).await;

                // Terminate __after__ we do any cleanup.
                self.adapter_client.terminate().await;

                // We must wait for the client to send a request before we can send the error response.
                // Due to the PG wire protocol, we can't send an ErrorResponse unless it is in response
                // to a client message.
                let _ = self.conn.recv().await?;
                return error_state;
            },
            // `recv()` is cancel-safe as per it's docs.
            message = self.conn.recv() => message?,
        };

        // Take the metrics since just before the `recv`.
        let interval = self
            .tokio_metrics_intervals
            .next()
            .expect("infinite iterator");
        let recv_scheduling_delay_ms = interval.total_scheduled_duration.as_secs_f64() * 1000.0;

        // TODO(ggevay): Consider subtracting the scheduling delay from `received`. It's not obvious
        // whether we should do this, because the result wouldn't exactly correspond to either first
        // byte received or last byte received (for msgs that arrive in more than one network packet).
        let received = SYSTEM_TIME();

        self.adapter_client
            .remove_idle_in_transaction_session_timeout();

        // NOTE(guswynn): we could consider adding spans to all message types. Currently
        // only a few message types seem useful.
        let message_name = message.as_ref().map(|m| m.name()).unwrap_or_default();

        let start = message.as_ref().map(|_| Instant::now());
        let next_state = match message {
            Some(FrontendMessage::Query { sql }) => {
                let query_root_span =
                    tracing::info_span!(parent: None, "advance_ready", otel.name = message_name);
                query_root_span.follows_from(tracing::Span::current());
                self.query(sql, received)
                    .instrument(query_root_span)
                    .await?
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
                    tracing::info_span!(parent: None, "advance_ready", otel.name = message_name);
                execute_root_span.follows_from(tracing::Span::current());
                let state = self
                    .execute(
                        portal_name,
                        max_rows,
                        portal_exec_message,
                        None,
                        ExecuteTimeout::None,
                        None,
                        Some(received),
                    )
                    .instrument(execute_root_span)
                    .await?;
                // In PostgreSQL, when using the extended query protocol, some statements may
                // trigger an eager commit of the current implicit transaction,
                // see: <https://git.postgresql.org/gitweb/?p=postgresql.git&a=commitdiff&h=f92944137>.
                //
                // In Materialize, however, we eagerly commit every statement outside of an explicit
                // transaction when using the extended query protocol. This allows us to eliminate
                // the possibility of a multiple statement implicit transaction, which in turn
                // allows us to apply single-statement optimizations to queries issued in implicit
                // transactions in the extended query protocol.
                //
                // We don't immediately commit here to allow users to page through the portal if
                // necessary. Committing the transaction would destroy the portal before the next
                // Execute command has a chance to resume it. So we instead mark the transaction
                // for commit the next time that `ensure_transaction` is called.
                if self.adapter_client.session().transaction().is_implicit() {
                    self.txn_needs_commit = true;
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
            | Some(FrontendMessage::Password { .. })
            | Some(FrontendMessage::RawAuthentication(_))
            | Some(FrontendMessage::SASLInitialResponse { .. })
            | Some(FrontendMessage::SASLResponse(_)) => State::Drain,
            None => State::Done,
        };

        if let Some(start) = start {
            self.adapter_client
                .inner()
                .metrics()
                .pgwire_message_processing_seconds
                .with_label_values(&[message_name])
                .observe(start.elapsed().as_secs_f64());
        }
        self.adapter_client
            .inner()
            .metrics()
            .pgwire_recv_scheduling_delay_ms
            .with_label_values(&[message_name])
            .observe(recv_scheduling_delay_ms);

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

    /// Note that `lifecycle_timestamps` belongs to the whole "Simple Query", because the whole
    /// Simple Query is received and parsed together. This means that if there are multiple
    /// statements in a Simple Query, then all of them have the same `lifecycle_timestamps`.
    #[instrument(level = "debug")]
    async fn one_query(
        &mut self,
        stmt: Statement<Raw>,
        sql: String,
        lifecycle_timestamps: LifecycleTimestamps,
    ) -> Result<State, io::Error> {
        // Bind the portal. Note that this does not set the empty string prepared
        // statement.
        const EMPTY_PORTAL: &str = "";
        if let Err(e) = self
            .adapter_client
            .declare(EMPTY_PORTAL.to_string(), stmt, sql)
            .await
        {
            return self
                .send_error_and_get_state(e.into_response(Severity::Error))
                .await;
        }
        let portal = self
            .adapter_client
            .session()
            .get_portal_unverified_mut(EMPTY_PORTAL)
            .expect("unnamed portal should be present");

        *portal.lifecycle_timestamps = Some(lifecycle_timestamps);

        let stmt_desc = portal.desc.clone();
        if !stmt_desc.param_types.is_empty() {
            return self
                .send_error_and_get_state(ErrorResponse::error(
                    SqlState::UNDEFINED_PARAMETER,
                    "there is no parameter $1",
                ))
                .await;
        }

        // Maybe send row description.
        if let Some(relation_desc) = &stmt_desc.relation_desc {
            if !stmt_desc.is_copy {
                let formats = vec![Format::Text; stmt_desc.arity()];
                self.send(BackendMessage::RowDescription(
                    message::encode_row_description(relation_desc, &formats),
                ))
                .await?;
            }
        }

        let result = match self
            .adapter_client
            .execute(EMPTY_PORTAL.to_string(), self.conn.wait_closed(), None)
            .await
        {
            Ok((response, execute_started)) => {
                self.send_pending_notices().await?;
                self.send_execute_response(
                    response,
                    stmt_desc.relation_desc,
                    EMPTY_PORTAL.to_string(),
                    ExecuteCount::All,
                    portal_exec_message,
                    None,
                    ExecuteTimeout::None,
                    execute_started,
                )
                .await
            }
            Err(e) => {
                self.send_pending_notices().await?;
                self.send_error_and_get_state(e.into_response(Severity::Error))
                    .await
            }
        };

        // Destroy the portal.
        self.adapter_client.session().remove_portal(EMPTY_PORTAL);

        result
    }

    async fn ensure_transaction(
        &mut self,
        num_stmts: usize,
        message_type: &str,
    ) -> Result<(), io::Error> {
        let start = Instant::now();
        if self.txn_needs_commit {
            self.commit_transaction().await?;
        }
        // start_transaction can't error (but assert that just in case it changes in
        // the future.
        let res = self.adapter_client.start_transaction(Some(num_stmts));
        assert_ok!(res);
        self.adapter_client
            .inner()
            .metrics()
            .pgwire_ensure_transaction_seconds
            .with_label_values(&[message_type])
            .observe(start.elapsed().as_secs_f64());
        Ok(())
    }

    fn parse_sql<'b>(&self, sql: &'b str) -> Result<Vec<StatementParseResult<'b>>, ErrorResponse> {
        let parse_start = Instant::now();
        let result = match self.adapter_client.parse(sql) {
            Ok(result) => result.map_err(|e| {
                // Convert our 0-based byte position to pgwire's 1-based character
                // position.
                let pos = sql[..e.error.pos].chars().count() + 1;
                ErrorResponse::error(SqlState::SYNTAX_ERROR, e.error.message).with_position(pos)
            }),
            Err(msg) => Err(ErrorResponse::error(SqlState::PROGRAM_LIMIT_EXCEEDED, msg)),
        };
        self.adapter_client
            .inner()
            .metrics()
            .parse_seconds
            .observe(parse_start.elapsed().as_secs_f64());
        result
    }

    /// Executes a "Simple Query", see
    /// <https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY>
    ///
    /// For implicit transaction handling, see "Multiple Statements in a Simple Query" in the above.
    #[instrument(level = "debug")]
    async fn query(&mut self, sql: String, received: EpochMillis) -> Result<State, io::Error> {
        // Parse first before doing any transaction checking.
        let stmts = match self.parse_sql(&sql) {
            Ok(stmts) => stmts,
            Err(err) => {
                self.send_error_and_get_state(err).await?;
                return self.ready().await;
            }
        };

        let num_stmts = stmts.len();

        // Compare with postgres' backend/tcop/postgres.c exec_simple_query.
        for StatementParseResult { ast: stmt, sql } in stmts {
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
            self.ensure_transaction(num_stmts, "query").await?;

            match self
                .one_query(stmt, sql.to_string(), LifecycleTimestamps { received })
                .await?
            {
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

    #[instrument(level = "debug")]
    async fn parse(
        &mut self,
        name: String,
        sql: String,
        param_oids: Vec<u32>,
    ) -> Result<State, io::Error> {
        // Start a transaction if we aren't in one.
        self.ensure_transaction(1, "parse").await?;

        let mut param_types = vec![];
        for oid in param_oids {
            match mz_pgrepr::Type::from_oid(oid) {
                Ok(ty) => match SqlScalarType::try_from(&ty) {
                    Ok(ty) => param_types.push(Some(ty)),
                    Err(err) => {
                        return self
                            .send_error_and_get_state(ErrorResponse::error(
                                SqlState::INVALID_PARAMETER_VALUE,
                                err.to_string(),
                            ))
                            .await;
                    }
                },
                Err(_) if oid == 0 => param_types.push(None),
                Err(e) => {
                    return self
                        .send_error_and_get_state(ErrorResponse::error(
                            SqlState::PROTOCOL_VIOLATION,
                            e.to_string(),
                        ))
                        .await;
                }
            }
        }

        let stmts = match self.parse_sql(&sql) {
            Ok(stmts) => stmts,
            Err(err) => {
                return self.send_error_and_get_state(err).await;
            }
        };
        if stmts.len() > 1 {
            return self
                .send_error_and_get_state(ErrorResponse::error(
                    SqlState::INTERNAL_ERROR,
                    "cannot insert multiple commands into a prepared statement",
                ))
                .await;
        }
        let (maybe_stmt, sql) = match stmts.into_iter().next() {
            None => (None, ""),
            Some(StatementParseResult { ast, sql }) => (Some(ast), sql),
        };
        if self.is_aborted_txn() && !is_txn_exit_stmt(maybe_stmt.as_ref()) {
            return self.aborted_txn_error().await;
        }
        match self
            .adapter_client
            .prepare(name, maybe_stmt, sql.to_string(), param_types)
            .await
        {
            Ok(()) => {
                self.send(BackendMessage::ParseComplete).await?;
                Ok(State::Ready)
            }
            Err(e) => {
                self.send_error_and_get_state(e.into_response(Severity::Error))
                    .await
            }
        }
    }

    /// Commits and clears the current transaction.
    #[instrument(level = "debug")]
    async fn commit_transaction(&mut self) -> Result<(), io::Error> {
        self.end_transaction(EndTransactionAction::Commit).await
    }

    /// Rollback and clears the current transaction.
    #[instrument(level = "debug")]
    async fn rollback_transaction(&mut self) -> Result<(), io::Error> {
        self.end_transaction(EndTransactionAction::Rollback).await
    }

    /// End a transaction and report to the user if an error occurred.
    #[instrument(level = "debug")]
    async fn end_transaction(&mut self, action: EndTransactionAction) -> Result<(), io::Error> {
        self.txn_needs_commit = false;
        let resp = self.adapter_client.end_transaction(action).await;
        if let Err(err) = resp {
            self.send(BackendMessage::ErrorResponse(
                err.into_response(Severity::Error),
            ))
            .await?;
        }
        Ok(())
    }

    #[instrument(level = "debug")]
    async fn bind(
        &mut self,
        portal_name: String,
        statement_name: String,
        param_formats: Vec<Format>,
        raw_params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<Format>,
    ) -> Result<State, io::Error> {
        // Start a transaction if we aren't in one.
        self.ensure_transaction(1, "bind").await?;

        let aborted_txn = self.is_aborted_txn();
        let stmt = match self
            .adapter_client
            .get_prepared_statement(&statement_name)
            .await
        {
            Ok(stmt) => stmt,
            Err(err) => {
                return self
                    .send_error_and_get_state(err.into_response(Severity::Error))
                    .await;
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
                .send_error_and_get_state(ErrorResponse::error(
                    SqlState::PROTOCOL_VIOLATION,
                    message,
                ))
                .await;
        }
        let param_formats = match pad_formats(param_formats, raw_params.len()) {
            Ok(param_formats) => param_formats,
            Err(msg) => {
                return self
                    .send_error_and_get_state(ErrorResponse::error(
                        SqlState::PROTOCOL_VIOLATION,
                        msg,
                    ))
                    .await;
            }
        };
        if aborted_txn && !is_txn_exit_stmt(stmt.stmt()) {
            return self.aborted_txn_error().await;
        }
        let buf = RowArena::new();
        let mut params = vec![];
        for ((raw_param, mz_typ), format) in raw_params
            .into_iter()
            .zip_eq(param_types)
            .zip_eq(param_formats)
        {
            let pg_typ = mz_pgrepr::Type::from(mz_typ);
            let datum = match raw_param {
                None => Datum::Null,
                Some(bytes) => match mz_pgrepr::Value::decode(format, &pg_typ, &bytes) {
                    Ok(param) => match param.into_datum_decode_error(&buf, &pg_typ, "parameter") {
                        Ok(datum) => datum,
                        Err(msg) => {
                            return self
                                .send_error_and_get_state(ErrorResponse::error(
                                    SqlState::INVALID_PARAMETER_VALUE,
                                    msg,
                                ))
                                .await;
                        }
                    },
                    Err(err) => {
                        let msg = format!("unable to decode parameter: {}", err);
                        return self
                            .send_error_and_get_state(ErrorResponse::error(
                                SqlState::INVALID_PARAMETER_VALUE,
                                msg,
                            ))
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
                    .send_error_and_get_state(ErrorResponse::error(
                        SqlState::PROTOCOL_VIOLATION,
                        msg,
                    ))
                    .await;
            }
        };

        // Binary encodings are disabled for list, map, and aclitem types, but this doesn't
        // apply to COPY TO statements.
        if !stmt.stmt().map_or(false, |stmt| {
            matches!(
                stmt,
                Statement::Copy(CopyStatement {
                    direction: CopyDirection::To,
                    ..
                })
            )
        }) {
            if let Some(desc) = stmt.desc().relation_desc.clone() {
                for (format, ty) in result_formats.iter().zip_eq(desc.iter_types()) {
                    match (format, &ty.scalar_type) {
                        (Format::Binary, mz_repr::SqlScalarType::List { .. }) => {
                            return self
                                .send_error_and_get_state(ErrorResponse::error(
                                    SqlState::PROTOCOL_VIOLATION,
                                    "binary encoding of list types is not implemented",
                                ))
                                .await;
                        }
                        (Format::Binary, mz_repr::SqlScalarType::Map { .. }) => {
                            return self
                                .send_error_and_get_state(ErrorResponse::error(
                                    SqlState::PROTOCOL_VIOLATION,
                                    "binary encoding of map types is not implemented",
                                ))
                                .await;
                        }
                        (Format::Binary, mz_repr::SqlScalarType::AclItem) => {
                            return self
                                .send_error_and_get_state(ErrorResponse::error(
                                    SqlState::PROTOCOL_VIOLATION,
                                    "binary encoding of aclitem types does not exist",
                                ))
                                .await;
                        }
                        _ => (),
                    }
                }
            }
        }

        let desc = stmt.desc().clone();
        let logging = Arc::clone(stmt.logging());
        let stmt_ast = stmt.stmt().cloned();
        let state_revision = stmt.state_revision;
        if let Err(err) = self.adapter_client.session().set_portal(
            portal_name,
            desc,
            stmt_ast,
            logging,
            params,
            result_formats,
            state_revision,
        ) {
            return self
                .send_error_and_get_state(err.into_response(Severity::Error))
                .await;
        }

        self.send(BackendMessage::BindComplete).await?;
        Ok(State::Ready)
    }

    /// `outer_ctx_extra` is Some when we are executing as part of an outer statement, e.g., a FETCH
    /// triggering the execution of the underlying query.
    fn execute(
        &mut self,
        portal_name: String,
        max_rows: ExecuteCount,
        get_response: GetResponse,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
        outer_ctx_extra: Option<ExecuteContextGuard>,
        received: Option<EpochMillis>,
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
                    let msg = format!("portal {} does not exist", portal_name.quoted());
                    if let Some(outer_ctx_extra) = outer_ctx_extra {
                        self.adapter_client.retire_execute(
                            outer_ctx_extra,
                            StatementEndedExecutionReason::Errored { error: msg.clone() },
                        );
                    }
                    return self
                        .send_error_and_get_state(ErrorResponse::error(
                            SqlState::INVALID_CURSOR_NAME,
                            msg,
                        ))
                        .await;
                }
            };

            *portal.lifecycle_timestamps = received.map(LifecycleTimestamps::new);

            // In an aborted transaction, reject all commands except COMMIT/ROLLBACK.
            let txn_exit_stmt = is_txn_exit_stmt(portal.stmt.as_deref());
            if aborted_txn && !txn_exit_stmt {
                if let Some(outer_ctx_extra) = outer_ctx_extra {
                    self.adapter_client.retire_execute(
                        outer_ctx_extra,
                        StatementEndedExecutionReason::Errored {
                            error: ABORTED_TXN_MSG.to_string(),
                        },
                    );
                }
                return self.aborted_txn_error().await;
            }

            let row_desc = portal.desc.relation_desc.clone();
            match portal.state {
                PortalState::NotStarted => {
                    // Start a transaction if we aren't in one.
                    self.ensure_transaction(1, "execute").await?;
                    match self
                        .adapter_client
                        .execute(
                            portal_name.clone(),
                            self.conn.wait_closed(),
                            outer_ctx_extra,
                        )
                        .await
                    {
                        Ok((response, execute_started)) => {
                            self.send_pending_notices().await?;
                            self.send_execute_response(
                                response,
                                row_desc,
                                portal_name,
                                max_rows,
                                get_response,
                                fetch_portal_name,
                                timeout,
                                execute_started,
                            )
                            .await
                        }
                        Err(e) => {
                            self.send_pending_notices().await?;
                            self.send_error_and_get_state(e.into_response(Severity::Error))
                                .await
                        }
                    }
                }
                PortalState::InProgress(rows) => {
                    let rows = rows.take().expect("InProgress rows must be populated");
                    let (result, statement_ended_execution_reason) = match self
                        .send_rows(
                            row_desc.expect("portal missing row desc on resumption"),
                            portal_name,
                            rows,
                            max_rows,
                            get_response,
                            fetch_portal_name,
                            timeout,
                        )
                        .await
                    {
                        Err(e) => {
                            // This is an error communicating with the connection.
                            // We consider that to be a cancelation, rather than a query error.
                            (Err(e), StatementEndedExecutionReason::Canceled)
                        }
                        Ok((ok, SendRowsEndedReason::Canceled)) => {
                            (Ok(ok), StatementEndedExecutionReason::Canceled)
                        }
                        // NOTE: For now the values for `result_size` and
                        // `rows_returned` in fetches are a bit confusing.
                        // We record `Some(n)` for the first fetch, where `n` is
                        // the number of bytes/rows returned by the inner
                        // execute (regardless of how many rows the
                        // fetch fetched), and `None` for subsequent fetches.
                        //
                        // This arguably makes sense since the size/rows
                        // returned measures how much work the compute
                        // layer had to do to satisfy the query, but
                        // we should revisit it if/when we start
                        // logging the inner execute separately.
                        Ok((
                            ok,
                            SendRowsEndedReason::Success {
                                result_size: _,
                                rows_returned: _,
                            },
                        )) => (
                            Ok(ok),
                            StatementEndedExecutionReason::Success {
                                result_size: None,
                                rows_returned: None,
                                execution_strategy: None,
                            },
                        ),
                        Ok((ok, SendRowsEndedReason::Errored { error })) => {
                            (Ok(ok), StatementEndedExecutionReason::Errored { error })
                        }
                    };
                    if let Some(outer_ctx_extra) = outer_ctx_extra {
                        self.adapter_client
                            .retire_execute(outer_ctx_extra, statement_ended_execution_reason);
                    }
                    result
                }
                // FETCH is an awkward command for our current architecture. In Postgres it
                // will extract <count> rows from the target portal, cache them, and return
                // them to the user as requested. Its command tag is always FETCH <num rows
                // extracted>. In Materialize, since we have chosen to not fully support FETCH,
                // we must remember the number of rows that were returned. Use this tag to
                // remember that information and return it.
                PortalState::Completed(Some(tag)) => {
                    let tag = tag.to_string();
                    if let Some(outer_ctx_extra) = outer_ctx_extra {
                        self.adapter_client.retire_execute(
                            outer_ctx_extra,
                            StatementEndedExecutionReason::Success {
                                result_size: None,
                                rows_returned: None,
                                execution_strategy: None,
                            },
                        );
                    }
                    self.send(BackendMessage::CommandComplete { tag }).await?;
                    Ok(State::Ready)
                }
                PortalState::Completed(None) => {
                    let error = format!(
                        "portal {} cannot be run",
                        Ident::new_unchecked(portal_name).to_ast_string_stable()
                    );
                    if let Some(outer_ctx_extra) = outer_ctx_extra {
                        self.adapter_client.retire_execute(
                            outer_ctx_extra,
                            StatementEndedExecutionReason::Errored {
                                error: error.clone(),
                            },
                        );
                    }
                    self.send_error_and_get_state(ErrorResponse::error(
                        SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE,
                        error,
                    ))
                    .await
                }
            }
        }
        .instrument(debug_span!("execute"))
        .boxed()
    }

    #[instrument(level = "debug")]
    async fn describe_statement(&mut self, name: &str) -> Result<State, io::Error> {
        // Start a transaction if we aren't in one.
        self.ensure_transaction(1, "describe_statement").await?;

        let stmt = match self.adapter_client.get_prepared_statement(name).await {
            Ok(stmt) => stmt,
            Err(err) => {
                return self
                    .send_error_and_get_state(err.into_response(Severity::Error))
                    .await;
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
        let formats = vec![Format::Text; stmt.desc().arity()];
        let row_desc = describe_rows(stmt.desc(), &formats);
        self.send_all([parameter_desc, row_desc]).await?;
        Ok(State::Ready)
    }

    #[instrument(level = "debug")]
    async fn describe_portal(&mut self, name: &str) -> Result<State, io::Error> {
        // Start a transaction if we aren't in one.
        self.ensure_transaction(1, "describe_portal").await?;

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
                self.send_error_and_get_state(ErrorResponse::error(
                    SqlState::INVALID_CURSOR_NAME,
                    format!("portal {} does not exist", name.quoted()),
                ))
                .await
            }
        }
    }

    #[instrument(level = "debug")]
    async fn close_statement(&mut self, name: String) -> Result<State, io::Error> {
        self.adapter_client
            .session()
            .remove_prepared_statement(&name);
        self.send(BackendMessage::CloseComplete).await?;
        Ok(State::Ready)
    }

    #[instrument(level = "debug")]
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
        *portal.state = PortalState::Completed(None);
    }

    async fn fetch(
        &mut self,
        name: String,
        count: Option<FetchDirection>,
        max_rows: ExecuteCount,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
        ctx_extra: ExecuteContextGuard,
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
                    let msg = "Execute with max_rows < a FETCH's count is not supported";
                    self.adapter_client.retire_execute(
                        ctx_extra,
                        StatementEndedExecutionReason::Errored {
                            error: msg.to_string(),
                        },
                    );
                    return self
                        .send_error_and_get_state(ErrorResponse::error(
                            SqlState::FEATURE_NOT_SUPPORTED,
                            msg,
                        ))
                        .await;
                }
                ExecuteCount::Count(count)
            }
            (ExecuteCount::Count(_), FetchDirection::ForwardAll) => {
                let msg = "Execute with max_rows of a FETCH ALL is not supported";
                self.adapter_client.retire_execute(
                    ctx_extra,
                    StatementEndedExecutionReason::Errored {
                        error: msg.to_string(),
                    },
                );
                return self
                    .send_error_and_get_state(ErrorResponse::error(
                        SqlState::FEATURE_NOT_SUPPORTED,
                        msg,
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
            Some(ctx_extra),
            None,
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
    #[instrument(level = "debug")]
    async fn send<M>(&mut self, message: M) -> Result<(), io::Error>
    where
        M: Into<BackendMessage>,
    {
        let message: BackendMessage = message.into();
        let is_error =
            matches!(&message, BackendMessage::ErrorResponse(e) if e.severity.is_error());

        self.conn.send(message).await?;

        // Flush immediately after sending an error response, as some clients
        // expect to be able to read the error response before sending a Sync
        // message. This is arguably in violation of the protocol specification,
        // but the specification is somewhat ambiguous, and easier to match
        // PostgreSQL here than to fix all the clients that have this
        // expectation.
        if is_error {
            self.conn.flush().await?;
        }

        Ok(())
    }

    #[instrument(level = "debug")]
    pub async fn send_all(
        &mut self,
        messages: impl IntoIterator<Item = BackendMessage>,
    ) -> Result<(), io::Error> {
        for m in messages {
            self.send(m).await?;
        }
        Ok(())
    }

    #[instrument(level = "debug")]
    async fn sync(&mut self) -> Result<State, io::Error> {
        // Close the current transaction if we are in an implicit transaction.
        if self.adapter_client.session().transaction().is_implicit() {
            self.commit_transaction().await?;
        }
        self.ready().await
    }

    #[instrument(level = "debug")]
    async fn ready(&mut self) -> Result<State, io::Error> {
        let txn_state = self.adapter_client.session().transaction().into();
        self.send(BackendMessage::ReadyForQuery(txn_state)).await?;
        self.flush().await
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug")]
    async fn send_execute_response(
        &mut self,
        response: ExecuteResponse,
        row_desc: Option<RelationDesc>,
        portal_name: String,
        max_rows: ExecuteCount,
        get_response: GetResponse,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
        execute_started: Instant,
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
                ctx_extra,
            } => {
                self.fetch(
                    name,
                    count,
                    max_rows,
                    Some(portal_name.to_string()),
                    timeout,
                    ctx_extra,
                )
                .await
            }
            ExecuteResponse::SendingRowsStreaming {
                rows,
                instance_id,
                strategy,
            } => {
                let row_desc = row_desc
                    .expect("missing row description for ExecuteResponse::SendingRowsStreaming");

                let span = tracing::debug_span!("sending_rows_streaming");

                self.send_rows(
                    row_desc,
                    portal_name,
                    InProgressRows::new(RecordFirstRowStream::new(
                        Box::new(rows),
                        execute_started,
                        &self.adapter_client,
                        Some(instance_id),
                        Some(strategy),
                    )),
                    max_rows,
                    get_response,
                    fetch_portal_name,
                    timeout,
                )
                .instrument(span)
                .await
                .map(|(state, _)| state)
            }
            ExecuteResponse::SendingRowsImmediate { rows } => {
                let row_desc = row_desc
                    .expect("missing row description for ExecuteResponse::SendingRowsImmediate");

                let span = tracing::debug_span!("sending_rows_immediate");

                let stream =
                    futures::stream::once(futures::future::ready(PeekResponseUnary::Rows(rows)));
                self.send_rows(
                    row_desc,
                    portal_name,
                    InProgressRows::new(RecordFirstRowStream::new(
                        Box::new(stream),
                        execute_started,
                        &self.adapter_client,
                        None,
                        Some(StatementExecutionStrategy::Constant),
                    )),
                    max_rows,
                    get_response,
                    fetch_portal_name,
                    timeout,
                )
                .instrument(span)
                .await
                .map(|(state, _)| state)
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
            ExecuteResponse::Subscribing {
                rx,
                ctx_extra,
                instance_id,
            } => {
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
                    row_desc.expect("missing row description for ExecuteResponse::Subscribing");
                let (result, statement_ended_execution_reason) = match self
                    .send_rows(
                        row_desc,
                        portal_name,
                        InProgressRows::new(RecordFirstRowStream::new(
                            Box::new(UnboundedReceiverStream::new(rx)),
                            execute_started,
                            &self.adapter_client,
                            Some(instance_id),
                            None,
                        )),
                        max_rows,
                        get_response,
                        fetch_portal_name,
                        timeout,
                    )
                    .await
                {
                    Err(e) => {
                        // This is an error communicating with the connection.
                        // We consider that to be a cancelation, rather than a query error.
                        (Err(e), StatementEndedExecutionReason::Canceled)
                    }
                    Ok((ok, SendRowsEndedReason::Canceled)) => {
                        (Ok(ok), StatementEndedExecutionReason::Canceled)
                    }
                    Ok((
                        ok,
                        SendRowsEndedReason::Success {
                            result_size,
                            rows_returned,
                        },
                    )) => (
                        Ok(ok),
                        StatementEndedExecutionReason::Success {
                            result_size: Some(result_size),
                            rows_returned: Some(rows_returned),
                            execution_strategy: None,
                        },
                    ),
                    Ok((ok, SendRowsEndedReason::Errored { error })) => {
                        (Ok(ok), StatementEndedExecutionReason::Errored { error })
                    }
                };
                self.adapter_client
                    .retire_execute(ctx_extra, statement_ended_execution_reason);
                return result;
            }
            ExecuteResponse::CopyTo { format, resp } => {
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::CopyTo");
                match *resp {
                    ExecuteResponse::Subscribing {
                        rx,
                        ctx_extra,
                        instance_id,
                    } => {
                        let (result, statement_ended_execution_reason) = match self
                            .copy_rows(
                                format,
                                row_desc,
                                RecordFirstRowStream::new(
                                    Box::new(UnboundedReceiverStream::new(rx)),
                                    execute_started,
                                    &self.adapter_client,
                                    Some(instance_id),
                                    None,
                                ),
                            )
                            .await
                        {
                            Err(e) => {
                                // This is an error communicating with the connection.
                                // We consider that to be a cancelation, rather than a query error.
                                (Err(e), StatementEndedExecutionReason::Canceled)
                            }
                            Ok((
                                state,
                                SendRowsEndedReason::Success {
                                    result_size,
                                    rows_returned,
                                },
                            )) => (
                                Ok(state),
                                StatementEndedExecutionReason::Success {
                                    result_size: Some(result_size),
                                    rows_returned: Some(rows_returned),
                                    execution_strategy: None,
                                },
                            ),
                            Ok((state, SendRowsEndedReason::Errored { error })) => {
                                (Ok(state), StatementEndedExecutionReason::Errored { error })
                            }
                            Ok((state, SendRowsEndedReason::Canceled)) => {
                                (Ok(state), StatementEndedExecutionReason::Canceled)
                            }
                        };
                        self.adapter_client
                            .retire_execute(ctx_extra, statement_ended_execution_reason);
                        return result;
                    }
                    ExecuteResponse::SendingRowsStreaming {
                        rows,
                        instance_id,
                        strategy,
                    } => {
                        // We don't need to finalize execution here;
                        // it was already done in the
                        // coordinator. Just extract the state and
                        // return that.
                        return self
                            .copy_rows(
                                format,
                                row_desc,
                                RecordFirstRowStream::new(
                                    Box::new(rows),
                                    execute_started,
                                    &self.adapter_client,
                                    Some(instance_id),
                                    Some(strategy),
                                ),
                            )
                            .await
                            .map(|(state, _)| state);
                    }
                    ExecuteResponse::SendingRowsImmediate { rows } => {
                        let span = tracing::debug_span!("sending_rows_immediate");

                        let rows = futures::stream::once(futures::future::ready(
                            PeekResponseUnary::Rows(rows),
                        ));
                        // We don't need to finalize execution here;
                        // it was already done in the
                        // coordinator. Just extract the state and
                        // return that.
                        return self
                            .copy_rows(
                                format,
                                row_desc,
                                RecordFirstRowStream::new(
                                    Box::new(rows),
                                    execute_started,
                                    &self.adapter_client,
                                    None,
                                    Some(StatementExecutionStrategy::Constant),
                                ),
                            )
                            .instrument(span)
                            .await
                            .map(|(state, _)| state);
                    }
                    _ => {
                        return self
                            .send_error_and_get_state(ErrorResponse::error(
                                SqlState::INTERNAL_ERROR,
                                "unsupported COPY response type".to_string(),
                            ))
                            .await;
                    }
                };
            }
            ExecuteResponse::CopyFrom {
                target_id,
                target_name,
                columns,
                params,
                ctx_extra,
            } => {
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::CopyFrom");
                self.copy_from(target_id, target_name, columns, params, row_desc, ctx_extra)
                    .await
            }
            ExecuteResponse::TransactionCommitted { params }
            | ExecuteResponse::TransactionRolledBack { params } => {
                let notify_set: mz_ore::collections::HashSet<String> = self
                    .adapter_client
                    .session()
                    .vars()
                    .notify_set()
                    .map(|v| v.name().to_string())
                    .collect();

                // Only report on parameters that are in the notify set.
                for (name, value) in params
                    .into_iter()
                    .filter(|(name, _v)| notify_set.contains(*name))
                {
                    let msg = BackendMessage::ParameterStatus(name, value);
                    self.send(msg).await?;
                }
                command_complete!()
            }

            ExecuteResponse::AlteredDefaultPrivileges
            | ExecuteResponse::AlteredObject(..)
            | ExecuteResponse::AlteredRole
            | ExecuteResponse::AlteredSystemConfiguration
            | ExecuteResponse::CreatedCluster { .. }
            | ExecuteResponse::CreatedClusterReplica { .. }
            | ExecuteResponse::CreatedConnection { .. }
            | ExecuteResponse::CreatedDatabase { .. }
            | ExecuteResponse::CreatedIndex { .. }
            | ExecuteResponse::CreatedIntrospectionSubscribe
            | ExecuteResponse::CreatedMaterializedView { .. }
            | ExecuteResponse::CreatedContinualTask { .. }
            | ExecuteResponse::CreatedRole
            | ExecuteResponse::CreatedSchema { .. }
            | ExecuteResponse::CreatedSecret { .. }
            | ExecuteResponse::CreatedSink { .. }
            | ExecuteResponse::CreatedSource { .. }
            | ExecuteResponse::CreatedTable { .. }
            | ExecuteResponse::CreatedType
            | ExecuteResponse::CreatedView { .. }
            | ExecuteResponse::CreatedViews { .. }
            | ExecuteResponse::CreatedNetworkPolicy
            | ExecuteResponse::Comment
            | ExecuteResponse::Deallocate { .. }
            | ExecuteResponse::Deleted(..)
            | ExecuteResponse::DiscardedAll
            | ExecuteResponse::DiscardedTemp
            | ExecuteResponse::DroppedObject(_)
            | ExecuteResponse::DroppedOwned
            | ExecuteResponse::GrantedPrivilege
            | ExecuteResponse::GrantedRole
            | ExecuteResponse::Inserted(..)
            | ExecuteResponse::Copied(..)
            | ExecuteResponse::Prepare
            | ExecuteResponse::Raised
            | ExecuteResponse::ReassignOwned
            | ExecuteResponse::RevokedPrivilege
            | ExecuteResponse::RevokedRole
            | ExecuteResponse::StartedTransaction { .. }
            | ExecuteResponse::Updated(..)
            | ExecuteResponse::ValidatedConnection => {
                command_complete!()
            }
        };

        assert_none!(tag, "tag created but not consumed: {:?}", tag);
        r
    }

    #[allow(clippy::too_many_arguments)]
    // TODO(guswynn): figure out how to get it to compile without skip_all
    #[mz_ore::instrument(level = "debug")]
    async fn send_rows(
        &mut self,
        row_desc: RelationDesc,
        portal_name: String,
        mut rows: InProgressRows,
        max_rows: ExecuteCount,
        get_response: GetResponse,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
    ) -> Result<(State, SendRowsEndedReason), io::Error> {
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
            ExecuteTimeout::Seconds(t) => (
                false,
                Some(tokio::time::Instant::now() + tokio::time::Duration::from_secs_f64(t)),
            ),
            ExecuteTimeout::WaitOnce => (true, None),
        };

        // Sanity check that the various `RelationDesc`s match up.
        {
            let portal_name_desc = &self
                .adapter_client
                .session()
                .get_portal_unverified(portal_name.as_str())
                .expect("portal should exist")
                .desc
                .relation_desc;
            if let Some(portal_name_desc) = portal_name_desc {
                soft_assert_eq_or_log!(portal_name_desc, &row_desc);
            }
            if let Some(fetch_portal_name) = &fetch_portal_name {
                let fetch_portal_desc = &self
                    .adapter_client
                    .session()
                    .get_portal_unverified(fetch_portal_name)
                    .expect("portal should exist")
                    .desc
                    .relation_desc;
                if let Some(fetch_portal_desc) = fetch_portal_desc {
                    soft_assert_eq_or_log!(fetch_portal_desc, &row_desc);
                }
            }
        }

        self.conn.set_encode_state(
            row_desc
                .typ()
                .column_types
                .iter()
                .map(|ty| mz_pgrepr::Type::from(&ty.scalar_type))
                .zip_eq(result_formats)
                .collect(),
        );

        let mut total_sent_rows = 0;
        let mut total_sent_bytes = 0;
        // want_rows is the maximum number of rows the client wants.
        let mut want_rows = match max_rows {
            ExecuteCount::All => usize::MAX,
            ExecuteCount::Count(count) => count,
        };

        // Send rows while the client still wants them and there are still rows to send.
        loop {
            // Fetch next batch of rows, waiting for a possible requested
            // timeout or notice.
            let batch = if rows.current.is_some() {
                FetchResult::Rows(rows.current.take())
            } else if want_rows == 0 {
                FetchResult::Rows(None)
            } else {
                let notice_fut = self.adapter_client.session().recv_notice();
                tokio::select! {
                    err = self.conn.wait_closed() => return Err(err),
                    _ = time::sleep_until(
                        deadline.unwrap_or_else(tokio::time::Instant::now),
                    ), if deadline.is_some() => FetchResult::Rows(None),
                    notice = notice_fut => {
                        FetchResult::Notice(notice)
                    }
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
                    if let Err(err) = verify_datum_desc(&row_desc, &mut batch_rows) {
                        let msg = err.to_string();
                        return self
                            .send_error_and_get_state(err.into_response(Severity::Error))
                            .await
                            .map(|state| (state, SendRowsEndedReason::Errored { error: msg }));
                    }

                    // If wait_once is true: the first time this fn is called it blocks (same as
                    // deadline == None). The second time this fn is called it should behave the
                    // same a 0s timeout.
                    if wait_once && batch_rows.peek().is_some() {
                        deadline = Some(tokio::time::Instant::now());
                        wait_once = false;
                    }

                    // Send a portion of the rows.
                    let mut sent_rows = 0;
                    let mut sent_bytes = 0;
                    let messages = (&mut batch_rows)
                        // TODO(parkmycar): This is a fair bit of juggling between iterator types
                        // to count the total number of bytes. Alternatively we could track the
                        // total sent bytes in this .map(...) call, but having side effects in map
                        // is a code smell.
                        .map(|row| {
                            let row_len = row.byte_len();
                            let values = mz_pgrepr::values_from_row(row, row_desc.typ());
                            (row_len, BackendMessage::DataRow(values))
                        })
                        .inspect(|(row_len, _)| {
                            sent_bytes += row_len;
                            sent_rows += 1
                        })
                        .map(|(_row_len, row)| row)
                        .take(want_rows);
                    self.send_all(messages).await?;

                    total_sent_rows += sent_rows;
                    total_sent_bytes += sent_bytes;
                    want_rows -= sent_rows;

                    // If we have sent the number of requested rows, put the remainder of the batch
                    // (if any) back and stop sending.
                    if want_rows == 0 {
                        if batch_rows.peek().is_some() {
                            rows.current = Some(batch_rows);
                        }
                        break;
                    }

                    self.conn.flush().await?;
                }
                FetchResult::Notice(notice) => {
                    self.send(notice.into_response()).await?;
                    self.conn.flush().await?;
                }
                FetchResult::Error(text) => {
                    return self
                        .send_error_and_get_state(ErrorResponse::error(
                            SqlState::INTERNAL_ERROR,
                            text.clone(),
                        ))
                        .await
                        .map(|state| (state, SendRowsEndedReason::Errored { error: text }));
                }
                FetchResult::Canceled => {
                    return self
                        .send_error_and_get_state(ErrorResponse::error(
                            SqlState::QUERY_CANCELED,
                            "canceling statement due to user request",
                        ))
                        .await
                        .map(|state| (state, SendRowsEndedReason::Canceled));
                }
            }
        }

        let portal = self
            .adapter_client
            .session()
            .get_portal_unverified_mut(&portal_name)
            .expect("valid portal name for send rows");

        let saw_rows = rows.remaining.saw_rows;
        let no_more_rows = rows.no_more_rows();
        let recorded_first_row_instant = rows.remaining.recorded_first_row_instant;

        // Always return rows back, even if it's empty. This prevents an unclosed
        // portal from re-executing after it has been emptied.
        *portal.state = PortalState::InProgress(Some(rows));

        let fetch_portal = fetch_portal_name.map(|name| {
            self.adapter_client
                .session()
                .get_portal_unverified_mut(&name)
                .expect("valid fetch portal")
        });
        let response_message = get_response(max_rows, total_sent_rows, fetch_portal);
        self.send(response_message).await?;

        // Attend to metrics if there are no more rows.
        if no_more_rows {
            let statement_type = if let Some(stmt) = &self
                .adapter_client
                .session()
                .get_portal_unverified(&portal_name)
                .expect("valid portal name for send_rows")
                .stmt
            {
                metrics::statement_type_label_value(stmt.deref())
            } else {
                "no-statement"
            };
            let duration = if saw_rows {
                recorded_first_row_instant
                    .expect("recorded_first_row_instant because saw_rows")
                    .elapsed()
            } else {
                // If the result is empty, then we define time from first to last row as 0.
                // (Note that, currently, an empty result involves a PeekResponse with 0 rows, which
                // does flip `saw_rows`, so this code path is currently not exercised.)
                Duration::ZERO
            };
            self.adapter_client
                .inner()
                .metrics()
                .result_rows_first_to_last_byte_seconds
                .with_label_values(&[statement_type])
                .observe(duration.as_secs_f64());
        }

        Ok((
            State::Ready,
            SendRowsEndedReason::Success {
                result_size: u64::cast_from(total_sent_bytes),
                rows_returned: u64::cast_from(total_sent_rows),
            },
        ))
    }

    #[mz_ore::instrument(level = "debug")]
    async fn copy_rows(
        &mut self,
        format: CopyFormat,
        row_desc: RelationDesc,
        mut stream: RecordFirstRowStream,
    ) -> Result<(State, SendRowsEndedReason), io::Error> {
        let (row_format, encode_format) = match format {
            CopyFormat::Text => (
                CopyFormatParams::Text(CopyTextFormatParams::default()),
                Format::Text,
            ),
            CopyFormat::Binary => (CopyFormatParams::Binary, Format::Binary),
            CopyFormat::Csv => (
                CopyFormatParams::Csv(CopyCsvFormatParams::default()),
                Format::Text,
            ),
            CopyFormat::Parquet => {
                let text = "Parquet format is not supported".to_string();
                return self
                    .send_error_and_get_state(ErrorResponse::error(
                        SqlState::INTERNAL_ERROR,
                        text.clone(),
                    ))
                    .await
                    .map(|state| (state, SendRowsEndedReason::Errored { error: text }));
            }
        };

        let encode_fn = |row: &RowRef, typ: &SqlRelationType, out: &mut Vec<u8>| {
            mz_pgcopy::encode_copy_format(&row_format, row, typ, out)
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
        let mut total_sent_bytes = 0;
        loop {
            tokio::select! {
                e = self.conn.wait_closed() => return Err(e),
                batch = stream.recv() => match batch {
                    None => break,
                    Some(PeekResponseUnary::Error(text)) => {
                        let err =
                            ErrorResponse::error(SqlState::INTERNAL_ERROR, text.clone());
                        return self
                            .send_error_and_get_state(err)
                            .await
                            .map(|state| (state, SendRowsEndedReason::Errored { error: text }));
                    }
                    Some(PeekResponseUnary::Canceled) => {
                        return self.send_error_and_get_state(ErrorResponse::error(
                                SqlState::QUERY_CANCELED,
                                "canceling statement due to user request",
                            ))
                            .await.map(|state| (state, SendRowsEndedReason::Canceled));
                    }
                    Some(PeekResponseUnary::Rows(mut rows)) => {
                        count += rows.count();
                        while let Some(row) = rows.next() {
                            total_sent_bytes += row.byte_len();
                            encode_fn(row, typ, &mut out)?;
                            self.send(BackendMessage::CopyData(mem::take(&mut out)))
                                .await?;
                        }
                    }
                },
                notice = self.adapter_client.session().recv_notice() => {
                    self.send(notice.into_response())
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
        Ok((
            State::Ready,
            SendRowsEndedReason::Success {
                result_size: u64::cast_from(total_sent_bytes),
                rows_returned: u64::cast_from(count),
            },
        ))
    }

    /// Handles the copy-in mode of the postgres protocol from transferring
    /// data to the server.
    #[instrument(level = "debug")]
    async fn copy_from(
        &mut self,
        target_id: CatalogItemId,
        target_name: String,
        columns: Vec<ColumnIndex>,
        params: CopyFormatParams<'static>,
        row_desc: RelationDesc,
        mut ctx_extra: ExecuteContextGuard,
    ) -> Result<State, io::Error> {
        let res = self
            .copy_from_inner(
                target_id,
                target_name,
                columns,
                params,
                row_desc,
                &mut ctx_extra,
            )
            .await;
        match &res {
            Ok(State::Ready) => {
                self.adapter_client.retire_execute(
                    ctx_extra,
                    StatementEndedExecutionReason::Success {
                        result_size: None,
                        rows_returned: None,
                        execution_strategy: None,
                    },
                );
            }
            Ok(State::Done) => {
                // The connection closed gracefully without sending us a `CopyDone`,
                // causing us to just drop the copy request.
                // For the purposes of statement logging, we count this as a cancellation.
                self.adapter_client
                    .retire_execute(ctx_extra, StatementEndedExecutionReason::Canceled);
            }
            Err(e) => {
                self.adapter_client.retire_execute(
                    ctx_extra,
                    StatementEndedExecutionReason::Errored {
                        error: format!("{e}"),
                    },
                );
            }
            Ok(state) if matches!(state, State::Drain) => {}
            other => {
                mz_ore::soft_panic_or_log!(
                    "unexpected COPY FROM state in copy_from: {other:?}; \
                     relying on ExecuteContextGuard::drop to retire as Aborted"
                );
            }
        }
        res
    }

    async fn copy_from_inner(
        &mut self,
        target_id: CatalogItemId,
        target_name: String,
        columns: Vec<ColumnIndex>,
        params: CopyFormatParams<'static>,
        row_desc: RelationDesc,
        ctx_extra: &mut ExecuteContextGuard,
    ) -> Result<State, io::Error> {
        let typ = row_desc.typ();
        let column_formats = vec![Format::Text; typ.column_types.len()];
        self.send(BackendMessage::CopyInResponse {
            overall_format: Format::Text,
            column_formats,
        })
        .await?;
        self.conn.flush().await?;

        // Set up the parallel streaming batch builders in the coordinator.
        let writer = match self
            .adapter_client
            .start_copy_from_stdin(
                target_id,
                target_name.clone(),
                columns.clone(),
                row_desc.clone(),
                params.clone(),
            )
            .await
        {
            Ok(writer) => writer,
            Err(e) => {
                // Drain remaining CopyData/CopyDone/CopyFail messages from the
                // socket. Since CopyInResponse was already sent, the client may
                // have pipelined copy data that we must consume before returning
                // the error, otherwise they'd be misinterpreted as top-level
                // protocol messages and cause a deadlock.
                loop {
                    match self.conn.recv().await? {
                        Some(FrontendMessage::CopyData(_)) => {}
                        Some(FrontendMessage::CopyDone) | Some(FrontendMessage::CopyFail(_)) => {
                            break;
                        }
                        Some(FrontendMessage::Flush) | Some(FrontendMessage::Sync) => {}
                        Some(_) => break,
                        None => return Ok(State::Done),
                    }
                }
                self.adapter_client.retire_execute(
                    std::mem::take(ctx_extra),
                    StatementEndedExecutionReason::Errored {
                        error: e.to_string(),
                    },
                );
                return self
                    .send_error_and_get_state(e.into_response(Severity::Error))
                    .await;
            }
        };

        // Enable copy mode on the codec to skip aggregate buffer size checks.
        self.conn.set_copy_mode(true);

        // Batch size for splitting raw data across parallel workers (~32MB).
        const BATCH_SIZE: usize = 32 * 1024 * 1024;
        let max_copy_from_row_size = self
            .adapter_client
            .get_system_vars()
            .await
            .max_copy_from_row_size()
            .try_into()
            .unwrap_or(usize::MAX);

        let mut data = Vec::new();
        let mut row_scanner = CopyRowScanner::new(&params);
        let num_workers = writer.batch_txs.len();
        let mut next_worker: usize = 0;
        let mut saw_copy_done = false;
        let mut saw_end_marker = false;
        let mut copy_from_error: Option<(SqlState, String)> = None;

        // Receive loop: accumulate CopyData, split at row boundaries,
        // round-robin raw chunks to parallel batch builder workers.
        loop {
            let message = self.conn.recv().await?;
            match message {
                Some(FrontendMessage::CopyData(buf)) => {
                    if saw_end_marker {
                        // Per PostgreSQL COPY behavior, ignore all bytes after
                        // the end-of-copy marker until CopyDone.
                        continue;
                    }
                    data.extend(buf);
                    row_scanner.scan_new_bytes(&data);

                    if let Some(end_pos) = row_scanner.end_marker_end() {
                        data.truncate(end_pos);
                        row_scanner.on_truncate(end_pos);
                        saw_end_marker = true;
                    }

                    // Guard against pathological single rows that never terminate.
                    if row_scanner.current_row_size(data.len()) > max_copy_from_row_size {
                        copy_from_error = Some((
                            SqlState::INSUFFICIENT_RESOURCES,
                            format!(
                                "COPY FROM STDIN row exceeded max_copy_from_row_size \
                                 ({max_copy_from_row_size} bytes)"
                            ),
                        ));
                        break;
                    }

                    // When buffer exceeds batch size, split at the last complete row
                    // and send the complete rows chunk to the next worker.
                    let mut send_failed = false;
                    while data.len() >= BATCH_SIZE {
                        let split_pos = match row_scanner.last_row_end() {
                            Some(pos) => pos,
                            None => break, // no complete row yet
                        };
                        let remainder = data.split_off(split_pos);
                        let chunk = std::mem::replace(&mut data, remainder);
                        row_scanner.on_split(split_pos);
                        if writer.batch_txs[next_worker].send(chunk).await.is_err() {
                            send_failed = true;
                            break;
                        }
                        next_worker = (next_worker + 1) % num_workers;
                    }
                    // Worker dropped (likely errored) — stop sending,
                    // fall through to completion_rx for the real error.
                    if send_failed {
                        break;
                    }
                }
                Some(FrontendMessage::CopyDone) => {
                    // Send any remaining data to the next worker.
                    if !data.is_empty() {
                        let chunk = std::mem::take(&mut data);
                        // Ignore send failure — completion_rx will have the error.
                        let _ = writer.batch_txs[next_worker].send(chunk).await;
                    }
                    saw_copy_done = true;
                    break;
                }
                Some(FrontendMessage::CopyFail(err)) => {
                    self.adapter_client.retire_execute(
                        std::mem::take(ctx_extra),
                        StatementEndedExecutionReason::Canceled,
                    );
                    // Drop the writer to signal cancellation to the background tasks.
                    drop(writer);
                    self.conn.set_copy_mode(false);
                    return self
                        .send_error_and_get_state(ErrorResponse::error(
                            SqlState::QUERY_CANCELED,
                            format!("COPY from stdin failed: {}", err),
                        ))
                        .await;
                }
                Some(FrontendMessage::Flush) | Some(FrontendMessage::Sync) => {}
                Some(_) => {
                    let msg = "unexpected message type during COPY from stdin";
                    self.adapter_client.retire_execute(
                        std::mem::take(ctx_extra),
                        StatementEndedExecutionReason::Errored {
                            error: msg.to_string(),
                        },
                    );
                    drop(writer);
                    self.conn.set_copy_mode(false);
                    return self
                        .send_error_and_get_state(ErrorResponse::error(
                            SqlState::PROTOCOL_VIOLATION,
                            msg,
                        ))
                        .await;
                }
                None => {
                    drop(writer);
                    self.conn.set_copy_mode(false);
                    return Ok(State::Done);
                }
            }
        }

        // If we exited the receive loop before seeing `CopyDone` (e.g. because
        // a worker failed and dropped its channel), keep draining COPY input to
        // avoid desynchronizing the protocol state machine.
        if !saw_copy_done {
            loop {
                match self.conn.recv().await? {
                    Some(FrontendMessage::CopyData(_)) => {}
                    Some(FrontendMessage::CopyDone) | Some(FrontendMessage::CopyFail(_)) => {
                        break;
                    }
                    Some(FrontendMessage::Flush) | Some(FrontendMessage::Sync) => {}
                    Some(_) => {
                        let msg = "unexpected message type during COPY from stdin";
                        self.adapter_client.retire_execute(
                            std::mem::take(ctx_extra),
                            StatementEndedExecutionReason::Errored {
                                error: msg.to_string(),
                            },
                        );
                        drop(writer);
                        self.conn.set_copy_mode(false);
                        return self
                            .send_error_and_get_state(ErrorResponse::error(
                                SqlState::PROTOCOL_VIOLATION,
                                msg,
                            ))
                            .await;
                    }
                    None => {
                        drop(writer);
                        self.conn.set_copy_mode(false);
                        return Ok(State::Done);
                    }
                }
            }
        }

        if let Some((code, msg)) = copy_from_error {
            self.adapter_client.retire_execute(
                std::mem::take(ctx_extra),
                StatementEndedExecutionReason::Errored { error: msg.clone() },
            );
            drop(writer);
            self.conn.set_copy_mode(false);
            return self
                .send_error_and_get_state(ErrorResponse::error(code, msg))
                .await;
        }

        self.conn.set_copy_mode(false);

        // Drop all senders to signal EOF to the background batch builders.
        // If copy_err is set, a worker already failed — dropping the senders
        // will cause remaining workers to stop, and we'll get the real error
        // from completion_rx below.
        drop(writer.batch_txs);

        // Wait for all parallel workers to finish building batches.
        let (proto_batches, row_count) = match writer.completion_rx.await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => {
                self.adapter_client.retire_execute(
                    std::mem::take(ctx_extra),
                    StatementEndedExecutionReason::Errored {
                        error: e.to_string(),
                    },
                );
                return self
                    .send_error_and_get_state(e.into_response(Severity::Error))
                    .await;
            }
            Err(_) => {
                let msg = "COPY FROM STDIN: background batch builder tasks dropped";
                self.adapter_client.retire_execute(
                    std::mem::take(ctx_extra),
                    StatementEndedExecutionReason::Errored {
                        error: msg.to_string(),
                    },
                );
                return self
                    .send_error_and_get_state(ErrorResponse::error(SqlState::INTERNAL_ERROR, msg))
                    .await;
            }
        };

        // Stage all batches in the session's transaction for atomic commit.
        if let Err(e) = self
            .adapter_client
            .stage_copy_from_stdin_batches(target_id, proto_batches)
        {
            self.adapter_client.retire_execute(
                std::mem::take(ctx_extra),
                StatementEndedExecutionReason::Errored {
                    error: e.to_string(),
                },
            );
            return self
                .send_error_and_get_state(e.into_response(Severity::Error))
                .await;
        }

        let tag = format!("COPY {}", row_count);
        self.send(BackendMessage::CommandComplete { tag }).await?;

        Ok(State::Ready)
    }

    #[instrument(level = "debug")]
    async fn send_pending_notices(&mut self) -> Result<(), io::Error> {
        let notices = self
            .adapter_client
            .session()
            .drain_notices()
            .into_iter()
            .map(|notice| BackendMessage::ErrorResponse(notice.into_response()));
        self.send_all(notices).await?;
        Ok(())
    }

    #[instrument(level = "debug")]
    async fn send_error_and_get_state(&mut self, err: ErrorResponse) -> Result<State, io::Error> {
        assert!(err.severity.is_error());
        debug!(
            "cid={} error code={}",
            self.adapter_client.session().conn_id(),
            err.code.code()
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

    #[instrument(level = "debug")]
    async fn aborted_txn_error(&mut self) -> Result<State, io::Error> {
        self.send(BackendMessage::ErrorResponse(ErrorResponse::error(
            SqlState::IN_FAILED_SQL_TRANSACTION,
            ABORTED_TXN_MSG,
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

fn pad_formats(formats: Vec<Format>, n: usize) -> Result<Vec<Format>, String> {
    match (formats.len(), n) {
        (0, e) => Ok(vec![Format::Text; e]),
        (1, e) => Ok(iter::repeat(formats[0]).take(e).collect()),
        (a, e) if a == e => Ok(formats),
        (a, e) => Err(format!(
            "expected {} field format specifiers, but got {}",
            e, a
        )),
    }
}

fn describe_rows(stmt_desc: &StatementDesc, formats: &[Format]) -> BackendMessage {
    match &stmt_desc.relation_desc {
        Some(desc) if !stmt_desc.is_copy => {
            BackendMessage::RowDescription(message::encode_row_description(desc, formats))
        }
        _ => BackendMessage::NoData,
    }
}

type GetResponse = fn(
    max_rows: ExecuteCount,
    total_sent_rows: usize,
    fetch_portal: Option<PortalRefMut>,
) -> BackendMessage;

// A GetResponse used by send_rows during execute messages on portals or for
// simple query messages.
fn portal_exec_message(
    max_rows: ExecuteCount,
    total_sent_rows: usize,
    _fetch_portal: Option<PortalRefMut>,
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
    fetch_portal: Option<PortalRefMut>,
) -> BackendMessage {
    let tag = format!("FETCH {}", total_sent_rows);
    if let Some(portal) = fetch_portal {
        *portal.state = PortalState::Completed(Some(tag.clone()));
    }
    BackendMessage::CommandComplete { tag }
}

fn get_authenticator(
    authenticator_kind: AuthenticatorKind,
    frontegg: Option<FronteggAuthenticator>,
    oidc: GenericOidcAuthenticator,
    adapter_client: mz_adapter::Client,
    // If oidc_auth_enabled exists as an option in the pgwire connection's
    // `option` parameter
    oidc_auth_option_enabled: bool,
) -> Authenticator {
    match authenticator_kind {
        AuthenticatorKind::Frontegg => Authenticator::Frontegg(
            frontegg.expect("Frontegg authenticator should exist with AuthenticatorKind::Frontegg"),
        ),
        AuthenticatorKind::Password => Authenticator::Password(adapter_client),
        AuthenticatorKind::Sasl => Authenticator::Sasl(adapter_client),
        AuthenticatorKind::Oidc => {
            if oidc_auth_option_enabled {
                Authenticator::Oidc(oidc)
            } else {
                // Fallback to password authentication if oidc auth is not enabled
                // through options.
                Authenticator::Password(adapter_client)
            }
        }
        AuthenticatorKind::None => Authenticator::None,
    }
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
    Rows(Option<Box<dyn RowIterator + Send + Sync>>),
    Canceled,
    Error(String),
    Notice(AdapterNotice),
}

#[derive(Debug)]
struct CopyRowScanner {
    scan_pos: usize,
    last_row_end: Option<usize>,
    end_marker_end: Option<usize>,
    csv: Option<CsvScanState>,
}

#[derive(Debug)]
struct CsvScanState {
    reader: csv_core::Reader,
    output: Vec<u8>,
    ends: Vec<usize>,
    record: Vec<u8>,
    record_ends: Vec<usize>,
    skip_first_record: bool,
}

impl CopyRowScanner {
    fn new(params: &CopyFormatParams<'_>) -> Self {
        let csv = match params {
            CopyFormatParams::Csv(CopyCsvFormatParams {
                delimiter,
                quote,
                escape,
                header,
                ..
            }) => Some(CsvScanState::new(*delimiter, *quote, *escape, *header)),
            _ => None,
        };

        CopyRowScanner {
            scan_pos: 0,
            last_row_end: None,
            end_marker_end: None,
            csv,
        }
    }

    fn scan_new_bytes(&mut self, data: &[u8]) {
        if self.scan_pos >= data.len() {
            return;
        }

        if let Some(csv) = self.csv.as_mut() {
            let mut input = &data[self.scan_pos..];
            let mut consumed = 0usize;
            while !input.is_empty() {
                let (result, n_input, n_output, n_ends) =
                    csv.reader
                        .read_record(input, &mut csv.output, &mut csv.ends);
                consumed += n_input;
                input = &input[n_input..];
                if !csv.output.is_empty() {
                    csv.record.extend_from_slice(&csv.output[..n_output]);
                }
                if !csv.ends.is_empty() {
                    csv.record_ends.extend_from_slice(&csv.ends[..n_ends]);
                }

                match result {
                    ReadRecordResult::InputEmpty => break,
                    ReadRecordResult::OutputFull => {
                        if n_input == 0 {
                            csv.output
                                .resize(csv.output.len().saturating_mul(2).max(1), 0);
                        }
                    }
                    ReadRecordResult::OutputEndsFull => {
                        if n_input == 0 {
                            csv.ends.resize(csv.ends.len().saturating_mul(2).max(1), 0);
                        }
                    }
                    ReadRecordResult::Record | ReadRecordResult::End => {
                        let row_end = self.scan_pos + consumed;
                        self.last_row_end = Some(row_end);
                        if self.end_marker_end.is_none() {
                            let is_marker = if csv.skip_first_record {
                                csv.skip_first_record = false;
                                false
                            } else if csv.record_ends.len() == 1 {
                                let end = csv.record_ends[0];
                                end == 2 && csv.record.get(0..end) == Some(b"\\.")
                            } else {
                                false
                            };
                            if is_marker {
                                self.end_marker_end = Some(row_end);
                                break;
                            }
                        }
                        csv.record.clear();
                        csv.record_ends.clear();
                    }
                }
            }
        } else {
            let mut row_start = self.last_row_end.unwrap_or(0);
            for (offset, b) in data[self.scan_pos..].iter().enumerate() {
                if *b == b'\n' {
                    let row_end = self.scan_pos + offset + 1;
                    self.last_row_end = Some(row_end);
                    if self.end_marker_end.is_none() {
                        let row = &data[row_start..row_end];
                        if row.get(0..2) == Some(b"\\.") {
                            self.end_marker_end = Some(row_end);
                            break;
                        }
                    }
                    row_start = row_end;
                }
            }
        }

        self.scan_pos = data.len();
    }

    fn last_row_end(&self) -> Option<usize> {
        self.last_row_end
    }

    fn end_marker_end(&self) -> Option<usize> {
        self.end_marker_end
    }

    fn current_row_size(&self, data_len: usize) -> usize {
        data_len.saturating_sub(self.last_row_end.unwrap_or(0))
    }

    fn on_split(&mut self, split_pos: usize) {
        self.scan_pos = self.scan_pos.saturating_sub(split_pos);
        self.last_row_end = None;
        self.end_marker_end = self
            .end_marker_end
            .and_then(|end| end.checked_sub(split_pos));
    }

    fn on_truncate(&mut self, new_len: usize) {
        self.scan_pos = self.scan_pos.min(new_len);
        self.last_row_end = self.last_row_end.filter(|&end| end <= new_len);
        self.end_marker_end = self.end_marker_end.filter(|&end| end <= new_len);
    }
}

impl CsvScanState {
    fn new(delimiter: u8, quote: u8, escape: u8, header: bool) -> Self {
        let (double_quote, escape) = if quote == escape {
            (true, None)
        } else {
            (false, Some(escape))
        };
        CsvScanState {
            reader: csv_core::ReaderBuilder::new()
                .delimiter(delimiter)
                .quote(quote)
                .double_quote(double_quote)
                .escape(escape)
                .build(),
            output: vec![0; 1],
            ends: vec![0; 1],
            record: Vec::new(),
            record_ends: Vec::new(),
            skip_first_record: header,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[mz_ore::test]
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

    #[mz_ore::test]
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

    #[mz_ore::test]
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
