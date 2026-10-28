// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Connection setup: resolve, TCP, TLS, ApiVersions, SASL.
//!
//! The steps and their failure texts follow librdkafka's broker state
//! machine (`CONNECT`, `SSL_HANDSHAKE`, `APIVERSION_QUERY`, `AUTH_HANDSHAKE`,
//! `AUTH_REQ`). Setup requests are sent one at a time on the raw stream;
//! pipelining starts once the connection is up.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, SaslAuthenticateRequest, SaslHandshakeRequest,
};
use kafka_protocol::protocol::{Request, StrBytes};
use mz_ore::cast::CastFrom;
use tokio::net::{TcpSocket, TcpStream};

use crate::config::RDKafkaLogLevel;
use crate::config::settings::{ClientSettings, SaslMechanism};
use crate::internal::hooks::{self, Hooks, Logger};
use crate::internal::protocol::{self, ApiVersions};
use crate::types::RDKafkaErrorCode;

use super::frame::{self, FrameError};
use super::sasl::{Mechanism, OAuthTokens, Step};
use super::tls::TlsConfig;
use super::{
    BoxedIo, BrokerCounters, BrokerState, CloseCallback, Connection, ConnectionParams, Failure,
    StateCell,
};

/// What a client needs to connect to its brokers. Built once per client.
#[derive(Clone)]
pub(crate) struct ConnectContext {
    pub settings: Arc<ClientSettings>,
    pub hooks: Arc<dyn Hooks>,
    pub logger: Logger,
    pub tls: Option<TlsConfig>,
    pub oauth: Option<Arc<OAuthTokens>>,
}

/// The broker to connect to.
pub(crate) struct Target<'a> {
    /// The host as configured or advertised, used for resolution, SNI and
    /// certificate verification.
    pub host: &'a str,
    pub port: u16,
    /// librdkafka's broker name, e.g. `sasl_ssl://host:9093/1`, for log lines.
    pub name: &'a str,
}

/// The C `strerror` text of an I/O error: Rust's message without the
/// ` (os error N)` suffix.
pub(crate) fn strerror(e: &io::Error) -> String {
    let s = e.to_string();
    match e.raw_os_error() {
        Some(code) => s
            .strip_suffix(&format!(" (os error {code})"))
            .map(str::to_string)
            .unwrap_or(s),
        None => s,
    }
}

/// The text librdkafka's transport layer uses for a failed read.
pub(crate) fn read_error_text(e: &io::Error) -> String {
    match e.kind() {
        io::ErrorKind::UnexpectedEof => {
            "Disconnected: connection closed by peer: receive 0 after POLLIN".to_string()
        }
        io::ErrorKind::ConnectionReset => "Disconnected: connection reset by peer".to_string(),
        _ => strerror(e),
    }
}

/// The `gai_strerror` text librdkafka reports for a failed resolve hook,
/// following rust-rdkafka's trampoline: the error's text is matched against
/// the `EAI_NODATA`, `EAI_NONAME` and `EAI_AGAIN` texts, anything else is
/// `EAI_FAIL`.
fn gai_text(e: &io::Error) -> &'static str {
    const KNOWN: &[&str] = &[
        "No address associated with hostname",
        "Name or service not known",
        "Temporary failure in name resolution",
    ];
    let s = e.to_string();
    KNOWN
        .iter()
        .find(|k| s.ends_with(*k))
        .copied()
        .unwrap_or("Non-recoverable failure in name resolution")
}

/// librdkafka's `rd_sockaddr2str` with family and port: `ipv4#1.2.3.4:9092`
/// or `ipv6#[::1]:9092`.
pub(crate) fn sockaddr_str(addr: &SocketAddr) -> String {
    match addr {
        SocketAddr::V4(a) => format!("ipv4#{a}"),
        SocketAddr::V6(a) => format!("ipv6#[{}]:{}", a.ip(), a.port()),
    }
}

/// Establishes an authenticated connection.
///
/// Calls the resolve hook (on a blocking thread) on every attempt and
/// connects to the `cursor`-th address it returns, advancing the cursor, so
/// successive attempts rotate through the addresses. `state` follows the
/// setup steps for failure messages. Everything after resolution is bounded
/// by `socket.connection.setup.timeout.ms`.
pub(crate) async fn establish(
    ctx: &ConnectContext,
    target: &Target<'_>,
    cursor: &AtomicUsize,
    state: &StateCell,
    counters: Arc<BrokerCounters>,
    on_close: CloseCallback,
) -> Result<Connection, Failure> {
    let settings = &ctx.settings;
    state.set(BrokerState::Connect);
    let addrs = hooks::resolve_broker_addr(&ctx.hooks, target.host.to_string(), target.port)
        .await
        .map_err(|e| {
            Failure::error(
                RDKafkaErrorCode::Resolve,
                format!(
                    "Failed to resolve '{}:{}': {}",
                    target.host,
                    target.port,
                    gai_text(&e)
                ),
            )
        })?;
    if addrs.is_empty() {
        return Err(Failure::error(
            RDKafkaErrorCode::Resolve,
            format!(
                "Failed to resolve '{}:{}': {}",
                target.host, target.port, "Non-recoverable failure in name resolution"
            ),
        ));
    }
    let addr = addrs[cursor.fetch_add(1, Ordering::Relaxed) % addrs.len()];

    let setup = setup(ctx, target, addr, state, &counters);
    let (io, versions, reauth_at) =
        match tokio::time::timeout(settings.socket_connection_setup_timeout, setup).await {
            Ok(result) => result?,
            Err(_) => {
                return Err(Failure {
                    level: RDKafkaLogLevel::Warning,
                    code: RDKafkaErrorCode::BrokerTransportFailure,
                    message: format!("Connection setup timed out in state {}", state.get().name()),
                    planned: false,
                });
            }
        };
    let params = connection_params(settings);
    Ok(Connection::start(
        io, versions, params, counters, reauth_at, on_close,
    ))
}

/// The connection parameters derived from the client settings.
pub(crate) fn connection_params(settings: &ClientSettings) -> ConnectionParams {
    let max_idle = settings.connections_max_idle.map(|idle| {
        // librdkafka subtracts up to 2 s of jitter per broker so that
        // connections do not all close at once.
        let jitter = Duration::from_millis(rand::RngExt::random_range(&mut rand::rng(), 0..=2000));
        if idle >= Duration::from_secs(4) {
            idle - jitter
        } else {
            idle
        }
    });
    ConnectionParams {
        client_id: Arc::from(settings.client_id.as_str()),
        socket_timeout: settings.socket_timeout,
        receive_message_max_bytes: usize::try_from(settings.receive_message_max_bytes)
            .unwrap_or(usize::MAX),
        max_in_flight: usize::try_from(settings.max_in_flight).unwrap_or(1),
        max_idle,
        log_connection_close: settings.log_connection_close,
        tls: settings.security_protocol.uses_tls(),
    }
}

/// A setup-phase stream with sequential request/response.
struct SetupIo<'a> {
    io: BoxedIo,
    client_id: &'a str,
    max_size: usize,
    next_correlation_id: i32,
    counters: &'a BrokerCounters,
    log_connection_close: bool,
    tls: bool,
}

impl SetupIo<'_> {
    /// The failure for a transport error during setup. Disconnects are
    /// logged at info level (one request in flight), as librdkafka's
    /// `rd_kafka_broker_conn_closed` does for fresh connections.
    fn transport_failure(&self, e: FrameError) -> Failure {
        match e {
            FrameError::Size(size) => Failure::error(
                RDKafkaErrorCode::BadMessage,
                format!(
                    "Receive failed: Invalid response size {size} (0..{}): \
                     increase receive.message.max.bytes",
                    self.max_size
                ),
            ),
            FrameError::Io(e) => {
                let text = if self.tls && e.kind() == io::ErrorKind::UnexpectedEof {
                    "Disconnected: SSL connection closed by peer".to_string()
                } else {
                    read_error_text(&e)
                };
                if text.starts_with("Disconnected") {
                    Failure {
                        level: if self.log_connection_close {
                            RDKafkaLogLevel::Info
                        } else {
                            RDKafkaLogLevel::Debug
                        },
                        code: RDKafkaErrorCode::BrokerTransportFailure,
                        message: text,
                        planned: false,
                    }
                } else {
                    Failure::error(
                        RDKafkaErrorCode::BrokerTransportFailure,
                        format!("Receive failed: {text}"),
                    )
                }
            }
        }
    }

    async fn write(&mut self, frame: &[u8]) -> Result<(), Failure> {
        frame::write_frame(&mut self.io, frame).await.map_err(|e| {
            self.counters.txerrs.fetch_add(1, Ordering::Relaxed);
            Failure::error(
                RDKafkaErrorCode::BrokerTransportFailure,
                format!("Send failed: {}", strerror(&e)),
            )
        })?;
        self.counters.tx.fetch_add(1, Ordering::Relaxed);
        self.counters
            .tx_bytes
            .fetch_add(u64::cast_from(frame.len()), Ordering::Relaxed);
        Ok(())
    }

    async fn read(&mut self) -> Result<Bytes, Failure> {
        let frame = frame::read_frame(&mut self.io, self.max_size)
            .await
            .map_err(|e| self.transport_failure(e))?;
        self.counters.rx.fetch_add(1, Ordering::Relaxed);
        self.counters.rx_bytes.fetch_add(
            u64::cast_from(frame.len() + protocol::FRAME_LEN_SIZE),
            Ordering::Relaxed,
        );
        Ok(frame)
    }

    /// Sends a request and reads its response frame.
    async fn call<R: Request>(&mut self, request: &R, version: i16) -> Result<Bytes, Failure> {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id += 1;
        let frame = protocol::encode_request(request, version, correlation_id, self.client_id)
            .map_err(|e| Failure::error(RDKafkaErrorCode::InvalidArgument, e))?;
        self.write(&frame).await?;
        let response = self.read().await?;
        if protocol::peek_correlation_id(&response) != Some(correlation_id) {
            return Err(Failure::error(
                RDKafkaErrorCode::BadMessage,
                format!(
                    "Receive failed: Response correlation id {} does not match request {}",
                    protocol::peek_correlation_id(&response).unwrap_or(-1),
                    correlation_id
                ),
            ));
        }
        Ok(response)
    }
}

async fn setup(
    ctx: &ConnectContext,
    target: &Target<'_>,
    addr: SocketAddr,
    state: &StateCell,
    counters: &BrokerCounters,
) -> Result<(BoxedIo, ApiVersions, Option<Instant>), Failure> {
    let settings = &ctx.settings;
    let tcp = connect_tcp(settings, addr).await?;
    let io: BoxedIo = match &ctx.tls {
        Some(tls) if settings.security_protocol.uses_tls() => {
            state.set(BrokerState::SslHandshake);
            Box::new(
                tls.connect(target.host, tcp, &ctx.logger, target.name)
                    .await?,
            )
        }
        _ => Box::new(tcp),
    };
    let mut io = SetupIo {
        io,
        client_id: &settings.client_id,
        max_size: usize::try_from(settings.receive_message_max_bytes).unwrap_or(usize::MAX),
        next_correlation_id: 1,
        counters,
        log_connection_close: settings.log_connection_close,
        tls: settings.security_protocol.uses_tls(),
    };

    state.set(BrokerState::ApiVersionQuery);
    let versions =
        match tokio::time::timeout(settings.api_version_request_timeout, api_versions(&mut io))
            .await
        {
            Ok(result) => result?,
            Err(_) => return Err(api_versions_failure(RDKafkaErrorCode::OperationTimedOut)),
        };

    let mut reauth_at = None;
    if let Some(mechanism) = settings
        .sasl
        .mechanism
        .filter(|_| settings.security_protocol.uses_sasl())
    {
        reauth_at = authenticate(ctx, &mut io, &versions, mechanism, state).await?;
    }
    Ok((io.io, versions, reauth_at))
}

async fn connect_tcp(settings: &ClientSettings, addr: SocketAddr) -> Result<TcpStream, Failure> {
    let fail = |e: io::Error| {
        Failure::error(
            RDKafkaErrorCode::BrokerTransportFailure,
            format!(
                "Connect to {} failed: {}",
                sockaddr_str(&addr),
                strerror(&e)
            ),
        )
    };
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }
    .map_err(|e| {
        Failure::error(
            RDKafkaErrorCode::BrokerTransportFailure,
            format!("Failed to create socket: {}", strerror(&e)),
        )
    })?;
    // Socket options are best effort, as in librdkafka, which only logs
    // failures.
    if settings.socket_keepalive {
        let _ = socket.set_keepalive(true);
    }
    if let Some(size) = settings.socket_send_buffer_bytes {
        let _ = socket.set_send_buffer_size(size);
    }
    if let Some(size) = settings.socket_receive_buffer_bytes {
        let _ = socket.set_recv_buffer_size(size);
    }
    let _ = socket.set_nodelay(settings.socket_nagle_disable);
    socket.connect(addr).await.map_err(fail)
}

fn api_versions_failure(code: RDKafkaErrorCode) -> Failure {
    Failure {
        level: RDKafkaLogLevel::Warning,
        code: RDKafkaErrorCode::BrokerTransportFailure,
        message: format!(
            "ApiVersionRequest failed: {}: probably due to broker version < 0.10 \
             (see api.version.request configuration)",
            code.err2str()
        ),
        planned: false,
    }
}

/// Sends ApiVersions v3, falling back to an older version when the broker
/// answers `UNSUPPORTED_VERSION`, like librdkafka.
async fn api_versions(io: &mut SetupIo<'_>) -> Result<ApiVersions, Failure> {
    let mut version = protocol::API_VERSIONS_FIRST_VERSION;
    loop {
        let request = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str(protocol::CLIENT_SOFTWARE_NAME))
            .with_client_software_version(StrBytes::from_static_str(
                protocol::CLIENT_SOFTWARE_VERSION,
            ));
        let frame = io.call(&request, version).await?;
        let error_code = protocol::api_versions_error_code(&frame).unwrap_or(-1);
        // A broker that does not support the version answers with a v0
        // response.
        let decode_version = if error_code == 35 { 0 } else { version };
        let response = protocol::decode_response::<ApiVersionsRequest>(frame, decode_version)
            .map_err(|_| api_versions_failure(RDKafkaErrorCode::BadMessage))?;
        match protocol::broker_error(response.error_code) {
            None => {
                return Ok(ApiVersions::negotiate(
                    response
                        .api_keys
                        .iter()
                        .map(|k| (k.api_key, k.min_version, k.max_version)),
                ));
            }
            Some(RDKafkaErrorCode::UnsupportedVersion) if version > 0 => {
                let broker_max = response
                    .api_keys
                    .iter()
                    .find(|k| Some(k.api_key) == protocol::api_key_value(ApiKey::ApiVersions))
                    .map(|k| k.max_version);
                version = match broker_max {
                    Some(max) => (version - 1).min(max).max(0),
                    None => 0,
                };
            }
            Some(RDKafkaErrorCode::InvalidRequest) if version > 0 => version = 0,
            Some(code) => return Err(api_versions_failure(code)),
        }
    }
}

/// SaslHandshake and the SaslAuthenticate exchange. Returns when the
/// session must be renewed (90% of the broker's session lifetime, KIP-368).
async fn authenticate(
    ctx: &ConnectContext,
    io: &mut SetupIo<'_>,
    versions: &ApiVersions,
    mechanism: SaslMechanism,
    state: &StateCell,
) -> Result<Option<Instant>, Failure> {
    let settings = &ctx.settings;
    let mech_name = mechanism.name();
    state.set(BrokerState::AuthHandshake);
    let handshake_version = versions.version(ApiKey::SaslHandshake).unwrap_or(0);
    let request =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str(mech_name));
    let frame = io.call(&request, handshake_version).await?;
    let response = protocol::decode_response::<SaslHandshakeRequest>(frame, handshake_version)
        .map_err(|_| {
            Failure::error(
                RDKafkaErrorCode::Authentication,
                format!(
                    "SASL {mech_name} mechanism handshake failed: {}: broker's supported \
                     mechanisms: ",
                    RDKafkaErrorCode::BadMessage.err2str()
                ),
            )
        })?;
    if let Some(code) = protocol::broker_error(response.error_code) {
        let mechs: Vec<&str> = response.mechanisms.iter().map(|m| m.as_str()).collect();
        return Err(Failure::error(
            RDKafkaErrorCode::Authentication,
            format!(
                "SASL {mech_name} mechanism handshake failed: {}: broker's supported \
                 mechanisms: {}",
                code.err2str(),
                mechs.join(",")
            ),
        ));
    }

    let token = match (&ctx.oauth, mechanism) {
        (Some(oauth), SaslMechanism::OAuthBearer) => Some(oauth.current().await.map_err(|e| {
            Failure::error(
                RDKafkaErrorCode::Authentication,
                format!("Failed to initialize SASL authentication: {e}"),
            )
        })?),
        _ => None,
    };
    let mut exchange = Mechanism::new(mechanism, &settings.sasl, token.as_ref()).map_err(|e| {
        Failure::error(
            RDKafkaErrorCode::Authentication,
            format!("Failed to initialize SASL authentication: {e}"),
        )
    })?;

    state.set(BrokerState::AuthReq);
    let auth_err = |msg: String| {
        Failure::error(
            RDKafkaErrorCode::Authentication,
            format!("SASL authentication error: {msg}"),
        )
    };
    // SaslHandshake v0 means the SASL tokens follow as raw size-prefixed
    // frames instead of SaslAuthenticate requests.
    let framed = handshake_version >= 1 && versions.version(ApiKey::SaslAuthenticate).is_some();
    let auth_version = versions.version(ApiKey::SaslAuthenticate).unwrap_or(0);
    let mut server: Option<Bytes> = None;
    let mut session_lifetime_ms = 0;
    loop {
        let msg = match exchange.step(server.as_deref()).map_err(auth_err)? {
            Step::Send(msg) => msg,
            Step::Done => break,
        };
        if framed {
            let request = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(msg));
            let frame = io.call(&request, auth_version).await?;
            let response =
                protocol::decode_response::<SaslAuthenticateRequest>(frame, auth_version).map_err(
                    |_| {
                        auth_err(format!(
                            "SaslAuthenticateResponse parsing failed: {}",
                            RDKafkaErrorCode::BadMessage.err2str()
                        ))
                    },
                )?;
            if let Some(code) = protocol::broker_error(response.error_code) {
                // librdkafka translates the broker's authentication failure
                // to its local code.
                let code = if code == RDKafkaErrorCode::SaslAuthenticationFailed {
                    RDKafkaErrorCode::Authentication
                } else {
                    code
                };
                let text = response
                    .error_message
                    .as_ref()
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default();
                return Err(Failure::error(
                    code,
                    format!("SASL authentication error: {text}"),
                ));
            }
            session_lifetime_ms = response.session_lifetime_ms;
            server = Some(response.auth_bytes);
        } else {
            let mut frame = Vec::with_capacity(4 + msg.len());
            let len = i32::try_from(msg.len()).expect("SASL message fits");
            frame.extend_from_slice(&len.to_be_bytes());
            frame.extend_from_slice(&msg);
            io.write(&frame).await?;
            let response = frame::read_raw_frame(&mut io.io, io.max_size)
                .await
                .map_err(|e| {
                    let mut failure = io.transport_failure(e);
                    if failure.message.starts_with("Disconnected") {
                        failure.message = format!(
                            "{}: check client {} credentials and broker logs",
                            failure.message, mech_name
                        );
                    }
                    failure
                })?;
            server = Some(response);
        }
    }
    Ok((session_lifetime_ms > 0).then(|| {
        let lifetime = Duration::from_millis(u64::try_from(session_lifetime_ms).unwrap_or(0));
        Instant::now() + lifetime.mul_f64(0.9)
    }))
}
