// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! One connection to one broker.
//!
//! [`connect::establish`] resolves the broker address through the user's
//! hook, connects TCP, performs the TLS handshake, negotiates API versions and
//! authenticates with SASL, all bounded by `socket.connection.setup.timeout.ms`.
//! The result is a [`Connection`]: a writer task and a reader task over the
//! two halves of the stream, with requests pipelined by correlation id.
//!
//! Failures carry librdkafka's error texts and log levels (see
//! [`Failure`]); the owning broker turns them into `FAIL` log lines and error
//! callbacks.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use kafka_protocol::messages::{ApiKey, ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::Request;
use mz_ore::cast::CastFrom;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::config::RDKafkaLogLevel;
use crate::internal::hooks::Logger;
use crate::internal::protocol::{self, ApiVersions};
use crate::internal::runtime;
use crate::statistics::Window;
use crate::types::RDKafkaErrorCode;

pub(crate) mod connect;
mod frame;
pub(crate) mod sasl;
pub(crate) mod tls;

/// A failed request or connection, with a librdkafka error code and text.
///
/// The text is what librdkafka would put in its broker failure message, e.g.
/// `Connect to ipv4#127.0.0.1:9092 failed: Connection refused`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NetError {
    code: RDKafkaErrorCode,
    message: String,
}

impl NetError {
    pub(crate) fn new(code: RDKafkaErrorCode, message: impl Into<String>) -> NetError {
        NetError {
            code,
            message: message.into(),
        }
    }

    /// A request that failed because its connection failed.
    pub(crate) fn transport(message: impl Into<String>) -> NetError {
        NetError::new(RDKafkaErrorCode::BrokerTransportFailure, message)
    }

    /// A request that timed out.
    pub(crate) fn timed_out() -> NetError {
        NetError::new(
            RDKafkaErrorCode::OperationTimedOut,
            RDKafkaErrorCode::OperationTimedOut.err2str(),
        )
    }

    /// The code: `BrokerTransportFailure` for connection failures,
    /// `OperationTimedOut` for timeouts, `UnsupportedFeature` if the broker
    /// does not support the API, `BadMessage` for undecodable responses,
    /// `BrokerDestroy` when the client is shutting down, and others for
    /// connection setup (`Resolve`, `SSL`, `Authentication`).
    pub(crate) fn code(&self) -> RDKafkaErrorCode {
        self.code
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for NetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for NetError {}

/// A connection failure as librdkafka reports it: log level, error code and
/// message (`errstr` before the ` (after Nms in state S)` suffix).
#[derive(Clone, Debug)]
pub(crate) struct Failure {
    pub level: RDKafkaLogLevel,
    pub code: RDKafkaErrorCode,
    pub message: String,
    /// librdkafka's "planned" failures (idle close, request timeouts, client
    /// shutdown) do not count towards "all brokers are down".
    pub planned: bool,
}

impl Failure {
    pub(crate) fn error(code: RDKafkaErrorCode, message: impl Into<String>) -> Failure {
        Failure {
            level: RDKafkaLogLevel::Error,
            code,
            message: message.into(),
            planned: false,
        }
    }

    pub(crate) fn net_error(&self) -> NetError {
        NetError::new(self.code, self.message.clone())
    }
}

/// librdkafka's broker states, whose names appear in failure messages and
/// statistics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BrokerState {
    Init,
    Down,
    TryConnect,
    Connect,
    SslHandshake,
    AuthLegacy,
    Up,
    Update,
    ApiVersionQuery,
    AuthHandshake,
    AuthReq,
    Reauth,
}

impl BrokerState {
    pub(crate) fn name(self) -> &'static str {
        match self {
            BrokerState::Init => "INIT",
            BrokerState::Down => "DOWN",
            BrokerState::TryConnect => "TRY_CONNECT",
            BrokerState::Connect => "CONNECT",
            BrokerState::SslHandshake => "SSL_HANDSHAKE",
            BrokerState::AuthLegacy => "AUTH_LEGACY",
            BrokerState::Up => "UP",
            BrokerState::Update => "UPDATE",
            BrokerState::ApiVersionQuery => "APIVERSION_QUERY",
            BrokerState::AuthHandshake => "AUTH_HANDSHAKE",
            BrokerState::AuthReq => "AUTH_REQ",
            BrokerState::Reauth => "REAUTH",
        }
    }

    pub(crate) fn is_up(self) -> bool {
        matches!(
            self,
            BrokerState::Up | BrokerState::Update | BrokerState::Reauth
        )
    }

    fn to_u8(self) -> u8 {
        match self {
            BrokerState::Init => 0,
            BrokerState::Down => 1,
            BrokerState::TryConnect => 2,
            BrokerState::Connect => 3,
            BrokerState::SslHandshake => 4,
            BrokerState::AuthLegacy => 5,
            BrokerState::Up => 6,
            BrokerState::Update => 7,
            BrokerState::ApiVersionQuery => 8,
            BrokerState::AuthHandshake => 9,
            BrokerState::AuthReq => 10,
            BrokerState::Reauth => 11,
        }
    }

    fn from_u8(v: u8) -> BrokerState {
        match v {
            0 => BrokerState::Init,
            1 => BrokerState::Down,
            2 => BrokerState::TryConnect,
            3 => BrokerState::Connect,
            4 => BrokerState::SslHandshake,
            5 => BrokerState::AuthLegacy,
            6 => BrokerState::Up,
            7 => BrokerState::Update,
            8 => BrokerState::ApiVersionQuery,
            9 => BrokerState::AuthHandshake,
            10 => BrokerState::AuthReq,
            _ => BrokerState::Reauth,
        }
    }
}

/// A broker's state and the time it was entered, readable without locks.
#[derive(Debug)]
pub(crate) struct StateCell {
    state: std::sync::atomic::AtomicU8,
    since: Mutex<Instant>,
}

impl StateCell {
    pub(crate) fn new(state: BrokerState) -> StateCell {
        StateCell {
            state: std::sync::atomic::AtomicU8::new(state.to_u8()),
            since: Mutex::new(Instant::now()),
        }
    }

    pub(crate) fn get(&self) -> BrokerState {
        BrokerState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Sets the state, returning the previous one. Re-setting the current
    /// state does not reset its age.
    pub(crate) fn set(&self, state: BrokerState) -> BrokerState {
        let prev = BrokerState::from_u8(self.state.swap(state.to_u8(), Ordering::AcqRel));
        if prev != state {
            *self.since.lock().expect("lock poisoned") = Instant::now();
        }
        prev
    }

    /// Time since the current state was entered.
    pub(crate) fn age(&self) -> Duration {
        self.since.lock().expect("lock poisoned").elapsed()
    }
}

/// Per-broker counters for statistics. They survive reconnects.
#[derive(Debug, Default)]
pub(crate) struct BrokerCounters {
    /// Requests sent.
    pub tx: AtomicU64,
    pub tx_bytes: AtomicU64,
    /// Send errors.
    pub txerrs: AtomicU64,
    /// Requests retried by engines (engines increment this).
    pub txretries: AtomicU64,
    pub req_timeouts: AtomicU64,
    /// Responses received.
    pub rx: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub rxerrs: AtomicU64,
    /// Connection attempts.
    pub connects: AtomicI64,
    /// Disconnects of connections that had reached UP.
    pub disconnects: AtomicI64,
    /// Requests waiting to be sent.
    pub outbuf_cnt: AtomicI64,
    /// Messages in requests waiting to be sent.
    pub outbuf_msg_cnt: AtomicI64,
    /// Requests sent and awaiting a response.
    pub waitresp_cnt: AtomicI64,
    /// Messages in requests awaiting a response.
    pub waitresp_msg_cnt: AtomicI64,
    rtt: Mutex<RttWindow>,
}

/// Round-trip times of one statistics interval, in microseconds.
#[derive(Debug, Default)]
struct RttWindow {
    min: i64,
    max: i64,
    sum: i64,
    cnt: i64,
}

impl BrokerCounters {
    fn record_rtt(&self, rtt: Duration) {
        let us = i64::try_from(rtt.as_micros()).unwrap_or(i64::MAX);
        let mut w = self.rtt.lock().expect("lock poisoned");
        if w.cnt == 0 || us < w.min {
            w.min = us;
        }
        w.max = w.max.max(us);
        w.sum = w.sum.saturating_add(us);
        w.cnt += 1;
    }

    /// The round-trip times since the previous call, as a statistics window.
    pub(crate) fn take_rtt(&self) -> Window {
        let w = std::mem::take(&mut *self.rtt.lock().expect("lock poisoned"));
        let avg = if w.cnt > 0 { w.sum / w.cnt } else { 0 };
        Window {
            min: w.min,
            max: w.max,
            avg,
            sum: w.sum,
            cnt: w.cnt,
            p50: avg,
            p75: avg,
            p90: avg,
            p95: avg,
            p99: avg,
            p99_99: avg,
            ..Window::default()
        }
    }

    /// The average round-trip time of the current window, if any.
    fn avg_rtt(&self) -> Option<Duration> {
        let w = self.rtt.lock().expect("lock poisoned");
        (w.cnt > 0).then(|| Duration::from_micros(u64::try_from(w.sum / w.cnt).unwrap_or(0)))
    }
}

/// Options for one request.
#[derive(Clone, Debug)]
pub(crate) struct RequestOptions {
    /// Time from enqueueing until the response must have arrived. `None`
    /// means `socket.timeout.ms`. A timed out request fails with
    /// `OperationTimedOut` and tears down the connection, as in librdkafka.
    pub timeout: Option<Duration>,
    /// Number of messages the request carries, for the `outbuf_msg_cnt` and
    /// `waitresp_msg_cnt` statistics.
    pub msg_cnt: u64,
}

impl Default for RequestOptions {
    fn default() -> RequestOptions {
        RequestOptions {
            timeout: None,
            msg_cnt: 0,
        }
    }
}

impl RequestOptions {
    pub(crate) fn with_timeout(timeout: Duration) -> RequestOptions {
        RequestOptions {
            timeout: Some(timeout),
            ..RequestOptions::default()
        }
    }
}

/// The byte stream under a connection: TCP, possibly wrapped in TLS.
pub(crate) trait Io: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Io for T {}

pub(crate) type BoxedIo = Box<dyn Io>;

/// Settings a connection needs after setup.
#[derive(Clone)]
pub(crate) struct ConnectionParams {
    pub client_id: Arc<str>,
    pub socket_timeout: Duration,
    pub receive_message_max_bytes: usize,
    pub max_in_flight: usize,
    /// `connections.max.idle.ms`, already jittered.
    pub max_idle: Option<Duration>,
    pub log_connection_close: bool,
    /// Whether the stream is TLS, which changes the text of a peer close.
    pub tls: bool,
    /// For OpenSSL error texts and the `SSL` log lines that precede them.
    pub logger: Logger,
    pub broker_name: Arc<str>,
    /// The client's shutdown token, which closes the connection.
    pub shutdown: CancellationToken,
}

/// How a connection ended, for the owner's failure log.
#[derive(Debug)]
pub(crate) struct Closed {
    pub failure: Failure,
    /// For closes caused by request timeouts: the number of timed out
    /// requests that were in flight and that were still queued, for
    /// librdkafka's `REQTMOUT` warning.
    pub timeouts: Option<(usize, usize)>,
}

pub(crate) type CloseCallback = Box<dyn FnOnce(Closed) + Send>;

/// An established, authenticated connection with pipelined requests.
///
/// Requests are written in the order they are submitted. Each waits for its
/// response by correlation id. The connection closes on the first transport
/// error, on a request timeout, after `connections.max.idle.ms` without
/// activity, on [`Connection::close`], or when dropped; then all pending
/// requests fail and the close callback runs once.
pub(crate) struct Connection {
    shared: Arc<Shared>,
    tx: mpsc::UnboundedSender<Outgoing>,
    api_versions: ApiVersions,
    in_flight: Arc<Semaphore>,
    /// When the SASL session must be renewed (KIP-368), if it expires.
    reauth_at: Option<Instant>,
}

struct Shared {
    params: ConnectionParams,
    counters: Arc<BrokerCounters>,
    next_correlation_id: AtomicI32,
    state: Mutex<SharedState>,
    /// Cancelled when the connection closes.
    closed: CancellationToken,
    /// When the connection reached UP.
    up_since: Instant,
    /// Time of the last send, as an offset from `up_since` in microseconds.
    last_send_us: AtomicU64,
    /// Time of the last receive, as an offset from `up_since` in microseconds.
    last_recv_us: AtomicU64,
    /// Requests submitted and not yet answered (in the channel or in flight).
    outstanding: AtomicUsize,
    /// Wakes the reader to recompute its next timeout when a request is
    /// added.
    wake: tokio::sync::Notify,
    /// Set when a newer connection replaced this one.
    superseded: AtomicBool,
    /// Set when the last handle was dropped: close once nothing is
    /// outstanding.
    released: AtomicBool,
    /// The client's shutdown token: closes the connection.
    shutdown: CancellationToken,
}

struct SharedState {
    pending: BTreeMap<i32, Pending>,
    /// Set once; then `pending` is empty and stays empty.
    closed: Option<NetError>,
    on_close: Option<CloseCallback>,
}

struct Pending {
    api_key: ApiKey,
    deadline: Instant,
    sent_at: Option<Instant>,
    msg_cnt: u64,
    reply: Option<oneshot::Sender<Result<Bytes, NetError>>>,
    _permit: OwnedSemaphorePermit,
}

struct Outgoing {
    correlation_id: i32,
    frame: protocol::Frame,
    /// Requests that expect no response (acks=0 produce) are complete once
    /// written.
    expects_response: bool,
}

impl Connection {
    /// Starts the reader and writer tasks over an established stream.
    ///
    /// `on_close` runs once, from a connection task or from the thread that
    /// dropped the connection, when the connection closes.
    pub(crate) fn start(
        io: BoxedIo,
        api_versions: ApiVersions,
        params: ConnectionParams,
        counters: Arc<BrokerCounters>,
        reauth_at: Option<Instant>,
        on_close: CloseCallback,
    ) -> Connection {
        let max_in_flight = params.max_in_flight.clamp(1, Semaphore::MAX_PERMITS);
        let receive_max = params.receive_message_max_bytes;
        let shared = Arc::new(Shared {
            params: params.clone(),
            counters,
            next_correlation_id: AtomicI32::new(1),
            state: Mutex::new(SharedState {
                pending: BTreeMap::new(),
                closed: None,
                on_close: Some(on_close),
            }),
            closed: CancellationToken::new(),
            up_since: Instant::now(),
            last_send_us: AtomicU64::new(0),
            last_recv_us: AtomicU64::new(0),
            outstanding: AtomicUsize::new(0),
            wake: tokio::sync::Notify::new(),
            superseded: AtomicBool::new(false),
            released: AtomicBool::new(false),
            shutdown: params.shutdown.clone(),
        });
        let (tx, rx) = mpsc::unbounded_channel();
        let (read, write) = tokio::io::split(io);
        runtime::spawn(
            "kafka-conn-writer",
            write_loop(Arc::clone(&shared), write, rx),
        );
        runtime::spawn(
            "kafka-conn-reader",
            read_loop(
                Arc::clone(&shared),
                frame::FrameReader::new(read, receive_max),
            ),
        );
        Connection {
            shared,
            tx,
            api_versions,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
            reauth_at,
        }
    }

    /// The API versions negotiated with the broker.
    pub(crate) fn api_versions(&self) -> &ApiVersions {
        &self.api_versions
    }

    /// Reports whether the connection has closed.
    pub(crate) fn is_closed(&self) -> bool {
        self.shared.closed.is_cancelled()
    }

    /// Reports whether the SASL session is due for renewal: new requests
    /// should go to a new connection.
    pub(crate) fn needs_reauth(&self) -> bool {
        self.reauth_at.is_some_and(|at| Instant::now() >= at)
    }

    /// When the SASL session must be renewed (KIP-368), if it expires.
    pub(crate) fn reauth_at(&self) -> Option<Instant> {
        self.reauth_at
    }

    /// Marks the connection as replaced by a newer one, e.g. with a fresh
    /// SASL session. Requests already sent complete normally.
    pub(crate) fn supersede(&self) {
        self.shared.superseded.store(true, Ordering::Release);
    }

    /// Reports whether a newer connection replaced this one. Holders of a
    /// superseded connection should send no new requests on it and switch
    /// to [`Broker::connection`](crate::internal::cluster::Broker::connection)
    /// once their in-flight requests completed.
    pub(crate) fn is_superseded(&self) -> bool {
        self.shared.superseded.load(Ordering::Acquire)
    }

    /// Reports whether new requests should be sent on this connection: it is
    /// open and not superseded.
    pub(crate) fn is_usable(&self) -> bool {
        !self.is_closed() && !self.is_superseded()
    }

    /// A token cancelled when the connection closes.
    pub(crate) fn closed_token(&self) -> CancellationToken {
        self.shared.closed.clone()
    }

    /// Closes the connection, failing pending requests with `failure`.
    pub(crate) fn close(&self, failure: Failure) {
        self.shared.close(failure);
    }

    /// Sends a request and waits for its response.
    ///
    /// `build` receives the negotiated version and must only set fields
    /// valid for it. Fails with `UnsupportedFeature` if the broker supports
    /// no version of the API this client supports.
    pub(crate) async fn request<R, F>(
        &self,
        build: F,
        opts: &RequestOptions,
    ) -> Result<R::Response, NetError>
    where
        R: Request,
        F: FnOnce(i16) -> R,
    {
        let (version, frame) = self.send::<R, F>(build, opts, true).await?;
        let frame = frame.expect("response expected");
        protocol::decode_response::<R>(frame, version)
            .map_err(|e| NetError::new(RDKafkaErrorCode::BadMessage, e))
    }

    /// Queues a ProduceRequest for writing and returns a future of its
    /// response.
    ///
    /// Unlike concurrent [`Connection::request`] calls, whose requests may be
    /// written in any order, requests queued by consecutive calls are written
    /// in the order of the calls. The request is sent whether or not the
    /// returned future is polled. Its record batches are written from their
    /// own buffers instead of being copied into the request frame.
    pub(crate) async fn queue_produce<F>(
        &self,
        build: F,
        opts: &RequestOptions,
    ) -> Result<impl Future<Output = Result<ProduceResponse, NetError>> + Send + 'static, NetError>
    where
        F: FnOnce(i16) -> ProduceRequest,
    {
        let (version, reply) = self
            .enqueue::<ProduceRequest, F>(build, opts, true, protocol::encode_produce_request)
            .await?;
        Ok(async move {
            let frame = Self::reply(reply, true).await?.expect("response expected");
            protocol::decode_response::<ProduceRequest>(frame, version)
                .map_err(|e| NetError::new(RDKafkaErrorCode::BadMessage, e))
        })
    }

    /// Sends a ProduceRequest with `acks=0`, which has no response, and
    /// waits until it is written, like [`Connection::queue_produce`].
    pub(crate) async fn send_produce_oneway<F>(
        &self,
        build: F,
        opts: &RequestOptions,
    ) -> Result<(), NetError>
    where
        F: FnOnce(i16) -> ProduceRequest,
    {
        let (_, reply) = self
            .enqueue::<ProduceRequest, F>(build, opts, false, protocol::encode_produce_request)
            .await?;
        Self::reply(reply, false).await.map(|_| ())
    }

    async fn send<R, F>(
        &self,
        build: F,
        opts: &RequestOptions,
        expects_response: bool,
    ) -> Result<(i16, Option<Bytes>), NetError>
    where
        R: Request,
        F: FnOnce(i16) -> R,
    {
        let (version, reply) = self
            .enqueue::<R, F>(build, opts, expects_response, encode_frame::<R>)
            .await?;
        Ok((version, Self::reply(reply, expects_response).await?))
    }

    /// Waits for the reply of a queued request.
    async fn reply(
        reply: oneshot::Receiver<Result<Bytes, NetError>>,
        expects_response: bool,
    ) -> Result<Option<Bytes>, NetError> {
        match reply.await {
            Ok(Ok(frame)) if expects_response => Ok(Some(frame)),
            Ok(Ok(_)) => Ok(None),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(NetError::transport("Connection closed")),
        }
    }

    /// Queues a request for writing once an in-flight slot is free, encoded
    /// by `encode` from the request, version, correlation id and client id.
    /// Returns the negotiated version and the receiver of the reply.
    async fn enqueue<R, F>(
        &self,
        build: F,
        opts: &RequestOptions,
        expects_response: bool,
        encode: fn(&R, i16, i32, &str) -> Result<protocol::Frame, String>,
    ) -> Result<(i16, oneshot::Receiver<Result<Bytes, NetError>>), NetError>
    where
        R: Request,
        F: FnOnce(i16) -> R,
    {
        let api_key = ApiKey::try_from(R::KEY)
            .map_err(|_| NetError::new(RDKafkaErrorCode::InvalidArgument, "unknown API key"))?;
        let version = self.api_versions.version(api_key).ok_or_else(|| {
            NetError::new(
                RDKafkaErrorCode::UnsupportedFeature,
                format!("{api_key:?} not supported by broker"),
            )
        })?;
        let timeout = opts.timeout.unwrap_or(self.shared.params.socket_timeout);
        let deadline = Instant::now() + timeout;
        let request = build(version);
        let permit = match tokio::time::timeout_at(
            deadline.into(),
            Arc::clone(&self.in_flight).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => unreachable!("semaphore never closed"),
            Err(_) => {
                return Err(NetError::new(
                    RDKafkaErrorCode::TimedOutQueue,
                    RDKafkaErrorCode::TimedOutQueue.err2str(),
                ));
            }
        };
        let correlation_id = self
            .shared
            .next_correlation_id
            .fetch_add(1, Ordering::Relaxed);
        let frame = encode(
            &request,
            version,
            correlation_id,
            &self.shared.params.client_id,
        )
        .map_err(|e| NetError::new(RDKafkaErrorCode::InvalidArgument, e))?;
        drop(request);
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let mut state = self.shared.state.lock().expect("lock poisoned");
            if let Some(err) = &state.closed {
                return Err(err.clone());
            }
            state.pending.insert(
                correlation_id,
                Pending {
                    api_key,
                    deadline,
                    sent_at: None,
                    msg_cnt: opts.msg_cnt,
                    reply: Some(reply_tx),
                    _permit: permit,
                },
            );
            self.shared.outstanding.fetch_add(1, Ordering::AcqRel);
            // `notify_one` stores a permit if the reader is not waiting, so
            // the wakeup is not lost.
            self.shared.wake.notify_one();
            let c = &self.shared.counters;
            c.outbuf_cnt.fetch_add(1, Ordering::Relaxed);
            c.outbuf_msg_cnt
                .fetch_add(msg_i64(opts.msg_cnt), Ordering::Relaxed);
            // Sent under the lock so that a concurrent close either sees the
            // pending entry or the send fails.
            if self
                .tx
                .send(Outgoing {
                    correlation_id,
                    frame,
                    expects_response,
                })
                .is_err()
            {
                // The writer is gone; `close` has run or is about to.
                drop(state);
                self.shared.close(Failure::error(
                    RDKafkaErrorCode::BrokerTransportFailure,
                    "Connection closed",
                ));
            }
        }
        Ok((version, reply_rx))
    }
}

fn encode_frame<R: Request>(
    request: &R,
    version: i16,
    correlation_id: i32,
    client_id: &str,
) -> Result<protocol::Frame, String> {
    protocol::encode_request(request, version, correlation_id, client_id).map(protocol::Frame::from)
}

impl Drop for Connection {
    /// Closes the connection once the requests already submitted have
    /// completed (or timed out), so that dropping the last handle, e.g. of a
    /// connection superseded for re-authentication, loses no responses.
    fn drop(&mut self) {
        self.shared.released.store(true, Ordering::Release);
        self.shared.close_if_released();
    }
}

fn msg_i64(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

impl Shared {
    fn micros_since_up(&self) -> u64 {
        u64::try_from(self.up_since.elapsed().as_micros()).unwrap_or(u64::MAX)
    }

    /// Closes the connection once: fails pending requests, stops the tasks
    /// and runs the close callback.
    fn close(&self, failure: Failure) {
        self.close_inner(failure, None)
    }

    fn close_inner(&self, failure: Failure, timeouts: Option<(usize, usize)>) {
        let (pending, on_close) = {
            let mut state = self.state.lock().expect("lock poisoned");
            if state.closed.is_some() {
                return;
            }
            state.closed = Some(failure.net_error());
            (std::mem::take(&mut state.pending), state.on_close.take())
        };
        self.closed.cancel();
        let request_err = failure.net_error();
        for (_, p) in pending {
            self.finish(&p);
            // Requests not yet written fail like librdkafka's output queue:
            // with __TIMED_OUT_QUEUE when the cause was a timeout.
            let err = if p.sent_at.is_none()
                && request_err.code() == RDKafkaErrorCode::OperationTimedOut
            {
                NetError::new(
                    RDKafkaErrorCode::TimedOutQueue,
                    RDKafkaErrorCode::TimedOutQueue.err2str(),
                )
            } else {
                request_err.clone()
            };
            if let Some(reply) = p.reply {
                let _ = reply.send(Err(err));
            }
        }
        if let Some(on_close) = on_close {
            on_close(Closed { failure, timeouts });
        }
    }

    /// Closes a connection whose last handle was dropped once no request is
    /// outstanding.
    fn close_if_released(&self) {
        if self.released.load(Ordering::SeqCst) && self.outstanding.load(Ordering::SeqCst) == 0 {
            self.close(Failure {
                level: RDKafkaLogLevel::Debug,
                code: RDKafkaErrorCode::BrokerTransportFailure,
                message: "Connection closed by client".into(),
                planned: true,
            });
        }
    }

    /// Updates counters for a pending request that leaves the connection.
    fn finish(&self, p: &Pending) {
        self.outstanding.fetch_sub(1, Ordering::SeqCst);
        let c = &self.counters;
        if p.sent_at.is_some() {
            c.waitresp_cnt.fetch_sub(1, Ordering::Relaxed);
            c.waitresp_msg_cnt
                .fetch_sub(msg_i64(p.msg_cnt), Ordering::Relaxed);
        } else {
            c.outbuf_cnt.fetch_sub(1, Ordering::Relaxed);
            c.outbuf_msg_cnt
                .fetch_sub(msg_i64(p.msg_cnt), Ordering::Relaxed);
        }
        // The pending entry is already out of the map, so closing here does
        // not fail it.
        self.close_if_released();
    }

    /// The log level of a peer disconnect, like librdkafka's
    /// `rd_kafka_broker_conn_closed`: debug for idle connections (likely the
    /// broker's idle reaper), warning with more than one request in flight,
    /// else info.
    fn disconnect_level(&self) -> RDKafkaLogLevel {
        if !self.params.log_connection_close {
            return RDKafkaLogLevel::Debug;
        }
        let min_idle = self.params.socket_timeout.max(Duration::from_secs(60));
        let min_idle_us = u64::try_from(min_idle.as_micros()).unwrap_or(u64::MAX);
        let now = self.micros_since_up();
        let state = self.state.lock().expect("lock poisoned");
        let in_flight = state
            .pending
            .values()
            .filter(|p| p.sent_at.is_some())
            .count();
        let queued = state.pending.len() - in_flight;
        let last_send = self.last_send_us.load(Ordering::Relaxed);
        if now > min_idle_us && last_send + min_idle_us < now && in_flight + queued == 0 {
            RDKafkaLogLevel::Debug
        } else if in_flight > 1 {
            RDKafkaLogLevel::Warning
        } else {
            RDKafkaLogLevel::Info
        }
    }
}

async fn write_loop(
    shared: Arc<Shared>,
    mut write: WriteHalf<BoxedIo>,
    mut rx: mpsc::UnboundedReceiver<Outgoing>,
) {
    loop {
        let out = tokio::select! {
            biased;
            _ = shared.closed.cancelled() => break,
            out = rx.recv() => match out {
                Some(out) => out,
                // The last handle was dropped: keep the write half open
                // until the outstanding requests completed and the
                // connection closes, since a broker closes on EOF.
                None => {
                    shared.closed.cancelled().await;
                    break;
                }
            },
        };
        let mut frame = out.frame;
        let len = frame.remaining();
        let res = tokio::select! {
            biased;
            _ = shared.closed.cancelled() => break,
            res = write.write_all_buf(&mut frame) => res,
        };
        if let Err(e) = res {
            shared.counters.txerrs.fetch_add(1, Ordering::Relaxed);
            shared.close(Failure::error(
                RDKafkaErrorCode::BrokerTransportFailure,
                format!(
                    "Send failed: {}",
                    connect::send_error_text(
                        &e,
                        shared
                            .params
                            .tls
                            .then_some((&shared.params.logger, shared.params.broker_name.as_ref()))
                    )
                ),
            ));
            break;
        }
        let now = Instant::now();
        shared
            .last_send_us
            .store(shared.micros_since_up(), Ordering::Relaxed);
        let c = &shared.counters;
        c.tx.fetch_add(1, Ordering::Relaxed);
        c.tx_bytes.fetch_add(u64::cast_from(len), Ordering::Relaxed);
        let mut state = shared.state.lock().expect("lock poisoned");
        if out.expects_response {
            if let Some(p) = state.pending.get_mut(&out.correlation_id) {
                p.sent_at = Some(now);
                c.outbuf_cnt.fetch_sub(1, Ordering::Relaxed);
                c.outbuf_msg_cnt
                    .fetch_sub(msg_i64(p.msg_cnt), Ordering::Relaxed);
                c.waitresp_cnt.fetch_add(1, Ordering::Relaxed);
                c.waitresp_msg_cnt
                    .fetch_add(msg_i64(p.msg_cnt), Ordering::Relaxed);
            }
        } else if let Some(mut p) = state.pending.remove(&out.correlation_id) {
            drop(state);
            shared.finish(&p);
            if let Some(reply) = p.reply.take() {
                let _ = reply.send(Ok(Bytes::new()));
            }
        }
    }
    let _ = write.shutdown().await;
}

async fn read_loop(shared: Arc<Shared>, mut frames: frame::FrameReader<ReadHalf<BoxedIo>>) {
    loop {
        let wake_at = next_wakeup(&shared);
        let next = tokio::select! {
            biased;
            _ = shared.closed.cancelled() => return,
            _ = shared.shutdown.cancelled() => {
                shared.close(Failure {
                    level: RDKafkaLogLevel::Debug,
                    code: RDKafkaErrorCode::BrokerDestroy,
                    message: "Broker handle is terminating".into(),
                    planned: true,
                });
                return;
            }
            next = frames.next() => next,
            _ = sleep_until_opt(wake_at) => {
                check_timeouts(&shared);
                continue;
            }
            _ = shared.wake.notified() => continue,
        };
        match next {
            Some(Ok(frame)) => {
                let now = Instant::now();
                shared
                    .last_recv_us
                    .store(shared.micros_since_up(), Ordering::Relaxed);
                let c = &shared.counters;
                c.rx.fetch_add(1, Ordering::Relaxed);
                c.rx_bytes.fetch_add(
                    u64::cast_from(frame.len() + protocol::FRAME_LEN_SIZE),
                    Ordering::Relaxed,
                );
                let Some(correlation_id) = protocol::peek_correlation_id(&frame) else {
                    c.rxerrs.fetch_add(1, Ordering::Relaxed);
                    shared.close(Failure::error(
                        RDKafkaErrorCode::BadMessage,
                        format!("Receive failed: Invalid response size {} (0..{}): increase receive.message.max.bytes", frame.len(), shared.params.receive_message_max_bytes),
                    ));
                    return;
                };
                let pending = shared
                    .state
                    .lock()
                    .expect("lock poisoned")
                    .pending
                    .remove(&correlation_id);
                match pending {
                    Some(mut p) => {
                        shared.finish(&p);
                        if let Some(sent_at) = p.sent_at {
                            c.record_rtt(now.saturating_duration_since(sent_at));
                        }
                        if let Some(reply) = p.reply.take() {
                            let _ = reply.send(Ok(frame));
                        }
                    }
                    None => {
                        // librdkafka logs and drops responses for unknown
                        // correlation ids (`rxcorriderrs`), e.g. for requests
                        // that timed out.
                        tracing::debug!(
                            target: "librdkafka",
                            "response for unknown correlation id {correlation_id}"
                        );
                    }
                }
            }
            Some(Err(e)) => {
                shared.counters.rxerrs.fetch_add(1, Ordering::Relaxed);
                let failure = match e {
                    frame::FrameError::Io(e) => read_error_failure(&shared, &e),
                    frame::FrameError::Size(size) => Failure::error(
                        RDKafkaErrorCode::BadMessage,
                        format!(
                            "Receive failed: Invalid response size {size} (0..{}): \
                             increase receive.message.max.bytes",
                            shared.params.receive_message_max_bytes
                        ),
                    ),
                };
                shared.close(failure);
                return;
            }
            None => {
                // With TLS, librdkafka sees the close as SSL_ERROR_ZERO_RETURN
                // (it sets SSL_OP_IGNORE_UNEXPECTED_EOF).
                let text = if shared.params.tls {
                    "Disconnected: SSL connection closed by peer"
                } else {
                    "Disconnected: connection closed by peer: receive 0 after POLLIN"
                };
                let mut failure = Failure::error(RDKafkaErrorCode::BrokerTransportFailure, text);
                failure.level = shared.disconnect_level();
                shared.close(failure);
                return;
            }
        }
    }
}

/// The failure for a read error on an established connection.
fn read_error_failure(shared: &Shared, e: &std::io::Error) -> Failure {
    let params = &shared.params;
    let message = connect::read_error_text(
        e,
        params
            .tls
            .then_some((&params.logger, params.broker_name.as_ref())),
    );
    if message.starts_with("Disconnected") {
        let mut failure = Failure::error(RDKafkaErrorCode::BrokerTransportFailure, message);
        failure.level = shared.disconnect_level();
        failure
    } else {
        Failure::error(
            RDKafkaErrorCode::BrokerTransportFailure,
            format!("Receive failed: {message}"),
        )
    }
}

async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at.into()).await,
        None => std::future::pending().await,
    }
}

/// The earliest request deadline or idle-close time.
fn next_wakeup(shared: &Shared) -> Option<Instant> {
    let state = shared.state.lock().expect("lock poisoned");
    let deadline = state.pending.values().map(|p| p.deadline).min();
    let idle = if state.pending.is_empty() {
        shared.params.max_idle.map(|max_idle| {
            let last = shared
                .last_send_us
                .load(Ordering::Relaxed)
                .max(shared.last_recv_us.load(Ordering::Relaxed));
            shared.up_since + Duration::from_micros(last) + max_idle
        })
    } else {
        None
    };
    match (deadline, idle) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// Fails timed out requests and closes the connection, as librdkafka's
/// `rd_kafka_broker_timeout_scan` does with `socket.max.fails=1`; closes idle
/// connections after `connections.max.idle.ms`.
fn check_timeouts(shared: &Shared) {
    let now = Instant::now();
    let (timed_out, idle_ms) = {
        let mut state = shared.state.lock().expect("lock poisoned");
        let ids: Vec<i32> = state
            .pending
            .iter()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        let timed_out: Vec<Pending> = ids
            .into_iter()
            .filter_map(|id| state.pending.remove(&id))
            .collect();
        let idle_ms = if state.pending.is_empty() && timed_out.is_empty() {
            shared.params.max_idle.and_then(|max_idle| {
                let last = shared
                    .last_send_us
                    .load(Ordering::Relaxed)
                    .max(shared.last_recv_us.load(Ordering::Relaxed));
                let idle = shared
                    .up_since
                    .elapsed()
                    .saturating_sub(Duration::from_micros(last));
                (idle >= max_idle).then_some(idle.as_millis())
            })
        } else {
            None
        };
        (timed_out, idle_ms)
    };
    if let Some(idle_ms) = idle_ms {
        shared.close(Failure {
            level: RDKafkaLogLevel::Debug,
            code: RDKafkaErrorCode::BrokerTransportFailure,
            message: format!("Connection max idle time exceeded ({idle_ms}ms since last activity)"),
            planned: true,
        });
        return;
    }
    if timed_out.is_empty() {
        return;
    }
    let in_flight = timed_out.iter().filter(|p| p.sent_at.is_some()).count();
    let out_queue = timed_out.len() - in_flight;
    shared
        .counters
        .req_timeouts
        .fetch_add(u64::cast_from(timed_out.len()), Ordering::Relaxed);
    let mut logged = 0;
    for mut p in timed_out {
        shared.finish(&p);
        // librdkafka logs the first 5 timed out in-flight requests.
        if let Some(sent_at) = p.sent_at.filter(|_| logged < 5) {
            let name = &shared.params.broker_name;
            shared.params.logger.log_fmt(
                RDKafkaLogLevel::Notice,
                "REQTMOUT",
                format_args!(
                    "[thrd:{name}]: {name}: Timed out {}Request in flight (after {}ms, \
                     timeout #{logged})",
                    protocol::api_name(p.api_key),
                    now.saturating_duration_since(sent_at).as_millis()
                ),
            );
            logged += 1;
        }
        let err = if p.sent_at.is_some() {
            NetError::timed_out()
        } else {
            NetError::new(
                RDKafkaErrorCode::TimedOutQueue,
                RDKafkaErrorCode::TimedOutQueue.err2str(),
            )
        };
        if let Some(reply) = p.reply.take() {
            let _ = reply.send(Err(err));
        }
    }
    let rtt = shared
        .counters
        .avg_rtt()
        .map(|rtt| format!(" (average rtt {:.3}ms)", rtt.as_secs_f64() * 1000.0))
        .unwrap_or_default();
    shared.close_inner(
        Failure {
            level: RDKafkaLogLevel::Debug,
            code: RDKafkaErrorCode::OperationTimedOut,
            message: format!(
                "{} request(s) timed out: disconnect{rtt}",
                in_flight + out_queue
            ),
            planned: true,
        },
        Some((in_flight, out_queue)),
    );
}
