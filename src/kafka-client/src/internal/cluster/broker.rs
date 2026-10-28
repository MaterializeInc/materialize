// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! One broker: its lazily (re)connected connection, reconnect backoff,
//! state, failure logging and counters.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use kafka_protocol::messages::{MetadataRequest, MetadataResponse};
use kafka_protocol::protocol::Request;
use tokio::sync::watch;

use crate::config::RDKafkaLogLevel;
use crate::config::settings::DebugContext;
use crate::error::KafkaError;
use crate::internal::net::connect::{self, ConnectContext, Target};
use crate::internal::net::{
    BrokerCounters, BrokerState, Closed, Connection, Failure, NetError, RequestOptions, StateCell,
};
use crate::internal::runtime;
use crate::types::RDKafkaErrorCode;
use crate::util::Deadline;

use super::Cluster;
use super::bootstrap::{self, Backoff};

fn as_any<T: Any>(t: &T) -> &dyn Any {
    t
}

/// Where a broker came from, as in librdkafka's statistics (`source`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BrokerSource {
    /// From `bootstrap.servers`.
    Configured,
    /// From a Metadata or FindCoordinator response.
    Learned,
}

impl BrokerSource {
    pub(crate) fn name(self) -> &'static str {
        match self {
            BrokerSource::Configured => "configured",
            BrokerSource::Learned => "learned",
        }
    }
}

#[derive(Clone, Debug)]
struct Address {
    /// As configured or advertised; IPv6 hosts may carry brackets.
    host: String,
    port: u16,
    name: String,
}

#[derive(Default)]
struct ConnSlot {
    conn: Option<Arc<Connection>>,
    /// The generation of `conn`; close callbacks of other generations are
    /// for retired connections.
    generation: u64,
}

#[derive(Clone, Debug, Default)]
struct AttemptState {
    epoch: u64,
    running: bool,
    /// The outcome of attempt `epoch`, once it finished.
    result: Option<Result<(), NetError>>,
}

/// librdkafka's last-error record for duplicate suppression.
#[derive(Default)]
struct LastError {
    code: Option<RDKafkaErrorCode>,
    text: String,
    cnt: u32,
    /// `rd_interval` state of the 30 s suppression window.
    interval: Option<Instant>,
}

/// One broker of a client's cluster.
///
/// The connection is established on demand by [`Broker::request`] (and by
/// the cluster's lookups) and re-established after failures, waiting out
/// the reconnect backoff. One connection attempt runs at a time; concurrent
/// requests share it.
pub(crate) struct Broker {
    id: i32,
    source: BrokerSource,
    address: Mutex<Address>,
    state: StateCell,
    counters: Arc<BrokerCounters>,
    slot: Mutex<ConnSlot>,
    next_generation: AtomicUsize,
    attempt: watch::Sender<AttemptState>,
    backoff: Mutex<Backoff>,
    last_error: Mutex<LastError>,
    addr_cursor: AtomicUsize,
    /// Whether this broker currently counts as down for "all brokers down".
    down_reported: AtomicBool,
    /// Set when a connection attempt was made, for selecting never-tried
    /// brokers first.
    tried: AtomicBool,
    /// Retired brokers (bootstrap brokers once brokers are learned, learned
    /// brokers no longer in metadata) are not selected or counted.
    retired: AtomicBool,
    cluster: Weak<Cluster>,
    ctx: ConnectContext,
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker")
            .field("id", &self.id)
            .field("name", &self.name())
            .field("state", &self.state.get())
            .finish_non_exhaustive()
    }
}

impl Broker {
    pub(crate) fn new(
        id: Option<i32>,
        host: String,
        port: u16,
        cluster: Weak<Cluster>,
        ctx: ConnectContext,
    ) -> Broker {
        let settings = &ctx.settings;
        let name = bootstrap::broker_name(settings.security_protocol, &host, port, id);
        let (attempt, _) = watch::channel(AttemptState::default());
        Broker {
            id: id.unwrap_or(-1),
            source: if id.is_some() {
                BrokerSource::Learned
            } else {
                BrokerSource::Configured
            },
            address: Mutex::new(Address { host, port, name }),
            state: StateCell::new(BrokerState::Init),
            counters: Arc::new(BrokerCounters::default()),
            slot: Mutex::new(ConnSlot::default()),
            next_generation: AtomicUsize::new(1),
            attempt,
            backoff: Mutex::new(Backoff::new(
                settings.reconnect_backoff,
                settings.reconnect_backoff_max,
            )),
            last_error: Mutex::new(LastError::default()),
            addr_cursor: AtomicUsize::new(0),
            down_reported: AtomicBool::new(false),
            tried: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            cluster,
            ctx,
        }
    }

    /// The broker id; `-1` for bootstrap brokers.
    pub(crate) fn id(&self) -> i32 {
        self.id
    }

    pub(crate) fn source(&self) -> BrokerSource {
        self.source
    }

    /// librdkafka's broker name, e.g. `localhost:9092/1` or
    /// `sasl_ssl://b:9093/bootstrap`.
    pub(crate) fn name(&self) -> String {
        self.address.lock().expect("lock poisoned").name.clone()
    }

    /// `host:port` as configured or advertised.
    pub(crate) fn nodename(&self) -> String {
        let a = self.address.lock().expect("lock poisoned");
        bootstrap::nodename(&a.host, a.port)
    }

    pub(crate) fn state(&self) -> BrokerState {
        self.state.get()
    }

    pub(crate) fn state_age(&self) -> Duration {
        self.state.age()
    }

    /// Counters for statistics. Engines may increment `txretries` and read
    /// the others.
    pub(crate) fn counters(&self) -> &Arc<BrokerCounters> {
        &self.counters
    }

    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    pub(crate) fn was_tried(&self) -> bool {
        self.tried.load(Ordering::Acquire)
    }

    pub(crate) fn down_reported(&self) -> &AtomicBool {
        &self.down_reported
    }

    /// Reports whether a connection attempt is in progress or waiting out
    /// its backoff.
    pub(crate) fn is_connecting(&self) -> bool {
        self.attempt.borrow().running
    }

    /// The current connection if it is usable for new requests.
    fn usable_connection(self: &Arc<Self>) -> Option<Arc<Connection>> {
        let conn = {
            let slot = self.slot.lock().expect("lock poisoned");
            slot.conn.clone()
        }?;
        if conn.is_closed() {
            return None;
        }
        if conn.needs_reauth() {
            // KIP-368: the SASL session expires soon. Connect anew in the
            // background; the old connection keeps serving until then, and
            // closes once its in-flight requests complete.
            self.start_attempt();
        }
        Some(conn)
    }

    /// Reports whether the broker has a usable connection.
    pub(crate) fn is_up(self: &Arc<Self>) -> bool {
        self.state.get().is_up() && self.usable_connection().is_some()
    }

    /// Starts a connection attempt in the background unless one is running.
    /// Returns the attempt's epoch.
    pub(crate) fn start_attempt(self: &Arc<Self>) -> u64 {
        let mut spawn = false;
        let mut epoch = 0;
        self.attempt.send_if_modified(|s| {
            if !s.running {
                s.epoch += 1;
                s.running = true;
                s.result = None;
                spawn = true;
            }
            epoch = s.epoch;
            spawn
        });
        if spawn {
            runtime::spawn("kafka-broker-connect", Arc::clone(self).run_attempt(epoch));
        }
        epoch
    }

    /// Waits until the broker has a usable connection, starting connection
    /// attempts as needed. Fails with the error of a failed attempt that
    /// started after this call, or with `TimedOutQueue` at the deadline.
    pub(crate) async fn connection(
        self: &Arc<Self>,
        deadline: Deadline,
    ) -> Result<Arc<Connection>, NetError> {
        let mut rx = self.attempt.subscribe();
        loop {
            if let Some(conn) = self.usable_connection() {
                return Ok(conn);
            }
            if self.is_retired() && self.source == BrokerSource::Learned {
                return Err(NetError::new(
                    RDKafkaErrorCode::DestroyBroker,
                    RDKafkaErrorCode::DestroyBroker.err2str(),
                ));
            }
            let epoch = self.start_attempt();
            let wait = rx.wait_for(|s| !s.running && s.epoch >= epoch);
            let state = match deadline.instant() {
                Some(at) => match tokio::time::timeout_at(at.into(), wait).await {
                    Ok(r) => r.map(|s| s.clone()),
                    Err(_) => {
                        return Err(NetError::new(
                            RDKafkaErrorCode::TimedOutQueue,
                            RDKafkaErrorCode::TimedOutQueue.err2str(),
                        ));
                    }
                },
                None => wait.await.map(|s| s.clone()),
            };
            match state {
                Ok(AttemptState {
                    result: Some(Err(e)),
                    ..
                }) => return Err(e),
                Ok(_) => continue,
                Err(_) => unreachable!("sender owned by self"),
            }
        }
    }

    /// Sends a request to this broker and waits for the response, connecting
    /// first if needed.
    ///
    /// `build` receives the version negotiated with the broker and must only
    /// set fields valid in it. `opts.timeout` (default `socket.timeout.ms`)
    /// bounds connecting and the request together; a request that times out
    /// after it was sent fails with `OperationTimedOut` and closes the
    /// connection, as in librdkafka.
    ///
    /// Errors have codes `BrokerTransportFailure` (connection failed or
    /// closed; retriable), `OperationTimedOut`, `TimedOutQueue` (not sent in
    /// time; retriable), `UnsupportedFeature` (no common API version),
    /// `BadMessage` (undecodable response), `DestroyBroker` (the broker was
    /// removed from the cluster; look it up again), `BrokerDestroy` (the
    /// client is shutting down), or a connection setup error (`Resolve`,
    /// `SSL`, `Authentication`). Broker error codes inside the response are
    /// the caller's to interpret.
    ///
    /// Metadata responses also update the cluster's metadata cache.
    pub(crate) async fn request<R, F>(
        self: &Arc<Self>,
        build: F,
        opts: RequestOptions,
    ) -> Result<R::Response, NetError>
    where
        R: Request + Any,
        R::Response: Any,
        F: FnOnce(i16) -> R,
    {
        let timeout = opts.timeout.unwrap_or(self.ctx.settings.socket_timeout);
        let deadline = Deadline::new(timeout);
        let conn = self.connection(deadline).await?;
        let mut opts = opts;
        opts.timeout = Some(deadline.remaining_duration().unwrap_or(timeout));
        let mut all_topics = None;
        let response = conn
            .request::<R, _>(
                |version| {
                    let request = build(version);
                    if let Some(md) = as_any(&request).downcast_ref::<MetadataRequest>() {
                        all_topics = Some(match &md.topics {
                            None => true,
                            Some(topics) => version == 0 && topics.is_empty(),
                        });
                    }
                    request
                },
                &opts,
            )
            .await?;
        if let (Some(all_topics), Some(md)) = (
            all_topics,
            as_any(&response).downcast_ref::<MetadataResponse>(),
        ) {
            if let Some(cluster) = self.cluster.upgrade() {
                cluster.apply_metadata(md, all_topics);
            }
        }
        Ok(response)
    }

    /// Sends a request that has no response (Produce with `acks=0`); returns
    /// once it is written.
    pub(crate) async fn send_oneway<R, F>(
        self: &Arc<Self>,
        build: F,
        opts: RequestOptions,
    ) -> Result<(), NetError>
    where
        R: Request,
        F: FnOnce(i16) -> R,
    {
        let timeout = opts.timeout.unwrap_or(self.ctx.settings.socket_timeout);
        let deadline = Deadline::new(timeout);
        let conn = self.connection(deadline).await?;
        let mut opts = opts;
        opts.timeout = Some(deadline.remaining_duration().unwrap_or(timeout));
        conn.send_oneway::<R, F>(build, &opts).await
    }

    /// The API version that requests of `R` would use on the current
    /// connection, if connected.
    pub(crate) fn api_version(&self, key: kafka_protocol::messages::ApiKey) -> Option<i16> {
        let slot = self.slot.lock().expect("lock poisoned");
        slot.conn
            .as_ref()
            .and_then(|c| c.api_versions().version(key))
    }

    async fn run_attempt(self: Arc<Self>, epoch: u64) {
        let result = self.attempt_inner().await;
        self.attempt.send_modify(|s| {
            debug_assert_eq!(s.epoch, epoch);
            s.running = false;
            s.result = Some(result);
        });
        if let Some(cluster) = self.cluster.upgrade() {
            cluster.notify_state_change();
        }
    }

    async fn attempt_inner(self: &Arc<Self>) -> Result<(), NetError> {
        let destroyed = || {
            NetError::new(
                RDKafkaErrorCode::BrokerDestroy,
                RDKafkaErrorCode::BrokerDestroy.err2str(),
            )
        };
        let Some(shutdown) = self.cluster.upgrade().map(|c| c.shutdown_token()) else {
            return Err(destroyed());
        };
        let next = self.backoff.lock().expect("lock poisoned").next_attempt();
        if let Some(at) = next {
            if self.state.get() == BrokerState::Init {
                self.state.set(BrokerState::Down);
            }
            tokio::select! {
                _ = shutdown.cancelled() => return Err(destroyed()),
                _ = tokio::time::sleep_until(at.into()) => {}
            }
        }
        self.backoff
            .lock()
            .expect("lock poisoned")
            .on_attempt(Instant::now());
        self.counters.connects.fetch_add(1, Ordering::Relaxed);
        self.tried.store(true, Ordering::Release);

        // A connection that is still open is being replaced for SASL
        // re-authentication: its setup states must not disturb the broker's.
        let reauth = {
            let slot = self.slot.lock().expect("lock poisoned");
            slot.conn.as_ref().is_some_and(|c| !c.is_closed())
        };
        let reauth_state = StateCell::new(BrokerState::Connect);
        let state = if reauth { &reauth_state } else { &self.state };
        if !reauth {
            self.set_state(BrokerState::TryConnect, false);
        }
        let generation =
            u64::try_from(self.next_generation.fetch_add(1, Ordering::Relaxed)).expect("fits");
        let weak = Arc::downgrade(self);
        let on_close = Box::new(move |closed: Closed| {
            if let Some(broker) = weak.upgrade() {
                broker.on_connection_closed(generation, closed);
            }
        });
        let address = self.address.lock().expect("lock poisoned").clone();
        let target = Target {
            host: bootstrap::bare_host(&address.host),
            port: address.port,
            name: &address.name,
        };
        let result = tokio::select! {
            _ = shutdown.cancelled() => return Err(destroyed()),
            r = connect::establish(
                &self.ctx,
                &target,
                &self.addr_cursor,
                state,
                Arc::clone(&self.counters),
                on_close,
            ) => r,
        };
        match result {
            Ok(conn) => {
                let old = {
                    let mut slot = self.slot.lock().expect("lock poisoned");
                    slot.generation = generation;
                    slot.conn.replace(Arc::new(conn))
                };
                // A replaced connection closes once its in-flight requests
                // complete and their handles drop.
                drop(old);
                self.set_state(BrokerState::Up, false);
                Ok(())
            }
            Err(failure) => {
                if reauth {
                    self.log_failure(&failure, &reauth_state);
                } else {
                    self.fail(&failure);
                }
                Err(failure.net_error())
            }
        }
    }

    /// Handles the close of a connection of `generation`.
    fn on_connection_closed(self: &Arc<Self>, generation: u64, closed: Closed) {
        let current = {
            let mut slot = self.slot.lock().expect("lock poisoned");
            if slot.generation == generation {
                slot.conn.take()
            } else {
                None
            }
        };
        let was_current = current.is_some();
        // Drop outside the lock; this may run the connection's destructor.
        drop(current);
        if !was_current {
            self.ctx.logger.debug(
                DebugContext::Broker,
                "CLOSE",
                format_args!(
                    "[thrd:{0}]: {0}: Retired connection closed: {1}",
                    self.name(),
                    closed.failure.message
                ),
            );
            return;
        }
        if let Some((in_flight, out_queue)) = closed.timeouts {
            let name = self.name();
            self.ctx.logger.log_fmt(
                RDKafkaLogLevel::Warning,
                "REQTMOUT",
                format_args!(
                    "[thrd:{name}]: {name}: Timed out {in_flight} in-flight, 0 retry-queued, \
                     {out_queue} out-queue, 0 partially-sent requests"
                ),
            );
        }
        if self.state.get().is_up() {
            self.counters.disconnects.fetch_add(1, Ordering::Relaxed);
        }
        self.fail(&closed.failure);
        if let Some(cluster) = self.cluster.upgrade() {
            cluster.notify_state_change();
        }
    }

    /// Closes the current connection, e.g. at shutdown or when the broker is
    /// retired. Pending requests fail with `failure`'s code.
    pub(crate) fn close_connection(&self, failure: Failure) {
        // The close callback removes the connection and records the failure.
        let conn = self.slot.lock().expect("lock poisoned").conn.clone();
        if let Some(conn) = conn {
            conn.close(failure);
        }
    }

    /// Drops the broker's reference to its connection, which then closes
    /// once in-flight requests complete. Marks the broker retired.
    pub(crate) fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        self.down_reported.store(false, Ordering::Release);
        let conn = self.slot.lock().expect("lock poisoned").conn.take();
        drop(conn);
        self.state.set(BrokerState::Init);
    }

    pub(crate) fn unretire(&self) {
        self.retired.store(false, Ordering::Release);
    }

    /// Updates the advertised address. A change closes the connection and
    /// lets the next attempt start immediately, as librdkafka does on a
    /// nodename change.
    pub(crate) fn set_address(&self, host: &str, port: u16) {
        let changed = {
            let mut a = self.address.lock().expect("lock poisoned");
            if a.host == host && a.port == port {
                false
            } else {
                a.host = host.to_string();
                a.port = port;
                a.name = bootstrap::broker_name(
                    self.ctx.settings.security_protocol,
                    host,
                    port,
                    Some(self.id),
                );
                true
            }
        };
        if changed {
            self.backoff.lock().expect("lock poisoned").expedite();
            self.close_connection(Failure {
                level: RDKafkaLogLevel::Debug,
                code: RDKafkaErrorCode::BrokerTransportFailure,
                message: "Closing connection due to nodename change".into(),
                planned: true,
            });
        }
    }

    /// Changes the state, maintaining the cluster's "all brokers down"
    /// accounting. Planned failures do not count as down.
    fn set_state(self: &Arc<Self>, state: BrokerState, planned: bool) {
        let prev = self.state.set(state);
        if prev == state {
            return;
        }
        let Some(cluster) = self.cluster.upgrade() else {
            return;
        };
        if state == BrokerState::Down && !planned && !self.is_retired() {
            if !self.down_reported.swap(true, Ordering::AcqRel) {
                cluster.on_broker_down();
            }
        } else if state.is_up() && !prev.is_up() {
            self.down_reported.store(false, Ordering::Release);
            cluster.reset_down_reported();
        }
        cluster.notify_state_change();
    }

    /// librdkafka's `rd_kafka_broker_fail`: logs the failure and sets the
    /// state to DOWN.
    fn fail(self: &Arc<Self>, failure: &Failure) {
        self.log_failure(failure, &self.state);
        self.set_state(BrokerState::Down, failure.planned);
    }

    /// librdkafka's `rd_kafka_broker_set_error`: the `FAIL` log line with the
    /// time spent in the current state, 30 s suppression of identical
    /// errors, and an error callback for errors.
    fn log_failure(&self, failure: &Failure, state: &StateCell) {
        let name = self.name();
        let errstr = failure.message.as_str();
        let state_ms = state.age().as_millis();
        let state_name = state.get().name();
        let (full, identical, suppress) = {
            let mut last = self.last_error.lock().expect("lock poisoned");
            let identical = last.code == Some(failure.code) && last.text == errstr;
            let now = Instant::now();
            let suppress = identical
                && match last.interval {
                    Some(at) if now.duration_since(at) < Duration::from_secs(30) => true,
                    _ => {
                        last.interval = Some(now);
                        false
                    }
                };
            last.code = Some(failure.code);
            last.text = errstr.to_string();
            let mut extra = format!("after {state_ms}ms in state {state_name}");
            if identical && !suppress && last.cnt >= 1 {
                extra.push_str(&format!(", {} identical error(s) suppressed", last.cnt));
            }
            if suppress {
                last.cnt += 1;
            } else {
                last.cnt = 1;
            }
            (format!("{errstr} ({extra})"), identical, suppress)
        };
        let logger = &self.ctx.logger;
        logger.debug(
            DebugContext::Broker,
            "FAIL",
            format_args!(
                "[thrd:{name}]: {name}: {full} ({}){}{}",
                failure.code.name(),
                if identical {
                    ": identical to last error"
                } else {
                    ""
                },
                if suppress {
                    ": error log suppressed"
                } else {
                    ""
                },
            ),
        );
        let level = failure.level.severity();
        if level <= RDKafkaLogLevel::Critical.severity()
            || (level <= RDKafkaLogLevel::Info.severity() && !suppress)
        {
            logger.log_fmt(
                failure.level,
                "FAIL",
                format_args!("[thrd:{name}]: {name}: {full}"),
            );
            if level <= RDKafkaLogLevel::Error.severity() {
                if let Some(cluster) = self.cluster.upgrade() {
                    cluster
                        .events()
                        .error(KafkaError::Global(failure.code), &format!("{name}: {full}"));
                }
            }
        }
    }
}
