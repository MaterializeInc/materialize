// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Push delivery for the MCP Events draft at revision 28ec35e:
//! <https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/28ec35e905daa241f019981e2836b4a02f1c0368/docs/design-sketch-proposal.md>.
//!
//! Each `events/stream` request owns one SSE response. [`start`] runs an event's producer in a
//! task that owns an [`EventStream`]. The stream frames notifications, bounds their size and
//! delivery time, emits heartbeats, and ends the response with `terminated` followed by the
//! JSON-RPC result.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use futures::FutureExt;
use futures::future::BoxFuture;
use mz_repr::role_id::RoleId;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, Interval, timeout};
use uuid::Uuid;

use super::events_protocol::{
    Control, EventParams, ListResult, Notification, RequestId, StreamParams,
};
use super::{EventsCompleteResult, McpEndpointConfig, McpError, McpResponse, McpResult};
use crate::http::mcp_metrics::McpMetrics;
use crate::http::{AuthLifetime, AuthedClient};

mod subscribe;

/// Lifetime of a stream whose request names none.
const DEFAULT_TTL: Duration = Duration::from_secs(60 * 60);
/// Upper bound on any stream lifetime, regardless of `mcp_events_max_lifetime`.
const MAX_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_CONTROL_SIZE: usize = 64 * 1024;
/// How long one notification may wait for the client before the stream ends as a slow consumer.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) fn list(config: &McpEndpointConfig) -> ListResult {
    ListResult {
        events: config
            .subscribe_enabled
            .then(subscribe::definition)
            .into_iter()
            .collect(),
    }
}

pub(super) async fn stream(
    client: AuthedClient,
    id: RequestId,
    params: Value,
    config: &McpEndpointConfig,
    state: SubscriptionState,
    metrics: McpMetrics,
    permit: SubscriptionPermit,
) -> Result<StartedSubscription, McpError> {
    let params = StreamParams::parse(params)?;
    let start = Start {
        id,
        auth: client.lifetime.clone(),
        config,
        state,
        metrics,
        permit,
    };
    match params.name.as_str() {
        "subscribe" if config.subscribe_enabled => subscribe::stream(client, params, start).await,
        _ => Err(McpError {
            code: -32011,
            message: "unknown event name".into(),
            data: None,
        }),
    }
}

/// Admission state shared by every MCP listener in one environmentd process.
#[derive(Clone, Debug, Default)]
pub struct SubscriptionRegistry(Arc<Mutex<BTreeMap<RoleId, usize>>>);

pub(super) struct SubscriptionPermit {
    registry: SubscriptionRegistry,
    role: RoleId,
}

impl SubscriptionRegistry {
    pub(super) fn acquire(
        &self,
        role: RoleId,
        max_per_role: usize,
        max_concurrent: usize,
    ) -> Result<SubscriptionPermit, &'static str> {
        let mut counts = self.0.lock().expect("subscription registry poisoned");
        if counts.values().sum::<usize>() >= max_concurrent {
            return Err("mcp_events_max_concurrent");
        }
        let count = counts.get(&role).copied().unwrap_or(0);
        if count >= max_per_role {
            return Err("mcp_events_max_per_role");
        }
        counts.insert(role, count + 1);
        Ok(SubscriptionPermit {
            registry: self.clone(),
            role,
        })
    }
}

impl Drop for SubscriptionPermit {
    fn drop(&mut self) {
        let mut counts = self
            .registry
            .0
            .lock()
            .expect("subscription registry poisoned");
        let count = counts
            .get_mut(&self.role)
            .expect("permit owns a registry entry");
        *count -= 1;
        if *count == 0 {
            counts.remove(&self.role);
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub(super) enum StreamEnd {
    #[error("completed")]
    Completed,
    #[error("ttl expired")]
    Expired,
    #[error("authentication expired")]
    AuthenticationExpired,
    #[error("{0}", mz_adapter::AdapterError::Canceled)]
    Cancelled,
    #[error("slow consumer")]
    SlowConsumer,
    #[error("{message}")]
    Failed { code: i32, message: String },
}

impl StreamEnd {
    fn metric_reason(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Expired => "expired",
            Self::AuthenticationExpired => "auth_expired",
            Self::Cancelled => "cancelled",
            Self::SlowConsumer => "slow_consumer",
            Self::Failed { .. } => "error",
        }
    }

    fn error(&self) -> McpError {
        McpError {
            code: match self {
                Self::AuthenticationExpired => -32012,
                Self::SlowConsumer => -32013,
                Self::Expired => -32000,
                Self::Failed { code, .. } => *code,
                Self::Completed | Self::Cancelled => -32603,
            },
            message: self.to_string().chars().take(1024).collect(),
            data: Some(json!({"reason": self.metric_reason()})),
        }
    }

    /// Whether the stream delivered everything it owed. Expiry ends a stream as requested.
    pub(super) fn is_success(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Expired | Self::AuthenticationExpired
        )
    }

    pub(super) fn oversized(message: &str) -> Self {
        Self::Failed {
            code: -32013,
            message: message.into(),
        }
    }
}

impl From<serde_json::Error> for StreamEnd {
    fn from(error: serde_json::Error) -> Self {
        Self::Failed {
            code: -32603,
            message: error.to_string(),
        }
    }
}

/// Shared with the request timeout so aborting startup retains its failure cause.
#[derive(Clone, Default)]
pub(super) struct SubscriptionState(Arc<Mutex<Option<StreamEnd>>>);

impl SubscriptionState {
    fn finish(&self, outcome: StreamEnd) {
        let mut current = self.0.lock().expect("subscription state lock poisoned");
        // A producer can fail after a successful stream, for example while committing its
        // transaction. Preserve a failure that already stopped the stream, including a request
        // timeout recorded before task abort.
        if current.as_ref().is_none_or(StreamEnd::is_success) {
            *current = Some(outcome);
        }
    }

    fn outcome(&self) -> StreamEnd {
        self.0
            .lock()
            .expect("subscription state lock poisoned")
            .clone()
            .unwrap_or(StreamEnd::Cancelled)
    }

    pub(super) fn startup_failed(&self, message: String) {
        self.finish(StreamEnd::Failed {
            code: -32000,
            message,
        });
    }
}

struct MetricsGuard {
    metrics: McpMetrics,
    active: bool,
    state: SubscriptionState,
}

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        if self.active {
            self.metrics.active_subscriptions.dec();
        }
        self.metrics
            .subscription_ends
            .with_label_values(&[self.state.outcome().metric_reason()])
            .inc();
    }
}

struct Lifetime {
    auth_expired: BoxFuture<'static, ()>,
    expires: Option<Instant>,
}

impl Lifetime {
    fn unlimited() -> Self {
        Self {
            auth_expired: Box::pin(futures::future::pending()),
            expires: None,
        }
    }

    fn check(&mut self) -> Result<(), StreamEnd> {
        if self.auth_expired.as_mut().now_or_never().is_some() {
            return Err(StreamEnd::AuthenticationExpired);
        }
        if self
            .expires
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(StreamEnd::Expired);
        }
        Ok(())
    }

    async fn ended(&mut self) -> StreamEnd {
        tokio::select! {
            biased;
            _ = &mut self.auth_expired => StreamEnd::AuthenticationExpired,
            _ = async {
                match self.expires {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => futures::future::pending().await,
                }
            } => StreamEnd::Expired,
        }
    }
}

pub(super) struct StartedSubscription {
    rx: mpsc::Receiver<Event>,
    task: mz_ore::task::AbortOnDropHandle<()>,
}

impl StartedSubscription {
    async fn wait_for_startup(
        self,
        startup: oneshot::Receiver<Result<(), McpError>>,
        state: &SubscriptionState,
    ) -> Result<Self, McpError> {
        match startup.await {
            Ok(Ok(())) => Ok(self),
            Ok(Err(error)) => Err(error),
            Err(_) => {
                state.startup_failed("subscription startup failed".into());
                Err(McpError {
                    code: -32603,
                    message: "subscription startup failed".into(),
                    data: None,
                })
            }
        }
    }
}

impl IntoResponse for StartedSubscription {
    fn into_response(self) -> Response {
        let body = futures::stream::unfold((self.rx, self.task), |(mut rx, task)| async move {
            rx.recv()
                .await
                .map(|event| (Ok::<_, Infallible>(event), (rx, task)))
        });
        Sse::new(body).into_response()
    }
}

/// Request-scoped resources an event hands to [`start`].
pub(super) struct Start<'a> {
    id: RequestId,
    auth: AuthLifetime,
    config: &'a McpEndpointConfig,
    state: SubscriptionState,
    metrics: McpMetrics,
    permit: SubscriptionPermit,
}

/// Runs `produce` in a task that owns the stream, and returns once the stream activates or fails
/// to start.
///
/// The task ends the response with `terminated` and the JSON-RPC result if the stream activated.
/// Otherwise the outcome becomes the `events/stream` error response.
pub(super) async fn start<C, F, Fut>(
    start: Start<'_>,
    name: &'static str,
    ttl: Option<Duration>,
    produce: F,
) -> Result<StartedSubscription, McpError>
where
    C: Serialize + Ord + Clone + Send + Sync + 'static,
    F: FnOnce(EventStream<C>) -> Fut,
    Fut: Future<Output = EventStream<C>> + Send + 'static,
{
    let Start {
        id,
        auth,
        config,
        state,
        metrics,
        permit,
    } = start;
    let ttl = ttl.unwrap_or(DEFAULT_TTL);
    if ttl.is_zero() || ttl > config.events_max_lifetime.min(MAX_TTL) {
        return Err(McpError {
            code: -32602,
            message: "ttlMs must be positive and within the configured maximum".into(),
            data: None,
        });
    }
    let (tx, rx) = mpsc::channel(16);
    let (startup_tx, startup_rx) = oneshot::channel();
    let response_id = id.clone().into_value();
    let stream = EventStream {
        tx,
        name,
        subscription_id: Uuid::new_v4().to_string(),
        frame_sequence: 0,
        event_sequence: 0,
        max_response_size: config.max_response_size,
        cursor: None,
        request_id: id,
        startup: Some(startup_tx),
        heartbeat_interval: config
            .events_heartbeat_interval
            .min(Duration::from_secs(30))
            .max(Duration::from_millis(1)),
        heartbeats: None,
        ttl,
        lifetime: Lifetime {
            auth_expired: Box::pin(auth.expired()),
            expires: None,
        },
        guard: MetricsGuard {
            metrics,
            active: false,
            state: state.clone(),
        },
    };
    let produce = produce(stream);
    let task = mz_ore::task::spawn(|| "mcp-events-stream", async move {
        let _permit = permit;
        let mut stream = produce.await;
        let outcome = stream.guard.state.outcome();
        if let Some(startup) = stream.startup.take() {
            let mut error = outcome.error();
            if error.code == -32603 {
                error.code = -32000;
            }
            error.data = None;
            let _ = startup.send(Err(error));
        }
        if stream.guard.active {
            stream.finish(&outcome, response_id).await;
        }
    })
    .abort_on_drop();
    StartedSubscription { rx, task }
        .wait_for_startup(startup_rx, &state)
        .await
}

/// The SSE side of one subscription, owned by its producer.
///
/// `C` is the event's resume cursor. Every notification carries the latest cursor, which promises
/// that the client has received everything before it.
pub(super) struct EventStream<C> {
    tx: mpsc::Sender<Event>,
    name: &'static str,
    subscription_id: String,
    frame_sequence: u64,
    event_sequence: u64,
    max_response_size: usize,
    cursor: Option<C>,
    request_id: RequestId,
    /// Carries the JSON-RPC error object for a subscription that fails before activation.
    startup: Option<oneshot::Sender<Result<(), McpError>>>,
    heartbeat_interval: Duration,
    heartbeats: Option<Interval>,
    ttl: Duration,
    lifetime: Lifetime,
    guard: MetricsGuard,
}

impl<C: Serialize + Ord + Clone + Send + Sync> EventStream<C> {
    /// Sends `active`, answers the `events/stream` request with the SSE response, and starts the
    /// TTL and heartbeats.
    pub async fn activate(&mut self, cursor: Option<C>) -> Result<(), StreamEnd> {
        self.cursor = cursor;
        self.control(Control::Active { truncated: false }).await?;
        self.guard.active = true;
        self.guard.metrics.active_subscriptions.inc();
        if let Some(startup) = self.startup.take() {
            let _ = startup.send(Ok(()));
        }
        self.lifetime.expires = Some(Instant::now() + self.ttl);
        let mut heartbeats = tokio::time::interval_at(
            Instant::now() + self.heartbeat_interval,
            self.heartbeat_interval,
        );
        heartbeats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        self.heartbeats = Some(heartbeats);
        Ok(())
    }

    /// Fails the `events/stream` request with `error` instead of activating.
    pub fn reject(&mut self, error: McpError) {
        self.record(StreamEnd::Failed {
            code: error.code,
            message: error.message.clone(),
        });
        if let Some(startup) = self.startup.take() {
            let _ = startup.send(Err(error));
        }
    }

    /// Records `outcome` unless an earlier failure already ended the stream, and returns the
    /// outcome that stands.
    pub fn record(&self, outcome: StreamEnd) -> StreamEnd {
        self.guard.state.finish(outcome);
        self.guard.state.outcome()
    }

    /// Awaits `input` while sending heartbeats. Ends early when the client disconnects or the
    /// stream's lifetime ends. `input` must be cancel safe.
    pub async fn next<T>(&mut self, input: impl Future<Output = T>) -> Result<T, StreamEnd> {
        let mut input = std::pin::pin!(input);
        loop {
            let heartbeats = self.heartbeats.as_mut().expect("stream is active");
            tokio::select! {
                biased;
                _ = self.tx.closed() => return Err(StreamEnd::Cancelled),
                outcome = self.lifetime.ended() => return Err(outcome),
                _ = heartbeats.tick() => {},
                value = &mut input => return Ok(value),
            }
            self.control(Control::Heartbeat {}).await?;
        }
    }

    /// Fails if the stream's lifetime has ended. Producers call this between notifications they
    /// can send without awaiting [`EventStream::next`].
    pub fn check(&mut self) -> Result<(), StreamEnd> {
        self.lifetime.check()
    }

    /// Resolves once the client disconnects.
    pub fn closed(&self) -> impl Future<Output = ()> + Send + 'static {
        let tx = self.tx.clone();
        async move { tx.closed().await }
    }

    /// Raises the cursor without an event, for progress that delivered nothing.
    pub fn advance(&mut self, cursor: C) {
        self.cursor = self.cursor.clone().max(Some(cursor));
    }

    /// Sends one event. A `cursor` raises the stream's cursor only once the event is queued, so a
    /// failed send never advances it.
    pub async fn event<P: Serialize + Send>(
        &mut self,
        data: P,
        cursor: Option<C>,
    ) -> Result<(), StreamEnd> {
        self.event_sequence += 1;
        let cursor = self.cursor.clone().max(cursor);
        let params = self.event_params(data);
        let notification = Notification::event(params, cursor.as_ref(), &self.request_id);
        let data = serde_json::to_string(&notification)?;
        self.send(data, self.max_response_size).await?;
        self.cursor = cursor;
        self.guard.metrics.subscription_events.inc();
        Ok(())
    }

    /// Serialized size of an event carrying `data` at the current cursor.
    pub fn event_len<P: Serialize>(&self, data: P) -> Result<usize, StreamEnd> {
        let params = self.event_params(data);
        let notification = Notification::event(params, self.cursor.as_ref(), &self.request_id);
        Ok(serde_json::to_string(&notification)?.len())
    }

    fn event_params<P>(&self, data: P) -> EventParams<P> {
        EventParams {
            name: self.name,
            timestamp: chrono::Utc::now().to_rfc3339(),
            event_id: format!("{}:{}", self.subscription_id, self.event_sequence),
            data,
        }
    }

    async fn control(&mut self, body: Control) -> Result<(), StreamEnd> {
        let notification = Notification::control(body, self.cursor.as_ref(), &self.request_id);
        let data = serde_json::to_string(&notification)?;
        self.send(data, MAX_CONTROL_SIZE).await
    }

    async fn send(&mut self, data: String, maximum: usize) -> Result<(), StreamEnd> {
        if data.len() > maximum {
            return Err(StreamEnd::oversized(
                "subscription event exceeds maximum response size",
            ));
        }
        self.frame_sequence += 1;
        let send = self.tx.send(
            Event::default()
                .event("message")
                .id(format!("{}:{}", self.subscription_id, self.frame_sequence))
                .data(data),
        );
        tokio::select! {
            biased;
            outcome = self.lifetime.ended() => Err(outcome),
            result = timeout(SEND_TIMEOUT, send) => {
                result.map_err(|_| StreamEnd::SlowConsumer)?
                    .map_err(|_| StreamEnd::Cancelled)
            },
        }
    }

    async fn finish(&mut self, outcome: &StreamEnd, response_id: Value) {
        // Cleanup is bounded by the send timeout and cannot change the subscription outcome.
        self.lifetime = Lifetime::unlimited();
        let _ = self
            .control(Control::Terminated {
                error: outcome.error(),
            })
            .await;
        let result = McpResponse::success(
            response_id,
            McpResult::EventsComplete(EventsCompleteResult::default()),
        );
        if let Ok(data) = serde_json::to_string(&result) {
            let _ = self.send(data, MAX_CONTROL_SIZE).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_timer_elapsed(start: Instant, expected: Duration) {
        let elapsed = start.elapsed();
        // Tokio's hashed-wheel timer can round deadlines up by one millisecond.
        assert!(
            elapsed >= expected && elapsed <= expected + Duration::from_millis(1),
            "elapsed {elapsed:?}, expected {expected:?} within one timer tick",
        );
    }

    fn stream() -> (EventStream<u64>, mpsc::Receiver<Event>) {
        let (tx, rx) = mpsc::channel(1);
        let metrics = McpMetrics::register_into(&mz_ore::metrics::MetricsRegistry::new());
        (
            EventStream {
                tx,
                name: "test",
                subscription_id: "test-stream".into(),
                frame_sequence: 0,
                event_sequence: 0,
                max_response_size: 1024,
                cursor: None,
                request_id: RequestId::Signed(7),
                startup: None,
                heartbeat_interval: Duration::from_secs(30),
                heartbeats: Some(tokio::time::interval_at(
                    Instant::now() + Duration::from_secs(30),
                    Duration::from_secs(30),
                )),
                ttl: DEFAULT_TTL,
                lifetime: Lifetime::unlimited(),
                guard: MetricsGuard {
                    metrics,
                    active: false,
                    state: SubscriptionState::default(),
                },
            },
            rx,
        )
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn startup_timeout_aborts_nested_task_and_releases_permit() {
        let subscriptions = SubscriptionRegistry::default();
        let role = RoleId::User(1);
        let permit = subscriptions.acquire(role, 1, 1).unwrap();
        let metrics = McpMetrics::register_into(&mz_ore::metrics::MetricsRegistry::new());
        let state = SubscriptionState::default();
        let guard = MetricsGuard {
            metrics: metrics.clone(),
            active: false,
            state: state.clone(),
        };
        let (finished_tx, finished_rx) = oneshot::channel::<()>();
        let (startup_tx, startup_rx) = oneshot::channel();
        let (tx, rx) = mpsc::channel(1);
        let task = mz_ore::task::spawn(|| "test_subscription", async move {
            let _owned = (permit, guard, finished_tx, startup_tx, tx);
            futures::future::pending::<()>().await;
        })
        .abort_on_drop();
        let started = StartedSubscription { rx, task };
        let failed = state.clone();
        let mut request_task = mz_ore::task::spawn(|| "test_request", async move {
            started.wait_for_startup(startup_rx, &failed).await
        })
        .abort_on_drop();
        assert!(
            timeout(Duration::from_millis(1), &mut request_task)
                .await
                .is_err()
        );
        state.startup_failed("request timed out".into());
        drop(request_task);
        assert!(finished_rx.await.is_err());
        assert!(subscriptions.acquire(role, 1, 1).is_ok());
        let ends = |reason| metrics.subscription_ends.with_label_values(&[reason]).get();
        assert_eq!(ends("error"), 1);
        assert_eq!(ends("cancelled"), 0);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn expiry_ends_the_stream_at_every_wait_point() {
        for auth in [false, true] {
            for wait in ["input", "send", "check"] {
                let (mut stream, _receiver) = stream();
                if wait == "send" {
                    stream.control(Control::Heartbeat {}).await.unwrap();
                }
                let expiry = Instant::now() + Duration::from_millis(10);
                if auth {
                    stream.lifetime.auth_expired = Box::pin(tokio::time::sleep_until(expiry));
                } else {
                    stream.lifetime.expires = Some(expiry);
                }
                let outcome = match wait {
                    "input" => stream
                        .next(futures::future::pending::<()>())
                        .await
                        .unwrap_err(),
                    "send" => stream.control(Control::Heartbeat {}).await.unwrap_err(),
                    "check" => {
                        tokio::time::advance(Duration::from_millis(10)).await;
                        stream.check().unwrap_err()
                    }
                    _ => unreachable!(),
                };
                assert_eq!(
                    outcome.metric_reason(),
                    if auth { "auth_expired" } else { "expired" },
                    "{wait}"
                );
                assert!(outcome.is_success(), "{wait}");
            }
        }
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn slow_consumer_ends_the_stream() {
        let (mut stream, _receiver) = stream();
        stream.control(Control::Heartbeat {}).await.unwrap();
        let before = Instant::now();
        assert!(matches!(
            stream.control(Control::Heartbeat {}).await,
            Err(StreamEnd::SlowConsumer)
        ));
        assert_timer_elapsed(before, SEND_TIMEOUT);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn cleanup_failure_preserves_termination_metrics() {
        for closed in [false, true] {
            let (mut stream, receiver) = stream();
            let metrics = stream.guard.metrics.clone();
            let outcome = stream.record(StreamEnd::Expired);
            stream.control(Control::Heartbeat {}).await.unwrap();
            let _receiver = (!closed).then_some(receiver);
            let start = Instant::now();
            stream.finish(&outcome, json!(7)).await;
            // An open but full channel times out on `terminated` and again on the result.
            let expected = if closed {
                Duration::ZERO
            } else {
                2 * SEND_TIMEOUT
            };
            assert_timer_elapsed(start, expected);
            drop(stream);
            let ends = |reason| metrics.subscription_ends.with_label_values(&[reason]).get();
            assert_eq!(ends("expired"), 1);
            assert_eq!(ends("slow_consumer"), 0);
            assert_eq!(ends("cancelled"), 0);
        }
    }

    #[mz_ore::test]
    fn failure_supersedes_success_but_preserves_initial_failure() {
        for provisional in [
            StreamEnd::Completed,
            StreamEnd::Expired,
            StreamEnd::AuthenticationExpired,
        ] {
            let state = SubscriptionState::default();
            state.finish(provisional);
            state.finish(StreamEnd::Failed {
                code: -32603,
                message: "commit failed".into(),
            });
            assert_eq!(state.outcome().to_string(), "commit failed");
        }
        let state = SubscriptionState::default();
        state.startup_failed("request timed out".into());
        state.finish(StreamEnd::Completed);
        state.finish(StreamEnd::Cancelled);
        assert_eq!(state.outcome().to_string(), "request timed out");
        assert_eq!(state.outcome().metric_reason(), "error");
    }
}
