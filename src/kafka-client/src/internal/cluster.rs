// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cluster state shared by all engines of one client: bootstrap, broker
//! connections, the metadata cache and refresh, leader, coordinator and
//! controller lookup.
//!
//! # Engine-facing API
//!
//! * Brokers: [`Cluster::broker`] by id, [`Cluster::any_broker`] (waits for a
//!   connected broker), [`Cluster::leader`], [`Cluster::coordinator`],
//!   [`Cluster::controller`]. Requests go through [`Broker::request`], whose
//!   closure builds the request for the negotiated API version.
//! * Metadata: [`Cluster::metadata`] is the current snapshot,
//!   [`Cluster::subscribe_metadata`] a `watch` of it that changes with every
//!   Metadata response (from any request), [`Cluster::refresh_metadata`]
//!   fetches now, [`Cluster::request_metadata_refresh`] asks the background
//!   task to refresh soon (after leader errors), and
//!   [`Cluster::add_known_topics`] adds topics to the periodic refresh.
//! * Shutdown: [`Cluster::shutdown`] (called when the client's last user
//!   handle drops) cancels [`Cluster::shutdown_token`], which stops all
//!   background tasks; connections close and pending requests fail with
//!   `BrokerDestroy`. Engine tasks should select on a child of the token.
//! * Statistics: [`Cluster::register_stats_source`].
//! * Errors and statistics reach the application through
//!   [`Cluster::events`], like librdkafka's main queue: consumers deliver
//!   them when polled, producers right away, admin clients never.
//!
//! # All brokers down
//!
//! As in librdkafka, a broker counts as down from its first failure until any
//! broker is up again. When every broker counts as down, the error callback
//! receives `AllBrokersDown` with `N/N brokers are down`, once per such
//! transition, and bootstrap brokers become eligible again (librdkafka's
//! re-bootstrap). Bootstrap brokers stop being used and counted once brokers
//! are learned from metadata.
//!
//! NOTE: librdkafka 2.12.1 did not emit `AllBrokersDown` in any scenario
//! tried (unreachable bootstrap brokers, a learned broker going down), even
//! though its code intends to. Materialize only parses the text.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use rand::seq::IndexedRandom;
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

use crate::config::RDKafkaLogLevel;
use crate::config::settings::{ClientKind, ClientSettings, SaslMechanism};
use crate::error::KafkaError;
use crate::internal::hooks::{Hooks, Logger};
use crate::internal::net::connect::ConnectContext;
use crate::internal::net::sasl::OAuthTokens;
use crate::internal::net::tls::TlsConfig;
use crate::internal::net::{Failure, NetError, RequestOptions};
use crate::internal::protocol;
use crate::internal::runtime;
use crate::types::RDKafkaErrorCode;
use crate::util::Deadline;

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod auth_tests;
mod bootstrap;
mod broker;
mod events;
mod metadata;
pub(crate) mod stats;
#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests;

// Part of the engine-facing API.
#[allow(unused_imports)]
pub(crate) use broker::{Broker, BrokerSource};
pub(crate) use events::EventQueue;
#[allow(unused_imports)]
pub(crate) use metadata::{
    BrokerMetadata, MetadataSnapshot, PartitionMetadata, SharedMetadata, TopicMetadata,
    to_public as metadata_to_public,
};
pub(crate) use stats::StatsSource;

/// The kind of coordinator to look up with FindCoordinator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CoordinatorKind {
    Group,
    Transaction,
}

impl CoordinatorKind {
    fn key_type(self) -> i8 {
        match self {
            CoordinatorKind::Group => 0,
            CoordinatorKind::Transaction => 1,
        }
    }
}

/// Which topics a Metadata request asks for.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MetadataTopics<'a> {
    /// All topics in the cluster.
    All,
    /// No topics: brokers and controller only.
    None,
    /// These topics.
    Some(&'a [String]),
}

#[derive(Default)]
struct Registry {
    bootstrap: Vec<Arc<Broker>>,
    learned: BTreeMap<i32, Arc<Broker>>,
}

impl Registry {
    fn all(&self) -> impl Iterator<Item = &Arc<Broker>> {
        self.bootstrap.iter().chain(self.learned.values())
    }

    fn active(&self) -> impl Iterator<Item = &Arc<Broker>> {
        self.all().filter(|b| !b.is_retired())
    }
}

/// Process-wide counters for librdkafka's handle names (`rdkafka#producer-1`).
static HANDLE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The cluster state of one client.
pub(crate) struct Cluster {
    weak: Weak<Cluster>,
    ctx: ConnectContext,
    events: Arc<EventQueue>,
    shutdown: CancellationToken,
    registry: Mutex<Registry>,
    /// Bumped on every broker state change and connection attempt outcome.
    state_version: watch::Sender<u64>,
    metadata: watch::Sender<SharedMetadata>,
    known_topics: Mutex<BTreeSet<String>>,
    coordinators: Mutex<BTreeMap<(CoordinatorKind, String), i32>>,
    refresh: Arc<Notify>,
    last_connect_any: Mutex<Option<Instant>>,
    stats_sources: Mutex<Vec<Weak<dyn StatsSource>>>,
    created: Instant,
    handle_name: String,
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("name", &self.handle_name)
            .finish_non_exhaustive()
    }
}

impl Cluster {
    /// Creates the cluster state and starts its background tasks (metadata
    /// refresh, statistics, OAUTHBEARER token refresh). No connection is
    /// made until something needs a broker.
    pub(crate) fn new(
        settings: Arc<ClientSettings>,
        hooks: Arc<dyn Hooks>,
        logger: Logger,
        tls: Option<TlsConfig>,
    ) -> Arc<Cluster> {
        let shutdown = CancellationToken::new();
        let events = Arc::new(EventQueue::new(Arc::clone(&hooks), settings.kind));
        let oauth = (settings.security_protocol.uses_sasl()
            && settings.sasl.mechanism == Some(SaslMechanism::OAuthBearer))
        .then(|| {
            OAuthTokens::start(
                Arc::clone(&hooks),
                Arc::clone(&events),
                settings.sasl.oauthbearer_config.clone(),
                hooks.oauth_enabled(),
                shutdown.child_token(),
            )
        });
        let ctx = ConnectContext {
            settings: Arc::clone(&settings),
            hooks,
            logger,
            tls,
            oauth,
        };
        let kind = match settings.kind {
            ClientKind::Consumer => "consumer",
            ClientKind::Producer | ClientKind::Admin => "producer",
        };
        let n = HANDLE_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        let handle_name = format!("{}#{kind}-{n}", settings.client_id);
        let (state_version, _) = watch::channel(0);
        let (metadata, _) = watch::channel(Arc::new(MetadataSnapshot {
            controller_id: -1,
            ..MetadataSnapshot::default()
        }));
        let cluster = Arc::new_cyclic(|weak: &Weak<Cluster>| {
            let mut registry = Registry::default();
            for entry in &settings.bootstrap_servers {
                match bootstrap::parse_server(entry, settings.security_protocol) {
                    Ok(server) => {
                        let dup = registry.bootstrap.iter().any(|b| {
                            b.nodename() == bootstrap::nodename(&server.host, server.port)
                        });
                        if !dup {
                            registry.bootstrap.push(Arc::new(Broker::new(
                                None,
                                server.host,
                                server.port,
                                Weak::clone(weak),
                                ctx.clone(),
                            )));
                        }
                    }
                    Err(msg) => ctx.logger.log_fmt(
                        RDKafkaLogLevel::Warning,
                        "BROKER",
                        format_args!("[thrd:app]: {msg}"),
                    ),
                }
            }
            Cluster {
                weak: Weak::clone(weak),
                ctx,
                events,
                shutdown,
                registry: Mutex::new(registry),
                state_version,
                metadata,
                known_topics: Mutex::new(BTreeSet::new()),
                coordinators: Mutex::new(BTreeMap::new()),
                refresh: Arc::new(Notify::new()),
                last_connect_any: Mutex::new(None),
                stats_sources: Mutex::new(Vec::new()),
                created: Instant::now(),
                handle_name,
            }
        });
        runtime::spawn(
            "kafka-metadata-refresh",
            refresh_loop(Arc::downgrade(&cluster), cluster.shutdown.clone()),
        );
        if let Some(interval) = settings.statistics_interval {
            runtime::spawn(
                "kafka-statistics",
                stats::stats_loop(Arc::downgrade(&cluster), cluster.shutdown.clone(), interval),
            );
        }
        cluster
    }

    pub(crate) fn settings(&self) -> &Arc<ClientSettings> {
        &self.ctx.settings
    }

    pub(crate) fn hooks(&self) -> &Arc<dyn Hooks> {
        &self.ctx.hooks
    }

    /// The client's error and statistics events. Raise errors and statistics
    /// through it rather than calling the hooks, so that they reach the
    /// application like librdkafka's main queue.
    pub(crate) fn events(&self) -> &EventQueue {
        &self.events
    }

    pub(crate) fn logger(&self) -> &Logger {
        &self.ctx.logger
    }

    /// librdkafka's handle name, e.g. `rdkafka#consumer-1`.
    pub(crate) fn handle_name(&self) -> &str {
        &self.handle_name
    }

    pub(crate) fn created(&self) -> Instant {
        self.created
    }

    /// A token cancelled when the client shuts down. Engines should use a
    /// child token (`shutdown_token().child_token()`) for their tasks.
    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub(crate) fn is_shut_down(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    /// Stops all background tasks and closes all connections. Pending
    /// requests fail with `BrokerDestroy`. Idempotent.
    pub(crate) fn shutdown(&self) {
        if self.shutdown.is_cancelled() {
            return;
        }
        self.shutdown.cancel();
        let brokers: Vec<_> = self
            .registry
            .lock()
            .expect("lock poisoned")
            .all()
            .cloned()
            .collect();
        for b in brokers {
            b.close_connection(Failure {
                level: RDKafkaLogLevel::Debug,
                code: RDKafkaErrorCode::BrokerDestroy,
                message: "Broker handle is terminating".into(),
                planned: true,
            });
        }
        self.notify_state_change();
    }

    pub(crate) fn notify_state_change(&self) {
        self.state_version.send_modify(|v| *v += 1);
    }

    /// The learned broker with `id`, if it is in the current metadata.
    pub(crate) fn broker(&self, id: i32) -> Option<Arc<Broker>> {
        self.registry
            .lock()
            .expect("lock poisoned")
            .learned
            .get(&id)
            .filter(|b| !b.is_retired())
            .cloned()
    }

    /// All brokers that are in use (not retired), bootstrap brokers first.
    pub(crate) fn brokers(&self) -> Vec<Arc<Broker>> {
        self.registry
            .lock()
            .expect("lock poisoned")
            .active()
            .cloned()
            .collect()
    }

    fn shutdown_error() -> NetError {
        NetError::new(
            RDKafkaErrorCode::BrokerDestroy,
            RDKafkaErrorCode::BrokerDestroy.err2str(),
        )
    }

    /// A broker with a usable connection, chosen at random among those that
    /// are up. Waits up to `deadline` for one, starting connection attempts
    /// one broker at a time (never tried brokers first), as librdkafka's
    /// `rd_kafka_broker_any_usable` with sparse connections.
    ///
    /// Fails with `BrokerTransportFailure` if no broker came up in time.
    pub(crate) async fn any_broker(&self, deadline: Deadline) -> Result<Arc<Broker>, NetError> {
        let mut rx = self.state_version.subscribe();
        loop {
            if self.is_shut_down() {
                return Err(Self::shutdown_error());
            }
            rx.borrow_and_update();
            let up: Vec<Arc<Broker>> = self.brokers().into_iter().filter(|b| b.is_up()).collect();
            if let Some(b) = up.choose(&mut rand::rng()) {
                return Ok(Arc::clone(b));
            }
            let retry_in = self.connect_any();
            let wait = async {
                tokio::select! {
                    _ = rx.changed() => {}
                    _ = tokio::time::sleep(retry_in) => {}
                    _ = self.shutdown.cancelled() => {}
                }
            };
            match deadline.instant() {
                Some(at) => {
                    if tokio::time::timeout_at(at.into(), wait).await.is_err() {
                        return Err(NetError::transport(
                            RDKafkaErrorCode::BrokerTransportFailure.err2str(),
                        ));
                    }
                }
                None => wait.await,
            }
        }
    }

    /// librdkafka's `rd_kafka_connect_any`: unless a broker is up or
    /// connecting, starts a connection attempt to a random broker, preferring
    /// brokers never tried, at most once per sparse-connect interval. Returns
    /// when to check again.
    fn connect_any(&self) -> Duration {
        let settings = &self.ctx.settings;
        let interval = (settings.reconnect_backoff / 2)
            .min(Duration::from_secs(1))
            .max(Duration::from_millis(11));
        let brokers = self.brokers();
        if brokers
            .iter()
            .any(|b| b.state().is_up() || b.is_connecting())
        {
            return interval;
        }
        {
            let mut last = self.last_connect_any.lock().expect("lock poisoned");
            if let Some(at) = *last {
                let elapsed = at.elapsed();
                if elapsed < interval {
                    return interval - elapsed;
                }
            }
            *last = Some(Instant::now());
        }
        let never_tried: Vec<&Arc<Broker>> = brokers.iter().filter(|b| !b.was_tried()).collect();
        let pick = never_tried
            .choose(&mut rand::rng())
            .copied()
            .or_else(|| brokers.choose(&mut rand::rng()));
        if let Some(b) = pick {
            b.start_attempt();
        }
        interval
    }

    /// Called by a broker on its first transition to down since it was last
    /// up. Emits `AllBrokersDown` when every broker is down.
    fn on_broker_down(&self) {
        if self.is_shut_down() {
            return;
        }
        let (down, total) = {
            let registry = self.registry.lock().expect("lock poisoned");
            let active: Vec<_> = registry.active().collect();
            let down = active
                .iter()
                .filter(|b| b.down_reported().load(Ordering::Acquire))
                .count();
            (down, active.len())
        };
        if total > 0 && down == total {
            self.rebootstrap();
            self.events.error(
                KafkaError::Global(RDKafkaErrorCode::AllBrokersDown),
                &format!("{down}/{total} brokers are down"),
            );
        }
    }

    /// librdkafka's `rd_kafka_reset_any_broker_down_reported`.
    fn reset_down_reported(&self) {
        let registry = self.registry.lock().expect("lock poisoned");
        for b in registry.all() {
            b.down_reported().store(false, Ordering::Release);
        }
    }

    /// librdkafka's re-bootstrap: bootstrap brokers become eligible again.
    ///
    /// NOTE: Unlike librdkafka, the down accounting is not reset here, so that
    /// the error is reported once per transition to "all down" rather than
    /// after every failed reconnect of a single broker.
    fn rebootstrap(&self) {
        let registry = self.registry.lock().expect("lock poisoned");
        for b in &registry.bootstrap {
            b.unretire();
        }
    }

    /// The current metadata snapshot.
    pub(crate) fn metadata(&self) -> SharedMetadata {
        Arc::clone(&self.metadata.borrow())
    }

    /// A receiver that observes every metadata update.
    pub(crate) fn subscribe_metadata(&self) -> watch::Receiver<SharedMetadata> {
        self.metadata.subscribe()
    }

    /// Adds topics to the periodic metadata refresh, like librdkafka's
    /// "locally known" topics.
    pub(crate) fn add_known_topics<'a>(&self, topics: impl IntoIterator<Item = &'a str>) {
        let mut known = self.known_topics.lock().expect("lock poisoned");
        for t in topics {
            if !known.contains(t) {
                known.insert(t.to_string());
            }
        }
    }

    /// Asks the background task to refresh the metadata of the known topics
    /// soon, e.g. after a leader error. Coalesces repeated calls.
    pub(crate) fn request_metadata_refresh(&self) {
        self.refresh.notify_one();
    }

    /// Applies a Metadata response to the cache and the broker registry.
    pub(crate) fn apply_metadata(&self, response: &MetadataResponse, all_topics: bool) {
        self.update_brokers(response);
        self.metadata.send_modify(|md| {
            *md = Arc::new(md.apply(response, all_topics));
        });
    }

    fn update_brokers(&self, response: &MetadataResponse) {
        if response.brokers.is_empty() {
            return;
        }
        let mut retired = Vec::new();
        {
            let mut registry = self.registry.lock().expect("lock poisoned");
            let mut seen = BTreeSet::new();
            for b in &response.brokers {
                let id = b.node_id.0;
                let port = u16::try_from(b.port).unwrap_or(0);
                seen.insert(id);
                match registry.learned.get(&id) {
                    Some(broker) => {
                        broker.unretire();
                        broker.set_address(b.host.as_str(), port);
                    }
                    None => {
                        let broker = Arc::new(Broker::new(
                            Some(id),
                            b.host.as_str().to_string(),
                            port,
                            Weak::clone(&self.weak),
                            self.ctx.clone(),
                        ));
                        registry.learned.insert(id, broker);
                    }
                }
            }
            for (id, broker) in &registry.learned {
                if !seen.contains(id) && !broker.is_retired() {
                    retired.push(Arc::clone(broker));
                }
            }
            for broker in &registry.bootstrap {
                if !broker.is_retired() {
                    retired.push(Arc::clone(broker));
                }
            }
        }
        for broker in retired {
            broker.retire();
        }
        self.notify_state_change();
    }

    /// Requests metadata from any broker, waiting up to `deadline` for one to
    /// be up and for the response, like librdkafka's `rd_kafka_metadata`.
    ///
    /// Fails with `BrokerTransportFailure` if no broker came up and
    /// `OperationTimedOut` if the response did not arrive in time. The
    /// response updates the cache. Returns the broker that answered.
    pub(crate) async fn fetch_metadata(
        &self,
        topics: MetadataTopics<'_>,
        deadline: Deadline,
    ) -> Result<(Arc<Broker>, MetadataResponse), NetError> {
        let allow_auto_create = self.ctx.settings.allow_auto_create_topics;
        let mut retries = 0;
        let mut last_error = None;
        loop {
            // A retry that finds no broker fails with the request's error.
            let broker = match self.any_broker(deadline).await {
                Ok(broker) => broker,
                Err(e) => return Err(last_error.unwrap_or(e)),
            };
            let request = broker.request::<MetadataRequest, _>(
                |version| {
                    let mut req = MetadataRequest::default();
                    req.topics = match topics {
                        MetadataTopics::All if version == 0 => Some(vec![]),
                        MetadataTopics::All => None,
                        MetadataTopics::None => Some(vec![]),
                        MetadataTopics::Some(names) => Some(
                            names
                                .iter()
                                .map(|n| {
                                    MetadataRequestTopic::default().with_name(Some(TopicName(
                                        StrBytes::from_string(n.clone()),
                                    )))
                                })
                                .collect(),
                        ),
                    };
                    if version >= 4 {
                        req.allow_auto_topic_creation = allow_auto_create;
                    }
                    req
                },
                RequestOptions::default(),
            );
            let result = match deadline.instant() {
                Some(at) => match tokio::time::timeout_at(at.into(), request).await {
                    Ok(r) => r,
                    Err(_) => return Err(NetError::timed_out()),
                },
                None => request.await,
            };
            match result {
                Ok(response) => return Ok((broker, response)),
                // The broker was removed meanwhile: try another one.
                Err(e) if e.code() == RDKafkaErrorCode::DestroyBroker => continue,
                // Like librdkafka's request retries (2 for non-produce
                // requests), retry transport failures and timeouts on a
                // broker that is up.
                Err(e)
                    if retries < 2
                        && matches!(
                            e.code(),
                            RDKafkaErrorCode::BrokerTransportFailure
                                | RDKafkaErrorCode::TimedOutQueue
                                | RDKafkaErrorCode::OperationTimedOut
                        ) =>
                {
                    retries += 1;
                    last_error = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Refreshes the metadata of `topics` (and adds them to the known
    /// topics), returning the updated snapshot.
    pub(crate) async fn refresh_metadata(
        &self,
        topics: &[String],
        deadline: Deadline,
    ) -> Result<SharedMetadata, NetError> {
        self.add_known_topics(topics.iter().map(String::as_str));
        self.fetch_metadata(MetadataTopics::Some(topics), deadline)
            .await?;
        Ok(self.metadata())
    }

    /// The leader of a partition, from the cache or, if unknown, from fresh
    /// metadata.
    ///
    /// Fails with the topic's or partition's error (e.g.
    /// `UnknownTopicOrPartition` for a missing topic), `UnknownPartition` if
    /// the topic has no such partition, `LeaderNotAvailable` if there is no
    /// leader by the deadline, or a transport error.
    pub(crate) async fn leader(
        &self,
        topic: &str,
        partition: i32,
        deadline: Deadline,
    ) -> Result<Arc<Broker>, NetError> {
        let topics = [topic.to_string()];
        let mut refreshed = false;
        loop {
            let md = self.metadata();
            let mut retriable_error = None;
            match md.topic(topic) {
                Some(t) if t.error.is_none() => match t.partition(partition) {
                    Some(p) if p.leader >= 0 => {
                        if let Some(b) = self.broker(p.leader) {
                            return Ok(b);
                        }
                        retriable_error = Some(RDKafkaErrorCode::LeaderNotAvailable);
                    }
                    Some(p) => {
                        retriable_error =
                            Some(p.error.unwrap_or(RDKafkaErrorCode::LeaderNotAvailable));
                    }
                    None if refreshed => {
                        return Err(NetError::new(
                            RDKafkaErrorCode::UnknownPartition,
                            RDKafkaErrorCode::UnknownPartition.err2str(),
                        ));
                    }
                    None => {}
                },
                Some(t) if refreshed => {
                    let code = t.error.expect("checked");
                    if !protocol::is_retriable(code)
                        || code == RDKafkaErrorCode::UnknownTopicOrPartition
                    {
                        return Err(NetError::new(code, code.err2str()));
                    }
                    retriable_error = Some(code);
                }
                _ => {}
            }
            if refreshed {
                // Retriable: back off before asking again.
                let backoff = self.ctx.settings.retry_backoff;
                if deadline.remaining_duration().is_some_and(|r| r <= backoff) {
                    let code = retriable_error.unwrap_or(RDKafkaErrorCode::LeaderNotAvailable);
                    return Err(NetError::new(code, code.err2str()));
                }
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = self.shutdown.cancelled() => return Err(Self::shutdown_error()),
                }
            }
            self.refresh_metadata(&topics, deadline).await?;
            refreshed = true;
        }
    }

    /// The controller broker, for admin requests.
    pub(crate) async fn controller(&self, deadline: Deadline) -> Result<Arc<Broker>, NetError> {
        loop {
            let id = self.metadata().controller_id;
            if id >= 0 {
                if let Some(b) = self.broker(id) {
                    return Ok(b);
                }
            }
            self.fetch_metadata(MetadataTopics::None, deadline).await?;
            let id = self.metadata().controller_id;
            if id >= 0 {
                if let Some(b) = self.broker(id) {
                    return Ok(b);
                }
            }
            if deadline.is_elapsed() {
                return Err(NetError::new(
                    RDKafkaErrorCode::BrokerNotAvailable,
                    RDKafkaErrorCode::BrokerNotAvailable.err2str(),
                ));
            }
            tokio::time::sleep(self.ctx.settings.retry_backoff).await;
        }
    }

    /// The group or transaction coordinator for `key`, cached until
    /// [`Cluster::invalidate_coordinator`].
    ///
    /// Retries `COORDINATOR_NOT_AVAILABLE`, `NOT_COORDINATOR`,
    /// `COORDINATOR_LOAD_IN_PROGRESS` and transport errors until the
    /// deadline. Fails with other broker errors (e.g.
    /// `GroupAuthorizationFailed`) immediately.
    pub(crate) async fn coordinator(
        &self,
        kind: CoordinatorKind,
        key: &str,
        deadline: Deadline,
    ) -> Result<Arc<Broker>, NetError> {
        let cache_key = (kind, key.to_string());
        if let Some(id) = self
            .coordinators
            .lock()
            .expect("lock poisoned")
            .get(&cache_key)
        {
            if let Some(b) = self.broker(*id) {
                return Ok(b);
            }
        }
        let mut last_error;
        loop {
            let result = async {
                let broker = self.any_broker(deadline).await?;
                broker
                    .request::<FindCoordinatorRequest, _>(
                        |version| {
                            let mut req = FindCoordinatorRequest::default()
                                .with_key(StrBytes::from_string(key.to_string()));
                            if version >= 1 {
                                req.key_type = kind.key_type();
                            }
                            req
                        },
                        RequestOptions::default(),
                    )
                    .await
            };
            let result = match deadline.instant() {
                Some(at) => tokio::time::timeout_at(at.into(), result)
                    .await
                    .unwrap_or_else(|_| Err(NetError::timed_out())),
                None => result.await,
            };
            match result {
                Ok(response) => match protocol::broker_error(response.error_code) {
                    None => {
                        let id = response.node_id.0;
                        let broker = self.ensure_broker(
                            id,
                            response.host.as_str(),
                            u16::try_from(response.port).unwrap_or(0),
                        );
                        self.coordinators
                            .lock()
                            .expect("lock poisoned")
                            .insert(cache_key, id);
                        return Ok(broker);
                    }
                    Some(
                        code @ (RDKafkaErrorCode::CoordinatorNotAvailable
                        | RDKafkaErrorCode::NotCoordinator
                        | RDKafkaErrorCode::CoordinatorLoadInProgress),
                    ) => {
                        last_error = NetError::new(code, code.err2str());
                    }
                    Some(code) => {
                        let message = response
                            .error_message
                            .as_ref()
                            .map(|m| m.as_str().to_string())
                            .unwrap_or_else(|| code.err2str());
                        return Err(NetError::new(code, message));
                    }
                },
                Err(e) if e.code() == RDKafkaErrorCode::BrokerDestroy => return Err(e),
                Err(e) => last_error = e,
            }
            let backoff = self.ctx.settings.retry_backoff;
            if deadline.remaining_duration().is_some_and(|r| r <= backoff) {
                return Err(last_error);
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = self.shutdown.cancelled() => return Err(Self::shutdown_error()),
            }
        }
    }

    /// Forgets a cached coordinator, after `NOT_COORDINATOR` or a transport
    /// error from it.
    pub(crate) fn invalidate_coordinator(&self, kind: CoordinatorKind, key: &str) {
        self.coordinators
            .lock()
            .expect("lock poisoned")
            .remove(&(kind, key.to_string()));
    }

    /// The learned broker `id`, created with the given address if unknown.
    fn ensure_broker(&self, id: i32, host: &str, port: u16) -> Arc<Broker> {
        let mut registry = self.registry.lock().expect("lock poisoned");
        if let Some(b) = registry.learned.get(&id) {
            if b.is_retired() {
                b.unretire();
            }
            return Arc::clone(b);
        }
        let broker = Arc::new(Broker::new(
            Some(id),
            host.to_string(),
            port,
            Weak::clone(&self.weak),
            self.ctx.clone(),
        ));
        registry.learned.insert(id, Arc::clone(&broker));
        broker
    }

    /// Registers a source of top-level statistics. Sources are held weakly;
    /// dropped sources are skipped.
    pub(crate) fn register_stats_source(&self, source: Weak<dyn StatsSource>) {
        self.stats_sources
            .lock()
            .expect("lock poisoned")
            .push(source);
    }

    fn stats_sources(&self) -> Vec<Arc<dyn StatsSource>> {
        let mut sources = self.stats_sources.lock().expect("lock poisoned");
        sources.retain(|s| s.strong_count() > 0);
        sources.iter().filter_map(Weak::upgrade).collect()
    }

    fn known_topics(&self) -> Vec<String> {
        self.known_topics
            .lock()
            .expect("lock poisoned")
            .iter()
            .cloned()
            .collect()
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// The periodic metadata refresh (`topic.metadata.refresh.interval.ms`) and
/// on-demand refreshes requested with [`Cluster::request_metadata_refresh`],
/// which are spaced at least `topic.metadata.refresh.fast.interval.ms`.
async fn refresh_loop(cluster: Weak<Cluster>, shutdown: CancellationToken) {
    // Hold no strong reference while waiting, so that dropping the client
    // drops the cluster.
    let (interval, fast, refresh) = match cluster.upgrade() {
        Some(c) => (
            c.ctx.settings.metadata_refresh_interval,
            c.ctx.settings.metadata_refresh_fast_interval,
            Arc::clone(&c.refresh),
        ),
        None => return,
    };
    let mut last_broker_refresh: Option<Instant> = None;
    let mut next_periodic = interval.map(|i| Instant::now() + i);
    loop {
        let periodic = async {
            match next_periodic {
                Some(at) => tokio::time::sleep_until(at.into()).await,
                None => std::future::pending().await,
            }
        };
        let is_periodic = tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = periodic => true,
            _ = refresh.notified() => false,
        };
        if is_periodic {
            next_periodic = interval.map(|i| Instant::now() + i);
        }
        let Some(c) = cluster.upgrade() else { return };
        let topics = c.known_topics();
        // Periodic refreshes only use a broker that is already up, as
        // librdkafka's; on-demand ones may wait for a connection.
        let deadline = Deadline::new(if is_periodic {
            Duration::ZERO
        } else {
            c.ctx.settings.socket_timeout
        });
        if topics.is_empty() {
            // Without known topics, refresh the broker list at most every
            // 10 s.
            if is_periodic
                && last_broker_refresh.map_or(true, |t| t.elapsed() >= Duration::from_secs(10))
            {
                last_broker_refresh = Some(Instant::now());
                let _ = c.fetch_metadata(MetadataTopics::None, deadline).await;
            }
        } else {
            let _ = c
                .fetch_metadata(MetadataTopics::Some(&topics), deadline)
                .await;
        }
        drop(c);
        if !is_periodic {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(fast) => {}
            }
        }
    }
}
