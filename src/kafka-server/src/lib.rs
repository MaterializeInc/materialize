// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A server for the Kafka wire protocol that lets Kafka producers write into Materialize.
//!
//! The server presents itself as a single-broker Kafka cluster. A consumer inside the process
//! (a Kafka listener source) claims a topic with [`KafkaServer::register_topic`] and receives
//! every record produced to it as a [`ProduceBatch`]. The consumer assigns offsets and decides
//! when a batch is acknowledged, which lets it delay the acknowledgement until the records are
//! durable. Batches from idempotent producers carry their [`ProducerBatchId`], and
//! [`ProducerStates`] implements the Kafka rules for deduplicating them. Keeping that state
//! durable is up to the consumer.
//!
//! Each topic has exactly one partition, partition 0, led by broker 0.
//!
//! Supported APIs: `ApiVersions`, `Metadata`, `Produce`, `InitProducerId`, `SaslHandshake` and
//! `SaslAuthenticate` with the `PLAIN` mechanism, the transaction APIs `FindCoordinator`,
//! `AddPartitionsToTxn` and `EndTxn`, and, for compacted topics only, `CreateTopics`,
//! `ListOffsets` and `Fetch`.
//!
//! # Transactions and compacted topics
//!
//! A transactional producer's records are held by the server until it commits the transaction,
//! and are then handed to the claimed topic as one [`ProduceBatch`]. A transaction may write to
//! at most one claimed topic, which makes the commit atomic.
//!
//! Topics created with `CreateTopics` are compacted topics: the server serves the latest record
//! of each key to consumers and stores nothing itself. Their records are written only in
//! transactions, and travel in [`ProduceBatch::compacted`] to the claimed topic of the
//! transaction, whose consumer stores them with the batch and hands them back to
//! [`KafkaServer::register_topic`] after a restart. A transaction that writes no claimed topic
//! sends its compacted records to the claimed topic that holds their keys. If no claimed topic
//! holds them, the server keeps them in memory only. This is what a Materialize Kafka sink needs
//! for its progress topic.
//!
//! Transaction state lives in memory. When the server restarts, every producer must call
//! `InitProducerId` again, which fences producers from before the restart.
//!
//! A topic may be registered with a [`RecordValidator`], which checks each produced batch on the
//! task of the producer's connection before the batch reaches the topic or joins a transaction.
//! A rejected batch never counts towards an idempotent producer's sequence numbers.
//!
//! [`KafkaServer::schema_registry`] serves the schemas of the claimed topics over HTTP, see
//! [`registry`].
//!
//! Authentication is optional per connection. A connection that does not authenticate can
//! produce to topics registered without credentials. A connection that authenticates with
//! SASL/PLAIN can additionally produce to topics registered with exactly those credentials.
//! Compacted records are readable and writable with the access their claimed topic requires.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;
use mz_ore::netio::Listener;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tracing::{debug, info, warn};

mod connection;
mod idempotence;
mod registry;
mod transaction;

pub use idempotence::{
    Admission, AppendedBatch, ProducerBatchId, ProducerStates, RETAINED_BATCHES,
};
pub use kafka_protocol::error::ResponseError;
pub use registry::TopicSchemas;

/// The broker ID that this server reports for itself.
const NODE_ID: i32 = 0;

/// How long the compacted records of a released topic are withheld. A topic is usually
/// registered again within seconds of being released, when its source restarts. A topic whose
/// source was dropped never is, and its records are forgotten after this long.
const PENDING_RETENTION: Duration = Duration::from_secs(5 * 60);

/// Configuration for a [`KafkaServer`].
#[derive(Debug, Clone)]
pub struct KafkaServerConfig {
    /// The host that clients are told to connect to in `Metadata` responses.
    ///
    /// Clients use the bootstrap address only for the first request and route every following
    /// request to the advertised address, so this must be reachable by clients.
    pub advertised_host: String,
    /// The port that clients are told to connect to in `Metadata` responses.
    pub advertised_port: u16,
    /// The cluster ID reported in `Metadata` responses.
    pub cluster_id: String,
}

/// SASL/PLAIN credentials that a producer must present to write to a topic.
#[derive(Clone)]
pub struct SaslPlainCredentials {
    /// The username.
    pub username: String,
    /// The password.
    pub password: String,
}

impl SaslPlainCredentials {
    fn matches(&self, other: &SaslPlainCredentials) -> bool {
        let username_eq = self.username.as_bytes().ct_eq(other.username.as_bytes());
        let password_eq = self.password.as_bytes().ct_eq(other.password.as_bytes());
        (username_eq & password_eq).into()
    }
}

impl fmt::Debug for SaslPlainCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslPlainCredentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// A Kafka protocol server. Cheaply cloneable.
#[derive(Debug, Clone)]
pub struct KafkaServer {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    config: KafkaServerConfig,
    state: Mutex<State>,
    /// Open and recently ended transactions, by transactional ID.
    transactions: Mutex<BTreeMap<String, transaction::TransactionState>>,
    next_registration_id: AtomicU64,
}

#[derive(Debug, Default)]
struct State {
    /// Claimed topics.
    topics: BTreeMap<String, TopicEntry>,
    compacted: BTreeMap<String, CompactedTopic>,
}

#[derive(Debug, Default)]
struct CompactedTopic {
    /// The offset of the next record.
    next_offset: i64,
    latest: BTreeMap<Bytes, CompactedValue>,
}

#[derive(Debug)]
struct CompactedValue {
    offset: i64,
    value: Option<Bytes>,
    holder: Holder,
}

/// What stores a compacted record.
#[derive(Debug, Clone)]
enum Holder {
    /// Only the server's memory.
    Memory,
    /// A claimed topic.
    Topic(Home),
    /// A claimed topic that was released at `since`. Its consumer may have stored a newer
    /// record that the server never learned of, so the record is not served until the topic is
    /// registered again, or [`PENDING_RETENTION`] passes.
    Pending { topic: String, since: Instant },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Home {
    topic: String,
    registration_id: u64,
}

#[derive(Debug)]
struct TopicEntry {
    registration_id: u64,
    credentials: Option<SaslPlainCredentials>,
    validator: Option<Arc<dyn RecordValidator>>,
    /// The key and value schemas.
    schemas: (
        Option<registry::ServedSchema>,
        Option<registry::ServedSchema>,
    ),
    tx: TopicSender,
}

/// The sending side of a topic's queue of batches, which is bounded in bytes.
#[derive(Debug, Clone)]
struct TopicSender {
    tx: mpsc::UnboundedSender<(ProduceBatch, OwnedSemaphorePermit)>,
    /// A permit per byte of the queue's batches.
    bytes: Arc<Semaphore>,
    max_bytes: usize,
}

impl TopicSender {
    fn new(max_bytes: usize) -> (Self, TopicReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        let sender = TopicSender {
            tx,
            bytes: Arc::new(Semaphore::new(max_bytes)),
            max_bytes,
        };
        (sender, rx)
    }

    /// Waits until the queue has room for `batch`, and queues it. A batch larger than the queue
    /// waits for the queue to empty. Fails if the topic's registration is gone.
    async fn send(&self, batch: ProduceBatch) -> Result<(), ResponseError> {
        let permits = batch.bytes.min(self.max_bytes);
        let permits = u32::try_from(permits).unwrap_or(u32::MAX);
        // NOTE: a dropped registration drops the queued batches and their permits, so this wait
        // ends.
        let permit = Arc::clone(&self.bytes)
            .acquire_many_owned(permits)
            .await
            .expect("never closed");
        self.tx
            .send((batch, permit))
            .map_err(|_| ResponseError::NotLeaderOrFollower)
    }
}

type TopicReceiver = mpsc::UnboundedReceiver<(ProduceBatch, OwnedSemaphorePermit)>;

/// How a topic is claimed, see [`KafkaServer::register_topic`].
#[derive(Debug)]
pub struct TopicOptions {
    /// If `Some`, only connections that authenticated with exactly these credentials may
    /// produce to the topic.
    pub credentials: Option<SaslPlainCredentials>,
    /// If `Some`, checks every batch produced to the topic.
    pub validator: Option<Arc<dyn RecordValidator>>,
    /// The schemas that the schema registry serves for the topic.
    pub schemas: TopicSchemas,
    /// Bounds the bytes, as [`ProduceBatch::bytes`] counts them, of undelivered batches.
    /// Producers whose batches do not fit wait, which applies backpressure.
    pub queue_bytes: usize,
    /// The compacted records that the consumer stored with earlier batches of the topic, which
    /// the server serves again.
    pub compacted: Vec<CompactedRecord>,
}

/// Checks the records produced to a topic before the server accepts them.
pub trait RecordValidator: fmt::Debug + Send + Sync {
    /// Checks a batch of records, which is accepted or rejected as a whole.
    ///
    /// Batches of different connections are checked concurrently, each on its connection's task.
    fn validate<'a>(
        &'a self,
        records: &'a [ProducedRecord],
    ) -> BoxFuture<'a, Result<(), Rejection>>;
}

/// The reason a produced batch was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    /// The error code reported to the producer. Whether the producer retries depends on it.
    pub error: ResponseError,
    /// A description of the error, reported to producers that use `Produce` version 8 or later.
    pub message: Option<String>,
    /// The index in the batch and a description of each invalid record, reported like
    /// `message`.
    pub record_errors: Vec<(usize, String)>,
}

impl From<ResponseError> for Rejection {
    fn from(error: ResponseError) -> Self {
        Rejection {
            error,
            message: None,
            record_errors: Vec::new(),
        }
    }
}

/// The topic is already registered with the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicAlreadyRegistered(pub String);

impl fmt::Display for TopicAlreadyRegistered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Kafka topic {} is already in use on this cluster",
            self.0
        )
    }
}

impl std::error::Error for TopicAlreadyRegistered {}

impl KafkaServer {
    /// Creates a server that serves no topics until they are registered.
    pub fn new(config: KafkaServerConfig) -> Self {
        KafkaServer {
            inner: Arc::new(Inner {
                config,
                state: Mutex::new(State::default()),
                transactions: Mutex::new(BTreeMap::new()),
                next_registration_id: AtomicU64::new(0),
            }),
        }
    }

    /// Returns the server's configuration.
    pub fn config(&self) -> &KafkaServerConfig {
        &self.inner.config
    }

    /// Accepts connections from `listener` forever.
    pub async fn serve(self, listener: Listener) {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("accepted Kafka connection from {addr}");
                    let server = self.clone();
                    mz_ore::task::spawn(|| format!("kafka_server_conn:{addr}"), async move {
                        server.serve_connection(stream).await
                    });
                }
                Err(e) => {
                    warn!("error accepting Kafka connection: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Serves Kafka requests on `stream` until the client disconnects or violates the protocol.
    pub async fn serve_connection<S>(&self, stream: S)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        if let Err(e) = connection::serve(self, stream).await {
            info!("closing Kafka connection: {e:#}");
        }
    }

    /// Claims `topic`, so that records produced to it are delivered to the returned
    /// registration.
    ///
    /// The topic is released when the registration is dropped.
    pub fn register_topic(
        &self,
        topic: String,
        options: TopicOptions,
    ) -> Result<TopicRegistration, TopicAlreadyRegistered> {
        let TopicOptions {
            credentials,
            validator,
            schemas,
            queue_bytes,
            compacted,
        } = options;
        let serve = |schema: Option<String>| {
            schema.and_then(|schema| match registry::ServedSchema::new(schema) {
                Ok(schema) => Some(schema),
                Err(e) => {
                    warn!("not serving a schema of Kafka topic {topic}: {e:#}");
                    None
                }
            })
        };
        let schemas = (serve(schemas.key), serve(schemas.value));
        let mut state = self.inner.state.lock().expect("lock poisoned");
        if state.topics.contains_key(&topic) || state.compacted.contains_key(&topic) {
            return Err(TopicAlreadyRegistered(topic));
        }
        let registration_id = self
            .inner
            .next_registration_id
            .fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = TopicSender::new(queue_bytes);
        state.topics.insert(
            topic.clone(),
            TopicEntry {
                registration_id,
                credentials,
                validator,
                schemas,
                tx,
            },
        );
        for compacted in state.compacted.values_mut() {
            compacted.latest.retain(
                |_, v| !matches!(&v.holder, Holder::Pending { topic: t, .. } if *t == topic),
            );
        }
        let holder = Holder::Topic(Home {
            topic: topic.clone(),
            registration_id,
        });
        state.apply_compacted(compacted, &holder);
        info!("registered Kafka topic {topic}");
        Ok(TopicRegistration {
            server: self.clone(),
            topic,
            registration_id,
            rx,
        })
    }

    /// Returns whether any registered topic accepts `credentials`.
    fn authenticate(&self, credentials: &SaslPlainCredentials) -> bool {
        let state = self.inner.state.lock().expect("lock poisoned");
        state.topics.values().any(|entry| {
            entry
                .credentials
                .as_ref()
                .is_some_and(|c| c.matches(credentials))
        })
    }

    /// Looks up the sender and validator for `topic` on behalf of a connection authenticated
    /// with `credentials`.
    fn topic_sender(
        &self,
        topic: &str,
        credentials: Option<&SaslPlainCredentials>,
    ) -> Result<(TopicSender, Option<Arc<dyn RecordValidator>>), ResponseError> {
        let state = self.inner.state.lock().expect("lock poisoned");
        let (sender, _) = state.claimed(topic, credentials)?;
        let validator = state.topics[topic].validator.clone();
        Ok((sender, validator))
    }

    /// The validator of claimed topic `topic`, if it is claimed and has one.
    fn validator(&self, topic: &str) -> Option<Arc<dyn RecordValidator>> {
        let state = self.inner.state.lock().expect("lock poisoned");
        state.topics.get(topic).and_then(|e| e.validator.clone())
    }

    /// Returns the claimed and compacted topics and whether a connection authenticated with
    /// `credentials` may produce to each.
    fn topics(&self, credentials: Option<&SaslPlainCredentials>) -> BTreeMap<String, bool> {
        let state = self.inner.state.lock().expect("lock poisoned");
        state
            .topics
            .iter()
            .map(|(name, entry)| (name.clone(), authorized(entry, credentials)))
            .chain(state.compacted.keys().map(|name| (name.clone(), true)))
            .collect()
    }

    /// Creates a compacted topic.
    fn create_compacted_topic(&self, topic: &str) -> Result<(), ResponseError> {
        let mut state = self.inner.state.lock().expect("lock poisoned");
        if state.topics.contains_key(topic) || state.compacted.contains_key(topic) {
            return Err(ResponseError::TopicAlreadyExists);
        }
        state
            .compacted
            .insert(topic.into(), CompactedTopic::default());
        info!("created compacted Kafka topic {topic}");
        Ok(())
    }

    /// Returns the offset of the next record of compacted topic `topic`.
    fn compacted_high_watermark(&self, topic: &str) -> Option<i64> {
        let state = self.inner.state.lock().expect("lock poisoned");
        state.compacted.get(topic).map(|t| t.next_offset)
    }

    /// Returns the latest records of compacted topic `topic` at or after `offset` that a
    /// connection authenticated with `credentials` may read, in offset order, and the topic's
    /// high watermark.
    ///
    /// Fails with a retriable error while a record is pending.
    fn compacted_records(
        &self,
        topic: &str,
        offset: i64,
        credentials: Option<&SaslPlainCredentials>,
    ) -> Result<(Vec<(i64, Bytes, Option<Bytes>)>, i64), ResponseError> {
        let mut state = self.inner.state.lock().expect("lock poisoned");
        state.forget_expired(topic);
        let state = &*state;
        let compacted = state
            .compacted
            .get(topic)
            .ok_or(ResponseError::UnknownTopicOrPartition)?;
        let mut records = Vec::new();
        for (key, v) in &compacted.latest {
            let readable = match &v.holder {
                Holder::Memory => true,
                Holder::Topic(home) => state
                    .topics
                    .get(&home.topic)
                    .is_some_and(|entry| authorized(entry, credentials)),
                Holder::Pending { .. } => return Err(ResponseError::LeaderNotAvailable),
            };
            if readable && v.offset >= offset {
                records.push((v.offset, key.clone(), v.value.clone()));
            }
        }
        records.sort_by_key(|(offset, _, _)| *offset);
        Ok((records, compacted.next_offset))
    }

    fn allocate_producer_id(&self) -> i64 {
        // Topics remember producers by ID across restarts and across replicas, so an ID must
        // never be handed out twice. A counter would restart, and each replica would have its
        // own, while 63 random bits make a collision vanishingly unlikely.
        let id: u64 = rand::random();
        i64::try_from(id >> 1).expect("63 bits fit")
    }
}

impl State {
    /// Looks up claimed topic `topic` on behalf of a connection authenticated with
    /// `credentials`.
    fn claimed(
        &self,
        topic: &str,
        credentials: Option<&SaslPlainCredentials>,
    ) -> Result<(TopicSender, u64), ResponseError> {
        let entry = self
            .topics
            .get(topic)
            .ok_or(ResponseError::UnknownTopicOrPartition)?;
        if !authorized(entry, credentials) {
            return Err(ResponseError::TopicAuthorizationFailed);
        }
        Ok((entry.tx.clone(), entry.registration_id))
    }

    /// The claimed topic that stores the latest record of `key` in compacted topic `topic`.
    ///
    /// Fails with a retriable error while the record is pending.
    fn compacted_home(&self, topic: &str, key: &Bytes) -> Result<Option<Home>, ResponseError> {
        let holder = self
            .compacted
            .get(topic)
            .and_then(|t| t.latest.get(key))
            .map(|v| &v.holder);
        match holder {
            None | Some(Holder::Memory) => Ok(None),
            Some(Holder::Topic(home)) => Ok(Some(home.clone())),
            Some(Holder::Pending { since, .. }) if since.elapsed() >= PENDING_RETENTION => Ok(None),
            Some(Holder::Pending { .. }) => Err(ResponseError::CoordinatorLoadInProgress),
        }
    }

    /// Forgets the records of compacted topic `topic` that were pending for too long.
    fn forget_expired(&mut self, topic: &str) {
        if let Some(compacted) = self.compacted.get_mut(topic) {
            compacted.latest.retain(|_, v| match &v.holder {
                Holder::Pending { since, .. } => since.elapsed() < PENDING_RETENTION,
                Holder::Memory | Holder::Topic(_) => true,
            });
        }
    }

    /// Makes `records` the latest records of their keys, held by `holder`. Does nothing if
    /// `holder` is a topic registration that was released.
    fn apply_compacted(&mut self, records: Vec<CompactedRecord>, holder: &Holder) {
        if let Holder::Topic(home) = holder {
            let current = self
                .topics
                .get(&home.topic)
                .is_some_and(|e| e.registration_id == home.registration_id);
            if !current {
                return;
            }
        }
        for record in records {
            let topic = self.compacted.entry(record.topic).or_default();
            let offset = topic.next_offset;
            topic.next_offset += 1;
            topic.latest.insert(
                record.key,
                CompactedValue {
                    offset,
                    value: record.value,
                    holder: holder.clone(),
                },
            );
        }
    }
}

fn authorized(entry: &TopicEntry, credentials: Option<&SaslPlainCredentials>) -> bool {
    match (&entry.credentials, credentials) {
        (None, _) => true,
        (Some(required), Some(presented)) => required.matches(presented),
        (Some(_), None) => false,
    }
}

/// A claim on a topic. Records produced to the topic are delivered through
/// [`TopicRegistration::recv`].
#[derive(Debug)]
pub struct TopicRegistration {
    server: KafkaServer,
    topic: String,
    registration_id: u64,
    rx: TopicReceiver,
}

impl TopicRegistration {
    /// The name of the registered topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Receives the next batch of produced records.
    ///
    /// Cancel safe.
    pub async fn recv(&mut self) -> ProduceBatch {
        // The server holds a sender for as long as this registration exists.
        let (batch, _permit) = self.rx.recv().await.expect("sender held by the server");
        batch
    }

    /// Polls for the next batch of produced records.
    pub fn poll_recv(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<ProduceBatch> {
        // The server holds a sender for as long as this registration exists.
        self.rx
            .poll_recv(cx)
            .map(|batch| batch.expect("sender held by the server").0)
    }

    /// Receives the next batch of produced records if one is waiting.
    pub fn try_recv(&mut self) -> Option<ProduceBatch> {
        self.rx.try_recv().ok().map(|(batch, _permit)| batch)
    }
}

impl Drop for TopicRegistration {
    fn drop(&mut self) {
        let mut state = self.server.inner.state.lock().expect("lock poisoned");
        if state
            .topics
            .get(&self.topic)
            .is_some_and(|e| e.registration_id == self.registration_id)
        {
            state.topics.remove(&self.topic);
            info!("deregistered Kafka topic {}", self.topic);
        }
        let now = Instant::now();
        for compacted in state.compacted.values_mut() {
            for v in compacted.latest.values_mut() {
                let released = matches!(
                    &v.holder,
                    Holder::Topic(home) if home.registration_id == self.registration_id
                );
                if released {
                    v.holder = Holder::Pending {
                        topic: self.topic.clone(),
                        since: now,
                    };
                }
            }
        }
    }
}

/// A record written by a Kafka producer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProducedRecord {
    /// The record key.
    pub key: Option<Bytes>,
    /// The record value. `None` is a tombstone.
    pub value: Option<Bytes>,
    /// The record headers, in the order the producer sent them.
    ///
    /// NOTE: if a producer repeats a header key, only the last value is kept.
    pub headers: Vec<(String, Option<Bytes>)>,
    /// The record timestamp, in milliseconds since the Unix epoch.
    pub timestamp: i64,
}

impl ProducedRecord {
    /// Approximately how many bytes the record takes in memory.
    pub fn size(&self) -> usize {
        let bytes = |b: &Option<Bytes>| b.as_ref().map_or(0, |b| b.len());
        std::mem::size_of::<Self>()
            + bytes(&self.key)
            + bytes(&self.value)
            + self
                .headers
                .iter()
                .map(|(k, v)| std::mem::size_of::<(String, Option<Bytes>)>() + k.len() + bytes(v))
                .sum::<usize>()
    }
}

/// A record of a compacted topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedRecord {
    /// The compacted topic.
    pub topic: String,
    /// The record key.
    pub key: Bytes,
    /// The record value. `None` is a tombstone.
    pub value: Option<Bytes>,
}

/// A batch of records produced to a topic, together with the means to acknowledge it.
#[derive(Debug)]
pub struct ProduceBatch {
    /// The records, in the order the producer sent them. Empty only for a committed transaction
    /// that wrote only `compacted`.
    pub records: Vec<ProducedRecord>,
    /// The memory the records take, as [`ProducedRecord::size`] counts it.
    pub bytes: usize,
    /// The batch's idempotence fields, if the producer is idempotent and not transactional.
    pub producer: Option<ProducerBatchId>,
    /// Compacted records that commit with the batch, at most one per key. Once the batch is
    /// acknowledged, they replace the earlier records of their keys, which the consumer must
    /// keep until then. The consumer passes the latest record of each key to
    /// [`KafkaServer::register_topic`] when it registers the topic again.
    pub compacted: Vec<CompactedRecord>,
    /// Completes the produce request for this batch.
    pub ack: ProduceAck,
}

/// The pending response to the producer of a [`ProduceBatch`].
///
/// Dropping it without calling [`ProduceAck::commit`] or [`ProduceAck::fail`] fails the batch
/// with a retriable error.
#[derive(Debug)]
pub struct ProduceAck {
    /// `None` if the producer does not wait for acknowledgements (`acks=0`).
    tx: Option<oneshot::Sender<Result<i64, ResponseError>>>,
}

impl ProduceAck {
    /// Reports that the batch is durable, with its first record at `base_offset`.
    pub fn commit(self, base_offset: i64) {
        if let Some(tx) = self.tx {
            let _ = tx.send(Ok(base_offset));
        }
    }

    /// Reports that the batch was rejected with `error`.
    pub fn fail(self, error: ResponseError) {
        if let Some(tx) = self.tx {
            let _ = tx.send(Err(error));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(bytes: usize) -> ProduceBatch {
        ProduceBatch {
            records: Vec::new(),
            bytes,
            producer: None,
            compacted: Vec::new(),
            ack: ProduceAck { tx: None },
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn topic_queue_is_bounded_in_bytes() {
        let (sender, mut rx) = TopicSender::new(100);
        sender.send(batch(60)).await.expect("room for 60 bytes");
        let mut second = std::pin::pin!(sender.send(batch(60)));
        assert!(
            futures::poll!(&mut second).is_pending(),
            "a second batch of 60 bytes waits"
        );
        drop(rx.recv().await);
        second.await.expect("room once the first batch is taken");
        drop(rx.recv().await);
        sender
            .send(batch(500))
            .await
            .expect("a batch larger than the queue fits an empty queue");
    }
}
