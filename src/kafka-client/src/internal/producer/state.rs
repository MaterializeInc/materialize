// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The producer's queues and the metadata they are routed by.
//!
//! Each partition has at most one open batch, which `send` encodes records
//! into, followed by sealed batches waiting to be sent. Batches that were sent
//! and must be retried go back to the front of that queue, in order. Records
//! for topics without metadata wait, copied, in the topic's unassigned (UA)
//! queue, like librdkafka's UA partition.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::config::RDKafkaLogLevel;
use crate::error::KafkaError;
use crate::internal::cluster::TopicMetadata;
use crate::internal::partitioner::StickyPartition;
use crate::internal::records::{BATCH_HEADER_LEN, BatchBuilder, EncodedBatch, RecordHeader};
use crate::message::OwnedHeaders;
use crate::types::RDKafkaErrorCode;

use super::Config;
use super::txn::{Eos, IdempState};

/// An index into [`State::topics`]. Topics are never removed.
pub(super) type TopicId = usize;

/// A partition of a topic known to the producer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct PartKey {
    pub topic: TopicId,
    pub partition: i32,
}

/// What the producer knows about a topic from metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TopicStatus {
    /// No usable metadata yet: records wait in the UA queue.
    Unknown,
    Exists,
    /// The topic does not exist (librdkafka's `NOTEXISTS`).
    NotExists,
    /// The topic has a permanent error, e.g. `TopicAuthorizationFailed`.
    Error(RDKafkaErrorCode),
}

/// The records of a batch, before and after encoding.
pub(super) enum BatchData {
    /// Uncompressed records, not yet stamped with a producer id or sequence.
    Open(BatchBuilder),
    /// Compressed and stamped, kept for retries until acknowledged.
    Encoded(EncodedBatch),
}

/// A batch of records for one partition.
pub(super) struct Batch<D> {
    pub data: BatchData,
    /// One per record, in record order.
    pub opaques: Vec<D>,
    /// Payload bytes, the unit of `queue.buffering.max.kbytes`.
    pub payload_bytes: u64,
    /// Key and payload bytes, for the `txmsg_bytes` statistic.
    pub kv_bytes: u64,
    /// When the first record was enqueued: starts `linger.ms` and
    /// `message.timeout.ms`.
    pub created: Instant,
    /// Retries counted against `message.send.max.retries`.
    pub retries: u32,
    /// Sends of this batch, including ones that did not count as retries.
    pub attempts: u32,
    pub retry_at: Option<Instant>,
    /// Whether the batch was stamped with a sequence number. Only stamped
    /// batches can leave gaps in a partition's sequence when they fail.
    pub sequenced: bool,
}

impl<D> Batch<D> {
    fn new(builder: BatchBuilder, created: Instant, capacity: usize) -> Batch<D> {
        Batch {
            data: BatchData::Open(builder),
            opaques: Vec::with_capacity(capacity),
            payload_bytes: 0,
            kv_bytes: 0,
            created,
            retries: 0,
            attempts: 0,
            retry_at: None,
            sequenced: false,
        }
    }

    pub fn len(&self) -> usize {
        self.opaques.len()
    }

    pub fn uncompressed_size(&self) -> usize {
        match &self.data {
            BatchData::Open(b) => b.uncompressed_size(),
            BatchData::Encoded(e) => e.len(),
        }
    }
}

/// A record waiting for its topic's metadata, owned.
pub(super) struct UaMessage<D> {
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub headers: Option<OwnedHeaders>,
    pub timestamp: i64,
    /// The partition the application asked for, if any.
    pub partition: Option<i32>,
    pub opaque: D,
    pub enqueued: Instant,
}

/// Whether a partition takes part in the current transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PartTxn {
    NotInTxn,
    /// Waiting to be added with AddPartitionsToTxn. Batches are not sent yet.
    Pending,
    InTxn,
}

pub(super) struct PartitionState<D> {
    /// The leader's broker id, -1 if unknown.
    pub leader: i32,
    pub open: Option<Batch<D>>,
    /// Sealed batches in order: retries first, then batches never sent.
    pub ready: VecDeque<Batch<D>>,
    /// Batches sent and not yet answered.
    pub in_flight: u32,
    /// Set after a retriable error or a leader change: no batch is sent until
    /// the in-flight ones are answered, so that retries keep their order.
    pub drain: bool,
    /// The sequence number of the next batch to stamp.
    pub next_seq: i32,
    /// The sequence number after the last acknowledged one.
    pub next_ack_seq: i32,
    /// The base sequence of the first batch waiting to be retried after a
    /// retriable error. Later in-flight batches then fail with
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER`, which is expected and retried.
    pub retry_seq: Option<i32>,
    /// Whether a batch was acknowledged (with idempotence: under the current
    /// producer id and epoch). Until then only one batch is in flight: a
    /// broker without state for the producer accepts any sequence number,
    /// so a later batch could overtake a first one that is retried.
    pub acked: bool,
    pub txn: PartTxn,
    /// The uncompressed size of the last sealed batch: the initial capacity
    /// of the next one.
    pub last_batch_size: usize,
    /// Messages and bytes delivered, for statistics.
    pub txmsgs: u64,
    pub txbytes: u64,
}

impl<D> PartitionState<D> {
    fn new(leader: i32) -> PartitionState<D> {
        PartitionState {
            leader,
            open: None,
            ready: VecDeque::new(),
            in_flight: 0,
            drain: false,
            next_seq: 0,
            next_ack_seq: 0,
            retry_seq: None,
            acked: false,
            txn: PartTxn::NotInTxn,
            last_batch_size: 0,
            txmsgs: 0,
            txbytes: 0,
        }
    }

    /// Whether the partition holds unsent records.
    pub fn has_queued(&self) -> bool {
        self.open.is_some() || !self.ready.is_empty()
    }

    /// Moves the open batch to the end of the ready queue.
    pub fn seal(&mut self) {
        if let Some(batch) = self.open.take() {
            self.last_batch_size = batch.uncompressed_size();
            self.ready.push_back(batch);
        }
    }

    /// Fails the batches whose `message.timeout.ms` expired, oldest first.
    /// Returns the number of records that expired and whether any of them
    /// had been stamped with a sequence number.
    pub fn expire(
        &mut self,
        topic: &Arc<str>,
        partition: i32,
        now: Instant,
        timeout: Duration,
        fx: &mut Effects<D>,
    ) -> (usize, bool) {
        let mut expired = 0;
        let mut sequenced = false;
        while self
            .ready
            .front()
            .is_some_and(|b| b.created + timeout <= now)
        {
            let batch = self.ready.pop_front().expect("checked");
            expired += batch.len();
            sequenced |= batch.sequenced;
            fx.fail(
                topic,
                partition,
                Undelivered::Batch(batch),
                RDKafkaErrorCode::MessageTimedOut,
            );
        }
        if self.ready.is_empty()
            && self
                .open
                .as_ref()
                .is_some_and(|b| b.created + timeout <= now)
        {
            let batch = self.open.take().expect("checked");
            expired += batch.len();
            fx.fail(
                topic,
                partition,
                Undelivered::Batch(batch),
                RDKafkaErrorCode::MessageTimedOut,
            );
        }
        (expired, sequenced)
    }

    /// Puts a batch that was sent back in front of the batches never sent,
    /// after any batches already waiting to be retried. Batches come back in
    /// the order they were sent, because responses for one partition arrive
    /// in order and nothing new is sent while the partition drains.
    pub fn requeue(&mut self, batch: Batch<D>) {
        let at = self.ready.iter().take_while(|b| b.attempts > 0).count();
        self.ready.insert(at, batch);
    }

    /// Appends a record to the open batch, sealing it first if the record
    /// does not fit, and seals it afterwards if it is full. Returns whether
    /// the partition's sender should be woken: the partition was empty or a
    /// batch was sealed.
    pub fn append(
        &mut self,
        record: &NewRecord<'_>,
        opaque: D,
        cfg: &Config,
        pool: &mut BufferPool,
        now: Instant,
    ) -> bool {
        let mut wake = !self.has_queued();
        if let Some(open) = &self.open {
            let count = open.len();
            if count >= cfg.batch_num_messages
                || (count > 0 && open.uncompressed_size() + record.wire_size > cfg.max_batch_bytes)
            {
                self.seal();
                wake = true;
            }
        }
        let open = match &mut self.open {
            Some(open) => open,
            None => {
                // Start at the size of the previous batch so that a steady
                // stream rarely grows the buffer.
                // Start a quarter above the size of the previous batch, so
                // that a steady stream does not grow the buffer. A batch
                // exceeds `max_batch_bytes` by at most one record.
                let capacity = (self.last_batch_size + self.last_batch_size / 4)
                    .max(record.wire_size)
                    .max(MIN_BATCH_CAPACITY)
                    .min(cfg.max_batch_bytes + record.wire_size);
                let builder = BatchBuilder::with_buffer(
                    cfg.compression,
                    cfg.compression_level,
                    cfg.transactional_id.is_some(),
                    pool.take(capacity + BATCH_HEADER_LEN),
                    capacity,
                );
                let records = if self.last_batch_size == 0 {
                    16
                } else {
                    cfg.batch_num_messages.min(1024)
                };
                self.open.insert(Batch::new(builder, now, records))
            }
        };
        let BatchData::Open(builder) = &mut open.data else {
            unreachable!("the open batch is never encoded");
        };
        builder.append(record.timestamp, record.key, record.value, record.headers);
        let size = builder.uncompressed_size();
        open.opaques.push(opaque);
        open.payload_bytes += record.payload_len;
        open.kv_bytes += record.kv_len;
        if open.len() >= cfg.batch_num_messages || size >= cfg.max_batch_bytes {
            self.seal();
            wake = true;
        }
        wake
    }
}

/// The smallest initial capacity of a batch buffer.
const MIN_BATCH_CAPACITY: usize = 1024;

/// Buffers of batches that were sent or compressed, for new batches to
/// reuse. Batches are large, short-lived allocations; recycling them saves
/// the allocator's work and keeps it from retaining freed pages.
#[derive(Debug, Default)]
pub(super) struct BufferPool {
    bufs: Vec<Vec<u8>>,
    bytes: usize,
    /// Whether a buffer was taken since the last [`BufferPool::trim_idle`].
    used: bool,
}

/// The pool's limits. Smaller buffers are cheap to allocate.
const POOL_MIN_BUFFER: usize = 64 << 10;
const POOL_MAX_BUFFERS: usize = 64;
const POOL_MAX_BYTES: usize = 32 << 20;

impl BufferPool {
    /// A buffer for about `size` bytes: the smallest pooled one that fits,
    /// unless that is much larger than needed, else a new one.
    pub fn take(&mut self, size: usize) -> Vec<u8> {
        if size < POOL_MIN_BUFFER {
            return Vec::with_capacity(size);
        }
        self.used = true;
        let best = self
            .bufs
            .iter()
            .enumerate()
            .filter(|(_, b)| b.capacity() >= size)
            .min_by_key(|(_, b)| b.capacity())
            .map(|(i, _)| i);
        match best {
            Some(i) if self.bufs[i].capacity() / 8 <= size => {
                let buf = self.bufs.swap_remove(i);
                self.bytes -= buf.capacity();
                buf
            }
            _ => Vec::with_capacity(size),
        }
    }

    pub fn put(&mut self, buf: Vec<u8>) {
        let capacity = buf.capacity();
        if capacity >= POOL_MIN_BUFFER
            && self.bufs.len() < POOL_MAX_BUFFERS
            && self.bytes + capacity <= POOL_MAX_BYTES
        {
            self.bytes += capacity;
            self.bufs.push(buf);
        }
    }

    /// Frees the pooled buffers if none was taken since the previous call.
    pub fn trim_idle(&mut self) {
        if !self.used {
            self.bufs.clear();
            self.bytes = 0;
        }
        self.used = false;
    }
}

/// A record to append, validated by `send`.
pub(super) struct NewRecord<'a> {
    pub timestamp: i64,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: &'a [RecordHeader<'a>],
    /// librdkafka's estimate of the encoded size, which `batch.size` and
    /// `message.max.bytes` apply to.
    pub wire_size: usize,
    pub payload_len: u64,
    pub kv_len: u64,
}

pub(super) struct TopicState<D> {
    pub name: Arc<str>,
    pub status: TopicStatus,
    pub partitions: Vec<PartitionState<D>>,
    pub ua: VecDeque<UaMessage<D>>,
    pub sticky: StickyPartition,
    /// When the producer first used the topic, which starts
    /// `topic.metadata.propagation.max.ms`.
    pub first_used: Instant,
    /// The version of the metadata snapshot last applied.
    pub md_version: u64,
}

impl<D> TopicState<D> {
    pub fn partition(&self, partition: i32) -> Option<&PartitionState<D>> {
        usize::try_from(partition)
            .ok()
            .and_then(|p| self.partitions.get(p))
    }

    pub fn partition_mut(&mut self, partition: i32) -> Option<&mut PartitionState<D>> {
        usize::try_from(partition)
            .ok()
            .and_then(|p| self.partitions.get_mut(p))
    }
}

/// A sender task's registration.
pub(super) struct SenderEntry {
    pub notify: Arc<Notify>,
    /// The partitions this broker leads.
    pub parts: BTreeSet<PartKey>,
    /// Whether the task was started.
    pub spawned: bool,
}

/// Records that will not be delivered, to report after the lock is released.
pub(super) enum Undelivered<D> {
    Batch(Batch<D>),
    Ua(UaMessage<D>),
}

impl<D> Undelivered<D> {
    pub fn len(&self) -> usize {
        match self {
            Undelivered::Batch(b) => b.len(),
            Undelivered::Ua(_) => 1,
        }
    }

    pub fn payload_bytes(&self) -> u64 {
        match self {
            Undelivered::Batch(b) => b.payload_bytes,
            Undelivered::Ua(m) => payload_len(m.payload.as_deref()),
        }
    }
}

/// A failed delivery to report.
pub(super) struct Failure<D> {
    pub topic: Arc<str>,
    pub partition: i32,
    pub records: Undelivered<D>,
    pub error: RDKafkaErrorCode,
}

/// Side effects of a state change, carried out once the lock is released.
pub(super) struct Effects<D> {
    /// Brokers whose sender tasks to wake.
    pub wake: BTreeSet<i32>,
    /// Wake every sender task.
    pub wake_all: bool,
    pub failures: Vec<Failure<D>>,
    pub refresh_metadata: bool,
    /// Wake the idempotence and transaction task.
    pub wake_eos: bool,
    /// Log lines for the context's `log` callback.
    pub logs: Vec<(RDKafkaLogLevel, &'static str, String)>,
    /// Errors for the context's `error` callback.
    pub errors: Vec<(KafkaError, String)>,
}

impl<D> Default for Effects<D> {
    fn default() -> Effects<D> {
        Effects {
            wake: BTreeSet::new(),
            wake_all: false,
            failures: Vec::new(),
            refresh_metadata: false,
            wake_eos: false,
            logs: Vec::new(),
            errors: Vec::new(),
        }
    }
}

impl<D> Effects<D> {
    pub fn log(&mut self, level: RDKafkaLogLevel, fac: &'static str, msg: String) {
        self.logs.push((level, fac, msg));
    }

    pub fn fail(
        &mut self,
        topic: &Arc<str>,
        partition: i32,
        records: Undelivered<D>,
        error: RDKafkaErrorCode,
    ) {
        self.failures.push(Failure {
            topic: Arc::clone(topic),
            partition,
            records,
            error,
        });
    }
}

/// The payload length accounted for `queue.buffering.max.kbytes`.
pub(super) fn payload_len(payload: Option<&[u8]>) -> u64 {
    u64::try_from(payload.map_or(0, <[u8]>::len)).expect("lengths fit u64")
}

/// All mutable state of a producer.
pub(super) struct State<D> {
    pub topics: Vec<TopicState<D>>,
    topic_ids: BTreeMap<Arc<str>, TopicId>,
    /// The topic of the previous `send`, to skip the lookup.
    last_topic: Option<TopicId>,
    pub senders: BTreeMap<i32, SenderEntry>,
    pub eos: Eos,
    /// Batches in flight over all partitions.
    pub in_flight: u32,
    pub buffers: BufferPool,
}

impl<D> State<D> {
    pub fn new(eos: Eos) -> State<D> {
        State {
            topics: Vec::new(),
            topic_ids: BTreeMap::new(),
            last_topic: None,
            senders: BTreeMap::new(),
            eos,
            in_flight: 0,
            buffers: BufferPool::default(),
        }
    }

    /// Looks up a topic, adding it if new. Returns its id and whether it is
    /// new.
    pub fn topic_id(&mut self, name: &str, now: Instant) -> (TopicId, bool) {
        if let Some(id) = self.last_topic {
            if &*self.topics[id].name == name {
                return (id, false);
            }
        }
        if let Some(id) = self.topic_ids.get(name) {
            self.last_topic = Some(*id);
            return (*id, false);
        }
        let id = self.topics.len();
        let name: Arc<str> = Arc::from(name);
        self.topics.push(TopicState {
            name: Arc::clone(&name),
            status: TopicStatus::Unknown,
            partitions: Vec::new(),
            ua: VecDeque::new(),
            sticky: StickyPartition::default(),
            first_used: now,
            md_version: 0,
        });
        self.topic_ids.insert(name, id);
        self.last_topic = Some(id);
        (id, true)
    }

    pub fn partition_mut(&mut self, key: PartKey) -> &mut PartitionState<D> {
        self.topics[key.topic]
            .partition_mut(key.partition)
            .expect("partition keys refer to existing partitions")
    }

    /// The sender registration of a broker, created if needed.
    pub fn sender(&mut self, broker: i32) -> &mut SenderEntry {
        self.senders.entry(broker).or_insert_with(|| SenderEntry {
            notify: Arc::new(Notify::new()),
            parts: BTreeSet::new(),
            spawned: false,
        })
    }

    /// Applies a topic's metadata from a snapshot of `version`, unless an
    /// equal or newer one was applied. `md` is `None` if the snapshot does
    /// not mention the topic. A `version` of `None` applies the metadata
    /// again, to re-evaluate time-dependent decisions.
    ///
    /// Updates the partition count and leaders, moves partitions between
    /// senders, re-routes UA records once the topic exists, and fails UA
    /// records of topics that turn out not to exist or have a permanent
    /// error. Partitions whose leader changed wait for their in-flight
    /// batches before sending to the new leader.
    pub fn apply_topic_metadata(
        &mut self,
        id: TopicId,
        md: Option<&TopicMetadata>,
        version: Option<u64>,
        cfg: &Config,
        now: Instant,
        fx: &mut Effects<D>,
    ) {
        let topic = &mut self.topics[id];
        if let Some(version) = version {
            if version <= topic.md_version {
                return;
            }
            topic.md_version = version;
        }
        let Some(md) = md else {
            if topic.status == TopicStatus::Unknown {
                fx.refresh_metadata = true;
            }
            return;
        };
        match md.error {
            None => {
                topic.status = TopicStatus::Exists;
                let count = md.partitions.len();
                let mut leader_changes = Vec::new();
                for (index, pm) in md.partitions.iter().enumerate() {
                    let partition = i32::try_from(index).expect("partition count fits i32");
                    // Metadata lists partitions sorted by id; a gap means a
                    // partition without metadata.
                    let leader = if pm.id == partition && pm.leader >= 0 {
                        pm.leader
                    } else {
                        -1
                    };
                    match topic.partitions.get_mut(index) {
                        Some(p) if p.leader != leader => {
                            leader_changes.push((partition, p.leader, leader));
                            p.leader = leader;
                            if p.in_flight > 0 {
                                p.drain = true;
                            }
                        }
                        Some(_) => {}
                        None => {
                            topic.partitions.push(PartitionState::new(leader));
                            leader_changes.push((partition, -1, leader));
                        }
                    }
                }
                if topic.partitions.len() > count {
                    // Partition counts only grow unless the topic was
                    // recreated. Records for partitions that are gone cannot
                    // be delivered.
                    let name = Arc::clone(&topic.name);
                    for (index, mut p) in topic.partitions.drain(count..).enumerate() {
                        let partition =
                            i32::try_from(count + index).expect("partition count fits i32");
                        p.seal();
                        for batch in p.ready.drain(..) {
                            fx.fail(
                                &name,
                                partition,
                                Undelivered::Batch(batch),
                                RDKafkaErrorCode::UnknownPartition,
                            );
                        }
                        if p.leader >= 0 {
                            leader_changes.push((partition, p.leader, -1));
                        }
                    }
                }
                for (partition, old, new) in leader_changes {
                    let key = PartKey {
                        topic: id,
                        partition,
                    };
                    if old >= 0 {
                        if let Some(s) = self.senders.get_mut(&old) {
                            s.parts.remove(&key);
                        }
                    }
                    if new >= 0 {
                        self.sender(new).parts.insert(key);
                        fx.wake.insert(new);
                    }
                }
                // Like librdkafka's fast leader queries: keep refreshing while
                // records wait for a leader.
                if self.topics[id]
                    .partitions
                    .iter()
                    .any(|p| p.leader < 0 && (p.has_queued() || p.in_flight > 0))
                {
                    fx.refresh_metadata = true;
                }
                self.route_ua(id, cfg, now, fx);
            }
            Some(RDKafkaErrorCode::UnknownTopicOrPartition) => {
                let known_before = topic.status == TopicStatus::Exists;
                if known_before
                    || now.saturating_duration_since(topic.first_used)
                        >= cfg.metadata_propagation_max
                {
                    topic.status = TopicStatus::NotExists;
                    // Like librdkafka, queued records fail with the
                    // metadata's error; later sends with `__UNKNOWN_TOPIC`.
                    let name = Arc::clone(&topic.name);
                    for m in topic.ua.drain(..) {
                        let partition = m.partition.unwrap_or(-1);
                        fx.fail(
                            &name,
                            partition,
                            Undelivered::Ua(m),
                            RDKafkaErrorCode::UnknownTopicOrPartition,
                        );
                    }
                } else {
                    fx.refresh_metadata = true;
                }
            }
            Some(code) if is_permanent_topic_error(code) => {
                topic.status = TopicStatus::Error(code);
                let name = Arc::clone(&topic.name);
                for m in topic.ua.drain(..) {
                    let partition = m.partition.unwrap_or(-1);
                    fx.fail(&name, partition, Undelivered::Ua(m), code);
                }
            }
            Some(_) => {
                // Retriable, e.g. LEADER_NOT_AVAILABLE while the topic is
                // being created.
                fx.refresh_metadata = true;
            }
        }
    }

    /// Moves UA records of a topic that now exists into their partitions,
    /// failing those whose explicit partition does not exist.
    fn route_ua(&mut self, id: TopicId, cfg: &Config, now: Instant, fx: &mut Effects<D>) {
        let topic = &mut self.topics[id];
        if topic.ua.is_empty() || topic.partitions.is_empty() {
            return;
        }
        let name = Arc::clone(&topic.name);
        let ua = std::mem::take(&mut topic.ua);
        let mut touched = BTreeSet::new();
        for m in ua {
            let topic = &mut self.topics[id];
            let partition = match choose_partition(topic, m.partition, m.key.as_deref(), cfg, now) {
                Ok(Some(p)) => p,
                Ok(None) => unreachable!("the topic exists with partitions"),
                Err(code) => {
                    let partition = m.partition.unwrap_or(-1);
                    fx.fail(&name, partition, Undelivered::Ua(m), code);
                    continue;
                }
            };
            let mut header_buf = Vec::new();
            if let Some(h) = &m.headers {
                header_buf.extend(super::record_headers(h));
            }
            let record = NewRecord {
                timestamp: m.timestamp,
                key: m.key.as_deref(),
                value: m.payload.as_deref(),
                headers: &header_buf,
                wire_size: super::wire_size(
                    m.key.as_deref(),
                    m.payload.as_deref(),
                    m.headers.as_ref(),
                ),
                payload_len: payload_len(m.payload.as_deref()),
                kv_len: payload_len(m.payload.as_deref()) + payload_len(m.key.as_deref()),
            };
            let p = topic
                .partition_mut(partition)
                .expect("chosen partitions exist");
            p.append(&record, m.opaque, cfg, &mut self.buffers, m.enqueued);
            touched.insert(partition);
        }
        for partition in touched {
            self.added_to_partition(
                PartKey {
                    topic: id,
                    partition,
                },
                true,
                fx,
            );
        }
    }

    /// Bookkeeping after records were appended to a partition: transaction
    /// registration and waking the leader's sender.
    pub fn added_to_partition(&mut self, key: PartKey, wake: bool, fx: &mut Effects<D>) {
        let transactional = self.eos.transactional;
        let p = self.partition_mut(key);
        let leader = p.leader;
        if transactional && p.txn == PartTxn::NotInTxn {
            p.txn = PartTxn::Pending;
            self.eos.pending.insert(key);
            fx.wake_eos = true;
        }
        if wake && leader >= 0 {
            fx.wake.insert(leader);
        }
    }

    /// Removes all records that were never sent, from the open and ready
    /// batches of every partition and from the UA queues, for librdkafka's
    /// `rd_kafka_purge(RD_KAFKA_PURGE_F_QUEUE)`. Batches that were sent before
    /// stay to keep sequence numbers gapless; they are delivered or fail on
    /// their own.
    pub fn purge_unsent(&mut self, fx: &mut Effects<D>) {
        for topic in &mut self.topics {
            let name = &topic.name;
            for m in topic.ua.drain(..) {
                let partition = m.partition.unwrap_or(-1);
                fx.fail(
                    name,
                    partition,
                    Undelivered::Ua(m),
                    RDKafkaErrorCode::PurgeQueue,
                );
            }
            for (index, p) in topic.partitions.iter_mut().enumerate() {
                let partition = i32::try_from(index).expect("partition count fits i32");
                p.seal();
                let sent = p.ready.iter().take_while(|b| b.attempts > 0).count();
                for batch in p.ready.drain(sent..) {
                    fx.fail(
                        name,
                        partition,
                        Undelivered::Batch(batch),
                        RDKafkaErrorCode::PurgeQueue,
                    );
                }
            }
        }
    }

    /// Reacts to records that timed out before delivery, like librdkafka's
    /// message timeout scan: the transaction becomes abortable. Records that
    /// had been sent leave a gap in the partition's sequence, which requires
    /// an epoch bump.
    pub fn on_timeout(
        &mut self,
        topic: &str,
        partition: i32,
        expired: usize,
        sequenced: bool,
        fx: &mut Effects<D>,
    ) {
        let reason = format!("{expired} message(s) timed out on {topic} [{partition}]");
        if self.eos.transactional {
            self.set_abortable(RDKafkaErrorCode::OperationTimedOut, sequenced, reason, fx);
        } else if sequenced && self.eos.idemp_state == IdempState::Assigned {
            self.drain_epoch_bump(false, RDKafkaErrorCode::OperationTimedOut, reason, fx);
        }
    }

    /// Resets the transaction membership of all partitions.
    pub fn clear_txn_partitions(&mut self, only_pending: bool) {
        for topic in &mut self.topics {
            for p in &mut topic.partitions {
                if !only_pending || p.txn == PartTxn::Pending {
                    p.txn = PartTxn::NotInTxn;
                }
            }
        }
        self.eos.pending.clear();
        self.eos.registering.clear();
    }
}

/// Topic errors that librdkafka treats as permanent (topic state `ERROR`).
fn is_permanent_topic_error(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::TopicAuthorizationFailed | RDKafkaErrorCode::InvalidTopic
    )
}

/// Picks the partition of a record like librdkafka's
/// `rd_kafka_msg_partitioner`. `Ok(None)` means the topic has no usable
/// metadata yet and the record goes to the UA queue.
///
/// An explicit partition of -1 is librdkafka's unassigned partition and runs
/// the partitioner.
pub(super) fn choose_partition<D>(
    topic: &mut TopicState<D>,
    explicit: Option<i32>,
    key: Option<&[u8]>,
    cfg: &Config,
    now: Instant,
) -> Result<Option<i32>, RDKafkaErrorCode> {
    match topic.status {
        TopicStatus::Unknown => return Ok(None),
        TopicStatus::NotExists => return Err(RDKafkaErrorCode::UnknownTopic),
        TopicStatus::Error(code) => return Err(code),
        TopicStatus::Exists => {}
    }
    let count = i32::try_from(topic.partitions.len()).expect("partition count fits i32");
    if count == 0 {
        return Ok(None);
    }
    let partition = match explicit.filter(|p| *p != -1) {
        Some(p) => p,
        None => {
            let parts = &topic.partitions;
            let available = |p: i32| {
                usize::try_from(p)
                    .ok()
                    .and_then(|p| parts.get(p))
                    .is_some_and(|p| p.leader >= 0)
            };
            if cfg.partitioner.is_sticky(key, cfg.sticky_linger) {
                topic
                    .sticky
                    .partition(count, cfg.sticky_linger, now, available)
            } else {
                cfg.partitioner.partition_with(key, count, available)
            }
        }
    };
    if partition < 0 || partition >= count {
        return Err(RDKafkaErrorCode::UnknownPartition);
    }
    Ok(Some(partition))
}
