// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The producer engine: accumulator, sender, idempotence and transactions.
//!
//! # Data path
//!
//! [`ProducerInner::send`] runs on the caller's thread. It validates a record
//! like librdkafka's `rd_kafka_producev` (fatal error, transaction state,
//! `message.max.bytes`, queue limits, topic and partition) and encodes it
//! straight into its partition's open batch, so a queued record exists only
//! as its encoding; per record the producer keeps just the delivery opaque. A
//! batch is sealed when the next record would exceed `batch.size` (capped by
//! `message.max.bytes`) or `batch.num.messages`. Records for topics without
//! metadata wait, copied, until metadata arrives.
//!
//! One sender task per leader broker ([`sender`]) sends sealed batches and
//! open ones whose `linger.ms` expired or that a flush or commit waits for,
//! compressing them on the I/O side. Delivery reports are called from the
//! sender task, in order per partition.
//!
//! [`txn`] implements idempotence and transactions. A metadata task applies
//! every metadata update (partition counts, leaders) and once a second
//! expires records that have no leader to be sent to.
//!
//! # Locking
//!
//! All mutable state is in one mutex. Context callbacks (delivery reports,
//! logs, errors) are never called with it held, since a delivery callback may
//! produce. State changes collect their side effects in [`state::Effects`],
//! which [`ProducerInner::finish`] carries out after unlocking.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::config::settings::{ClientSettings, CompressionCodec, PartitionerKind};
use crate::error::{KafkaError, KafkaResult};
use crate::internal::cluster::{Cluster, MetadataSnapshot, StatsSource};
use crate::internal::compression::Compression;
use crate::internal::partitioner::Partitioner;
use crate::internal::records::RecordHeader;
use crate::internal::runtime;
use crate::message::{Headers, OwnedHeaders, ToBytes};
use crate::producer::{BaseRecord, ProducerContext};
use crate::statistics::{self, ExactlyOnceSemantics, Statistics};
use crate::types::RDKafkaErrorCode;
use crate::util::{Deadline, Timeout, current_time_millis};

mod delivery;
mod sender;
mod state;
#[cfg(test)]
mod tests;
mod txn;

use self::state::{
    Batch, Effects, NewRecord, PartKey, State, TopicId, TopicStatus, UaMessage, Undelivered,
    payload_len,
};
use self::txn::Eos;

/// Producer settings, derived from the client settings.
pub(super) struct Config {
    pub transactional_id: Option<String>,
    pub idempotent: bool,
    pub acks: i16,
    /// The ack timeout brokers apply (the Produce request's timeout field).
    pub request_timeout: Duration,
    pub message_timeout: Option<Duration>,
    pub linger: Duration,
    /// `min(batch.size, message.max.bytes)`.
    pub max_batch_bytes: usize,
    pub batch_num_messages: usize,
    pub queue_max_messages: Option<u64>,
    pub queue_max_bytes: u64,
    pub compression: Compression,
    pub compression_level: Option<i32>,
    pub partitioner: Partitioner,
    pub sticky_linger: Duration,
    pub retries: u32,
    pub retry_backoff: Duration,
    pub retry_backoff_max: Duration,
    pub message_max_bytes: usize,
    /// Produce requests in flight per broker.
    pub max_in_flight: usize,
    /// Batches in flight per partition: at most 5 with idempotence, the
    /// window in which brokers detect duplicates.
    pub max_in_flight_per_partition: u32,
    pub socket_timeout: Duration,
    pub transaction_timeout: Duration,
    pub delivery_report_only_error: bool,
    pub metadata_propagation_max: Duration,
}

/// Brokers remember the last five batches per producer and partition.
const IDEMPOTENCE_MAX_IN_FLIGHT: u32 = 5;

/// The interval at which the metadata task expires records without a
/// leader.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);

/// The state of one producer, shared by its facade handles and tasks.
pub(crate) struct ProducerInner<C: ProducerContext> {
    cluster: Arc<Cluster>,
    context: Arc<C>,
    config: Config,
    state: Mutex<State<C::DeliveryOpaque>>,
    /// Signaled when a transactional API result is set and when the last
    /// enqueued message is reported.
    cond: Condvar,
    /// Messages enqueued and not yet reported, librdkafka's `curr_msgs`.
    msg_cnt: AtomicU64,
    /// Payload bytes of those messages.
    msg_bytes: AtomicU64,
    /// Messages and key plus payload bytes delivered.
    txmsgs: AtomicU64,
    txmsg_bytes: AtomicU64,
    /// Flushes in progress: open batches are sent without lingering.
    flushing: AtomicUsize,
    /// Whether a fatal error was raised, for `send`'s check without locking.
    fatal: AtomicBool,
    shutdown: CancellationToken,
    eos_notify: Notify,
    weak: Weak<Self>,
}

impl<C: ProducerContext> ProducerInner<C> {
    /// Creates the producer engine on a client's cluster and starts its
    /// background tasks.
    pub(crate) fn new(cluster: Arc<Cluster>, context: Arc<C>) -> Arc<ProducerInner<C>> {
        let config = Config::from_settings(cluster.settings());
        let now = Instant::now();
        let eos = Eos::new(config.idempotent, config.transactional_id.is_some(), now);
        let shutdown = cluster.shutdown_token().child_token();
        let inner = Arc::new_cyclic(|weak| ProducerInner {
            cluster: Arc::clone(&cluster),
            context,
            config,
            state: Mutex::new(State::new(eos)),
            cond: Condvar::new(),
            msg_cnt: AtomicU64::new(0),
            msg_bytes: AtomicU64::new(0),
            txmsgs: AtomicU64::new(0),
            txmsg_bytes: AtomicU64::new(0),
            flushing: AtomicUsize::new(0),
            fatal: AtomicBool::new(false),
            shutdown,
            eos_notify: Notify::new(),
            weak: Weak::clone(weak),
        });
        let source: Arc<dyn StatsSource> = Arc::<ProducerInner<C>>::clone(&inner);
        cluster.register_stats_source(Arc::downgrade(&source));
        runtime::spawn(
            "kafka-producer-metadata",
            Arc::clone(&inner).metadata_task(),
        );
        if inner.config.idempotent {
            runtime::spawn("kafka-producer-eos", Arc::clone(&inner).eos_task());
        }
        inner
    }
}

impl Config {
    /// The producer settings of a client.
    pub(super) fn from_settings(settings: &ClientSettings) -> Config {
        let ps = &settings.producer;
        let message_max_bytes = usize::try_from(settings.message_max_bytes).expect("positive");
        let max_in_flight = usize::try_from(settings.max_in_flight)
            .expect("positive")
            .max(1);
        Config {
            transactional_id: ps.transactional_id.clone(),
            idempotent: ps.enable_idempotence,
            acks: ps.acks,
            request_timeout: ps.request_timeout,
            message_timeout: ps.message_timeout,
            linger: ps.linger,
            max_batch_bytes: message_max_bytes
                .min(usize::try_from(ps.batch_size).expect("positive")),
            batch_num_messages: usize::try_from(ps.batch_num_messages).expect("positive"),
            queue_max_messages: ps.queue_buffering_max_messages.map(u64::from),
            queue_max_bytes: ps.queue_buffering_max_bytes,
            compression: match ps.compression {
                CompressionCodec::None => Compression::None,
                CompressionCodec::Gzip => Compression::Gzip,
                CompressionCodec::Snappy => Compression::Snappy,
                CompressionCodec::Lz4 => Compression::Lz4,
                CompressionCodec::Zstd => Compression::Zstd,
            },
            compression_level: ps.compression_level,
            partitioner: match ps.partitioner {
                PartitionerKind::Random => Partitioner::Random,
                PartitionerKind::Consistent => Partitioner::Consistent,
                PartitionerKind::ConsistentRandom => Partitioner::ConsistentRandom,
                PartitionerKind::Murmur2 => Partitioner::Murmur2,
                PartitionerKind::Murmur2Random => Partitioner::Murmur2Random,
                PartitionerKind::Fnv1a => Partitioner::Fnv1a,
                PartitionerKind::Fnv1aRandom => Partitioner::Fnv1aRandom,
            },
            sticky_linger: ps.sticky_partitioning_linger,
            retries: u32::try_from(ps.retries).unwrap_or(0),
            retry_backoff: settings.retry_backoff,
            retry_backoff_max: settings.retry_backoff_max.max(settings.retry_backoff),
            message_max_bytes,
            max_in_flight,
            max_in_flight_per_partition: if ps.enable_idempotence {
                IDEMPOTENCE_MAX_IN_FLIGHT.min(u32::try_from(max_in_flight).unwrap_or(u32::MAX))
            } else {
                u32::try_from(max_in_flight).unwrap_or(u32::MAX)
            },
            socket_timeout: settings.socket_timeout,
            transaction_timeout: ps.transaction_timeout,
            delivery_report_only_error: ps.delivery_report_only_error,
            metadata_propagation_max: settings.metadata_propagation_max,
        }
    }
}

impl<C: ProducerContext> ProducerInner<C> {
    fn lock(&self) -> MutexGuard<'_, State<C::DeliveryOpaque>> {
        self.state.lock().expect("lock poisoned")
    }

    /// Enqueues a record, librdkafka's `rd_kafka_producev`. See
    /// [`ThreadedProducer::send`](crate::producer::ThreadedProducer::send).
    pub(crate) fn send<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P, C::DeliveryOpaque>,
    ) -> Result<(), (KafkaError, BaseRecord<'a, K, P, C::DeliveryOpaque>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let fail =
            |code: RDKafkaErrorCode, record| Err((KafkaError::MessageProduction(code), record));
        if self.fatal.load(Ordering::Acquire) {
            return fail(RDKafkaErrorCode::Fatal, record);
        }
        let key = record.key.map(ToBytes::to_bytes);
        let value = record.payload.map(ToBytes::to_bytes);
        let wire_size = wire_size(key, value, record.headers.as_ref());
        if key.is_some_and(|k| k.len() > I32_MAX)
            || value.is_some_and(|v| v.len() > I32_MAX)
            || wire_size > self.config.message_max_bytes
        {
            return fail(RDKafkaErrorCode::MessageSizeTooLarge, record);
        }
        // librdkafka: a timestamp of 0, like none, means now.
        let timestamp = match record.timestamp {
            Some(t) if t != 0 => t,
            _ => current_time_millis(),
        };
        let payload_len = payload_len(value);
        let kv_len = payload_len + payload_len_of(key);
        let now = Instant::now();

        let mut st = self.lock();
        if st.eos.transactional && !st.eos.may_enq {
            drop(st);
            return fail(RDKafkaErrorCode::State, record);
        }
        let cnt = self.msg_cnt.load(Ordering::SeqCst);
        let bytes = self.msg_bytes.load(Ordering::SeqCst);
        if self
            .config
            .queue_max_messages
            .is_some_and(|max| cnt + 1 > max)
            || bytes + payload_len > self.config.queue_max_bytes
        {
            drop(st);
            return fail(RDKafkaErrorCode::QueueFull, record);
        }
        let mut fx = Effects::default();
        let (id, new) = st.topic_id(record.topic, now);
        if new {
            self.cluster.add_known_topics([record.topic]);
            self.sync_topic(&mut st, id, now, &mut fx);
        }
        let mut choice =
            state::choose_partition(&mut st.topics[id], record.partition, key, &self.config, now);
        if matches!(
            choice,
            Ok(None) | Err(RDKafkaErrorCode::UnknownPartition | RDKafkaErrorCode::UnknownTopic)
        ) && self.sync_topic(&mut st, id, now, &mut fx)
        {
            // The client learned newer metadata, e.g. from `fetch_metadata`.
            choice = state::choose_partition(
                &mut st.topics[id],
                record.partition,
                key,
                &self.config,
                now,
            );
        }
        match choice {
            Err(code) => {
                self.finish(st, fx);
                return fail(code, record);
            }
            Ok(None) => {
                let topic = &mut st.topics[id];
                topic.ua.push_back(UaMessage {
                    key: key.map(<[u8]>::to_vec),
                    payload: value.map(<[u8]>::to_vec),
                    headers: record.headers,
                    timestamp,
                    partition: record.partition,
                    opaque: record.delivery_opaque,
                    enqueued: now,
                });
                fx.refresh_metadata = true;
            }
            Ok(Some(partition)) => {
                let mut stack = [RecordHeader {
                    key: "",
                    value: None,
                }; STACK_HEADERS];
                let heap: Vec<RecordHeader<'_>>;
                let headers: &[RecordHeader<'_>] = match &record.headers {
                    None => &[],
                    Some(h) if h.count() <= STACK_HEADERS => {
                        for (i, header) in record_headers(h).enumerate() {
                            stack[i] = header;
                        }
                        &stack[..h.count()]
                    }
                    Some(h) => {
                        heap = record_headers(h).collect();
                        &heap
                    }
                };
                let new_record = NewRecord {
                    timestamp,
                    key,
                    value,
                    headers,
                    wire_size,
                    payload_len,
                    kv_len,
                };
                let key = PartKey {
                    topic: id,
                    partition,
                };
                let state = &mut *st;
                let wake = state.topics[id]
                    .partition_mut(partition)
                    .expect("chosen partitions exist")
                    .append(
                        &new_record,
                        record.delivery_opaque,
                        &self.config,
                        &mut state.buffers,
                        now,
                    );
                st.added_to_partition(key, wake, &mut fx);
            }
        }
        self.msg_cnt.fetch_add(1, Ordering::SeqCst);
        self.msg_bytes.fetch_add(payload_len, Ordering::SeqCst);
        self.finish(st, fx);
        Ok(())
    }

    /// Applies the client's current metadata for a topic if it is newer than
    /// what the producer applied. Returns whether it was.
    fn sync_topic(
        &self,
        st: &mut State<C::DeliveryOpaque>,
        id: TopicId,
        now: Instant,
        fx: &mut Effects<C::DeliveryOpaque>,
    ) -> bool {
        let md = self.cluster.metadata();
        if md.version <= st.topics[id].md_version {
            return false;
        }
        let topic_md = md.topic(&st.topics[id].name);
        st.apply_topic_metadata(id, topic_md, Some(md.version), &self.config, now, fx);
        true
    }

    /// The number of messages enqueued and not yet reported.
    pub(crate) fn in_flight_count(&self) -> i32 {
        i32::try_from(self.msg_cnt.load(Ordering::SeqCst)).unwrap_or(i32::MAX)
    }

    /// Waits until every enqueued message was reported.
    pub(crate) fn flush(&self, timeout: Timeout) -> KafkaResult<()> {
        self.wait_flushed(Deadline::new(timeout))
            .map_err(|()| KafkaError::Flush(RDKafkaErrorCode::OperationTimedOut))
    }

    fn wake_all_senders(&self) {
        let notifies: Vec<Arc<Notify>> = self
            .lock()
            .senders
            .values()
            .map(|s| Arc::clone(&s.notify))
            .collect();
        for n in notifies {
            n.notify_one();
        }
    }

    /// Releases the lock and carries out the side effects of a state change.
    fn finish(&self, st: MutexGuard<'_, State<C::DeliveryOpaque>>, fx: Effects<C::DeliveryOpaque>) {
        self.finish_with_deliveries(st, fx, Vec::new());
    }

    /// Like [`ProducerInner::finish`], also reporting delivered batches
    /// before the failures.
    fn finish_with_deliveries(
        &self,
        mut st: MutexGuard<'_, State<C::DeliveryOpaque>>,
        fx: Effects<C::DeliveryOpaque>,
        delivered: Vec<(Arc<str>, i32, Batch<C::DeliveryOpaque>, i64, i64)>,
    ) {
        let Effects {
            wake,
            wake_all,
            failures,
            refresh_metadata,
            wake_eos,
            logs,
            errors,
        } = fx;
        if st.eos.fatal.is_some() {
            self.fatal.store(true, Ordering::Release);
        }
        if st.eos.transactional {
            let failed: usize = failures.iter().map(|f| f.records.len()).sum();
            st.eos.dr_fails += u64::try_from(failed).expect("fits");
        }
        let mut notifies = Vec::new();
        if wake_all || !wake.is_empty() || st.senders.values().any(|s| !s.spawned) {
            for (broker, entry) in &mut st.senders {
                if !entry.spawned {
                    entry.spawned = true;
                    if let Some(inner) = self.weak.upgrade() {
                        runtime::spawn(
                            "kafka-producer-sender",
                            inner.sender_task(*broker, Arc::clone(&entry.notify)),
                        );
                    }
                }
                if wake_all || wake.contains(broker) {
                    notifies.push(Arc::clone(&entry.notify));
                }
            }
        }
        if st.eos.api.result.is_some() {
            self.cond.notify_all();
        }
        drop(st);
        for n in notifies {
            n.notify_one();
        }
        if wake_eos {
            self.eos_notify.notify_one();
        }
        if refresh_metadata {
            self.cluster.request_metadata_refresh();
        }
        let logger = self.cluster.logger();
        for (level, fac, msg) in logs {
            logger.log(level, fac, &msg);
        }
        for (error, reason) in errors {
            self.cluster.hooks().error(error, &reason);
        }
        if delivered.is_empty() && failures.is_empty() {
            return;
        }
        let mut done_msgs = 0;
        let mut done_bytes = 0;
        let mut buffers = Vec::new();
        for (topic, partition, batch, base_offset, log_append_time) in delivered {
            let n = u64::try_from(batch.len()).expect("fits");
            done_msgs += n;
            done_bytes += batch.payload_bytes;
            self.txmsgs.fetch_add(n, Ordering::Relaxed);
            self.txmsg_bytes
                .fetch_add(batch.kv_bytes, Ordering::Relaxed);
            buffers.extend(delivery::delivered(
                &*self.context,
                self.config.delivery_report_only_error,
                &topic,
                partition,
                batch,
                base_offset,
                log_append_time,
            ));
        }
        if !buffers.is_empty() {
            let mut st = self.lock();
            for buf in buffers {
                st.buffers.put(buf);
            }
        }
        for failure in failures {
            done_msgs += u64::try_from(failure.records.len()).expect("fits");
            done_bytes += failure.records.payload_bytes();
            delivery::failed(
                &*self.context,
                &failure.topic,
                failure.partition,
                failure.records,
                failure.error,
            );
        }
        self.msg_bytes.fetch_sub(done_bytes, Ordering::SeqCst);
        let before = self.msg_cnt.fetch_sub(done_msgs, Ordering::SeqCst);
        if before == done_msgs {
            // Wake flushes. Taking the lock orders this after a waiter's
            // check of the count.
            let _st = self.lock();
            self.cond.notify_all();
        }
    }

    /// Applies every metadata update, and once a second expires records that
    /// have no leader to be sent to.
    async fn metadata_task(self: Arc<Self>) {
        let mut rx = self.cluster.subscribe_metadata();
        let mut tick = tokio::time::interval(MAINTENANCE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let maintenance = tokio::select! {
                _ = self.shutdown.cancelled() => return,
                r = rx.changed() => {
                    if r.is_err() {
                        return;
                    }
                    false
                }
                _ = tick.tick() => true,
            };
            let md = Arc::clone(&rx.borrow_and_update());
            self.apply_metadata(&md, maintenance);
        }
    }

    fn apply_metadata(&self, md: &MetadataSnapshot, maintenance: bool) {
        let now = Instant::now();
        let mut st = self.lock();
        let mut fx = Effects::default();
        for id in 0..st.topics.len() {
            let name = Arc::clone(&st.topics[id].name);
            let version = (!maintenance).then_some(md.version);
            st.apply_topic_metadata(id, md.topic(&name), version, &self.config, now, &mut fx);
        }
        if maintenance {
            self.expire_unrouted(&mut st, now, &mut fx);
            st.buffers.trim_idle();
        }
        self.finish(st, fx);
    }

    /// Fails records past `message.timeout.ms` that no sender task looks at:
    /// those waiting for topic metadata or a partition leader.
    fn expire_unrouted(
        &self,
        st: &mut State<C::DeliveryOpaque>,
        now: Instant,
        fx: &mut Effects<C::DeliveryOpaque>,
    ) {
        // Without a timeout, nothing expires.
        let timeout = self.config.message_timeout.unwrap_or(Duration::MAX);
        let expired = |since: Instant| now.saturating_duration_since(since) >= timeout;
        let mut timed_out = Vec::new();
        for topic in &mut st.topics {
            let name = Arc::clone(&topic.name);
            let mut ua_expired = 0;
            while topic.ua.front().is_some_and(|m| expired(m.enqueued)) {
                let m = topic.ua.pop_front().expect("checked");
                let partition = m.partition.unwrap_or(-1);
                fx.fail(
                    &name,
                    partition,
                    Undelivered::Ua(m),
                    RDKafkaErrorCode::MessageTimedOut,
                );
                ua_expired += 1;
            }
            if ua_expired > 0 {
                timed_out.push((Arc::clone(&name), -1, ua_expired, false));
            }
            if topic.status == TopicStatus::Unknown {
                fx.refresh_metadata = true;
            }
            for (index, p) in topic.partitions.iter_mut().enumerate() {
                if p.leader >= 0 {
                    continue;
                }
                if p.has_queued() || p.in_flight > 0 {
                    // Records wait for a leader.
                    fx.refresh_metadata = true;
                }
                let Some(timeout) = self.config.message_timeout else {
                    continue;
                };
                let partition = i32::try_from(index).expect("fits");
                let (expired, sequenced) = p.expire(&name, partition, now, timeout, fx);
                if expired > 0 {
                    timed_out.push((Arc::clone(&name), partition, expired, sequenced));
                }
            }
        }
        for (topic, partition, expired, sequenced) in timed_out {
            st.on_timeout(&topic, partition, expired, sequenced, fx);
        }
    }
}

impl<C: ProducerContext> StatsSource for ProducerInner<C> {
    fn fill(&self, stats: &mut Statistics) {
        stats.msg_cnt = self.msg_cnt.load(Ordering::Relaxed);
        stats.msg_size = self.msg_bytes.load(Ordering::Relaxed);
        stats.txmsgs = i64::try_from(self.txmsgs.load(Ordering::Relaxed)).unwrap_or(i64::MAX);
        stats.txmsg_bytes =
            i64::try_from(self.txmsg_bytes.load(Ordering::Relaxed)).unwrap_or(i64::MAX);
        let st = self.lock();
        if st.eos.idempotent {
            let ms =
                |since: Instant| i64::try_from(since.elapsed().as_millis()).unwrap_or(i64::MAX);
            stats.eos = Some(ExactlyOnceSemantics {
                idemp_state: st.eos.idemp_state.name().to_string(),
                idemp_stateage: ms(st.eos.idemp_since),
                txn_state: st.eos.txn_state.name().to_string(),
                txn_stateage: ms(st.eos.txn_since),
                txn_may_enq: st.eos.may_enq,
                producer_id: st.eos.pid.producer_id,
                producer_epoch: i64::from(st.eos.pid.producer_epoch),
                epoch_cnt: st.eos.epoch_cnt,
            });
        }
        for topic in &st.topics {
            let entry = stats
                .topics
                .entry(topic.name.to_string())
                .or_insert_with(|| statistics::Topic {
                    topic: topic.name.to_string(),
                    ..Default::default()
                });
            for (index, p) in topic.partitions.iter().enumerate() {
                let partition = i32::try_from(index).expect("fits");
                let queued: usize = p.ready.iter().chain(p.open.iter()).map(Batch::len).sum();
                let queued_bytes: u64 = p
                    .ready
                    .iter()
                    .chain(p.open.iter())
                    .map(|b| b.payload_bytes)
                    .sum();
                let tp = entry.partitions.entry(partition).or_default();
                tp.partition = partition;
                tp.broker = p.leader;
                tp.leader = p.leader;
                tp.msgq_cnt = i64::try_from(queued).unwrap_or(i64::MAX);
                tp.msgq_bytes = queued_bytes;
                tp.txmsgs = p.txmsgs;
                tp.txbytes = p.txbytes;
                tp.msgs_inflight = i64::from(p.in_flight);
                tp.next_ack_seq = i64::from(p.next_ack_seq);
                tp.next_err_seq = i64::from(p.retry_seq.unwrap_or(0));
            }
            if !topic.ua.is_empty() {
                let tp = entry.partitions.entry(-1).or_default();
                tp.partition = -1;
                tp.broker = -1;
                tp.leader = -1;
                tp.msgq_cnt = i64::try_from(topic.ua.len()).unwrap_or(i64::MAX);
                tp.msgq_bytes = topic
                    .ua
                    .iter()
                    .map(|m| payload_len(m.payload.as_deref()))
                    .sum();
            }
        }
        for (id, sender) in &st.senders {
            if let Some(b) = stats.brokers.values_mut().find(|b| b.nodeid == *id) {
                b.wakeups = Some(b.wakeups.unwrap_or(0) + sender.wakeups);
            }
        }
    }
}

/// `i32::MAX` as a length.
const I32_MAX: usize = 0x7fff_ffff;

/// Headers up to this count are converted on the stack.
const STACK_HEADERS: usize = 8;

/// The headers of a record in the codec's form.
fn record_headers(headers: &OwnedHeaders) -> impl Iterator<Item = RecordHeader<'_>> {
    headers.iter().map(|h| RecordHeader {
        key: h.key,
        value: h.value,
    })
}

fn payload_len_of(b: Option<&[u8]>) -> u64 {
    payload_len(b)
}

/// The size of a zigzag varint.
fn varint_size(v: usize) -> usize {
    let mut zigzag = u64::try_from(v).expect("fits") << 1;
    let mut size = 1;
    while zigzag >= 0x80 {
        zigzag >>= 7;
        size += 1;
    }
    size
}

/// librdkafka's estimate of a record's size, which `message.max.bytes`
/// applies to: `RD_KAFKAP_MESSAGE_V2_MAX_OVERHEAD` (the largest varint
/// framing) plus key, value and serialized headers.
fn wire_size(key: Option<&[u8]>, value: Option<&[u8]>, headers: Option<&OwnedHeaders>) -> usize {
    // Length, attributes, timestamp delta, offset delta, key length, value
    // length and header count, at their largest varint sizes.
    const MAX_OVERHEAD: usize = 5 + 1 + 10 + 5 + 5 + 5 + 5;
    let headers_size: usize = headers.map_or(0, |h| {
        h.iter()
            .map(|h| {
                let value_len = h.value.map_or(0, <[u8]>::len);
                h.key.len() + value_len + varint_size(h.key.len()) + varint_size(value_len)
            })
            .sum()
    });
    MAX_OVERHEAD + key.map_or(0, <[u8]>::len) + value.map_or(0, <[u8]>::len) + headers_size
}
