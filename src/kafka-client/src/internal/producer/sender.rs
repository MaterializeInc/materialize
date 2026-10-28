// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The sender task of one leader broker.
//!
//! It picks at most one batch per partition the broker leads (sealing open
//! batches whose linger expired, or all of them while a flush or commit
//! waits), stamps and compresses them, and queues one ProduceRequest on the
//! broker's connection, repeating while partitions have batches and the
//! connection has in-flight slots. Requests are queued from this task only,
//! so they are written in the order their batches were stamped.
//!
//! Responses arrive in request order (Kafka answers a connection's requests
//! in order), and the task handles each one: delivery reports for delivered
//! batches, retries in order after the partition's in-flight batches are
//! answered, and idempotence and transaction error handling following
//! librdkafka's `rd_kafka_handle_Produce_error`.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse, TopicName, TransactionalId};
use kafka_protocol::protocol::StrBytes;
use rand::RngExt;
use tokio::sync::Notify;

use crate::internal::compression::Compression;
use crate::internal::net::{Connection, NetError, RequestOptions};
use crate::internal::protocol;
use crate::internal::records::{ProducerIdentity, increment_sequence};
use crate::producer::ProducerContext;
use crate::types::RDKafkaErrorCode;
use crate::util::Deadline;

use super::ProducerInner;
use super::state::{Batch, BatchData, Effects, PartKey, PartTxn, State, Undelivered};
use super::txn::{IdempState, pid_display};

/// A soft limit on the record bytes of one ProduceRequest.
const MAX_REQUEST_BYTES: usize = 32 << 20;

/// Batches at least this large are compressed with gzip or zstd on a blocking
/// thread, so that a `kafka-io` worker is not held for milliseconds.
const OFFLOAD_COMPRESSION_BYTES: usize = 256 << 10;

/// A batch picked for sending.
struct Picked<D> {
    key: PartKey,
    topic: Arc<str>,
    batch: Batch<D>,
    /// The identity and base sequence to stamp.
    ident: ProducerIdentity,
    base_seq: i32,
    /// A buffer to compress into.
    scratch: Option<Vec<u8>>,
}

/// A batch in a request awaiting its response.
pub(super) struct Sent<D> {
    key: PartKey,
    topic: Arc<str>,
    batch: Batch<D>,
    base_seq: i32,
}

type Response<D> = (Vec<Sent<D>>, Result<Option<ProduceResponse>, NetError>);

/// How a batch's produce result is handled.
enum Outcome {
    Delivered {
        base_offset: i64,
        log_append_time: i64,
    },
    Retry {
        /// The error, reported if the batch cannot be retried.
        code: RDKafkaErrorCode,
        /// Whether the retry counts against `message.send.max.retries`.
        counts: bool,
        refresh: bool,
    },
    Failed(RDKafkaErrorCode),
}

impl<C: ProducerContext> ProducerInner<C> {
    /// Runs the sender of broker `broker_id` until the client shuts down.
    pub(super) async fn sender_task(self: Arc<Self>, broker_id: i32, notify: Arc<Notify>) {
        let shutdown = self.shutdown.clone();
        let mut in_flight: FuturesOrdered<BoxFuture<'static, Response<C::DeliveryOpaque>>> =
            FuturesOrdered::new();
        let mut conn: Option<Arc<Connection>> = None;
        let mut connecting: Option<BoxFuture<'static, Result<Arc<Connection>, NetError>>> = None;
        let mut connect_retry_at: Option<Instant> = None;
        loop {
            let now = Instant::now();
            if conn.as_ref().is_some_and(|c| c.is_closed()) {
                conn = None;
            }
            let mut next_wake;
            loop {
                let slots = if conn.is_some() {
                    self.config.max_in_flight.saturating_sub(in_flight.len())
                } else {
                    0
                };
                let (picked, wants_conn, next) = {
                    let mut st = self.lock();
                    let mut fx = Effects::default();
                    let r = self.pick(&mut st, broker_id, now, slots > 0, &mut fx);
                    self.finish(st, fx);
                    r
                };
                next_wake = next;
                if wants_conn
                    && conn.is_none()
                    && connecting.is_none()
                    && connect_retry_at.is_none_or(|at| at <= now)
                {
                    match self.cluster.broker(broker_id) {
                        Some(broker) => {
                            let deadline = Deadline::new(self.config.socket_timeout);
                            connecting =
                                Some(Box::pin(async move { broker.connection(deadline).await }));
                        }
                        None => {
                            self.cluster.request_metadata_refresh();
                            connect_retry_at = Some(now + self.config.retry_backoff);
                        }
                    }
                }
                if picked.is_empty() {
                    break;
                }
                let conn = conn.as_ref().expect("picked batches need a connection");
                let response = self.send_request(conn, picked).await;
                in_flight.push_back(response);
            }
            if let Some(at) = connect_retry_at {
                next_wake = Some(next_wake.map_or(at, |n| n.min(at)));
            }
            let sleep = async {
                match next_wake {
                    Some(at) => tokio::time::sleep_until(at.into()).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = notify.notified() => {}
                _ = sleep => {}
                Some((sent, result)) = in_flight.next(), if !in_flight.is_empty() => {
                    self.handle_result(broker_id, sent, result);
                    // Handle the responses that are already in before picking
                    // again.
                    while let Some(Some((sent, result))) = futures::FutureExt::now_or_never(in_flight.next()) {
                        self.handle_result(broker_id, sent, result);
                    }
                }
                r = async { connecting.as_mut().expect("guarded").await }, if connecting.is_some() => {
                    connecting = None;
                    match r {
                        Ok(c) => {
                            conn = Some(c);
                            connect_retry_at = None;
                        }
                        Err(e) => {
                            if e.code() == RDKafkaErrorCode::BrokerDestroy {
                                return;
                            }
                            // The leader may have moved.
                            self.cluster.request_metadata_refresh();
                            connect_retry_at = Some(Instant::now() + self.config.retry_backoff);
                        }
                    }
                }
            }
        }
    }

    /// Picks at most one batch per partition led by `broker` for one request,
    /// and fails batches whose `message.timeout.ms` expired. Returns the
    /// picked batches, whether batches are waiting for a connection, and
    /// when to wake up next.
    fn pick(
        &self,
        st: &mut State<C::DeliveryOpaque>,
        broker: i32,
        now: Instant,
        may_send: bool,
        fx: &mut Effects<C::DeliveryOpaque>,
    ) -> (Vec<Picked<C::DeliveryOpaque>>, bool, Option<Instant>) {
        let cfg = &self.config;
        let Some(entry) = st.senders.get(&broker) else {
            return (Vec::new(), false, None);
        };
        let parts: Vec<PartKey> = entry.parts.iter().copied().collect();
        let flushing = self.flushing.load(Ordering::SeqCst) > 0;
        let eos_may_send = st.eos.may_send();
        let transactional = st.eos.transactional;
        let idempotent = st.eos.idempotent;
        let ident = if idempotent {
            st.eos.pid
        } else {
            ProducerIdentity::NONE
        };
        let mut next: Option<Instant> = None;
        let mut wake_at = |at: Instant| next = Some(next.map_or(at, |n: Instant| n.min(at)));
        let mut picked = Vec::new();
        let mut bytes = 0;
        let mut wants_conn = false;
        let mut timed_out = Vec::new();
        for key in parts {
            let topic = Arc::clone(&st.topics[key.topic].name);
            let Some(p) = st.topics[key.topic].partition_mut(key.partition) else {
                continue;
            };
            if let Some(timeout) = cfg.message_timeout {
                let (expired, sequenced) = p.expire(&topic, key.partition, now, timeout, fx);
                if expired > 0 {
                    timed_out.push((Arc::clone(&topic), key.partition, expired, sequenced));
                }
                if let Some(b) = p.ready.front().or(p.open.as_ref()) {
                    wake_at(b.created + timeout);
                }
            }
            if !p.has_queued() {
                continue;
            }
            let txn_ok = !transactional || p.txn == PartTxn::InTxn;
            if !eos_may_send
                || !txn_ok
                || p.drain
                || p.in_flight
                    >= if p.acked {
                        cfg.max_in_flight_per_partition
                    } else {
                        1
                    }
            {
                continue;
            }
            if p.ready.is_empty() {
                let open = p.open.as_ref().expect("has_queued");
                let ready_at = open.created + cfg.linger;
                if !flushing && ready_at > now {
                    wake_at(ready_at);
                    continue;
                }
                p.seal();
            }
            let front = p.ready.front().expect("sealed");
            if let Some(at) = front.retry_at.filter(|at| *at > now) {
                wake_at(at);
                continue;
            }
            if !may_send {
                wants_conn = true;
                continue;
            }
            if !picked.is_empty() && bytes + front.uncompressed_size() > MAX_REQUEST_BYTES {
                // The next request takes it.
                wake_at(now);
                continue;
            }
            let mut batch = p.ready.pop_front().expect("checked");
            let base_seq = if idempotent {
                let stale = match &batch.data {
                    BatchData::Encoded(e) => e.producer_identity() != ident,
                    BatchData::Open(_) => true,
                };
                if stale || !batch.sequenced {
                    let seq = p.next_seq;
                    p.next_seq = increment_sequence(
                        seq,
                        i32::try_from(batch.len()).expect("batch.num.messages fits i32"),
                    );
                    batch.sequenced = true;
                    seq
                } else {
                    match &batch.data {
                        BatchData::Encoded(e) => e.base_sequence(),
                        BatchData::Open(_) => unreachable!("open batches are stale"),
                    }
                }
            } else {
                -1
            };
            batch.attempts += 1;
            batch.retry_at = None;
            p.in_flight += 1;
            bytes += batch.uncompressed_size();
            let scratch = (matches!(batch.data, BatchData::Open(_))
                && cfg.compression != Compression::None)
                .then(|| st.buffers.take(batch.uncompressed_size()));
            picked.push(Picked {
                key,
                topic,
                batch,
                ident,
                base_seq,
                scratch,
            });
        }
        st.in_flight += u32::try_from(picked.len()).expect("few batches");
        for (topic, partition, expired, sequenced) in timed_out {
            st.on_timeout(&topic, partition, expired, sequenced, fx);
        }
        (picked, wants_conn, next)
    }

    /// Stamps, compresses and sends picked batches in one ProduceRequest.
    /// Returns the future of the response, or of the error if the request
    /// could not be queued.
    async fn send_request(
        &self,
        conn: &Arc<Connection>,
        picked: Vec<Picked<C::DeliveryOpaque>>,
    ) -> BoxFuture<'static, Response<C::DeliveryOpaque>> {
        let sent = self.encode(picked).await;
        let now = Instant::now();
        let mut msg_cnt = 0;
        let mut first_deadline: Option<Instant> = None;
        let mut topic_data: Vec<TopicProduceData> = Vec::new();
        for s in &sent {
            let BatchData::Encoded(encoded) = &s.batch.data else {
                unreachable!("sent batches are encoded");
            };
            msg_cnt += u64::try_from(s.batch.len()).expect("fits");
            if let Some(timeout) = self.config.message_timeout {
                let at = s.batch.created + timeout;
                first_deadline = Some(first_deadline.map_or(at, |d| d.min(at)));
            }
            let data = PartitionProduceData::default()
                .with_index(s.key.partition)
                .with_records(Some(encoded.bytes()));
            match topic_data.last_mut() {
                Some(t) if t.name.0.as_str() == &*s.topic => t.partition_data.push(data),
                _ => topic_data.push(
                    TopicProduceData::default()
                        .with_name(TopicName(StrBytes::from_string(s.topic.to_string())))
                        .with_partition_data(vec![data]),
                ),
            }
        }
        // Like librdkafka, the request may take until the first message's
        // timeout (at least 100 ms), capped by `socket.timeout.ms`.
        let timeout = match first_deadline {
            Some(at) => at
                .saturating_duration_since(now)
                .max(Duration::from_millis(100))
                .min(self.config.socket_timeout),
            None => self.config.socket_timeout,
        };
        let opts = RequestOptions {
            timeout: Some(timeout),
            msg_cnt,
        };
        let transactional_id = self
            .config
            .transactional_id
            .as_ref()
            .map(|id| TransactionalId(StrBytes::from_string(id.clone())));
        let request_timeout_ms =
            i32::try_from(self.config.request_timeout.as_millis()).unwrap_or(i32::MAX);
        let acks = self.config.acks;
        let build = move |_version: i16| {
            ProduceRequest::default()
                .with_transactional_id(transactional_id)
                .with_acks(acks)
                .with_timeout_ms(request_timeout_ms)
                .with_topic_data(topic_data)
        };
        if acks == 0 {
            let result = conn.send_oneway::<ProduceRequest, _>(build, &opts).await;
            return Box::pin(async move { (sent, result.map(|()| None)) });
        }
        match conn.queue_request::<ProduceRequest, _>(build, &opts).await {
            Ok(response) => Box::pin(async move { (sent, response.await.map(Some)) }),
            Err(e) => Box::pin(async move { (sent, Err(e)) }),
        }
    }

    /// Stamps and compresses picked batches. Large gzip and zstd batches are
    /// compressed on a blocking thread.
    async fn encode(&self, picked: Vec<Picked<C::DeliveryOpaque>>) -> Vec<Sent<C::DeliveryOpaque>> {
        let heavy = matches!(
            self.config.compression,
            Compression::Gzip | Compression::Zstd
        ) && picked.iter().any(|p| {
            matches!(p.batch.data, BatchData::Open(_))
                && p.batch.uncompressed_size() >= OFFLOAD_COMPRESSION_BYTES
        });
        let encode_all = move || {
            let mut spares = Vec::new();
            let sent: Vec<_> = picked
                .into_iter()
                .map(|p| encode_one(p, &mut spares))
                .collect();
            (sent, spares)
        };
        let (sent, spares) = if heavy {
            crate::internal::runtime::run_blocking(encode_all).await
        } else {
            encode_all()
        };
        if !spares.is_empty() {
            let mut st = self.lock();
            for buf in spares {
                st.buffers.put(buf);
            }
        }
        sent
    }

    /// Handles the result of a ProduceRequest: delivery reports, retries and
    /// errors.
    fn handle_result(
        &self,
        broker_id: i32,
        sent: Vec<Sent<C::DeliveryOpaque>>,
        result: Result<Option<ProduceResponse>, NetError>,
    ) {
        let now = Instant::now();
        let mut st = self.lock();
        let mut fx = Effects::default();
        let mut delivered = Vec::new();
        let mut retries = 0;
        let mut touched = Vec::with_capacity(sent.len());
        for s in sent {
            st.in_flight = st.in_flight.saturating_sub(1);
            let outcome = match &result {
                Ok(None) => Outcome::Delivered {
                    base_offset: -1,
                    log_append_time: -1,
                },
                Ok(Some(response)) => partition_outcome(response, &s.topic, s.key.partition),
                Err(e) if e.code() == RDKafkaErrorCode::BrokerDestroy => {
                    // The client is shutting down; nobody waits for reports.
                    continue;
                }
                Err(e) => classify(e.code()),
            };
            let outcome = self.idempotent_outcome(&mut st, &s, outcome, broker_id, &mut fx);
            let fatal = st.eos.fatal.is_some();
            touched.push(s.key);
            let count = s.batch.len();
            let sequenced = s.batch.sequenced;
            // The reaction of the producer as a whole, once the partition is
            // updated.
            let failed = {
                let Some(p) = st.topics[s.key.topic].partition_mut(s.key.partition) else {
                    fx.fail(
                        &s.topic,
                        s.key.partition,
                        Undelivered::Batch(s.batch),
                        RDKafkaErrorCode::UnknownPartition,
                    );
                    continue;
                };
                p.in_flight = p.in_flight.saturating_sub(1);
                match outcome {
                    Outcome::Delivered {
                        base_offset,
                        log_append_time,
                    } => {
                        if sequenced {
                            p.next_ack_seq =
                                increment_sequence(s.base_seq, i32::try_from(count).expect("fits"));
                            if p.retry_seq == Some(s.base_seq) {
                                p.retry_seq = None;
                            }
                        }
                        p.acked = true;
                        p.txmsgs += u64::try_from(count).expect("fits");
                        p.txbytes += s.batch.kv_bytes;
                        delivered.push((
                            Arc::clone(&s.topic),
                            s.key.partition,
                            s.batch,
                            base_offset,
                            log_append_time,
                        ));
                        None
                    }
                    Outcome::Retry {
                        code,
                        counts,
                        refresh,
                    } => {
                        let mut batch = s.batch;
                        let expired = self
                            .config
                            .message_timeout
                            .is_some_and(|t| batch.created + t <= now);
                        // After a fatal error nothing is sent anymore.
                        if fatal || expired || (counts && batch.retries >= self.config.retries) {
                            let code = if expired {
                                RDKafkaErrorCode::MessageTimedOut
                            } else {
                                code
                            };
                            fx.fail(&s.topic, s.key.partition, Undelivered::Batch(batch), code);
                            Some(code)
                        } else {
                            if counts {
                                batch.retries += 1;
                            }
                            let shift = batch.attempts.saturating_sub(1).min(10);
                            let backoff = (self.config.retry_backoff * (1u32 << shift))
                                .min(self.config.retry_backoff_max);
                            let jitter = rand::rng().random_range(0.8..1.2);
                            batch.retry_at = Some(now + backoff.mul_f64(jitter));
                            if sequenced && p.retry_seq.is_none_or(|r| seq_before(s.base_seq, r)) {
                                p.retry_seq = Some(s.base_seq);
                            }
                            p.drain = true;
                            p.requeue(batch);
                            retries += 1;
                            if refresh {
                                fx.refresh_metadata = true;
                            }
                            None
                        }
                    }
                    Outcome::Failed(code) => {
                        fx.fail(&s.topic, s.key.partition, Undelivered::Batch(s.batch), code);
                        Some(code)
                    }
                }
            };
            let Some(code) = failed else { continue };
            if st.eos.transactional {
                // librdkafka: any permanent produce failure in a transaction
                // makes it abortable. A stamped batch also leaves a sequence
                // gap that the abort must bump the epoch for.
                let reason = produce_failed_reason(
                    &s.topic,
                    s.key.partition,
                    count,
                    code,
                    broker_id,
                    st.eos.pid,
                    s.base_seq,
                );
                st.set_abortable(code, sequenced, reason, &mut fx);
            } else if sequenced && st.eos.idemp_state == IdempState::Assigned {
                st.drain_epoch_bump(false, code, "message sequence gap".to_string(), &mut fx);
            }
        }
        // Partitions with nothing in flight may send again.
        for key in touched {
            if let Some(p) = st.topics[key.topic].partition_mut(key.partition) {
                if p.drain && p.in_flight == 0 {
                    p.drain = false;
                    if p.leader >= 0 && p.has_queued() {
                        fx.wake.insert(p.leader);
                    }
                }
            }
        }
        if st.in_flight == 0
            && matches!(
                st.eos.idemp_state,
                IdempState::DrainBump | IdempState::DrainReset
            )
        {
            fx.wake_eos = true;
        }
        if retries > 0 {
            if let Some(broker) = self.cluster.broker(broker_id) {
                broker
                    .counters()
                    .txretries
                    .fetch_add(retries, Ordering::Relaxed);
            }
        }
        self.finish_with_deliveries(st, fx, delivered);
    }

    /// Applies librdkafka's idempotent producer error handling
    /// (`rd_kafka_handle_idempotent_Produce_error`) and fencing to a batch's
    /// outcome.
    fn idempotent_outcome(
        &self,
        st: &mut State<C::DeliveryOpaque>,
        s: &Sent<C::DeliveryOpaque>,
        outcome: Outcome,
        broker_id: i32,
        fx: &mut Effects<C::DeliveryOpaque>,
    ) -> Outcome {
        let Outcome::Failed(code) = outcome else {
            return outcome;
        };
        if !st.eos.idempotent {
            return outcome;
        }
        let transactional = st.eos.transactional;
        let pid = st.eos.pid;
        let count = s.batch.len();
        let Some(p) = st.topics[s.key.topic].partition(s.key.partition) else {
            return outcome;
        };
        let next_ack_seq = p.next_ack_seq;
        let retry_seq = p.retry_seq;
        let batch_pid = match &s.batch.data {
            BatchData::Encoded(e) => e.producer_identity(),
            BatchData::Open(_) => pid,
        };
        if batch_pid != pid {
            // Sent with an old identity: the sequence state no longer
            // applies. Resend under the new one.
            return Outcome::Retry {
                code,
                counts: false,
                refresh: false,
            };
        }
        let prefix = format!(
            "ProduceRequest for {} [{}] with {count} message(s) failed",
            s.topic, s.key.partition
        );
        match code {
            RDKafkaErrorCode::DuplicateSequenceNumber => {
                // A previous send was persisted but not acknowledged.
                Outcome::Delivered {
                    base_offset: -1,
                    log_append_time: -1,
                }
            }
            RDKafkaErrorCode::OutOfOrderSequenceNumber => {
                let after_retry = retry_seq.is_some_and(|r| seq_before(r, s.base_seq));
                if after_retry {
                    // An earlier batch failed and is retried first.
                    return Outcome::Retry {
                        code,
                        counts: false,
                        refresh: false,
                    };
                }
                if s.base_seq == next_ack_seq {
                    st.set_fatal(
                        code,
                        format!(
                            "{prefix} due to sequence desynchronization with broker {broker_id} \
                             ({}, base seq {})",
                            pid_display(pid),
                            s.base_seq
                        ),
                        fx,
                    );
                    outcome
                } else if seq_before(next_ack_seq, s.base_seq) {
                    st.drain_epoch_bump(true, code, "skipped sequence numbers".to_string(), fx);
                    Outcome::Retry {
                        code,
                        counts: false,
                        refresh: false,
                    }
                } else {
                    st.set_fatal(
                        code,
                        format!(
                            "{prefix} with rewound sequence number on broker {broker_id} ({}, \
                             base seq {} < next seq {next_ack_seq})",
                            pid_display(pid),
                            s.base_seq
                        ),
                        fx,
                    );
                    outcome
                }
            }
            RDKafkaErrorCode::UnknownProducerId => {
                if transactional {
                    st.drain_epoch_bump(
                        true,
                        code,
                        format!("{prefix} due to unknown producer id"),
                        fx,
                    );
                    outcome
                } else if s.batch.retries == 0 && s.base_seq == next_ack_seq {
                    st.drain_epoch_bump(false, code, "unknown producer id".to_string(), fx);
                    Outcome::Retry {
                        code,
                        counts: false,
                        refresh: false,
                    }
                } else {
                    st.set_fatal(
                        code,
                        format!(
                            "{prefix} due to unknown producer id (broker {broker_id} {}, base \
                             seq {}, {} retries): unable to retry without risking \
                             duplication/reordering",
                            pid_display(pid),
                            s.base_seq,
                            s.batch.retries
                        ),
                        fx,
                    );
                    outcome
                }
            }
            RDKafkaErrorCode::InvalidProducerEpoch | RDKafkaErrorCode::ProducerFenced
                if transactional =>
            {
                st.set_fatal(
                    RDKafkaErrorCode::Fenced,
                    format!(
                        "{prefix}: {} (broker {broker_id} {}, base seq {}): transactional \
                         producer fenced by newer producer instance",
                        code.err2str(),
                        pid_display(pid),
                        s.base_seq
                    ),
                    fx,
                );
                outcome
            }
            _ => outcome,
        }
    }
}

/// Stamps and compresses one batch. A batch that cannot be compressed is
/// sent uncompressed.
fn encode_one<D>(p: Picked<D>, spares: &mut Vec<Vec<u8>>) -> Sent<D> {
    let Picked {
        key,
        topic,
        mut batch,
        ident,
        base_seq,
        scratch,
    } = p;
    batch.data = match batch.data {
        BatchData::Open(builder) => match builder.build_reusing(ident, base_seq, scratch) {
            Ok((encoded, spare)) => {
                spares.extend(spare);
                BatchData::Encoded(encoded)
            }
            Err(e) => panic!("encoding a non-empty batch failed: {e}"),
        },
        BatchData::Encoded(mut encoded) => {
            if encoded.producer_identity() != ident || encoded.base_sequence() != base_seq {
                encoded.restamp(ident, base_seq);
            }
            BatchData::Encoded(encoded)
        }
    };
    Sent {
        key,
        topic,
        batch,
        base_seq,
    }
}

/// Whether sequence `a` comes before `b`, allowing for wraparound.
fn seq_before(a: i32, b: i32) -> bool {
    b.wrapping_sub(a) > 0
}

/// The outcome of a partition in a produce response.
fn partition_outcome(response: &ProduceResponse, topic: &str, partition: i32) -> Outcome {
    let found = response
        .responses
        .iter()
        .filter(|t| t.name.0.as_str() == topic)
        .flat_map(|t| t.partition_responses.iter())
        .find(|p| p.index == partition);
    let Some(p) = found else {
        return Outcome::Failed(RDKafkaErrorCode::BadMessage);
    };
    match protocol::broker_error(p.error_code) {
        None => Outcome::Delivered {
            base_offset: p.base_offset,
            log_append_time: p.log_append_time_ms,
        },
        Some(code) => classify(code),
    }
}

/// librdkafka's produce error actions (`rd_kafka_handle_Produce_error` with
/// `rd_kafka_err_action`'s defaults).
fn classify(code: RDKafkaErrorCode) -> Outcome {
    use RDKafkaErrorCode::*;
    match code {
        // Refresh metadata and retry; only failures after a send count.
        BrokerTransportFailure | KafkaStorageError => Outcome::Retry {
            code,
            counts: true,
            refresh: true,
        },
        UnknownTopicOrPartition
        | NotLeaderForPartition
        | LeaderNotAvailable
        | BrokerNotAvailable
        | ReplicaNotAvailable
        | DestroyBroker => Outcome::Retry {
            code,
            counts: false,
            refresh: true,
        },
        NotEnoughReplicas
        | NotEnoughReplicasAfterAppend
        | OperationTimedOut
        | RequestTimedOut
        | Retry => Outcome::Retry {
            code,
            counts: true,
            refresh: false,
        },
        // Never sent.
        TimedOutQueue => Outcome::Retry {
            code,
            counts: false,
            refresh: false,
        },
        code => Outcome::Failed(code),
    }
}

/// librdkafka's text for a permanent produce error in a transaction.
fn produce_failed_reason(
    topic: &str,
    partition: i32,
    count: usize,
    code: RDKafkaErrorCode,
    broker_id: i32,
    pid: ProducerIdentity,
    base_seq: i32,
) -> String {
    format!(
        "ProduceRequest for {topic} [{partition}] with {count} message(s) failed: {} (broker \
         {broker_id} {}, base seq {base_seq}): current transaction must be aborted",
        code.err2str(),
        pid_display(pid)
    )
}
