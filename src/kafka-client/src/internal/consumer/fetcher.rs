// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The fetcher of one leader broker.
//!
//! Like librdkafka's broker thread, it keeps at most one Fetch request in
//! flight, covering every assigned partition the broker leads that is
//! fetchable: actively fetching, not backing off after an error, and with a
//! queue below `queued.min.messages` messages and
//! `queued.max.messages.kbytes` * 1000 bytes. Partitions that are not split
//! off share the main queue, so like librdkafka's forwarded queues they are
//! limited by the main queue's total.
//!
//! # Prefetching
//!
//! Those limits are only ceilings. Each partition has a prefetch target:
//! [`TARGET_LATENCIES`] times the bytes its reader drains during one fetch
//! round trip, but at least [`TARGET_FLOOR`]. A partition triggers a fetch
//! once its queue holds less than half its target, and partitions below
//! three quarters of theirs ride along, each asking for what fills it up to
//! its target. So a fetch is in flight while the queue still holds about two
//! round trips of data, the reader does not wait, and memory stays at what
//! the drain rate needs instead of the limits. A target doubles whenever a
//! response finds its queue empty (the reader waited), up to the limits.
//! Readers wake the fetcher when a queue drops below its trigger.
//!
//! NOTE: With one request in flight, a request that only covers partitions
//! at the end of their logs would wait at the broker for up to
//! `fetch.wait.max.ms` while partitions left out because their queues were
//! full drain and need data. Such requests wait only until those queues are
//! expected to reach their triggers, at their drain rate, but at least
//! [`MIN_BROKER_WAIT`], and only for queues whose readers drained within
//! [`RECENT_DRAIN`]. A stalled reader (backpressure) leaves a stale rate
//! that would otherwise make every request come back after a round trip.
//!
//! Each partition's `PartitionMaxBytes` is also capped by what its queue can
//! still take before reaching the limits, so that a response does not
//! overshoot them by up to `fetch.max.bytes`, as librdkafka's can.
//!
//! Like the Java client (KIP-74), a request lists the partitions that got data
//! in the previous responses last, so that a partition with a large backlog,
//! which can fill whole responses, does not starve the others: brokers fill
//! a response in request order.
//!
//! Responses are not decoded: each partition's record set is queued as is,
//! after reading batch headers to find the next fetch offset. Record sets are
//! copied out of the response into pooled buffers (see
//! [`crate::internal::pool`]), so that the response is freed right away: the
//! connection then reuses its read buffer, and a partition whose queue
//! drains slowly does not keep a whole response alive.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::{BrokerId, FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::config::settings::{DebugContext, IsolationLevel};
use crate::error::KafkaError;
use crate::internal::cluster::MetadataSnapshot;
use crate::internal::compression::CodecError;
use crate::internal::net::RequestOptions;
use crate::internal::pool::LIMIT_DECAY;
use crate::internal::protocol;
use crate::internal::records;
use crate::types::RDKafkaErrorCode;

use super::partition::{FetchPhase, Item, Wake};
use super::reader::{Chunk, accounted_messages, codec_error};
use super::{ConsumerInner, Leader, Partition, leader_of, offset_reset};

/// The largest `PartitionMaxBytes` a partition's fetch size grows to when a
/// single batch does not fit, as in librdkafka.
const MAX_PARTITION_FETCH_BYTES: i32 = 1 << 30;

/// The smallest prefetch target, in queued bytes per partition.
const TARGET_FLOOR: u64 = 512 << 10;

/// How many fetch round trips of drained data a prefetch target covers.
const TARGET_LATENCIES: u128 = 4;

/// The smallest `PartitionMaxBytes` the fetcher asks for. Brokers return at
/// least one whole batch regardless (KIP-74).
const MIN_PARTITION_FETCH_BYTES: u64 = 64 << 10;

/// How recently the reader of a full queue must have drained for its drain
/// rate to shorten the broker wait.
const RECENT_DRAIN: Duration = Duration::from_millis(100);

/// The shortest broker wait a drain estimate shortens `fetch.wait.max.ms`
/// to.
const MIN_BROKER_WAIT: Duration = Duration::from_millis(10);

/// The largest `MaxBytes` of a Fetch request, below `fetch.max.bytes`
/// (default 50 MB).
///
/// NOTE: Each response is read into one allocation of its size. Responses of
/// tens of megabytes, freed at a high rate, made jemalloc retain several
/// hundred megabytes of freed pages: consuming 5M 220-byte records from 8
/// partitions with `fetch.message.max.bytes=128MiB` peaked at 370-460 MB RSS
/// for 140-170 MB allocated, against 230 MB RSS with 8 MiB responses and
/// unchanged throughput. Brokers still return at least one whole batch per
/// partition (KIP-74), and one 8 MiB response per round trip sustains over a
/// gigabyte per second at in-region latencies.
const MAX_RESPONSE_BYTES: i32 = 8 << 20;

/// One partition in a Fetch request.
struct Fetching {
    partition: Arc<Partition>,
    version: u64,
    offset: i64,
    max_bytes: i32,
}

/// Runs the fetcher for broker `broker_id` until the consumer shuts down.
pub(super) async fn run(inner: Arc<ConsumerInner>, broker_id: i32) {
    let shutdown = inner.shutdown.clone();
    let mut wake_rx = inner.wake.subscribe();
    let mut md_rx = inner.cluster.subscribe_metadata();
    let settings = Arc::clone(inner.settings());
    let consumer = &settings.consumer;
    let timeout = settings.socket_timeout + consumer.fetch_wait_max;
    let isolation_level: i8 = match settings.isolation_level {
        IsolationLevel::ReadUncommitted => 0,
        IsolationLevel::ReadCommitted => 1,
    };
    let max_wait_ms = i32::try_from(consumer.fetch_wait_max.as_millis()).unwrap_or(i32::MAX);
    let mut broker_backoff_until: Option<Instant> = None;
    // For each partition, when it last got data (a count of responses), to
    // order requests; see the module documentation.
    let mut got_data: BTreeMap<(Arc<str>, i32), u64> = BTreeMap::new();
    let mut responses: u64 = 0;
    // Smoothed round trip of Fetch requests to this broker.
    let mut latency = Duration::ZERO;
    loop {
        wake_rx.borrow_and_update();
        let md = Arc::clone(&md_rx.borrow_and_update());
        let now = Instant::now();
        if let Some(at) = broker_backoff_until.filter(|at| *at > now) {
            if !wait(&shutdown, &mut wake_rx, &mut md_rx, Some(at)).await {
                return;
            }
            continue;
        }
        broker_backoff_until = None;
        let request_id = inner.request_id();
        let (mut fetching, next_wakeup, soonest_need) =
            plan(&inner, broker_id, &md, request_id, now);
        let max_wait_ms = match soonest_need {
            Some(d) => max_wait_ms.min(i32::try_from(d.as_millis()).unwrap_or(i32::MAX)),
            None => max_wait_ms,
        };
        if fetching.is_empty() {
            if !wait(&shutdown, &mut wake_rx, &mut md_rx, next_wakeup).await {
                return;
            }
            continue;
        }
        let Some(broker) = inner.cluster.broker(broker_id) else {
            // The broker left the metadata; its partitions move to another
            // fetcher once the metadata names their new leader.
            release(&inner, &fetching, request_id);
            // Not woken by its own release.
            wake_rx.borrow_and_update();
            if !wait(&shutdown, &mut wake_rx, &mut md_rx, None).await {
                return;
            }
            continue;
        };

        // Partitions that got data least recently first. Topics come in the
        // order of their first partition.
        fetching.sort_by_key(|f| {
            got_data
                .get(&(Arc::clone(&f.partition.topic), f.partition.id))
                .copied()
                .unwrap_or(0)
        });
        let mut topics: Vec<(&str, Vec<&Fetching>)> = Vec::new();
        for f in &fetching {
            match topics.iter_mut().find(|(t, _)| *t == &*f.partition.topic) {
                Some((_, parts)) => parts.push(f),
                None => topics.push((&f.partition.topic, vec![f])),
            }
        }
        let sent_at = Instant::now();
        let request = broker.request::<FetchRequest, _>(
            |_version| {
                FetchRequest::default()
                    .with_replica_id(BrokerId(-1))
                    .with_max_wait_ms(max_wait_ms)
                    .with_min_bytes(consumer.fetch_min_bytes)
                    .with_max_bytes(consumer.fetch_max_bytes.min(MAX_RESPONSE_BYTES))
                    .with_isolation_level(isolation_level)
                    .with_session_id(0)
                    .with_session_epoch(-1)
                    .with_topics(
                        topics
                            .iter()
                            .map(|(topic, parts)| {
                                FetchTopic::default()
                                    .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
                                    .with_partitions(
                                        parts
                                            .iter()
                                            .map(|f| {
                                                FetchPartition::default()
                                                    .with_partition(f.partition.id)
                                                    .with_fetch_offset(f.offset)
                                                    .with_partition_max_bytes(f.max_bytes)
                                            })
                                            .collect(),
                                    )
                            })
                            .collect(),
                    )
            },
            RequestOptions::with_timeout(timeout),
        );
        tokio::pin!(request);
        // Abandon the request when one of its partitions moves to another
        // leader, so that it does not wait for a broker that is down.
        let result = loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                result = &mut request => break Some(result),
                changed = md_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let md = Arc::clone(&md_rx.borrow_and_update());
                    let moved = fetching.iter().any(|f| {
                        let leader = leader_of(&md, &f.partition);
                        !matches!(leader, Leader::Broker(id) if id == broker_id)
                    });
                    if moved {
                        break None;
                    }
                }
            }
        };
        let response = match result {
            None => {
                release(&inner, &fetching, request_id);
                continue;
            }
            Some(Ok(response)) => response,
            Some(Err(e)) => {
                release(&inner, &fetching, request_id);
                if e.code() == RDKafkaErrorCode::BrokerDestroy {
                    return;
                }
                inner.logger().debug(
                    DebugContext::Fetch,
                    "FETCHERR",
                    format_args!(
                        "[thrd:{}]: {}: Fetch failed: {}",
                        broker.name(),
                        broker.name(),
                        e.message()
                    ),
                );
                // Like librdkafka's broker-level fetch backoff. The leader
                // may have moved.
                broker_backoff_until = Some(Instant::now() + consumer.fetch_error_backoff);
                inner.cluster.request_metadata_refresh();
                continue;
            }
        };
        if let Some(code) = protocol::broker_error(response.error_code) {
            release(&inner, &fetching, request_id);
            inner.logger().debug(
                DebugContext::Fetch,
                "FETCHERR",
                format_args!(
                    "[thrd:{}]: {}: Fetch failed: {}",
                    broker.name(),
                    broker.name(),
                    code.err2str()
                ),
            );
            broker_backoff_until = Some(Instant::now() + consumer.fetch_error_backoff);
            continue;
        }

        let with_data = response
            .responses
            .iter()
            .flat_map(|t| t.partitions.iter())
            .filter(|p| p.records.as_ref().is_some_and(|r| !r.is_empty()))
            .count();
        // The round trip of responses with data; empty ones may have waited
        // up to `fetch.wait.max.ms` at the broker.
        if with_data > 0 {
            let sample = sent_at.elapsed();
            latency = if latency.is_zero() {
                sample
            } else {
                (latency + sample) / 2
            };
        }
        responses += 1;
        for topic in response.responses {
            for data in topic.partitions {
                let Some(f) = fetching.iter().find(|f| {
                    f.partition.id == data.partition_index
                        && topic.topic.0.as_str() == &*f.partition.topic
                }) else {
                    continue;
                };
                if data.records.as_ref().is_some_and(|r| !r.is_empty()) {
                    got_data.insert((Arc::clone(&f.partition.topic), f.partition.id), responses);
                }
                handle_partition(&inner, broker_id, request_id, f, data, latency);
            }
        }
        // Partitions the broker did not answer for. `release` only clears
        // this request's marks, so this is a no-op for answered ones.
        release(&inner, &fetching, request_id);
        // Partitions that moved to another leader while this request was in
        // flight can now be fetched there.
        let md = md_rx.borrow();
        if fetching
            .iter()
            .any(|f| !matches!(leader_of(&md, &f.partition), Leader::Broker(id) if id == broker_id))
        {
            inner.wake();
        }
        drop(md);
        // Forget partitions this fetcher no longer fetches.
        if got_data.len() > 2 * fetching.len().max(64) {
            got_data.retain(|(topic, id), _| {
                fetching
                    .iter()
                    .any(|f| f.partition.id == *id && f.partition.topic == *topic)
            });
        }
    }
}

/// Picks the fetchable partitions led by `broker_id` and marks them in
/// flight under `request_id`. Also returns when to look again for partitions
/// that are backing off or throttled.
fn plan(
    inner: &ConsumerInner,
    broker_id: i32,
    md: &MetadataSnapshot,
    request_id: u64,
    now: Instant,
) -> (Vec<Fetching>, Option<Instant>, Option<Duration>) {
    let settings = inner.settings();
    let max_bytes = settings.consumer.queued_max_bytes();
    let max_msgs = u64::try_from(settings.consumer.queued_min_messages).unwrap_or(u64::MAX);
    let partitions = inner.assigned_partitions();
    // The main queue's total, which bounds partitions that are not split.
    let (main_bytes, main_msgs) = partitions
        .iter()
        .filter(|p| !p.is_split())
        .map(|p| p.queued())
        .fold((0, 0), |(b, m), (pb, pm)| (b + pb, m + pm));
    let mut fetching = Vec::new();
    let mut triggered = false;
    // When the soonest partition left out because its queue is full is
    // expected to need data.
    let mut soonest_need: Option<Duration> = None;
    let mut next_wakeup: Option<Instant> = None;
    let mut wake_at = |at: Instant| {
        next_wakeup = Some(next_wakeup.map_or(at, |t| t.min(at)));
    };
    for partition in partitions {
        if !matches!(leader_of(md, &partition), Leader::Broker(id) if id == broker_id) {
            continue;
        }
        let mut state = partition.lock_state();
        let fetch = &mut state.fetch;
        if fetch.phase != FetchPhase::Active || fetch.in_flight.is_some() {
            continue;
        }
        if let Some(at) = fetch.backoff_until {
            if at > now {
                wake_at(at);
                continue;
            }
            fetch.backoff_until = None;
        }
        let (bytes, msgs) = if partition.is_split() {
            partition.queued()
        } else {
            (main_bytes, main_msgs)
        };
        let target = fetch.target.max(TARGET_FLOOR);
        let (queued, _) = partition.queued();
        let mut need_in = |fetch: &mut super::partition::FetchState| {
            // Sampled here too, as a full partition is in no response.
            sample_drain(fetch, partition.consumed(), now);
            let drained_recently = fetch
                .drained_at
                .is_some_and(|at| now.saturating_duration_since(at) <= RECENT_DRAIN);
            if drained_recently && fetch.drain_rate > 0 {
                let excess = u128::from(queued.saturating_sub(target / 2));
                let nanos = excess * 1_000_000_000 / u128::from(fetch.drain_rate);
                let d = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
                    .max(MIN_BROKER_WAIT);
                soonest_need = Some(soonest_need.map_or(d, |s| s.min(d)));
            }
        };
        if bytes >= max_bytes || msgs >= max_msgs {
            need_in(fetch);
            // Readers wake the fetchers when they drain; the timer is a
            // fallback, like librdkafka's `fetch.queue.backoff.ms`.
            inner
                .throttled
                .store(true, std::sync::atomic::Ordering::Release);
            wake_at(now + settings.consumer.fetch_queue_backoff);
            continue;
        }
        if queued >= target * 3 / 4 {
            // Readers wake the fetchers once the queue drops below the
            // trigger; the timer is a fallback.
            if !partition.fetchable_below(target / 2) {
                need_in(fetch);
                wake_at(now + settings.consumer.fetch_queue_backoff);
                continue;
            }
        }
        triggered |= queued < target / 2 || partition.fetchable_below(target / 2);
        let byte_budget = max_bytes - bytes;
        // Until a fetch has shown the partition's message size, fetch small:
        // one response may otherwise hold far more messages than
        // `queued.min.messages`.
        let msg_budget = match fetch.avg_msg_bytes {
            0 => TARGET_FLOOR,
            avg => (max_msgs - msgs).saturating_mul(avg),
        };
        let budget = byte_budget
            .min(msg_budget)
            .min(target.saturating_sub(queued))
            .max(MIN_PARTITION_FETCH_BYTES);
        let max_bytes = fetch
            .max_bytes
            .min(i32::try_from(budget).unwrap_or(i32::MAX));
        let (version, offset) = (state.version, state.fetch.offset);
        drop(state);
        fetching.push(Fetching {
            partition,
            version,
            offset,
            max_bytes,
        });
    }
    trim_pool(inner, now);
    if !triggered {
        // Nobody needs data yet; the candidates wait for their triggers.
        if !fetching.is_empty() {
            wake_at(now + settings.consumer.fetch_queue_backoff);
        }
        return (Vec::new(), next_wakeup, None);
    }
    fetching.retain(|f| {
        let mut state = f.partition.lock_state();
        // Checked again: the partition was unlocked meanwhile.
        if state.version != f.version
            || state.fetch.phase != FetchPhase::Active
            || state.fetch.offset != f.offset
            || state.fetch.in_flight.is_some()
        {
            return false;
        }
        state.fetch.in_flight = Some(request_id);
        true
    });
    (fetching, next_wakeup, soonest_need)
}

/// Sets the consumer's buffer pool limit: as much free capacity as the
/// queues of partitions whose readers drained within [`LIMIT_DECAY`] are
/// meant to hold, and nothing once none did. Returns whether the pool holds
/// more than that, to be trimmed again once the limit decays.
pub(super) fn trim_pool(inner: &ConsumerInner, now: Instant) -> bool {
    let total_target: u64 = inner
        .assigned_partitions()
        .iter()
        .map(|p| {
            let state = p.lock_state();
            let draining = state
                .fetch
                .drained_at
                .is_some_and(|at| now.saturating_duration_since(at) <= LIMIT_DECAY);
            if draining {
                state.fetch.target.max(TARGET_FLOOR)
            } else {
                0
            }
        })
        .sum();
    // Twice the targets: buffers circulate between the queues, the pool and
    // the response being copied out.
    let limit = usize::try_from(total_target.saturating_mul(2)).unwrap_or(usize::MAX);
    inner.pool.decay_limit(limit);
    inner.pool.pooled() > limit
}

/// Clears the in-flight marks of a request that got no answer for these
/// partitions, and wakes the fetchers if there were any: a partition that
/// moved to another leader meanwhile can only be fetched there now.
fn release(inner: &ConsumerInner, fetching: &[Fetching], request_id: u64) {
    let mut released = false;
    for f in fetching {
        let mut state = f.partition.lock_state();
        if state.fetch.in_flight == Some(request_id) {
            state.fetch.in_flight = None;
            released = true;
        }
    }
    if released {
        inner.wake();
    }
}

/// Waits for a reason to plan again. Returns false on shutdown.
async fn wait(
    shutdown: &tokio_util::sync::CancellationToken,
    wake_rx: &mut tokio::sync::watch::Receiver<u64>,
    md_rx: &mut tokio::sync::watch::Receiver<crate::internal::cluster::SharedMetadata>,
    until: Option<Instant>,
) -> bool {
    let sleep = async {
        match until {
            Some(at) => tokio::time::sleep_until(at.into()).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        _ = shutdown.cancelled() => false,
        r = wake_rx.changed() => r.is_ok(),
        r = md_rx.changed() => r.is_ok(),
        _ = sleep => true,
    }
}

/// Handles one partition of a Fetch response.
fn handle_partition(
    inner: &ConsumerInner,
    broker_id: i32,
    request_id: u64,
    f: &Fetching,
    data: PartitionData,
    latency: Duration,
) {
    let partition = &f.partition;
    let settings = inner.settings();
    let consumer = &settings.consumer;
    let split = partition.is_split();
    let mut state = partition.lock_state();
    if state.fetch.in_flight != Some(request_id) {
        // Restarted or stopped meanwhile.
        return;
    }
    state.fetch.in_flight = None;
    if state.version != f.version
        || state.fetch.phase != FetchPhase::Active
        || state.fetch.offset != f.offset
    {
        return;
    }
    state.fetch.lo_offset = data.log_start_offset;
    state.fetch.hi_offset = data.high_watermark;
    let mut end_offset = data.high_watermark;
    let mut aborted = Vec::new();
    if inner.read_committed() {
        // Brokers without LastStableOffset send -1: use the high watermark.
        if data.last_stable_offset >= 0 {
            end_offset = data.last_stable_offset;
        }
        if let Some(list) = &data.aborted_transactions {
            aborted = list
                .iter()
                .map(|a| (a.producer_id.0, a.first_offset))
                .collect();
            aborted.sort_by_key(|(_, first)| *first);
        }
    }
    state.fetch.ls_offset = end_offset;

    let mut wakes: Vec<Wake> = Vec::new();
    // Like librdkafka, the end of the partition is signaled once per end
    // offset when a fetch at that offset returns, overriding any error.
    if end_offset == f.offset && state.fetch.eof_offset != end_offset {
        state.fetch.eof_offset = end_offset;
        if consumer.enable_partition_eof {
            wakes.push(state.push(Item::Eof, split));
        }
    } else if let Some(code) = protocol::broker_error(data.error_code) {
        handle_error(inner, partition, &mut state, broker_id, code, &mut wakes);
    } else {
        state.fetch.last_error = None;
        let records = data.records.unwrap_or_default();
        // The reader had nothing left when data arrived: it waited.
        let starved = !records.is_empty() && partition.queued().0 == 0;
        update_target(inner, partition, &mut state.fetch, latency, starved);
        if !records.is_empty() {
            handle_records(inner, partition, &mut state, records, aborted, &mut wakes);
        }
    }
    // An offset reset hands the partition to the manager for an offset
    // lookup.
    let resolve = matches!(state.fetch.phase, FetchPhase::Resolve(_));
    drop(state);
    for wake in wakes {
        partition.notify(inner, wake);
    }
    if resolve {
        inner.wake();
    }
}

/// Updates a partition's prefetch target from its drain rate and the fetch
/// round trip, see the module documentation.
fn update_target(
    inner: &ConsumerInner,
    partition: &Partition,
    fetch: &mut super::partition::FetchState,
    latency: Duration,
    starved: bool,
) {
    sample_drain(fetch, partition.consumed(), Instant::now());
    let wanted =
        u128::from(fetch.drain_rate) * latency.as_nanos() * TARGET_LATENCIES / 1_000_000_000;
    let mut target = u64::try_from(wanted).unwrap_or(u64::MAX).max(TARGET_FLOOR);
    if starved {
        target = target.max(fetch.target.saturating_mul(2));
    }
    // The limits cap the target.
    let consumer = &inner.settings().consumer;
    let max_msgs = u64::try_from(consumer.queued_min_messages).unwrap_or(u64::MAX);
    let mut cap = consumer.queued_max_bytes();
    if fetch.avg_msg_bytes > 0 {
        cap = cap.min(max_msgs.saturating_mul(fetch.avg_msg_bytes));
    }
    fetch.target = target.min(cap.max(TARGET_FLOOR));
}

/// Samples the drain rate of a partition's reader from `consumed`, the
/// bytes it consumed in total. A sample that finds nothing drained halves
/// the rate.
fn sample_drain(fetch: &mut super::partition::FetchState, consumed: u64, now: Instant) {
    let Some((mark, at)) = fetch.drain_mark else {
        fetch.drain_mark = Some((consumed, now));
        return;
    };
    if consumed != mark {
        fetch.drained_at = Some(now);
    }
    let elapsed = now.saturating_duration_since(at);
    if elapsed >= Duration::from_millis(1) {
        let drained = u128::from(consumed.saturating_sub(mark));
        let rate = u64::try_from(drained * 1_000_000_000 / elapsed.as_nanos()).unwrap_or(u64::MAX);
        fetch.drain_rate = if fetch.drain_rate == 0 {
            rate
        } else {
            fetch.drain_rate / 2 + rate / 2
        };
        fetch.drain_mark = Some((consumed, now));
    }
}

/// Queues a partition's record set and advances its fetch offset.
pub(super) fn handle_records(
    inner: &ConsumerInner,
    partition: &Arc<Partition>,
    state: &mut super::partition::PartState,
    records: Bytes,
    aborted: Vec<(i64, i64)>,
    wakes: &mut Vec<Wake>,
) {
    let consumer = &inner.settings().consumer;
    let fetch_offset = state.fetch.offset;
    let mut batches = records::parse_batches(records.clone());
    let mut complete = 0;
    let mut bytes = 0u64;
    let mut msgs = 0u64;
    let mut next_offset = fetch_offset;
    let mut leader_epoch = state.fetch.leader_epoch;
    let mut undecodable = None;
    for batch in &mut batches {
        // Undecodable entries among decodable ones are reported by the
        // reader.
        let batch = match batch {
            Ok(batch) => batch,
            Err(e) => {
                undecodable.get_or_insert(e);
                continue;
            }
        };
        let header = batch.header();
        // An entry ending at the largest offset leaves nothing to fetch
        // after it.
        let Some(after) = header.last_offset.checked_add(1) else {
            undecodable.get_or_insert_with(|| CodecError::malformed("last offset out of range"));
            continue;
        };
        complete += 1;
        bytes += u64::try_from(header.size).expect("usize fits u64");
        msgs += accounted_messages(header);
        next_offset = next_offset.max(after);
        leader_epoch = header.partition_leader_epoch;
    }
    if complete == 0 {
        if let Some(e) = undecodable {
            // Nothing in this record set can be decoded, so the fetch
            // offset cannot move past it: report it, like librdkafka, and
            // retry after `fetch.error.backoff.ms` instead of refetching it
            // right away.
            wakes.push(state.push(Item::Error(codec_error(&e)), partition.is_split()));
            state.fetch.backoff_until = Some(Instant::now() + consumer.fetch_error_backoff);
            return;
        }
        if batches.truncated_bytes() > 0 {
            // A single batch is larger than the partition fetch size: grow
            // it, like librdkafka, up to a limit.
            let max_bytes = &mut state.fetch.max_bytes;
            if *max_bytes >= MAX_PARTITION_FETCH_BYTES {
                wakes.push(state.push(
                    Item::Error(KafkaError::MessageConsumption(
                        RDKafkaErrorCode::MessageSizeTooLarge,
                    )),
                    partition.is_split(),
                ));
                state.fetch.backoff_until = Some(Instant::now() + consumer.fetch_error_backoff);
            } else {
                *max_bytes = max_bytes.saturating_mul(2).min(MAX_PARTITION_FETCH_BYTES);
            }
        }
        return;
    }
    state.fetch.max_bytes = consumer.fetch_message_max_bytes;
    if msgs > 0 {
        state.fetch.avg_msg_bytes = (bytes / msgs).max(1);
    }
    state.fetch.leader_epoch = leader_epoch;
    state.fetch.offset = next_offset;
    // The incomplete batch at the end, if any, is fetched again.
    let complete = records.len() - batches.truncated_bytes();
    let (records, capacity) = inner.pool.copy(&records[..complete]);
    // The queue accounts for the buffer, which the limits and targets are
    // about, not just for the batches in it.
    let overhead = u64::try_from(capacity)
        .expect("usize fits u64")
        .saturating_sub(bytes);
    partition.add_queued(bytes + overhead, msgs);
    wakes.push(state.push(
        Item::Chunk(Chunk {
            records,
            fetch_offset,
            aborted,
            overhead,
        }),
        partition.is_split(),
    ));
}

/// Handles a partition-level Fetch error like librdkafka's
/// `rd_kafka_fetch_reply_handle_partition_error`.
fn handle_error(
    inner: &ConsumerInner,
    partition: &Arc<Partition>,
    state: &mut super::partition::PartState,
    broker_id: i32,
    code: RDKafkaErrorCode,
    wakes: &mut Vec<Wake>,
) {
    let consumer = &inner.settings().consumer;
    let mut backoff = consumer.fetch_error_backoff;
    match code {
        RDKafkaErrorCode::OffsetNotAvailable
        | RDKafkaErrorCode::UnknownTopicOrPartition
        | RDKafkaErrorCode::LeaderNotAvailable
        | RDKafkaErrorCode::NotLeaderForPartition
        | RDKafkaErrorCode::BrokerNotAvailable
        | RDKafkaErrorCode::ReplicaNotAvailable
        | RDKafkaErrorCode::KafkaStorageError
        | RDKafkaErrorCode::UnknownLeaderEpoch
        | RDKafkaErrorCode::FencedLeaderEpoch
        | RDKafkaErrorCode::UnknownTopicId => {
            inner.cluster.request_metadata_refresh();
        }
        RDKafkaErrorCode::OffsetOutOfRange => {
            let err_offset = state.fetch.offset;
            state.fetch.offset = super::partition::OFFSET_INVALID;
            if let Some(wake) = offset_reset(
                inner,
                partition,
                state,
                broker_id,
                err_offset,
                code,
                "fetch failed due to requested offset not available on the broker",
            ) {
                wakes.push(wake);
            }
        }
        RDKafkaErrorCode::TopicAuthorizationFailed => {
            // Reported once until another error or a successful fetch.
            if state.fetch.last_error != Some(code) {
                wakes.push(state.push(
                    Item::Error(KafkaError::MessageConsumption(code)),
                    partition.is_split(),
                ));
                state.fetch.last_error = Some(code);
            }
            backoff = Duration::from_millis(1000).max(consumer.fetch_error_backoff * 10);
        }
        _ => {
            wakes.push(state.push(
                Item::Error(KafkaError::MessageConsumption(code)),
                partition.is_split(),
            ));
        }
    }
    if !backoff.is_zero() {
        state.fetch.backoff_until = Some(Instant::now() + backoff);
    }
}
