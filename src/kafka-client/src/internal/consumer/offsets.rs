// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Offset lookups and commits: ListOffsets, OffsetFetch, OffsetCommit, and
//! the resolution of logical start offsets.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::{
    BrokerId, GroupId, ListOffsetsRequest, OffsetCommitRequest, OffsetFetchRequest, TopicName,
};
use kafka_protocol::protocol::StrBytes;

use crate::config::settings::IsolationLevel;
use crate::error::KafkaError;
use crate::internal::cluster::{Broker, Cluster, CoordinatorKind};
use crate::internal::net::{NetError, RequestOptions};
use crate::internal::protocol;
use crate::types::RDKafkaErrorCode;
use crate::util::Deadline;

use super::partition::{
    FetchPhase, Item, OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED, OFFSET_TAIL_BASE,
};
use super::{ConsumerInner, Partition, offset_reset};

/// How many times a request that failed with a retriable error is retried.
const MAX_RETRIES: u32 = 2;

/// The group membership that offset commits carry.
#[derive(Clone, Debug)]
pub(super) struct MemberIdentity {
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

impl Default for MemberIdentity {
    /// The identity of a consumer that only assigns partitions: generation
    /// -1 and no member id.
    fn default() -> MemberIdentity {
        MemberIdentity {
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
        }
    }
}

/// One offset to commit.
#[derive(Clone, Debug)]
pub(super) struct CommitEntry {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub metadata: String,
}

fn topic_name(name: &str) -> TopicName {
    TopicName(StrBytes::from_string(name.to_string()))
}

/// Whether a coordinator error means: look up the coordinator again.
fn is_coordinator_error(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::NotCoordinator
            | RDKafkaErrorCode::CoordinatorNotAvailable
            | RDKafkaErrorCode::BrokerTransportFailure
            | RDKafkaErrorCode::DestroyBroker
            | RDKafkaErrorCode::OperationTimedOut
            | RDKafkaErrorCode::TimedOutQueue
    )
}

/// Sleeps for the retry backoff, or fails if that would pass the deadline.
async fn backoff(cluster: &Cluster, attempt: u32, deadline: Deadline) -> Result<(), ()> {
    let settings = cluster.settings();
    let factor = 1u32 << attempt.min(10);
    let delay = (settings.retry_backoff * factor).min(settings.retry_backoff_max);
    if deadline.remaining_duration().is_some_and(|r| r <= delay) {
        return Err(());
    }
    let shutdown = cluster.shutdown_token();
    tokio::select! {
        _ = tokio::time::sleep(delay) => Ok(()),
        _ = shutdown.cancelled() => Err(()),
    }
}

/// A failed group coordinator request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CoordinatorError {
    pub code: RDKafkaErrorCode,
    /// The last attempt failed to look up the coordinator, so it sent no
    /// request.
    pub lookup: bool,
}

/// Runs `request` against the group coordinator, retrying coordinator and
/// transport errors until the deadline. `request` returns the response's
/// top-level error, if any, alongside its result.
async fn with_coordinator<T, F, Fut>(
    cluster: &Cluster,
    group_id: &str,
    deadline: Deadline,
    mut request: F,
) -> Result<T, CoordinatorError>
where
    F: FnMut(Arc<Broker>) -> Fut,
    Fut: std::future::Future<Output = Result<(Option<RDKafkaErrorCode>, T), NetError>>,
{
    let mut attempt = 0;
    loop {
        let mut lookup = false;
        let code = match cluster
            .coordinator(CoordinatorKind::Group, group_id, deadline)
            .await
        {
            Err(e) => {
                lookup = true;
                e.code()
            }
            Ok(broker) => {
                let response = request(broker);
                let result = match deadline.instant() {
                    Some(at) => tokio::time::timeout_at(at.into(), response)
                        .await
                        .unwrap_or_else(|_| Err(NetError::timed_out())),
                    None => response.await,
                };
                match result {
                    Ok((None, value)) => return Ok(value),
                    Ok((Some(code), _)) => code,
                    Err(e) => e.code(),
                }
            }
        };
        let error = CoordinatorError { code, lookup };
        match code {
            RDKafkaErrorCode::BrokerDestroy => return Err(error),
            code if is_coordinator_error(code) => {
                cluster.invalidate_coordinator(CoordinatorKind::Group, group_id);
            }
            RDKafkaErrorCode::CoordinatorLoadInProgress
            | RDKafkaErrorCode::UnstableOffsetCommit => {}
            _ => return Err(error),
        }
        if backoff(cluster, attempt, deadline).await.is_err() {
            return Err(error);
        }
        attempt += 1;
    }
}

/// Commits offsets for `group_id`. Returns the per-entry errors, or an error
/// for the whole request.
///
/// Coordinator and transport errors are retried until the deadline, then
/// reported as `WaitingForCoordinator`, like librdkafka's commit that waited
/// for a coordinator in vain.
pub(super) async fn commit(
    cluster: &Cluster,
    group_id: &str,
    member: &MemberIdentity,
    entries: &[CommitEntry],
    deadline: Deadline,
) -> Result<Vec<Option<RDKafkaErrorCode>>, RDKafkaErrorCode> {
    let mut by_topic: BTreeMap<&str, Vec<&CommitEntry>> = BTreeMap::new();
    for entry in entries {
        by_topic.entry(&entry.topic).or_default().push(entry);
    }
    let response = with_coordinator(cluster, group_id, deadline, |broker| {
        let by_topic = &by_topic;
        async move {
            let response = broker
                .request::<OffsetCommitRequest, _>(
                    |version| {
                        let mut req = OffsetCommitRequest::default()
                            .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
                            .with_generation_id_or_member_epoch(member.generation_id)
                            .with_member_id(StrBytes::from_string(member.member_id.clone()))
                            .with_topics(
                                by_topic
                                    .iter()
                                    .map(|(topic, entries)| {
                                        OffsetCommitRequestTopic::default()
                                            .with_name(topic_name(topic))
                                            .with_partitions(
                                                entries
                                                    .iter()
                                                    .map(|e| {
                                                        OffsetCommitRequestPartition::default()
                                                            .with_partition_index(e.partition)
                                                            .with_committed_offset(e.offset)
                                                            .with_committed_metadata(Some(
                                                                StrBytes::from_string(
                                                                    e.metadata.clone(),
                                                                ),
                                                            ))
                                                    })
                                                    .collect(),
                                            )
                                    })
                                    .collect(),
                            );
                        if version >= 7 {
                            req.group_instance_id =
                                member.group_instance_id.clone().map(StrBytes::from_string);
                        }
                        req
                    },
                    RequestOptions::default(),
                )
                .await?;
            // Coordinator errors come per partition; retry the whole
            // request on the first one.
            let coordinator_error = response
                .topics
                .iter()
                .flat_map(|t| t.partitions.iter())
                .filter_map(|p| protocol::broker_error(p.error_code))
                .find(|code| {
                    is_coordinator_error(*code)
                        || *code == RDKafkaErrorCode::CoordinatorLoadInProgress
                });
            Ok((coordinator_error, response))
        }
    })
    .await
    .map_err(|e| {
        if is_coordinator_error(e.code) || e.code == RDKafkaErrorCode::CoordinatorLoadInProgress {
            RDKafkaErrorCode::WaitingForCoordinator
        } else {
            e.code
        }
    })?;
    let mut errors: BTreeMap<(&str, i32), RDKafkaErrorCode> = BTreeMap::new();
    for topic in &response.topics {
        for p in &topic.partitions {
            if let Some(code) = protocol::broker_error(p.error_code) {
                if let Some(entry) = entries
                    .iter()
                    .find(|e| e.topic == topic.name.as_str() && e.partition == p.partition_index)
                {
                    errors.insert((&entry.topic, entry.partition), code);
                }
            }
        }
    }
    Ok(entries
        .iter()
        .map(|e| errors.get(&(e.topic.as_str(), e.partition)).copied())
        .collect())
}

/// Fetches the committed offsets of `items` for `group_id`: per item the
/// offset (-1 if none) and metadata, or the partition's error.
///
/// With `require_stable` (under `read_committed`), the broker withholds
/// offsets of pending transactional commits; those are retried until the
/// deadline, as librdkafka does.
pub(super) async fn fetch_committed(
    cluster: &Cluster,
    group_id: &str,
    items: &[(String, i32)],
    require_stable: bool,
    deadline: Deadline,
) -> Result<Vec<Result<(i64, String), RDKafkaErrorCode>>, CoordinatorError> {
    let mut by_topic: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
    for (topic, partition) in items {
        by_topic.entry(topic).or_default().push(*partition);
    }
    let response = with_coordinator(cluster, group_id, deadline, |broker| {
        let by_topic = &by_topic;
        async move {
            let response = broker
                .request::<OffsetFetchRequest, _>(
                    |version| {
                        let mut req = OffsetFetchRequest::default()
                            .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
                            .with_topics(Some(
                                by_topic
                                    .iter()
                                    .map(|(topic, partitions)| {
                                        OffsetFetchRequestTopic::default()
                                            .with_name(topic_name(topic))
                                            .with_partition_indexes(partitions.clone())
                                    })
                                    .collect(),
                            ));
                        if version >= 7 {
                            req.require_stable = require_stable;
                        }
                        req
                    },
                    RequestOptions::default(),
                )
                .await?;
            let mut error = protocol::broker_error(response.error_code);
            if error.is_none() {
                // Retry pending transactional commits and coordinator
                // errors reported per partition.
                error = response
                    .topics
                    .iter()
                    .flat_map(|t| t.partitions.iter())
                    .filter_map(|p| protocol::broker_error(p.error_code))
                    .find(|code| {
                        is_coordinator_error(*code)
                            || matches!(
                                code,
                                RDKafkaErrorCode::CoordinatorLoadInProgress
                                    | RDKafkaErrorCode::UnstableOffsetCommit
                            )
                    });
            }
            Ok((error, response))
        }
    })
    .await?;
    let mut found: BTreeMap<(&str, i32), Result<(i64, String), RDKafkaErrorCode>> = BTreeMap::new();
    for topic in &response.topics {
        for p in &topic.partitions {
            let result = match protocol::broker_error(p.error_code) {
                Some(code) => Err(code),
                None => Ok((
                    p.committed_offset,
                    p.metadata
                        .as_ref()
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_default(),
                )),
            };
            found.insert((topic.name.as_str(), p.partition_index), result);
        }
    }
    Ok(items
        .iter()
        .map(|(topic, partition)| {
            found
                .remove(&(topic.as_str(), *partition))
                .unwrap_or(Ok((-1, String::new())))
        })
        .collect())
}

/// One ListOffsets lookup: a timestamp, or -2 for the earliest and -1 for
/// the latest offset.
#[derive(Clone, Debug)]
pub(super) struct ListOffsetsItem {
    pub topic: String,
    pub partition: i32,
    pub timestamp: i64,
}

/// Errors after which a ListOffsets request is retried, as librdkafka does.
fn list_offsets_retriable(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::LeaderNotAvailable
            | RDKafkaErrorCode::FencedLeaderEpoch
            | RDKafkaErrorCode::UnknownLeaderEpoch
            | RDKafkaErrorCode::NotLeaderForPartition
            | RDKafkaErrorCode::BrokerTransportFailure
            | RDKafkaErrorCode::DestroyBroker
            | RDKafkaErrorCode::RequestTimedOut
    )
}

/// Whether `code` came from a response rather than from this client: local
/// codes are -100 and below.
fn from_response(code: RDKafkaErrorCode) -> bool {
    code.to_i32() > -100
}

/// Whether a failed ListOffsets lookup is reported to the application, like
/// librdkafka reports the errors it neither retries nor refreshes metadata
/// for. A partition without a known leader waits for one silently.
///
/// NOTE: Unlike librdkafka, `UNKNOWN_TOPIC_OR_PARTITION` is not reported
/// here: a leader answers so for a topic it does not know yet right after
/// the topic was created, and topics that do not exist are reported from the
/// metadata.
fn reported_list_offsets_error(e: &OffsetError) -> bool {
    !e.leader_lookup
        && from_response(e.code)
        && !list_offsets_retriable(e.code)
        && !matches!(
            e.code,
            RDKafkaErrorCode::ReplicaNotAvailable
                | RDKafkaErrorCode::KafkaStorageError
                | RDKafkaErrorCode::OffsetNotAvailable
                | RDKafkaErrorCode::UnknownTopicOrPartition
        )
}

/// Whether a failed OffsetFetch is reported to the application. Coordinator
/// changes and pending transactional commits are retried silently.
/// Coordinator lookup failures are reported by the consumer's coordinator
/// query instead, see `query_coordinator`.
fn reported_offset_fetch_error(e: CoordinatorError) -> bool {
    !e.lookup && reported_offset_fetch_code(e.code)
}

/// Whether an OffsetFetch error code in a response is reported. Topics that
/// do not exist are reported from the metadata.
fn reported_offset_fetch_code(code: RDKafkaErrorCode) -> bool {
    from_response(code)
        && !is_coordinator_error(code)
        && !matches!(
            code,
            RDKafkaErrorCode::CoordinatorLoadInProgress
                | RDKafkaErrorCode::UnstableOffsetCommit
                | RDKafkaErrorCode::UnknownTopicOrPartition
        )
}

/// How [`finish`] reports a lookup error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Report {
    /// Not at all.
    Silent,
    /// On the partition's queue, like librdkafka's ListOffsets errors.
    Partition,
    /// By the caller, on the main queue, like librdkafka's OffsetFetch
    /// errors.
    Main,
}

/// Why a ListOffsets lookup failed for one partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct OffsetError {
    pub code: RDKafkaErrorCode,
    /// The partition's leader could not be found, so no request was sent.
    pub leader_lookup: bool,
}

impl OffsetError {
    fn request(code: RDKafkaErrorCode) -> OffsetError {
        OffsetError {
            code,
            leader_lookup: false,
        }
    }
}

/// Looks up offsets at the partitions' leaders with the client's isolation
/// level: per item the offset (-1 if no message has a timestamp at or after
/// the requested one) or the partition's error.
pub(super) async fn list_offsets(
    cluster: &Cluster,
    isolation: IsolationLevel,
    items: &[ListOffsetsItem],
    deadline: Deadline,
) -> Vec<Result<i64, OffsetError>> {
    let mut results: Vec<Result<i64, OffsetError>> =
        vec![Err(OffsetError::request(RDKafkaErrorCode::OperationTimedOut)); items.len()];
    let mut pending: Vec<usize> = (0..items.len()).collect();
    let mut attempt = 0;
    loop {
        let mut by_leader: BTreeMap<i32, (Arc<Broker>, Vec<usize>)> = BTreeMap::new();
        for &i in &pending {
            let item = &items[i];
            match cluster.leader(&item.topic, item.partition, deadline).await {
                Ok(broker) => {
                    by_leader
                        .entry(broker.id())
                        .or_insert_with(|| (broker, Vec::new()))
                        .1
                        .push(i);
                }
                Err(e) => {
                    results[i] = Err(OffsetError {
                        code: e.code(),
                        leader_lookup: true,
                    })
                }
            }
        }
        let requests = by_leader
            .into_values()
            .map(|(broker, indexes)| list_offsets_at(broker, isolation, items, indexes));
        let responses = futures::future::join_all(requests);
        let responses = match deadline.instant() {
            Some(at) => match tokio::time::timeout_at(at.into(), responses).await {
                Ok(r) => r,
                Err(_) => return results,
            },
            None => responses.await,
        };
        let mut retry = Vec::new();
        let mut refresh = false;
        for (index, result) in responses.into_iter().flatten() {
            if let Err(code) = result {
                if list_offsets_retriable(code) {
                    retry.push(index);
                    refresh |= code != RDKafkaErrorCode::RequestTimedOut;
                }
            }
            results[index] = result.map_err(OffsetError::request);
        }
        if retry.is_empty() || attempt >= MAX_RETRIES {
            return results;
        }
        if refresh {
            let mut topics: Vec<String> = retry.iter().map(|i| items[*i].topic.clone()).collect();
            topics.sort();
            topics.dedup();
            let _ = cluster.refresh_metadata(&topics, deadline).await;
        }
        if backoff(cluster, attempt, deadline).await.is_err() {
            return results;
        }
        attempt += 1;
        pending = retry;
    }
}

/// Sends one ListOffsets request for `indexes` of `items` to `broker`.
async fn list_offsets_at(
    broker: Arc<Broker>,
    isolation: IsolationLevel,
    items: &[ListOffsetsItem],
    indexes: Vec<usize>,
) -> Vec<(usize, Result<i64, RDKafkaErrorCode>)> {
    let mut by_topic: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for &i in &indexes {
        by_topic.entry(&items[i].topic).or_default().push(i);
    }
    let isolation_level = match isolation {
        IsolationLevel::ReadUncommitted => 0,
        IsolationLevel::ReadCommitted => 1,
    };
    let response = broker
        .request::<ListOffsetsRequest, _>(
            |version| {
                let mut req = ListOffsetsRequest::default()
                    .with_replica_id(BrokerId(-1))
                    .with_topics(
                        by_topic
                            .iter()
                            .map(|(topic, indexes)| {
                                ListOffsetsTopic::default()
                                    .with_name(topic_name(topic))
                                    .with_partitions(
                                        indexes
                                            .iter()
                                            .map(|i| {
                                                ListOffsetsPartition::default()
                                                    .with_partition_index(items[*i].partition)
                                                    .with_timestamp(items[*i].timestamp)
                                            })
                                            .collect(),
                                    )
                            })
                            .collect(),
                    );
                if version >= 2 {
                    req.isolation_level = isolation_level;
                }
                req
            },
            RequestOptions::default(),
        )
        .await;
    let response = match response {
        Ok(r) => r,
        Err(e) => return indexes.into_iter().map(|i| (i, Err(e.code()))).collect(),
    };
    let mut found: BTreeMap<(&str, i32), Result<i64, RDKafkaErrorCode>> = BTreeMap::new();
    for topic in &response.topics {
        for p in &topic.partitions {
            let result = match protocol::broker_error(p.error_code) {
                Some(code) => Err(code),
                None => Ok(p.offset),
            };
            found.insert((topic.name.as_str(), p.partition_index), result);
        }
    }
    indexes
        .into_iter()
        .map(|i| {
            let result = found
                .remove(&(items[i].topic.as_str(), items[i].partition))
                .unwrap_or(Err(RDKafkaErrorCode::BadMessage));
            (i, result)
        })
        .collect()
}

/// Resolves the logical start offsets of `batch` (partition, version at
/// scheduling, target): committed offsets for `STORED`, with
/// `auto.offset.reset` when there is none, and ListOffsets for the rest.
/// Partitions that failed back off and are retried by the manager.
///
/// Errors that retrying is not expected to fix are reported as consumer
/// errors, once per partition until a different error or a success. Unlike
/// librdkafka, which stops such a partition until it is assigned again
/// (OffsetFetch) or applies `auto.offset.reset` (ListOffsets), the lookup is
/// retried, so the partition recovers once the cause is fixed.
pub(super) async fn resolve(inner: Arc<ConsumerInner>, batch: Vec<(Arc<Partition>, u64, i64)>) {
    let cluster = Arc::clone(&inner.cluster);
    let settings = Arc::clone(inner.settings());
    let deadline = Deadline::new(settings.socket_timeout);
    let (stored, mut logical): (Vec<_>, Vec<_>) = batch
        .into_iter()
        .partition(|(_, _, target)| *target == OFFSET_STORED);

    if !stored.is_empty() {
        let items: Vec<(String, i32)> = stored
            .iter()
            .map(|(p, _, _)| (p.topic.to_string(), p.id))
            .collect();
        let group_id = inner.group_id.clone().expect("assignment requires a group");
        let results = fetch_committed(
            &cluster,
            &group_id,
            &items,
            inner.read_committed(),
            deadline,
        )
        .await;
        // A request-level error is reported once for the whole batch.
        let mut report_request_error = false;
        for (i, (partition, version, target)) in stored.into_iter().enumerate() {
            match results.as_ref().map(|r| r[i].clone()).map_err(|e| *e) {
                Ok(Ok((offset, _))) if offset >= 0 => {
                    partition
                        .committed
                        .store(offset, std::sync::atomic::Ordering::Release);
                    finish(
                        &inner,
                        &partition,
                        version,
                        target,
                        Ok(offset),
                        Report::Silent,
                    );
                }
                Ok(Ok(_)) => {
                    // No committed offset: apply auto.offset.reset.
                    let mut state = partition.lock_state();
                    if state.version != version || state.fetch.phase != FetchPhase::Resolve(target)
                    {
                        continue;
                    }
                    state.fetch.resolving = false;
                    let wake = offset_reset(
                        &inner,
                        &partition,
                        &mut state,
                        -1,
                        OFFSET_STORED,
                        RDKafkaErrorCode::NoOffset,
                        "no previously committed offset available",
                    );
                    if let FetchPhase::Resolve(target) = state.fetch.phase {
                        state.fetch.resolving = true;
                        logical.push((Arc::clone(&partition), version, target));
                    }
                    drop(state);
                    if let Some(wake) = wake {
                        partition.notify(&inner, wake);
                    }
                }
                Ok(Err(code)) => {
                    let report = if reported_offset_fetch_code(code) {
                        Report::Main
                    } else {
                        Report::Silent
                    };
                    if finish(&inner, &partition, version, target, Err(code), report) {
                        inner.push_event(KafkaError::MessageConsumption(code));
                    }
                }
                Err(e) => {
                    let report = if reported_offset_fetch_error(e) {
                        Report::Main
                    } else {
                        Report::Silent
                    };
                    report_request_error |=
                        finish(&inner, &partition, version, target, Err(e.code), report);
                }
            }
        }
        if let Err(e) = results {
            if report_request_error {
                inner.push_event(KafkaError::MessageConsumption(e.code));
            }
        }
    }

    if !logical.is_empty() {
        let items: Vec<ListOffsetsItem> = logical
            .iter()
            .map(|(p, _, target)| ListOffsetsItem {
                topic: p.topic.to_string(),
                partition: p.id,
                timestamp: if *target == OFFSET_BEGINNING {
                    OFFSET_BEGINNING
                } else {
                    OFFSET_END
                },
            })
            .collect();
        let results = list_offsets(&cluster, settings.isolation_level, &items, deadline).await;
        assert_eq!(logical.len(), results.len());
        let mut results = results.into_iter();
        for (partition, version, target) in logical {
            let result = results.next().expect("one result per item");
            let report = match &result {
                Err(e) if reported_list_offsets_error(e) => Report::Partition,
                _ => Report::Silent,
            };
            let result = result.map_err(|e| e.code).map(|offset| {
                if target <= OFFSET_TAIL_BASE {
                    // Like librdkafka, a tail beyond the start reads from 0.
                    let tail = OFFSET_TAIL_BASE - target;
                    (offset - tail).max(0)
                } else {
                    offset
                }
            });
            finish(&inner, &partition, version, target, result, report);
        }
    }
    inner.wake();
}

/// Applies a resolved start offset, unless the partition was restarted
/// meanwhile. Returns whether the caller must report the error, for
/// [`Report::Main`].
fn finish(
    inner: &ConsumerInner,
    partition: &Arc<Partition>,
    version: u64,
    target: i64,
    result: Result<i64, RDKafkaErrorCode>,
    report: Report,
) -> bool {
    let mut state = partition.lock_state();
    if state.version != version || state.fetch.phase != FetchPhase::Resolve(target) {
        return false;
    }
    state.fetch.resolving = false;
    match result {
        Ok(offset) => {
            let fetch = &mut state.fetch;
            fetch.phase = FetchPhase::Active;
            fetch.offset = offset;
            fetch.resolve_failures = 0;
            fetch.backoff_until = None;
            fetch.last_error = None;
            false
        }
        Err(code) => {
            let new_error = report != Report::Silent && state.fetch.last_error != Some(code);
            let mut wake = None;
            if new_error {
                state.fetch.last_error = Some(code);
                if report == Report::Partition {
                    wake = Some(state.push(
                        Item::Error(KafkaError::MessageConsumption(code)),
                        partition.is_split(),
                    ));
                }
            }
            let fetch = &mut state.fetch;
            let settings = inner.settings();
            let factor = 1u32 << fetch.resolve_failures.min(10);
            let delay = (settings.retry_backoff * factor)
                .min(settings.retry_backoff_max)
                .max(Duration::from_millis(1));
            fetch.resolve_failures += 1;
            fetch.backoff_until = Some(Instant::now() + delay);
            inner.logger().debug(
                crate::config::settings::DebugContext::Fetch,
                "OFFSET",
                format_args!(
                    "[thrd:main]: {} [{}]: offset lookup failed: {}",
                    partition.topic,
                    partition.id,
                    code.err2str()
                ),
            );
            drop(state);
            if let Some(wake) = wake {
                partition.notify(inner, wake);
            }
            new_error && report == Report::Main
        }
    }
}
