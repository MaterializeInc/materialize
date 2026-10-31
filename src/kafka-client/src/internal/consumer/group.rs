// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Consumer group membership with the classic protocol and eager
//! rebalancing, for [`ConsumerInner::subscribe`].
//!
//! The group task loops through: refresh the subscribed topics' metadata,
//! JoinGroup (retrying `MEMBER_ID_REQUIRED` with the member id the
//! coordinator assigned), compute the assignment if elected leader,
//! SyncGroup, apply the assignment, then heartbeat every
//! `heartbeat.interval.ms` until the coordinator asks for a rebalance, the
//! partition counts of the subscribed topics change, or the application
//! stops polling for `max.poll.interval.ms`. Each rebalance first revokes the
//! whole assignment, as the eager protocol requires. Partitions start at
//! their committed offsets, or per `auto.offset.reset`.
//!
//! Errors the application should see (an unavailable subscribed topic,
//! authorization failures) are queued on the main queue as consumer errors.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition as AssignedTopic;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity as LeaveMember;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ConsumerProtocolAssignment, ConsumerProtocolSubscription, GroupId, HeartbeatRequest,
    JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, SyncGroupRequest, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use mz_ore::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::RDKafkaLogLevel;
use crate::config::settings::AssignmentStrategy;
use crate::consumer::Rebalance;
use crate::error::KafkaError;
use crate::internal::cluster::{CoordinatorKind, MetadataSnapshot};
use crate::internal::net::{NetError, RequestOptions};
use crate::internal::{protocol, runtime};
use crate::topic_partition_list::TopicPartitionList;
use crate::types::RDKafkaErrorCode;
use crate::util::Deadline;

use super::ConsumerInner;
use super::assignor::{self, Member};
use super::offsets::MemberIdentity;
use super::partition::{OFFSET_INVALID, OFFSET_STORED};

/// The consumer protocol version this client writes. Versions 1 to 3 only
/// add fields; readers accept any version.
const CONSUMER_PROTOCOL_VERSION: i16 = 3;

/// How often metadata is refreshed while a subscribed topic is missing.
const MISSING_TOPIC_REFRESH: Duration = Duration::from_secs(1);

/// The running membership of a subscribed consumer.
pub(super) struct GroupHandle {
    inner: Weak<ConsumerInner>,
    shared: Arc<Mutex<MemberIdentity>>,
    group_id: String,
    cancel: CancellationToken,
    task: JoinHandle<String>,
}

impl GroupHandle {
    /// Starts joining `group_id` with a subscription to `topics`.
    pub(super) fn start(
        inner: Arc<ConsumerInner>,
        group_id: String,
        topics: Vec<String>,
    ) -> GroupHandle {
        let shared = Arc::new(Mutex::new(MemberIdentity {
            group_instance_id: inner.settings().consumer.group_instance_id.clone(),
            ..MemberIdentity::default()
        }));
        let cancel = inner.shutdown.child_token();
        let task = runtime::spawn(
            "kafka-consumer-group",
            run(
                Arc::clone(&inner),
                Arc::clone(&shared),
                group_id.clone(),
                topics,
                cancel.clone(),
            ),
        );
        GroupHandle {
            inner: Arc::downgrade(&inner),
            shared,
            group_id,
            cancel,
            task,
        }
    }

    /// The generation and member id to commit offsets with.
    pub(super) fn identity(&self) -> MemberIdentity {
        self.shared.lock().expect("lock poisoned").clone()
    }

    /// Stops the membership, revokes the assignment and leaves the group.
    pub(super) async fn leave(self) {
        self.cancel.cancel();
        let identity = self.identity();
        let member_id = self.task.await;
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        revoke(&inner).await;
        if member_id.is_empty() {
            return;
        }
        let deadline = Deadline::new(inner.settings().socket_timeout);
        let Ok(coordinator) = inner
            .cluster
            .coordinator(CoordinatorKind::Group, &self.group_id, deadline)
            .await
        else {
            return;
        };
        let group_id = self.group_id.clone();
        let _ = coordinator
            .request::<LeaveGroupRequest, _>(
                |version| {
                    let mut req =
                        LeaveGroupRequest::default().with_group_id(group_id_of(&group_id));
                    if version >= 3 {
                        let mut member = LeaveMember::default()
                            .with_member_id(StrBytes::from_string(member_id.clone()))
                            .with_group_instance_id(
                                identity
                                    .group_instance_id
                                    .clone()
                                    .map(StrBytes::from_string),
                            );
                        if version >= 5 {
                            member.reason =
                                Some(StrBytes::from_static_str("the consumer is being closed"));
                        }
                        req.members = vec![member];
                    } else {
                        req.member_id = StrBytes::from_string(member_id.clone());
                    }
                    req
                },
                RequestOptions::default(),
            )
            .await;
    }
}

fn group_id_of(group_id: &str) -> GroupId {
    GroupId(StrBytes::from_string(group_id.to_string()))
}

/// Why joining the group failed: rejoin, after the retry backoff unless
/// `immediately`, with `member_id` if the coordinator assigned one.
struct Failure {
    code: RDKafkaErrorCode,
    immediately: bool,
    member_id: Option<String>,
    /// The coordinator lookup failed. Such errors reach the application
    /// through the consumer's coordinator query instead.
    lookup: bool,
}

impl Failure {
    fn new(code: RDKafkaErrorCode) -> Failure {
        Failure {
            code,
            immediately: false,
            member_id: None,
            lookup: false,
        }
    }
}

impl From<NetError> for Failure {
    fn from(e: NetError) -> Failure {
        Failure::new(e.code())
    }
}

/// The group task. Returns the member id it last held, for LeaveGroup.
async fn run(
    inner: Arc<ConsumerInner>,
    shared: Arc<Mutex<MemberIdentity>>,
    group_id: String,
    topics: Vec<String>,
    cancel: CancellationToken,
) -> String {
    let settings = Arc::clone(inner.settings());
    let consumer = &settings.consumer;
    let instance_id = consumer.group_instance_id.clone();
    let mut member_id = String::new();
    let mut failures: u32 = 0;
    let mut reported: BTreeMap<String, RDKafkaErrorCode> = BTreeMap::new();
    loop {
        if cancel.is_cancelled() {
            return member_id;
        }
        // Wait out a previous `max.poll.interval.ms` violation until the
        // application polls again.
        while poll_overdue(&inner) {
            tokio::select! {
                _ = cancel.cancelled() => return member_id,
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }

        // Keep metadata updates that arrive while JoinGroup or SyncGroup awaits.
        let mut md_rx = inner.cluster.subscribe_metadata();
        let md = refresh(&inner, &topics).await;
        report_missing(&inner, &md, &topics, &mut reported);

        let joined = tokio::select! {
            _ = cancel.cancelled() => return member_id,
            r = join_and_sync(
                &inner, &group_id, &member_id, instance_id.as_deref(), &topics, md,
            ) => r,
        };
        let (generation, assignment, md) = match joined {
            Ok(joined) => {
                failures = 0;
                member_id = joined.member_id;
                (joined.generation, joined.assignment, joined.metadata)
            }
            Err(Failure {
                code,
                immediately,
                member_id: assigned_id,
                lookup,
            }) => {
                if let Some(id) = assigned_id {
                    member_id = id;
                }
                match code {
                    RDKafkaErrorCode::MemberIdRequired => {}
                    RDKafkaErrorCode::UnknownMemberId => member_id.clear(),
                    code if is_coordinator_error(code) => {
                        inner
                            .cluster
                            .invalidate_coordinator(CoordinatorKind::Group, &group_id);
                    }
                    RDKafkaErrorCode::RebalanceInProgress
                    | RDKafkaErrorCode::IllegalGeneration
                    | RDKafkaErrorCode::CoordinatorLoadInProgress => {}
                    RDKafkaErrorCode::BrokerDestroy => return member_id,
                    _ if lookup => {}
                    code => {
                        inner.log(
                            RDKafkaLogLevel::Error,
                            "JOINGROUP",
                            format_args!("JoinGroup failed: {}", code.err2str()),
                        );
                        inner.push_event(KafkaError::MessageConsumption(code));
                    }
                }
                if !immediately {
                    let delay = retry_delay(&settings, failures);
                    failures += 1;
                    tokio::select! {
                        _ = cancel.cancelled() => return member_id,
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
                continue;
            }
        };
        // The leader refreshes metadata while assigning. Comparing with the
        // pre-join snapshot would trigger a second rebalance and replay records
        // if a topic appeared during the join.
        let counts = partition_counts(&md, &topics);
        *shared.lock().expect("lock poisoned") = MemberIdentity {
            generation_id: generation,
            member_id: member_id.clone(),
            group_instance_id: instance_id.clone(),
        };
        assign(&inner, assignment);

        // Heartbeat until a rebalance is needed.
        let mut last_ok = Instant::now();
        let mut next_heartbeat = Instant::now() + consumer.heartbeat_interval;
        let mut next_missing_refresh = Instant::now() + MISSING_TOPIC_REFRESH;
        loop {
            let missing = counts.len() < topics.len();
            let wake_at = if missing {
                next_heartbeat.min(next_missing_refresh)
            } else {
                next_heartbeat
            };
            tokio::select! {
                _ = cancel.cancelled() => return member_id,
                changed = md_rx.changed() => {
                    if changed.is_err() {
                        return member_id;
                    }
                    let md = Arc::clone(&md_rx.borrow_and_update());
                    report_missing(&inner, &md, &topics, &mut reported);
                    if partition_counts(&md, &topics) != counts {
                        inner.log(
                            RDKafkaLogLevel::Info,
                            "REJOIN",
                            format_args!("Group \"{group_id}\": subscribed topics changed: rejoining"),
                        );
                        break;
                    }
                }
                _ = tokio::time::sleep_until(wake_at.into()) => {
                    let now = Instant::now();
                    if missing && now >= next_missing_refresh {
                        inner.cluster.request_metadata_refresh();
                        next_missing_refresh = now + MISSING_TOPIC_REFRESH;
                    }
                    if now < next_heartbeat {
                        continue;
                    }
                    next_heartbeat = now + consumer.heartbeat_interval;
                    if poll_overdue(&inner) {
                        let max_poll = consumer.max_poll_interval;
                        let exceeded = inner.since_poll().saturating_sub(max_poll);
                        inner.log(
                            RDKafkaLogLevel::Warning,
                            "MAXPOLL",
                            format_args!(
                                "Application maximum poll interval ({}ms) exceeded by {}ms \
                                 (adjust max.poll.interval.ms for long-running message \
                                 processing): leaving group",
                                max_poll.as_millis(),
                                exceeded.as_millis()
                            ),
                        );
                        let error = KafkaError::MessageConsumption(RDKafkaErrorCode::PollExceeded);
                        inner.push_event(error);
                        revoke(&inner).await;
                        send_leave(&inner, &group_id, &member_id, instance_id.as_deref()).await;
                        member_id.clear();
                        break;
                    }
                    let instance = instance_id.as_deref();
                    match heartbeat(&inner, &group_id, generation, &member_id, instance).await {
                        Ok(()) => last_ok = Instant::now(),
                        Err(
                            RDKafkaErrorCode::RebalanceInProgress
                            | RDKafkaErrorCode::IllegalGeneration,
                        ) => break,
                        Err(RDKafkaErrorCode::UnknownMemberId) => {
                            member_id.clear();
                            break;
                        }
                        Err(RDKafkaErrorCode::BrokerDestroy) => return member_id,
                        Err(code @ RDKafkaErrorCode::FencedInstanceId) => {
                            inner.push_event(KafkaError::MessageConsumption(code));
                            member_id.clear();
                            break;
                        }
                        Err(code) => {
                            if is_coordinator_error(code) {
                                inner
                                    .cluster
                                    .invalidate_coordinator(CoordinatorKind::Group, &group_id);
                            }
                            if last_ok.elapsed() >= consumer.session_timeout {
                                // The coordinator has expired the session by now.
                                inner.log(
                                    RDKafkaLogLevel::Warning,
                                    "SESSTMOUT",
                                    format_args!(
                                        "Consumer group session timed out (in join-state steady) \
                                         over {}ms: revoking assignment and rejoining group",
                                        consumer.session_timeout.as_millis()
                                    ),
                                );
                                member_id.clear();
                                break;
                            }
                        }
                    }
                }
            }
        }
        revoke(&inner).await;
        shared.lock().expect("lock poisoned").generation_id = -1;
    }
}

fn retry_delay(settings: &crate::config::ClientSettings, failures: u32) -> Duration {
    let factor = 1u32 << failures.min(10);
    (settings.retry_backoff * factor).min(settings.retry_backoff_max.max(settings.retry_backoff))
}

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

/// The application has not polled for `max.poll.interval.ms`.
fn poll_overdue(inner: &ConsumerInner) -> bool {
    inner.since_poll() > inner.settings().consumer.max_poll_interval
}

/// Refreshes the metadata of the subscribed topics, falling back to the
/// cached metadata on errors.
async fn refresh(inner: &ConsumerInner, topics: &[String]) -> Arc<MetadataSnapshot> {
    let deadline = Deadline::new(inner.settings().socket_timeout);
    match inner.cluster.refresh_metadata(topics, deadline).await {
        Ok(md) => md,
        Err(_) => inner.cluster.metadata(),
    }
}

/// Partition counts of the subscribed topics that exist.
fn partition_counts(md: &MetadataSnapshot, topics: &[String]) -> BTreeMap<String, i32> {
    topics
        .iter()
        .filter_map(|t| {
            let count = md.partition_count(t)?;
            (count > 0).then(|| (t.clone(), i32::try_from(count).unwrap_or(i32::MAX)))
        })
        .collect()
}

/// Queues a consumer error for each subscribed topic that became
/// unavailable, once per error, like librdkafka's "Subscribed topic not
/// available".
fn report_missing(
    inner: &ConsumerInner,
    md: &MetadataSnapshot,
    topics: &[String],
    reported: &mut BTreeMap<String, RDKafkaErrorCode>,
) {
    for topic in topics {
        match md.topic(topic).and_then(|t| t.error) {
            Some(code) => {
                if reported.get(topic) != Some(&code) {
                    reported.insert(topic.clone(), code);
                    inner.log(
                        RDKafkaLogLevel::Debug,
                        "TOPICERR",
                        format_args!(
                            "Subscribed topic not available: {topic}: {}",
                            code.err2str()
                        ),
                    );
                    inner.push_event(KafkaError::MessageConsumption(code));
                }
            }
            None => {
                if md.topic(topic).is_some() {
                    reported.remove(topic);
                }
            }
        }
    }
}

struct Joined {
    generation: i32,
    member_id: String,
    assignment: Vec<(String, Vec<i32>)>,
    metadata: Arc<MetadataSnapshot>,
}

/// Encodes a consumer protocol message with its version prefix.
fn encode_versioned<T: Encodable>(message: &T, version: i16) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(version);
    message
        .encode(&mut buf, version)
        .expect("consumer protocol messages encode");
    buf.freeze()
}

/// Decodes a consumer protocol message with its version prefix. Versions
/// newer than this client knows only append fields, so they are read as the
/// newest known version.
fn decode_versioned<T: Decodable>(mut data: Bytes) -> Option<T> {
    if data.remaining() < 2 {
        return None;
    }
    let version = data.get_i16();
    if version < 0 {
        return None;
    }
    T::decode(&mut data, version.min(CONSUMER_PROTOCOL_VERSION)).ok()
}

/// One JoinGroup and SyncGroup round trip.
async fn join_and_sync(
    inner: &ConsumerInner,
    group_id: &str,
    member_id: &str,
    instance_id: Option<&str>,
    topics: &[String],
    metadata: Arc<MetadataSnapshot>,
) -> Result<Joined, Failure> {
    let settings = Arc::clone(inner.settings());
    let consumer = &settings.consumer;
    let deadline = Deadline::new(settings.socket_timeout);
    let coordinator = inner
        .cluster
        .coordinator(CoordinatorKind::Group, group_id, deadline)
        .await
        .map_err(|e| Failure {
            lookup: true,
            ..Failure::new(e.code())
        })?;
    let strategies: Vec<AssignmentStrategy> = consumer
        .partition_assignment_strategy
        .iter()
        .copied()
        .filter(|s| *s != AssignmentStrategy::CooperativeSticky)
        .collect();
    let subscription = encode_versioned(
        &ConsumerProtocolSubscription::default().with_topics(
            topics
                .iter()
                .map(|t| StrBytes::from_string(t.clone()))
                .collect(),
        ),
        CONSUMER_PROTOCOL_VERSION,
    );
    let session_timeout_ms = millis_i32(consumer.session_timeout);
    let rebalance_timeout_ms = millis_i32(consumer.max_poll_interval);
    // The coordinator holds JoinGroup until the rebalance completes, up to
    // the rebalance timeout.
    let join_timeout = consumer.max_poll_interval + Duration::from_secs(3);
    let response: JoinGroupResponse = coordinator
        .request::<JoinGroupRequest, _>(
            |version| {
                let mut req = JoinGroupRequest::default()
                    .with_group_id(group_id_of(group_id))
                    .with_session_timeout_ms(session_timeout_ms)
                    .with_member_id(StrBytes::from_string(member_id.to_string()))
                    .with_protocol_type(StrBytes::from_static_str("consumer"))
                    .with_protocols(
                        strategies
                            .iter()
                            .map(|s| {
                                JoinGroupRequestProtocol::default()
                                    .with_name(StrBytes::from_static_str(s.name()))
                                    .with_metadata(subscription.clone())
                            })
                            .collect(),
                    );
                if version >= 1 {
                    req.rebalance_timeout_ms = rebalance_timeout_ms;
                }
                if version >= 5 {
                    req.group_instance_id =
                        instance_id.map(|i| StrBytes::from_string(i.to_string()));
                }
                req
            },
            RequestOptions::with_timeout(join_timeout),
        )
        .await?;
    if let Some(code) = protocol::broker_error(response.error_code) {
        if code == RDKafkaErrorCode::MemberIdRequired {
            // Rejoin right away with the member id the coordinator chose.
            return Err(Failure {
                code,
                immediately: true,
                lookup: false,
                member_id: Some(response.member_id.as_str().to_string()),
            });
        }
        return Err(Failure::new(code));
    }
    let new_member_id = response.member_id.as_str().to_string();
    let generation = response.generation_id;
    let protocol_name = response
        .protocol_name
        .as_ref()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "range".to_string());
    inner.log(
        RDKafkaLogLevel::Debug,
        "JOINGROUP",
        format_args!(
            "JoinGroup response: GenerationId {generation}, Protocol {protocol_name}, \
             LeaderId {}, my MemberId {new_member_id}, member metadata count {}",
            response.leader.as_str(),
            response.members.len()
        ),
    );

    let (assignments, metadata) =
        if response.leader.as_str() == new_member_id && !response.skip_assignment {
            leader_assign(inner, &response, &protocol_name).await
        } else {
            (Vec::new(), metadata)
        };
    let sync = coordinator
        .request::<SyncGroupRequest, _>(
            |version| {
                let mut req = SyncGroupRequest::default()
                    .with_group_id(group_id_of(group_id))
                    .with_generation_id(generation)
                    .with_member_id(StrBytes::from_string(new_member_id.clone()))
                    .with_assignments(assignments.clone());
                if version >= 3 {
                    req.group_instance_id =
                        instance_id.map(|i| StrBytes::from_string(i.to_string()));
                }
                if version >= 5 {
                    req.protocol_type = Some(StrBytes::from_static_str("consumer"));
                    req.protocol_name = Some(StrBytes::from_string(protocol_name.clone()));
                }
                req
            },
            RequestOptions::with_timeout(join_timeout),
        )
        .await?;
    if let Some(code) = protocol::broker_error(sync.error_code) {
        return Err(Failure::new(code));
    }
    let assignment: Vec<(String, Vec<i32>)> = if sync.assignment.is_empty() {
        Vec::new()
    } else {
        match decode_versioned::<ConsumerProtocolAssignment>(sync.assignment) {
            Some(a) => a
                .assigned_partitions
                .into_iter()
                .map(|t| (t.topic.0.as_str().to_string(), t.partitions))
                .collect(),
            None => return Err(Failure::new(RDKafkaErrorCode::BadMessage)),
        }
    };
    Ok(Joined {
        generation,
        member_id: new_member_id,
        assignment,
        metadata,
    })
}

/// Computes the group's assignment as its leader.
async fn leader_assign(
    inner: &ConsumerInner,
    response: &JoinGroupResponse,
    protocol_name: &str,
) -> (Vec<SyncGroupRequestAssignment>, Arc<MetadataSnapshot>) {
    let members: Vec<Member> = response
        .members
        .iter()
        .map(|m| Member {
            member_id: m.member_id.as_str().to_string(),
            topics: decode_versioned::<ConsumerProtocolSubscription>(m.metadata.clone())
                .map(|s| s.topics.iter().map(|t| t.as_str().to_string()).collect())
                .unwrap_or_default(),
        })
        .collect();
    let mut all_topics: Vec<String> = members.iter().flat_map(|m| m.topics.clone()).collect();
    all_topics.sort();
    all_topics.dedup();
    let md = refresh(inner, &all_topics).await;
    let counts = partition_counts(&md, &all_topics);
    let assignment = match protocol_name {
        "roundrobin" => assignor::round_robin(&members, &counts),
        _ => assignor::range(&members, &counts),
    };
    inner.log(
        RDKafkaLogLevel::Debug,
        "ASSIGN",
        format_args!(
            "Group leader: assigned {} topic(s) to {} member(s) with {protocol_name}",
            counts.len(),
            members.len()
        ),
    );
    let assignments = assignment
        .into_iter()
        .map(|(member_id, topics)| {
            let message = ConsumerProtocolAssignment::default().with_assigned_partitions(
                topics
                    .into_iter()
                    .map(|(topic, partitions)| {
                        AssignedTopic::default()
                            .with_topic(TopicName(StrBytes::from_string(topic)))
                            .with_partitions(partitions)
                    })
                    .collect(),
            );
            SyncGroupRequestAssignment::default()
                .with_member_id(StrBytes::from_string(member_id))
                .with_assignment(encode_versioned(&message, CONSUMER_PROTOCOL_VERSION))
        })
        .collect();
    (assignments, md)
}

async fn heartbeat(
    inner: &ConsumerInner,
    group_id: &str,
    generation: i32,
    member_id: &str,
    instance_id: Option<&str>,
) -> Result<(), RDKafkaErrorCode> {
    let deadline = Deadline::new(inner.settings().socket_timeout);
    let coordinator = inner
        .cluster
        .coordinator(CoordinatorKind::Group, group_id, deadline)
        .await
        .map_err(|e| e.code())?;
    let response = coordinator
        .request::<HeartbeatRequest, _>(
            |version| {
                let mut req = HeartbeatRequest::default()
                    .with_group_id(group_id_of(group_id))
                    .with_generation_id(generation)
                    .with_member_id(StrBytes::from_string(member_id.to_string()));
                if version >= 3 {
                    req.group_instance_id =
                        instance_id.map(|i| StrBytes::from_string(i.to_string()));
                }
                req
            },
            RequestOptions::default(),
        )
        .await
        .map_err(|e| e.code())?;
    match protocol::broker_error(response.error_code) {
        None => Ok(()),
        Some(code) => Err(code),
    }
}

/// Sends LeaveGroup, ignoring errors.
async fn send_leave(
    inner: &ConsumerInner,
    group_id: &str,
    member_id: &str,
    instance_id: Option<&str>,
) {
    if member_id.is_empty() {
        return;
    }
    let deadline = Deadline::new(inner.settings().socket_timeout);
    let Ok(coordinator) = inner
        .cluster
        .coordinator(CoordinatorKind::Group, group_id, deadline)
        .await
    else {
        return;
    };
    let _ = coordinator
        .request::<LeaveGroupRequest, _>(
            |version| {
                let mut req = LeaveGroupRequest::default().with_group_id(group_id_of(group_id));
                if version >= 3 {
                    req.members = vec![
                        LeaveMember::default()
                            .with_member_id(StrBytes::from_string(member_id.to_string()))
                            .with_group_instance_id(
                                instance_id.map(|i| StrBytes::from_string(i.to_string())),
                            ),
                    ];
                } else {
                    req.member_id = StrBytes::from_string(member_id.to_string());
                }
                req
            },
            RequestOptions::default(),
        )
        .await;
}

/// Applies a new assignment from the group, around the rebalance callbacks.
/// Partitions start at their committed offsets.
fn assign(inner: &ConsumerInner, assignment: Vec<(String, Vec<i32>)>) {
    let mut entries: Vec<(String, i32, i64)> = assignment
        .into_iter()
        .flat_map(|(topic, partitions)| {
            partitions
                .into_iter()
                .map(move |p| (topic.clone(), p, OFFSET_STORED))
        })
        .collect();
    entries.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    entries.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
    let mut tpl = TopicPartitionList::with_capacity(entries.len());
    for (topic, partition, _) in &entries {
        tpl.push_entry(
            topic,
            *partition,
            OFFSET_INVALID,
            String::new(),
            RDKafkaErrorCode::NoError,
        );
    }
    let hooks = Arc::clone(inner.hooks());
    hooks.pre_rebalance(&Rebalance::Assign(&tpl));
    inner.replace_assignment(entries, false);
    hooks.post_rebalance(&Rebalance::Assign(&tpl));
}

/// Revokes the whole assignment, around the rebalance callbacks. With
/// `enable.auto.commit`, commits the offset store first.
async fn revoke(inner: &ConsumerInner) {
    let tpl = {
        let state = inner.lock_state();
        if state.assignment.is_empty() {
            return;
        }
        let mut tpl = TopicPartitionList::with_capacity(state.assignment.len());
        for a in &state.assignment {
            tpl.push_entry(
                &a.partition.topic,
                a.partition.id,
                OFFSET_INVALID,
                String::new(),
                RDKafkaErrorCode::NoError,
            );
        }
        tpl
    };
    let hooks = Arc::clone(inner.hooks());
    hooks.pre_rebalance(&Rebalance::Revoke(&tpl));
    if inner.settings().consumer.enable_auto_commit {
        let offsets = inner.stored_offsets();
        if !offsets.is_empty() {
            let timeout = inner.settings().socket_timeout;
            let _ = tokio::time::timeout(timeout, inner.commit_task(offsets)).await;
        }
    }
    inner.replace_assignment(Vec::new(), false);
    hooks.post_rebalance(&Rebalance::Revoke(&tpl));
}

fn millis_i32(d: Duration) -> i32 {
    i32::try_from(d.as_millis()).unwrap_or(i32::MAX)
}
