// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The metadata cache: an immutable snapshot replaced on every Metadata
//! response.

use std::collections::BTreeMap;
use std::sync::Arc;

use kafka_protocol::messages::MetadataResponse;

use crate::internal::protocol;
use crate::metadata::{Metadata, MetadataBroker, MetadataPartition, MetadataTopic};
use crate::types::RDKafkaErrorCode;

/// A broker as advertised in metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BrokerMetadata {
    pub id: i32,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
}

/// One partition of a topic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PartitionMetadata {
    pub id: i32,
    /// `-1` if there is no leader.
    pub leader: i32,
    /// `-1` if the broker did not report it (Metadata < v7).
    pub leader_epoch: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    /// The partition-level error, e.g. `LeaderNotAvailable`.
    pub error: Option<RDKafkaErrorCode>,
}

/// One topic.
#[derive(Clone, Debug)]
pub(crate) struct TopicMetadata {
    /// The topic-level error, e.g. `UnknownTopicOrPartition` for a topic
    /// that does not exist.
    pub error: Option<RDKafkaErrorCode>,
    /// Sorted by partition id.
    pub partitions: Vec<PartitionMetadata>,
}

impl TopicMetadata {
    pub(crate) fn partition(&self, id: i32) -> Option<&PartitionMetadata> {
        self.partitions
            .binary_search_by_key(&id, |p| p.id)
            .ok()
            .map(|i| &self.partitions[i])
    }
}

/// An immutable view of the cluster metadata.
///
/// Brokers and controller are those of the latest response; topics
/// accumulate across responses, each replaced by the latest response that
/// mentions it. A response for all topics also removes topics it does not
/// list.
#[derive(Clone, Debug, Default)]
pub(crate) struct MetadataSnapshot {
    /// Incremented with every update.
    pub version: u64,
    pub brokers: BTreeMap<i32, BrokerMetadata>,
    /// `-1` if unknown.
    pub controller_id: i32,
    pub cluster_id: Option<String>,
    pub topics: BTreeMap<String, TopicMetadata>,
}

impl MetadataSnapshot {
    pub(crate) fn topic(&self, name: &str) -> Option<&TopicMetadata> {
        self.topics.get(name)
    }

    /// The number of partitions of `topic`, if it is known and has no
    /// topic-level error.
    pub(crate) fn partition_count(&self, topic: &str) -> Option<usize> {
        self.topic(topic)
            .filter(|t| t.error.is_none())
            .map(|t| t.partitions.len())
    }

    /// The snapshot after applying `response`.
    pub(crate) fn apply(&self, response: &MetadataResponse, all_topics: bool) -> MetadataSnapshot {
        let mut next = self.clone();
        next.version += 1;
        next.brokers = response
            .brokers
            .iter()
            .map(|b| {
                let id = b.node_id.0;
                (
                    id,
                    BrokerMetadata {
                        id,
                        host: b.host.as_str().to_string(),
                        port: u16::try_from(b.port).unwrap_or(0),
                        rack: b.rack.as_ref().map(|r| r.as_str().to_string()),
                    },
                )
            })
            .collect();
        next.controller_id = response.controller_id.0;
        if let Some(id) = &response.cluster_id {
            next.cluster_id = Some(id.as_str().to_string());
        }
        if all_topics {
            next.topics.clear();
        }
        for t in &response.topics {
            let Some(name) = &t.name else { continue };
            let mut partitions: Vec<PartitionMetadata> = t
                .partitions
                .iter()
                .map(|p| PartitionMetadata {
                    id: p.partition_index,
                    leader: p.leader_id.0,
                    leader_epoch: p.leader_epoch,
                    replicas: p.replica_nodes.iter().map(|b| b.0).collect(),
                    isr: p.isr_nodes.iter().map(|b| b.0).collect(),
                    error: protocol::broker_error(p.error_code),
                })
                .collect();
            partitions.sort_by_key(|p| p.id);
            next.topics.insert(
                name.0.as_str().to_string(),
                TopicMetadata {
                    error: protocol::broker_error(t.error_code),
                    partitions,
                },
            );
        }
        next
    }
}

/// The public [`Metadata`] for a response, in response order, as librdkafka
/// returns it. `orig_broker_id` and `orig_broker_name` identify the broker
/// that answered.
pub(crate) fn to_public(
    response: &MetadataResponse,
    orig_broker_id: i32,
    orig_broker_name: String,
) -> Metadata {
    let brokers = response
        .brokers
        .iter()
        .map(|b| MetadataBroker::new(b.node_id.0, b.host.as_str().to_string(), b.port))
        .collect();
    let topics = response
        .topics
        .iter()
        .filter_map(|t| {
            let name = t.name.as_ref()?.0.as_str().to_string();
            let mut partitions: Vec<MetadataPartition> = t
                .partitions
                .iter()
                .map(|p| {
                    MetadataPartition::new(
                        p.partition_index,
                        p.leader_id.0,
                        protocol::broker_error(p.error_code).unwrap_or(RDKafkaErrorCode::NoError),
                        p.replica_nodes.iter().map(|b| b.0).collect(),
                        p.isr_nodes.iter().map(|b| b.0).collect(),
                    )
                })
                .collect();
            partitions.sort_by_key(|p| p.id());
            Some(MetadataTopic::new(
                name,
                partitions,
                protocol::broker_error(t.error_code).unwrap_or(RDKafkaErrorCode::NoError),
            ))
        })
        .collect();
    Metadata::new(orig_broker_id, orig_broker_name, brokers, topics)
}

/// A shared snapshot.
pub(crate) type SharedMetadata = Arc<MetadataSnapshot>;

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::{BrokerId, TopicName};
    use kafka_protocol::protocol::StrBytes;

    use super::*;

    fn topic(name: &str, error: i16, partitions: &[(i32, i32, i16)]) -> MetadataResponseTopic {
        MetadataResponseTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(name.into()))))
            .with_error_code(error)
            .with_partitions(
                partitions
                    .iter()
                    .map(|(id, leader, err)| {
                        MetadataResponsePartition::default()
                            .with_partition_index(*id)
                            .with_leader_id(BrokerId(*leader))
                            .with_error_code(*err)
                            .with_replica_nodes(vec![BrokerId(*leader)])
                            .with_isr_nodes(vec![BrokerId(*leader)])
                    })
                    .collect(),
            )
    }

    #[mz_ore::test]
    fn applies_responses() {
        let response = MetadataResponse::default()
            .with_brokers(vec![
                MetadataResponseBroker::default()
                    .with_node_id(BrokerId(1))
                    .with_host(StrBytes::from_static_str("b1"))
                    .with_port(9092),
            ])
            .with_controller_id(BrokerId(1))
            .with_topics(vec![
                topic("a", 0, &[(1, 1, 0), (0, 1, 0)]),
                topic("missing", 3, &[]),
            ]);
        let s0 = MetadataSnapshot::default();
        let s1 = s0.apply(&response, false);
        assert_eq!(s1.version, 1);
        assert_eq!(s1.brokers[&1].host, "b1");
        assert_eq!(s1.controller_id, 1);
        assert_eq!(s1.partition_count("a"), Some(2));
        assert_eq!(s1.topic("a").unwrap().partitions[0].id, 0);
        assert_eq!(s1.topic("a").unwrap().partition(1).unwrap().leader, 1);
        assert_eq!(s1.partition_count("missing"), None);
        assert_eq!(
            s1.topic("missing").unwrap().error,
            Some(RDKafkaErrorCode::UnknownTopicOrPartition)
        );

        // A partial response keeps other topics; an all-topics one drops them.
        let partial = MetadataResponse::default().with_topics(vec![topic("b", 0, &[(0, 2, 5)])]);
        let s2 = s1.apply(&partial, false);
        assert!(s2.topic("a").is_some());
        assert_eq!(
            s2.topic("b").unwrap().partitions[0].error,
            Some(RDKafkaErrorCode::LeaderNotAvailable)
        );
        let s3 = s2.apply(&partial, true);
        assert!(s3.topic("a").is_none());
        assert!(s3.topic("b").is_some());

        let public = to_public(&response, -1, "b1:9092/bootstrap".into());
        assert_eq!(public.orig_broker_name(), "b1:9092/bootstrap");
        assert_eq!(public.topics().len(), 2);
        assert_eq!(public.topics()[0].partitions()[0].id(), 0);
        assert_eq!(
            public.topics()[1].error(),
            Some(RDKafkaErrorCode::UnknownTopicOrPartition)
        );
    }
}
