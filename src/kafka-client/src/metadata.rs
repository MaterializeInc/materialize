// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cluster metadata.

use crate::error::IsError;
use crate::types::{RDKafkaErrorCode, RDKafkaRespErr};

/// Broker metadata information.
#[derive(Debug, Clone)]
pub struct MetadataBroker {
    id: i32,
    host: String,
    port: i32,
}

impl MetadataBroker {
    pub(crate) fn new(id: i32, host: String, port: i32) -> MetadataBroker {
        MetadataBroker { id, host, port }
    }

    /// Returns the id of the broker.
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Returns the host name of the broker.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the port of the broker.
    pub fn port(&self) -> i32 {
        self.port
    }
}

/// Partition metadata information.
#[derive(Debug, Clone)]
pub struct MetadataPartition {
    id: i32,
    leader: i32,
    error: Option<RDKafkaRespErr>,
    replicas: Vec<i32>,
    isr: Vec<i32>,
}

impl MetadataPartition {
    /// `error` is the partition's error code; `NoError` means none.
    pub(crate) fn new(
        id: i32,
        leader: i32,
        error: RDKafkaErrorCode,
        replicas: Vec<i32>,
        isr: Vec<i32>,
    ) -> MetadataPartition {
        MetadataPartition {
            id,
            leader,
            error: error.is_error().then_some(error),
            replicas,
            isr,
        }
    }

    /// Returns the id of the partition.
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Returns the broker id of the leader broker for the partition.
    pub fn leader(&self) -> i32 {
        self.leader
    }

    /// Returns the metadata error for the partition, or `None` if there is no
    /// error.
    pub fn error(&self) -> Option<RDKafkaRespErr> {
        self.error
    }

    /// Returns the broker IDs of the replicas.
    pub fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    /// Returns the broker IDs of the in-sync replicas.
    pub fn isr(&self) -> &[i32] {
        &self.isr
    }
}

/// Topic metadata information.
#[derive(Debug, Clone)]
pub struct MetadataTopic {
    name: String,
    partitions: Vec<MetadataPartition>,
    error: Option<RDKafkaRespErr>,
}

impl MetadataTopic {
    /// `error` is the topic's error code; `NoError` means none.
    pub(crate) fn new(
        name: String,
        partitions: Vec<MetadataPartition>,
        error: RDKafkaErrorCode,
    ) -> MetadataTopic {
        MetadataTopic {
            name,
            partitions,
            error: error.is_error().then_some(error),
        }
    }

    /// Returns the name of the topic.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the partition metadata information for all the partitions.
    pub fn partitions(&self) -> &[MetadataPartition] {
        &self.partitions
    }

    /// Returns the metadata error for the topic, or `None` if there was no
    /// error.
    pub fn error(&self) -> Option<RDKafkaRespErr> {
        self.error
    }
}

/// Metadata container.
///
/// This structure wraps the metadata returned by a Metadata request. It is an
/// owned snapshot.
#[derive(Debug, Clone)]
pub struct Metadata {
    orig_broker_id: i32,
    orig_broker_name: String,
    brokers: Vec<MetadataBroker>,
    topics: Vec<MetadataTopic>,
}

impl Metadata {
    /// `orig_broker_name` is the name of the broker that answered, in
    /// librdkafka's `host:port/id` form.
    pub(crate) fn new(
        orig_broker_id: i32,
        orig_broker_name: String,
        brokers: Vec<MetadataBroker>,
        topics: Vec<MetadataTopic>,
    ) -> Metadata {
        Metadata {
            orig_broker_id,
            orig_broker_name,
            brokers,
            topics,
        }
    }

    /// Returns the ID of the broker originating this metadata.
    pub fn orig_broker_id(&self) -> i32 {
        self.orig_broker_id
    }

    /// Returns the hostname of the broker originating this metadata.
    pub fn orig_broker_name(&self) -> &str {
        &self.orig_broker_name
    }

    /// Returns the metadata information for all the brokers in the cluster.
    pub fn brokers(&self) -> &[MetadataBroker] {
        &self.brokers
    }

    /// Returns the metadata information for all the topics in the cluster.
    pub fn topics(&self) -> &[MetadataTopic] {
        &self.topics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn accessors() {
        let md = Metadata::new(
            1,
            "localhost:9092/1".into(),
            vec![MetadataBroker::new(1, "localhost".into(), 9092)],
            vec![
                MetadataTopic::new(
                    "t".into(),
                    vec![MetadataPartition::new(
                        0,
                        1,
                        RDKafkaErrorCode::NoError,
                        vec![1, 2],
                        vec![1],
                    )],
                    RDKafkaErrorCode::NoError,
                ),
                MetadataTopic::new(
                    "missing".into(),
                    vec![],
                    RDKafkaErrorCode::UnknownTopicOrPartition,
                ),
            ],
        );
        assert_eq!(md.orig_broker_id(), 1);
        assert_eq!(md.orig_broker_name(), "localhost:9092/1");
        assert_eq!(md.brokers()[0].id(), 1);
        assert_eq!(md.brokers()[0].host(), "localhost");
        assert_eq!(md.brokers()[0].port(), 9092);
        let t = &md.topics()[0];
        assert_eq!(t.name(), "t");
        assert_eq!(t.error(), None);
        assert_eq!(t.partitions()[0].id(), 0);
        assert_eq!(t.partitions()[0].leader(), 1);
        assert_eq!(t.partitions()[0].error(), None);
        assert_eq!(t.partitions()[0].replicas(), &[1, 2]);
        assert_eq!(t.partitions()[0].isr(), &[1]);
        assert_eq!(
            md.topics()[1].error(),
            Some(RDKafkaErrorCode::UnknownTopicOrPartition)
        );
    }
}
