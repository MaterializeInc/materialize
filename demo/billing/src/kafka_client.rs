// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

//! Kafka topic management

use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::error::Result;

pub struct KafkaClient {
    producer: FutureProducer<DefaultClientContext>,
    kafka_url: String,
    topic: String,
}

impl KafkaClient {
    pub fn new(kafka_url: &str, group_id: &str, topic: &str) -> Result<KafkaClient> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", kafka_url);
        config.set("group.id", group_id);

        let producer: FutureProducer = config.create()?;

        Ok(KafkaClient {
            producer,
            kafka_url: kafka_url.to_string(),
            topic: topic.to_string(),
        })
    }

    pub async fn create_topic(&self, partitions: i32) -> Result<()> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.kafka_url);
        let new_topic = NewTopic::new(&self.topic, partitions, TopicReplication::Fixed(1))
            .set("cleanup.policy", "compact")
            .set("segment.ms", "144000")
            .set("segment.bytes", "1000000000")
            .set("delete.retention.ms", "100000000")
            .set("max.compaction.lag.ms", "10000000")
            .set("min.cleanable.dirty.ratio", "0.01");

        let res = config
            .create::<AdminClient<_>>()
            .expect("creating admin kafka client failed")
            .create_topics(
                &[new_topic],
                &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
            )
            .await?;

        if res.len() != 1 {
            return Err(format!(
                "error creating topic {}: \
             kafka topic creation returned {} results, but exactly one result was expected",
                self.topic,
                res.len()
            )
            .into());
        }

        if let Err((_, e)) = res[0] {
            return Err(format!("error creating topic {}: {}", self.topic, e).into());
        }

        Ok(())
    }

    pub fn send(
        &self,
        key: &[u8],
        message: &[u8],
    ) -> std::result::Result<rdkafka::producer::DeliveryFuture, rdkafka::error::KafkaError> {
        let record: FutureRecord<_, _> = FutureRecord::to(&self.topic)
            .key(key)
            .payload(message)
            .timestamp(chrono::Utc::now().timestamp_millis());
        self.producer.send_result(record).map_err(|(e, _message)| e)
    }
}
