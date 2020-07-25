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

use anyhow::Context;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

pub struct KafkaClient {
    producer: FutureProducer<DefaultClientContext>,
    messages: i64,
    kafka_url: String,
    topic: String,
}

impl KafkaClient {
    pub fn new(
        kafka_url: &str,
        group_id: &str,
        topic: &str,
        configs: &[(&str, &str)],
    ) -> Result<KafkaClient, anyhow::Error> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", kafka_url);
        config.set("group.id", group_id);
        for (key, val) in configs {
            config.set(key, val);
        }

        let producer: FutureProducer = config.create()?;

        Ok(KafkaClient {
            producer,
            messages: 0,
            kafka_url: kafka_url.to_string(),
            topic: topic.to_string(),
        })
    }

    pub async fn create_topic(
        &self,
        partitions: i32,
        replication: i32,
        configs: &[(&str, &str)],
        timeout: Option<Duration>,
    ) -> Result<(), anyhow::Error> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.kafka_url);

        let mut topic = NewTopic::new(
            &self.topic,
            partitions,
            TopicReplication::Fixed(replication),
        );

        for (key, val) in configs {
            topic = topic.set(key, val);
        }

        let res = config
            .create::<AdminClient<_>>()
            .expect("creating admin kafka client failed")
            .create_topics(&[topic], &AdminOptions::new().request_timeout(timeout))
            .await
            .context(format!("creating Kafka topic: {}", &self.topic))?;

        if res.len() != 1 {
            return Err(anyhow::anyhow!(
                "error creating topic {}: \
             kafka topic creation returned {} results, but exactly one result was expected",
                self.topic,
                res.len()
            ));
        }

        if let Err((_, e)) = res[0] {
            return Err(anyhow::anyhow!(
                "error creating topic {}: {}",
                self.topic,
                e
            ));
        }

        // Topic creation is asynchronous, and if we don't wait for it to
        // complete, we might produce a message (below) that causes it to
        // get automatically created with multiple partitions. (Since
        // multiple partitions have no ordering guarantees, this violates
        // many assumptions that our tests make.)
        ore::retry::retry_for(Duration::from_secs(8), |_| async {
            let metadata = self
                .producer
                .client()
                // N.B. It is extremely important not to ask specifically
                // about the topic here, even though the API supports it!
                // Asking about the topic will create it automatically...
                // with the wrong number of partitions. Yes, this is
                // unbelievably horrible.
                .fetch_metadata(None, Some(Duration::from_secs(1)))?;
            if metadata.topics().is_empty() {
                return Err(anyhow::anyhow!("metadata fetch returned no topics"));
            }
            let topic = match metadata.topics().iter().find(|t| t.name() == self.topic) {
                Some(topic) => topic,
                None => {
                    return Err(anyhow::anyhow!(
                        "metadata fetch did not return topic {}",
                        self.topic,
                    ))
                }
            };
            if topic.partitions().is_empty() {
                return Err(anyhow::anyhow!(
                    "metadata fetch returned a topic with no partitions"
                ));
            } else if topic.partitions().len() as i32 != partitions {
                return Err(anyhow::anyhow!(
                    "topic {} was created with {} partitions when exactly {} was expected",
                    self.topic,
                    topic.partitions().len(),
                    partitions
                ));
            }
            Ok(())
        })
        .await?;

        Ok(())
    }

    pub async fn send(&mut self, message: &[u8]) -> Result<(), anyhow::Error> {
        self.messages += 1;
        let record: FutureRecord<&Vec<u8>, _> = FutureRecord::to(&self.topic)
            .payload(message)
            .timestamp(chrono::Utc::now().timestamp_millis());
        if let Err((e, _message)) = self.producer.send(record, Duration::from_millis(500)).await {
            return Err(e.into());
        }

        Ok(())
    }

    pub fn send_key_value(
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
