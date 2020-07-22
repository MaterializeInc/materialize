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
