// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Kafka topic management

use std::time::Duration;

use anyhow::Context;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};

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

        let client = config
            .create::<AdminClient<_>>()
            .expect("creating admin kafka client failed");

        let admin_opts = AdminOptions::new().request_timeout(timeout);

        let mut topic = NewTopic::new(
            &self.topic,
            partitions,
            TopicReplication::Fixed(replication),
        );
        for (key, val) in configs {
            topic = topic.set(key, val);
        }

        kafka_util::admin::create_topic(&client, &admin_opts, &topic)
            .await
            .context(format!("creating Kafka topic: {}", &self.topic))?;

        Ok(())
    }

    pub fn send(
        &self,
        message: &[u8],
    ) -> std::result::Result<rdkafka::producer::DeliveryFuture, rdkafka::error::KafkaError> {
        let record: FutureRecord<&Vec<u8>, _> = FutureRecord::to(&self.topic)
            .payload(message)
            .timestamp(chrono::Utc::now().timestamp_millis());
        self.producer.send_result(record).map_err(|(e, _message)| e)
    }

    pub fn send_key_value(&self, key: &[u8], message: &[u8]) -> Result<DeliveryFuture, KafkaError> {
        let record: FutureRecord<_, _> = FutureRecord::to(&self.topic)
            .key(key)
            .payload(message)
            .timestamp(chrono::Utc::now().timestamp_millis());
        self.producer.send_result(record).map_err(|(e, _message)| e)
    }
}
