// Copyright Materialize, Inc. and contributors. All rights reserved.
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
use rdkafka::error::KafkaError;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};

use mz_kafka_util::client::{create_new_client_config_simple, MzClientContext};

pub struct KafkaClient {
    producer: FutureProducer<MzClientContext>,
    kafka_url: String,
}

impl KafkaClient {
    pub fn new(
        kafka_url: &str,
        group_id: &str,
        configs: &[(&str, &str)],
    ) -> Result<KafkaClient, anyhow::Error> {
        let mut config = create_new_client_config_simple();
        config.set("bootstrap.servers", kafka_url);
        config.set("group.id", group_id);
        for (key, val) in configs {
            config.set(*key, *val);
        }

        let producer = config.create_with_context(MzClientContext)?;

        Ok(KafkaClient {
            producer,
            kafka_url: kafka_url.to_string(),
        })
    }

    pub async fn create_topic(
        &self,
        topic_name: &str,
        partitions: i32,
        replication: i32,
        configs: &[(&str, &str)],
        timeout: Option<Duration>,
    ) -> Result<(), anyhow::Error> {
        let mut config = create_new_client_config_simple();
        config.set("bootstrap.servers", &self.kafka_url);

        let client = config
            .create::<AdminClient<_>>()
            .expect("creating admin kafka client failed");

        let admin_opts = AdminOptions::new().request_timeout(timeout);

        let mut topic = NewTopic::new(topic_name, partitions, TopicReplication::Fixed(replication));
        for (key, val) in configs {
            topic = topic.set(key, val);
        }

        mz_kafka_util::admin::ensure_topic(&client, &admin_opts, &topic)
            .await
            .context(format!("creating Kafka topic: {}", topic_name))?;

        Ok(())
    }

    pub fn send(&self, topic_name: &str, message: &[u8]) -> Result<DeliveryFuture, KafkaError> {
        let record: FutureRecord<&Vec<u8>, _> = FutureRecord::to(&topic_name)
            .payload(message)
            .timestamp(chrono::Utc::now().timestamp_millis());
        self.producer.send_result(record).map_err(|(e, _message)| e)
    }

    pub fn send_key_value(
        &self,
        topic_name: &str,
        key: &[u8],
        message: Option<Vec<u8>>,
    ) -> Result<DeliveryFuture, KafkaError> {
        let mut record: FutureRecord<_, _> = FutureRecord::to(&topic_name)
            .key(key)
            .timestamp(chrono::Utc::now().timestamp_millis());
        if let Some(message) = &message {
            record = record.payload(message);
        }
        self.producer.send_result(record).map_err(|(e, _message)| e)
    }
}
