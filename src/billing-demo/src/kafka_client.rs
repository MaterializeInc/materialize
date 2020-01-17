// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![allow(dead_code)]

//! Kafka topic management

use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::error::Result;

pub struct KafkaClient {
    producer: FutureProducer<DefaultClientContext>,
    messages: i64,
}

impl KafkaClient {
    pub fn new(kafka_url: &str, group_id: &str) -> Result<KafkaClient> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", kafka_url);
        config.set("group.id", group_id);

        let producer: FutureProducer = config.create()?;

        Ok(KafkaClient {
            producer,
            messages: 0,
        })
    }

    pub async fn send(&mut self, topic: &str, message: &Vec<u8>) -> Result<()> {
        let tn = topic.to_string();
        self.messages += 1;
        let record: FutureRecord<&Vec<u8>, _> = FutureRecord::to(&tn)
            .payload(message)
            .timestamp(chrono::Utc::now().timestamp_millis());
        if let Err((e, _message)) = self.producer.send(record, 500).await? {
            return Err(e.into());
        }

        Ok(())
    }
}
