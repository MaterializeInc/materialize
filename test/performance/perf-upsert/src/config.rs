// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use structopt::StructOpt;

pub static KAFKA_SOURCE_NAME: &str = "perf_upsert_source";

#[derive(Clone, Debug, StructOpt)]
pub struct Args {
    /// The materialized host
    #[structopt(long, default_value = "localhost")]
    pub materialized_host: String,

    #[structopt(long, default_value = "6875")]
    pub materialized_port: u16,

    /// The total number of messages to create
    #[structopt(long, default_value = "100000000")]
    pub message_count: usize,

    /// Number of messages to send per second
    #[structopt(long, default_value = "8000")]
    pub messages_per_second: usize,

    /// The kafka host
    #[structopt(long, default_value = "localhost")]
    pub kafka_host: String,

    /// The kafka port
    #[structopt(long, default_value = "9092")]
    pub kafka_port: u16,

    #[structopt(long, default_value = "perf-upsert")]
    pub kafka_topic: String,

    /// Whether or not to delete the sources and views before starting
    #[structopt(long, requires("create-topic"))]
    pub preserve_source: bool,

    /// Whether or not the perf test should create a new source topic.
    #[structopt(
        long,
        requires_all(&["replication-factor", "preserve-source", "replication-factor"])
    )]
    pub create_topic: bool,

    /// Number of partitions for the source topic. Has to be specified if --create-topic is true.
    #[structopt(long, requires("create-topic"))]
    partitions: Option<i32>,

    /// Replication factor for the source topic. Has to be specified if --create-topic is true.
    #[structopt(long, requires("create-topic"))]
    replication_factor: Option<i32>,
}

impl Args {
    pub(crate) fn kafka_config(&self) -> KafkaConfig {
        let create_topic = if self.create_topic {
            Some(CreateTopicConfig {
                partitions: self
                    .partitions
                    .expect("have to specify partitions when creating topic"),
                replication_factor: self
                    .replication_factor
                    .expect("have to specify replication factor when creating topic"),
            })
        } else {
            None
        };

        KafkaConfig {
            url: self.kafka_url(),
            group_id: "materialize.perf-upsert".into(),
            topic: self.kafka_topic.clone(),
            message_count: self.message_count,
            messages_per_second: self.messages_per_second,
            create_topic,
        }
    }

    pub(crate) fn mz_config(&self) -> MzConfig {
        MzConfig {
            host: self.materialized_host.clone(),
            port: self.materialized_port,
            kafka_url: self.kafka_url(),
            kafka_topic: self.kafka_topic.clone(),
            preserve_source: self.preserve_source,
        }
    }

    pub(crate) fn kafka_url(&self) -> String {
        format!("{}:{}", self.kafka_host, self.kafka_port)
    }
}

#[derive(Debug)]
pub struct CreateTopicConfig {
    pub partitions: i32,
    pub replication_factor: i32,
}

#[derive(Debug)]
pub struct KafkaConfig {
    pub url: String,
    pub group_id: String,
    pub topic: String,
    pub message_count: usize,
    pub messages_per_second: usize,
    pub create_topic: Option<CreateTopicConfig>,
}

#[derive(Debug)]
pub struct MzConfig {
    pub host: String,
    pub port: u16,
    pub kafka_url: String,
    pub kafka_topic: String,
    pub preserve_source: bool,
}
