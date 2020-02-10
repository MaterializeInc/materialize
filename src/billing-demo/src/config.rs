// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use parse_duration::parse as parse_duration;
use structopt::StructOpt;

pub static KAFKA_SOURCE_NAME: &str = "billing_source";
pub static CSV_SOURCE_NAME: &str = "price_source";

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

    /// Amount of time to sleep between messages, e.g. 2ms
    #[structopt(long, parse(try_from_str = parse_duration))]
    pub message_sleep: Option<Duration>,

    /// The kafka host
    #[structopt(long, default_value = "localhost")]
    pub kafka_host: String,

    /// The kafka port
    #[structopt(long, default_value = "9092")]
    pub kafka_port: u16,

    #[structopt(long, default_value = "billing")]
    pub kafka_topic: String,

    #[structopt(long)]
    pub csv_file_name: String,

    /// Whether or not to delete the sources and views before starting
    #[structopt(long)]
    pub preserve_source: bool,
}

impl Args {
    pub(crate) fn kafka_config(&self) -> KafkaConfig {
        KafkaConfig {
            url: self.kafka_url(),
            group_id: "materialize.billing".into(),
            topic: self.kafka_topic.clone(),
            message_count: self.message_count,
            message_sleep: self.message_sleep,
        }
    }

    pub(crate) fn mz_config(&self) -> MzConfig {
        MzConfig {
            host: self.materialized_host.clone(),
            port: self.materialized_port,
            kafka_url: self.kafka_url(),
            kafka_topic: self.kafka_topic.clone(),
            csv_file_name: self.csv_file_name.clone(),
            preserve_source: self.preserve_source,
        }
    }

    pub(crate) fn kafka_url(&self) -> String {
        format!("{}:{}", self.kafka_host, self.kafka_port)
    }
}

#[derive(Debug)]
pub struct KafkaConfig {
    pub url: String,
    pub group_id: String,
    pub topic: String,
    pub message_count: usize,
    pub message_sleep: Option<Duration>,
}

#[derive(Debug)]
pub struct MzConfig {
    pub host: String,
    pub port: u16,
    pub kafka_url: String,
    pub kafka_topic: String,
    pub csv_file_name: String,
    pub preserve_source: bool,
}
