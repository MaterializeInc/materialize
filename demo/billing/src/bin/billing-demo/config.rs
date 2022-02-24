// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::format::ParseResult;
use chrono::prelude::*;
use chrono::DateTime;
use tracing_subscriber::filter::EnvFilter;

pub static KAFKA_SOURCE_NAME: &str = "billing_source";
pub static CSV_SOURCE_NAME: &str = "price_source";
pub static KAFKA_SINK_NAME: &str = "billing_sink";
pub static KAFKA_SINK_TOPIC_NAME: &str = "billing_monthly_statements";
pub static REINGESTED_SINK_SOURCE_NAME: &str = "reingested_sink";

fn parse_utc_datetime_from_str(s: &str) -> ParseResult<DateTime<Utc>> {
    Ok(DateTime::<Utc>::from_utc(
        NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")?,
        Utc,
    ))
}

fn parse_seed(s: &str) -> u64 {
    s.parse().unwrap_or_else(|_| rand::random())
}

#[derive(Debug, clap::Parser)]
pub struct Args {
    /// The materialized host
    #[clap(long, default_value = "localhost")]
    pub materialized_host: String,

    #[clap(long, default_value = "6875")]
    pub materialized_port: u16,

    /// The total number of messages to create
    #[clap(long, default_value = "1000000")]
    pub message_count: usize,

    /// Number of messages to send per second
    #[clap(long, default_value = "8000")]
    pub messages_per_second: usize,

    /// The kafka host
    #[clap(long, default_value = "localhost")]
    pub kafka_host: String,

    /// The kafka port
    #[clap(long, default_value = "9092")]
    pub kafka_port: u16,

    #[clap(long, default_value = "billing")]
    pub kafka_topic: String,

    #[clap(long, default_value = "prices.csv")]
    pub csv_file_name: String,

    /// The schema-registry URL
    #[clap(long, default_value = "http://localhost:8081")]
    pub schema_registry_url: String,

    /// Whether or not to delete the sources and views before starting
    #[clap(long)]
    pub preserve_source: bool,

    /// Whether or not to run the billing-demo in a low memory mode
    #[clap(long)]
    pub low_memory: bool,

    /// A random seed for generating the records and prices
    #[clap(long, default_value = "", parse(from_str = parse_seed))]
    pub seed: u64,

    /// A date to start generating records from. Default is a week before now.
    /// The input time format should be "%Y-%m-%dT%H:%M:%S"
    #[clap(long, parse(try_from_str = parse_utc_datetime_from_str))]
    pub start_time: Option<DateTime<Utc>>,

    /// Whether or not to validate the sink matches its input view
    #[clap(long)]
    pub check_sink: bool,

    /// Whether or not the billing demo should create a new source topic.
    #[clap(
        long,
        requires_all(&["replication-factor", "partitions"])
    )]
    pub create_topic: bool,

    /// Number of partitions for the source topic. Has to be specified if --create-topic is true.
    #[clap(long, requires("create-topic"))]
    partitions: Option<i32>,

    /// Replication factor for the source topic. Has to be specified if --create-topic is true.
    #[clap(long, requires("create-topic"))]
    replication_factor: Option<i32>,

    // Whether or not to enable persistence for input Kafka sources
    #[clap(long)]
    enable_persistence: bool,

    /// Which log messages to emit.
    ///
    /// See materialized's `--log-filter` option for details.
    #[clap(long, value_name = "FILTER", default_value = "billing-demo=debug,info")]
    pub log_filter: EnvFilter,
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
            group_id: "materialize.billing".into(),
            topic: self.kafka_topic.clone(),
            message_count: self.message_count,
            messages_per_second: self.messages_per_second,
            seed: self.seed,
            start_time: match self.start_time {
                Some(start_time) => start_time,
                None => {
                    let now = Utc::now() - chrono::Duration::seconds(60 * 60 * 24 * 7);
                    Utc.ymd(now.year(), now.month(), now.day()).and_hms(
                        now.hour(),
                        now.minute(),
                        now.second(),
                    )
                }
            },
            create_topic,
        }
    }

    pub(crate) fn mz_config(&self) -> MzConfig {
        MzConfig {
            host: self.materialized_host.clone(),
            port: self.materialized_port,
            kafka_url: self.kafka_url(),
            schema_registry_url: self.schema_registry_url.clone(),
            kafka_topic: self.kafka_topic.clone(),
            csv_file_name: self.csv_file_name.clone(),
            preserve_source: self.preserve_source,
            low_memory: self.low_memory,
            seed: self.seed,
            check_sink: self.check_sink,
            enable_persistence: self.enable_persistence,
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
    pub seed: u64,
    pub start_time: DateTime<Utc>,
    pub create_topic: Option<CreateTopicConfig>,
}

#[derive(Debug)]
pub struct MzConfig {
    pub host: String,
    pub port: u16,
    pub kafka_url: String,
    pub schema_registry_url: String,
    pub kafka_topic: String,
    pub csv_file_name: String,
    pub preserve_source: bool,
    pub low_memory: bool,
    pub seed: u64,
    pub check_sink: bool,
    pub enable_persistence: bool,
}
