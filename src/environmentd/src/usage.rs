// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! System usage snapshot task
//!
//! environmentd takes regular snapshots of system usage, for internal observability
//! and billing purposes. These snapshots are written to an object store (e.g. AWS
//! S3, or Minio in testing).

use aws_sdk_s3::Client as S3Client;
use aws_smithy_http::endpoint::Endpoint;
use chrono::{SecondsFormat, Utc};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use tokio::time;
use tokio::time;
use tracing::{info, instrument};
use url::Url;

use mz_adapter::Client as AdapterClient;

#[derive(Serialize, Deserialize, Debug)]
struct Event {
    // Customer's Organization ID, so we can tie usage events to our customers
    external_customer_id: String,
    // Epoch timestamp, matching Orb's ingest structure
    timestamp: f64,
    // String made up of object ID, sync start timestamp, and event name
    idempotency_key: String,
    event_name: Variety,
    #[serde(flatten)]
    properties: Properties,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
enum Variety {
    Compute,
    Storage,
    Network,
    Sources,
    Sinks,
}
impl fmt::Display for Variety {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Variety::Compute => write!(f, "compute"),
            Variety::Storage => write!(f, "storage"),
            Variety::Network => write!(f, "network"),
            Variety::Sources => write!(f, "sources"),
            Variety::Sinks => write!(f, "sinks"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Properties {
    // This is redundant with the S3 prefix, but simplifies the ingester
    environment_id: String,
    // Just "aws" for now, but providing for future extensibility
    cloud_provider: String,
    // e.g. "us-east-1" or "eu-west-1"
    cloud_region: String,
    // This will contain different data depending on the UsageType
    #[serde(flatten)]
    details: EventDetails,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum EventDetails {
    Compute(ComputeDetails),
    Storage(StorageDetails),
    Network(NetworkDetails),
    Sources(SourceDetails),
    Sinks(SinkDetails),
}

#[derive(Serialize, Deserialize, Debug)]
struct ComputeDetails {
    // e.g. "foo" in `CREATE CLUSTER foo`
    cluster_name: String,
    // as above
    replica_name: String,
    // e.g. "xsmall"
    replica_size: String,
    // seconds in *this time window* that this cluster has been operating
    uptime_seconds: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct StorageDetails {
    object_id: Option<String>,
    bytes_used: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct NetworkDetails {
    // TODO: this is subject to change
    bytes_used: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct SourceDetails {
    uptime_seconds: u32,
    size: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct SinkDetails {
    // NOTE: This will change as we move to consumption-based sink pricing,
    // but the intended format for that is TBD.
    uptime_seconds: u32,
    size: String,
}

pub(crate) struct Config {
    interval: time::Duration,
    adapter_client: AdapterClient,
    s3_bucket: String,
    s3_prefix: String,
    s3_client: S3Client,
}

#[instrument(level = debug)]
async fn write_blobs(s3_client: &S3Client, bucket: &str, key: &str, snapshots: Vec<Event>) {
    // TODO: this is kind of crappy, building a byte vector in memory, but is OK for the moment
    let mut ndjson = Vec::<u8>::new();
    let slen = snapshots.len();
    for s in snapshots {
        serde_json::to_writer(&mut ndjson, &s);
        ndjson.push('\n' as u8);
    }
    s3_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(aws_smithy_http::byte_stream::ByteStream::from(ndjson))
        .send()
        .await
        .expect("Could not put to S3!");
}

const COMPUTE_QUERY: &str = r"
-- TODO: fix this
SELECT
    event_type,
    object_type,
    event_details,
    occurred_at
FROM mz_audit_events
WHERE occurred_at > (NOW() - DURATION {{sync_window}})
  AND event_type =
";

const STORAGE_QUERY: &str = r"
SELECT
  SUM(size_bytes)
FROM mz_storage_usage
WHERE collection_timestamp > (NOW() - DURATION {{sync_window}})
";

const SOURCES_QUERY: &str = r"
-- TODO: fix this
SELECT
    event_type,
    object_type,
    event_details,
    occurred_at
FROM mz_audit_events
WHERE occurred_at > (NOW() - DURATION {{sync_window}})
  AND event_type =
";

const SINKS_QUERY: &str = r"
-- TODO: fix this
SELECT
    event_type,
    object_type,
    event_details,
    occurred_at
FROM mz_audit_events
WHERE occurred_at > (NOW() - DURATION {{sync_window}})
  AND event_type =
";

#[instrument(level = debug)]
async fn query_usage() -> Vec<Event> {
    let mut events = Vec::<Event>::with_capacity(2);
    for _ in (1..rand::random::<u8>()) {
        events.push(dummy_usage().await)
    }
    events
}

pub(crate) struct Snapshotter {
    config: Config,
}

impl Snapshotter {
    pub fn new(cfg: Config) -> Self {
        Self { config: cfg }
    }

    pub async fn run_forever(&self) {
        let mut interval = time::interval(self.config.interval);
        loop {
            interval.tick().await;
            let snapshots = query_usage(/* TODO: adapter client */).await;
            let now = Utc::now();
            let key = format!(
                "{}/{}.json",
                self.config.s3_prefix,
                now.to_rfc3339_opts(SecondsFormat::Millis, true)
            );
            write_blobs(
                &self.config.s3_client,
                &self.config.s3_bucket,
                &key,
                snapshots,
            )
            .await;
        }
    }
}

/// TODO
async fn dummy_usage() -> UsageEvent {
    let mut rng = rand::thread_rng();
    let org_ids = vec![
        "536c75a1-68d9-4449-835c-215ea1915ce1",
        "82684ea7-fcee-4796-8b6a-ca319f5d70e6",
        "f9d68118-3640-42fa-be4f-b7c5df58658d",
        "e349d671-81bd-4539-bae9-3b90e6c51722",
    ];
    let clusters = vec!["foo", "bar", "baz", "quux"];
    let sizes = vec!["xsmall", "small", "medium", "large"];

    let typez = vec![UsageType::Compute, UsageType::Storage];

    let timestamp = (Utc::now().timestamp_millis() as f64 / 1000f64);
    let chosen_org = org_ids.choose(&mut rng).unwrap();
    let chosen_type = typez.choose(&mut rng).unwrap();
    let chosen_cluster = clusters.choose(&mut rng).unwrap();
    let chosen_size = sizes.choose(&mut rng).unwrap();

    let idemp_key = format!("{chosen_type}-{chosen_org}-{chosen_cluster}-{timestamp}");
    let chosen_dets = match chosen_type {
        UsageType::Compute => UsageEventDetails::Compute(ComputeUsageDetails {
            cluster_name: chosen_cluster.to_string(),
            replica_name: chosen_cluster.to_string(),
            replica_size: chosen_size.to_string(),
            uptime_seconds: rand::random::<u8>() as u32,
        }),
        UsageType::Storage => UsageEventDetails::Storage(StorageUsageDetails {
            object_id: Some("potato".to_string()),
            bytes_used: rand::random::<u16>() as u64,
        }),
        UsageType::Network => unreachable!(),
        UsageType::Sources => unreachable!(),
        UsageType::Sinks => unreachable!(),
    };
    let props = UsageProperties {
        environment_id: format!("environment-{}-0", chosen_org),
        cloud_provider: "aws".to_string(),
        cloud_region: "us-east-1".to_string(),
        details: chosen_dets,
    };

    UsageEvent {
        external_customer_id: chosen_org.to_string(),
        timestamp: timestamp,
        idempotency_key: idemp_key,
        event_name: chosen_type.clone(),
        properties: props,
    }
}
