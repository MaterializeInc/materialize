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
use aws_smithy_http::byte_stream::ByteStream;
use chrono::{SecondsFormat, Utc};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::time;
use tracing::{debug, instrument, warn};

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
        let s = serde_json::to_string(&self).map_err(|_| fmt::Error::default())?;
        write!(f, "{}", s)
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

#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) interval: time::Duration,
    pub(crate) adapter_client: AdapterClient,
    pub(crate) s3_bucket: String,
    pub(crate) s3_prefix: String,
    pub(crate) s3_client: S3Client,
    pub(crate) organization_id: String,
    pub(crate) environment_id: String,
    pub(crate) cloud_provider: String,
    pub(crate) cloud_region: String,
}

#[instrument(level = "debug")]
async fn write_blobs(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    snapshots: Vec<Event>,
) -> Result<(), anyhow::Error> {
    // TODO: this is kind of crappy, building a byte vector in memory, but is OK for the moment
    let mut ndjson = Vec::<u8>::new();
    for s in snapshots {
        serde_json::to_writer(&mut ndjson, &s)?;
        ndjson.push('\n' as u8);
    }
    s3_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(ndjson))
        .send()
        .await?;
    Ok(())
}

// const COMPUTE_QUERY: &str = r"
// -- TODO: fix this
// SELECT
//     event_type,
//     object_type,
//     event_details,
//     occurred_at
// FROM mz_audit_events
// WHERE occurred_at > (NOW() - DURATION {{sync_window}})
//   AND event_type =
// ";

// const STORAGE_QUERY: &str = r"
// SELECT
//   SUM(size_bytes)
// FROM mz_storage_usage
// WHERE collection_timestamp > (NOW() - DURATION {{sync_window}})
// ";

// const SOURCES_QUERY: &str = r"
// -- TODO: fix this
// SELECT
//     event_type,
//     object_type,
//     event_details,
//     occurred_at
// FROM mz_audit_events
// WHERE occurred_at > (NOW() - DURATION {{sync_window}})
//   AND event_type =
// ";

// const SINKS_QUERY: &str = r"
// -- TODO: fix this
// SELECT
//     event_type,
//     object_type,
//     event_details,
//     occurred_at
// FROM mz_audit_events
// WHERE occurred_at > (NOW() - DURATION {{sync_window}})
//   AND event_type =
// ";

#[instrument(level = "debug")]
#[allow(clippy::unused_async)] // TODO: fix
async fn query_usage(cfg: &Config, client: &AdapterClient) -> Result<Vec<Event>, anyhow::Error> {
    let events = query_compute(cfg, client)
        .await?
        .into_iter()
        .chain(query_storage(cfg, client).await?)
        // .chain(query_network(cfg, client).await?)
        // .chain(query_sources(cfg, client).await?)
        // .chain(query_sinks(cfg, client).await?)
        .collect::<Vec<Event>>();
    Ok(events)
}

async fn query_compute(cfg: &Config, client: &AdapterClient) -> Result<Vec<Event>, anyhow::Error> {
    let timestamp = Utc::now().timestamp_millis() as f64 / 1000f64;
    let variety = &Variety::Compute;
    let organization_id = &cfg.organization_id;

    let mut events = vec![];

    // TODO: make not dummy
    let clusters = vec!["foo", "bar", "baz", "quux"];
    let sizes = vec!["xsmall", "small", "medium", "large"];

    let mut rng = rand::thread_rng();
    for _ in 1..rand::random::<u8>() {
        let chosen_cluster = clusters.choose(&mut rng).unwrap();
        let chosen_size = sizes.choose(&mut rng).unwrap();
        let details = EventDetails::Compute(ComputeDetails {
            cluster_name: chosen_cluster.to_string(),
            replica_name: chosen_cluster.to_string(),
            replica_size: chosen_size.to_string(),
            uptime_seconds: rand::random::<u8>() as u32,
        });
        let properties = Properties {
            environment_id: format!("environment-{}-0", cfg.organization_id),
            cloud_provider: cfg.cloud_provider.to_string(),
            cloud_region: cfg.cloud_region.to_string(),
            details,
        };

        events.push(Event {
            external_customer_id: cfg.organization_id.to_string(),
            timestamp,
            idempotency_key: format!("{variety}-{organization_id}-{chosen_cluster}-{timestamp}"),
            event_name: variety.clone(),
            properties,
        })
    }
    Ok(events)
}

async fn query_storage(cfg: &Config, client: &AdapterClient) -> Result<Vec<Event>, anyhow::Error> {
    let timestamp = Utc::now().timestamp_millis() as f64 / 1000f64;
    let variety = &Variety::Storage;
    let organization_id = &cfg.organization_id;

    let mut events = vec![];

    // TODO: make not dummy!
    for _ in 1..rand::random::<u8>() {
        let details = EventDetails::Storage(StorageDetails {
            object_id: Some("potato".to_string()),
            bytes_used: rand::random::<u16>() as u64,
        });
        let properties = Properties {
            environment_id: format!("environment-{}-0", cfg.organization_id),
            cloud_provider: cfg.cloud_provider.to_string(),
            cloud_region: cfg.cloud_region.to_string(),
            details,
        };

        events.push(Event {
            external_customer_id: cfg.organization_id.to_string(),
            timestamp,
            idempotency_key: format!("{variety}-{organization_id}-STORAGE-{timestamp}"),
            event_name: variety.clone(),
            properties,
        })
    }
    Ok(events)
}

// async fn query_network(cfg: &Config, client: &AdapterClient) -> Result<Vec<Event>, anyhow::Error> {
//     Ok(vec![])
// }

// async fn query_sources(cfg: &Config, client: &AdapterClient) -> Result<Vec<Event>, anyhow::Error> {
//     Ok(vec![])
// }

// async fn query_sinks(cfg: &Config, client: &AdapterClient) -> Result<Vec<Event>, anyhow::Error> {
//     Ok(vec![])
// }

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
            let possible_snapshots = query_usage(&self.config, &self.config.adapter_client).await;
            if let Err(e) = possible_snapshots {
                warn!("Could not query usage: {}", e);
                continue;
            }
            let key = format!(
                "{}/{}.json",
                self.config.s3_prefix,
                Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
            );
            if let Err(e) = write_blobs(
                &self.config.s3_client,
                &self.config.s3_bucket,
                &key,
                possible_snapshots.unwrap(),
            )
            .await
            {
                warn!("Could not upload usage to S3: {}", e);
            }
        }
    }
}
