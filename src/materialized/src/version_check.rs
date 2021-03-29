// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::time::Instant;

use anyhow::bail;
use log::{debug, info, log, Level};
use reqwest::Client;
use semver::{Identifier, Version};
use serde::{Deserialize, Serialize};
use tokio::time::{self, Duration};
use uuid::Uuid;

use ore::retry::Retry;

use crate::server_metrics::{filter_metrics, load_prom_metrics, METRIC_WORKER_COUNT};
use crate::BUILD_INFO;

/// How often we report telemetry
const TELEMETRY_FREQUENCY: Duration = Duration::from_secs(3600);

/// Check for the latest version and report telemetry
///
/// Runs the telemetry reporting loop infinitely, attempting to report it once
/// every [`TELEMETRY_FREQUENCY`].
///
/// The first time we get the most recent version of materialized this will
/// report a warning if an upgrade is available.
pub async fn check_version_loop(
    telemetry_url: String,
    cluster_id: Uuid,
    session_id: Uuid,
    start_time: Instant,
) {
    let current_version =
        Version::parse(BUILD_INFO.version).expect("crate version is not valid semver");

    let client = match http_util::reqwest_client() {
        Ok(c) => c,
        Err(e) => {
            info!(
                "Unable to build http client to check if materialized is up to date: {}",
                e
            );
            return;
        }
    };
    let version_url = format!("{}/api/v1/version/{}", telemetry_url, cluster_id);
    let latest_version = fetch_latest_version(&client, &version_url, start_time, session_id).await;

    match Version::parse(&latest_version) {
        Ok(latest_version) if latest_version > current_version => {
            // We assume users running development builds are sophisticated, and
            // may be intentionally not running the latest release, so downgrade
            // the message from warn to info level.
            let level = match current_version.pre.as_slice() {
                [Identifier::AlphaNumeric(s)] if s == "dev" => Level::Info,
                _ => Level::Warn,
            };
            log!(
                level,
                "a new version of materialized is available: {}",
                latest_version
            );
        }
        Ok(_) => (),
        Err(e) => debug!("unable to parse fetched latest version: {}", e),
    }

    loop {
        time::sleep(TELEMETRY_FREQUENCY).await;

        fetch_latest_version(&client, &version_url, start_time, session_id).await;
    }
}

async fn fetch_latest_version(
    client: &Client,
    telemetry_url: &str,
    start_time: Instant,
    session_id: Uuid,
) -> String {
    Retry::default()
        .initial_backoff(Duration::from_secs(1))
        .clamp_backoff(TELEMETRY_FREQUENCY)
        .max_tries(usize::MAX)
        .retry(|_state| async {
            let version_request = V1VersionRequest {
                version: BUILD_INFO.version,
                status: telemetry_data(start_time, session_id),
            };

            let resp = client
                .post(telemetry_url)
                .timeout(Duration::from_secs(10))
                .json(&version_request)
                .send()
                .await?;
            if !resp.status().is_success() {
                bail!("failed request: {}", resp.status());
            }
            let version: V1VersionResponse = resp.json().await?;
            Ok(version.latest_release)
        })
        .await
        .expect("retry loop never terminates")
}

fn telemetry_data(start_time: Instant, session_id: Uuid) -> Status {
    const SOURCE_COUNT: &str = "mz_source_count";
    const VIEW_COUNT: &str = "mz_view_count";
    const SINK_COUNT: &str = "mz_sink_count";
    let metrics_to_collect: HashSet<_> =
        [METRIC_WORKER_COUNT, SOURCE_COUNT, VIEW_COUNT, SINK_COUNT]
            .iter()
            .copied()
            .collect();

    let metrics = load_prom_metrics(start_time);
    let filtered = filter_metrics(&metrics, &metrics_to_collect);

    let first_value_default = |name| {
        filtered
            .get(name)
            .and_then(|m| m.get(0).map(|m| m.value()))
            .unwrap_or(0.0)
    };
    let label_value = |name, label, value| {
        filtered
            .get(name)
            .and_then(|m| {
                m.iter()
                    .find(|m| m.label(label).map(|l| l == value).unwrap_or(false))
                    // all counts are guaranteed to be integers
                    .map(|m| m.value() as u32)
            })
            .unwrap_or(0)
    };
    Status {
        session_id,
        uptime_seconds: start_time.elapsed().as_secs(),
        num_workers: first_value_default(METRIC_WORKER_COUNT),
        sources: Sources {
            avro_ocf: InnerStatus {
                count: label_value(SOURCE_COUNT, "type", "avro-ocf"),
            },
            file: InnerStatus {
                count: label_value(SOURCE_COUNT, "type", "file"),
            },
            kinesis: InnerStatus {
                count: label_value(SOURCE_COUNT, "type", "kinesis"),
            },
            postgres: InnerStatus {
                count: label_value(SOURCE_COUNT, "type", "postgres"),
            },
            s3: InnerStatus {
                count: label_value(SOURCE_COUNT, "type", "s3"),
            },
            table: InnerStatus {
                count: label_value(SOURCE_COUNT, "type", "table"),
            },
        },
        views: InnerStatus {
            count: first_value_default(VIEW_COUNT) as u32,
        },
        sinks: Sinks {
            avro_ocf: InnerStatus {
                count: label_value(SINK_COUNT, "type", "avro-ocf"),
            },
            kafka: InnerStatus {
                count: label_value(SOURCE_COUNT, "type", "kafka"),
            },
            tail: InnerStatus {
                count: label_value(SOURCE_COUNT, "type", "tail"),
            },
        },
    }
}

#[derive(Serialize)]
struct V1VersionRequest<'a> {
    version: &'a str,
    status: Status,
}

/// General status of the materialized server, for telemetry
#[derive(Serialize)]
struct Status {
    /// Unique token for every time materialized is restarted
    session_id: Uuid,
    uptime_seconds: u64,
    num_workers: f64,
    sources: Sources,
    views: InnerStatus,
    sinks: Sinks,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Sources {
    avro_ocf: InnerStatus,
    file: InnerStatus,
    kinesis: InnerStatus,
    postgres: InnerStatus,
    s3: InnerStatus,
    table: InnerStatus,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Sinks {
    tail: InnerStatus,
    kafka: InnerStatus,
    avro_ocf: InnerStatus,
}

#[derive(Serialize)]
struct InnerStatus {
    count: u32,
}

#[derive(Deserialize)]
struct V1VersionResponse {
    latest_release: String,
}
