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
use log::{debug, log, Level};
use semver::{Identifier, Version};
use serde::{Deserialize, Serialize};
use tokio::time::{self, Duration};

use ore::retry::RetryBuilder;

use crate::server_metrics::{
    filter_metrics, load_prom_metrics, METRIC_SERVER_METADATA, METRIC_WORKER_COUNT,
};
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
pub async fn check_version_loop(telemetry_url: String, cluster_id: String, start_time: Instant) {
    let current_version =
        Version::parse(BUILD_INFO.version).expect("crate version is not valid semver");

    let version_url = format!("{}/api/v1/version/{}", telemetry_url, cluster_id);
    let latest_version = fetch_latest_version(&version_url, start_time).await;

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

        fetch_latest_version(&version_url, start_time).await;
    }
}

async fn fetch_latest_version(telemetry_url: &str, start_time: Instant) -> String {
    RetryBuilder::new()
        .max_sleep(None)
        .initial_backoff(Duration::from_secs(1))
        .build()
        .retry(|_state| async {
            let version_request = V1VersionRequest {
                version: BUILD_INFO.version,
                status: telemetry_data(start_time),
            };

            let resp = reqwest::Client::new()
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

fn telemetry_data(start_time: Instant) -> Status {
    let metrics_to_collect: HashSet<_> = [METRIC_SERVER_METADATA, METRIC_WORKER_COUNT]
        .iter()
        .copied()
        .collect();

    let metrics = load_prom_metrics(start_time);
    let filtered = filter_metrics(&metrics, &metrics_to_collect);
    let value_default = |name| filtered.get(name).map(|m| m.value()).unwrap_or(0.0);
    Status {
        uptime_seconds: start_time.elapsed().as_secs(),
        num_workers: value_default(METRIC_WORKER_COUNT),
    }
}

#[derive(Serialize)]
struct V1VersionRequest<'a> {
    version: &'a str,
    status: Status,
}

#[derive(Serialize)]
struct Status {
    uptime_seconds: u64,
    num_workers: f64,
}

#[derive(Deserialize)]
struct V1VersionResponse {
    latest_release: String,
}
