// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::bail;
use log::{debug, log, Level};
use semver::{Identifier, Version};
use serde::{Deserialize, Serialize};

use ore::retry::RetryBuilder;

use crate::VERSION;

// Runs fetch_latest_version in a backoff loop until it succeeds once, prints a
// warning if there is a newer version, then returns.
pub async fn check_version_loop(telemetry_url: String, cluster_id: String) {
    let current_version = Version::parse(VERSION).expect("crate version is not valid semver");

    let latest_version = RetryBuilder::new()
        .max_sleep(None)
        .initial_backoff(Duration::from_secs(1))
        .build()
        .retry(|_state| fetch_latest_version(&telemetry_url, &cluster_id, &VERSION))
        .await
        .expect("retry loop never terminates");

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
}

async fn fetch_latest_version(
    telemetry_url: &str,
    cluster_id: &str,
    current_version: &str,
) -> anyhow::Result<String> {
    let version_url = format!("{}/api/v1/version/{}", telemetry_url, cluster_id);
    let version_request = V1VersionRequest {
        version: current_version.to_string(),
    };

    let resp = reqwest::Client::new()
        .post(&version_url)
        .timeout(Duration::from_secs(10))
        .json(&version_request)
        .send()
        .await?;
    if !resp.status().is_success() {
        bail!("failed request: {}", resp.status());
    }
    let version: V1VersionResponse = resp.json().await?;
    Ok(version.latest_release)
}

#[derive(Serialize)]
struct V1VersionRequest {
    version: String,
}

#[derive(Deserialize)]
struct V1VersionResponse {
    latest_release: String,
}
