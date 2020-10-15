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
use log::warn;
use serde::{Deserialize, Serialize};

use crate::VERSION;
use ore::retry::RetryBuilder;

// Runs fetch_latest_version in a backoff loop until it succeeds once, prints a
// warning if there is another version, then returns.
pub async fn check_version_loop(telemetry_url: String, cluster_id: String) {
    let current_version = VERSION;

    let latest_version = RetryBuilder::new()
        .max_sleep(None)
        .initial_backoff(Duration::from_secs(1))
        .build()
        .retry(|_state| fetch_latest_version(&telemetry_url, &cluster_id, &current_version))
        .await
        .unwrap();

    if latest_version != current_version {
        warn!(
            "A new version of materialized is available: {}.",
            latest_version
        );
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
