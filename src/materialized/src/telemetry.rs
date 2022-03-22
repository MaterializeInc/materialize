// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Telemetry collection.
//
// WARNING: The code in this module must be tested manually. Please see
// misc/python/cli/mock_telemetry_server.py for details.

use serde::Deserialize;
use tokio::time::{self, Duration};
use tracing::{debug, warn};
use uuid::Uuid;

use mz_ore::retry::Retry;

use crate::BUILD_INFO;

/// Telemetry configuration.
pub struct Config {
    /// The domain to report telemetry data to.
    pub domain: String,
    /// How often to report telemetry data.
    pub interval: Duration,
    /// The ID of the Materialize cluster.
    pub cluster_id: Uuid,
    /// The number of workers the dataflow server is hosting.
    pub workers: usize,
    /// A client for the coordinator to introspect.
    pub coord_client: mz_coord::Client,
}

/// Runs the telemetry reporting loop.
///
/// The loop ticks at the interval specified in `config.interval`. On each turn,
/// it reports anonymous metadata about the system to the server running at
/// `config.domain`. If it learns of a new Materialize release in the process,
/// it logs a notice.
pub async fn report_loop(config: Config) {
    let mut interval = time::interval(config.interval);
    let mut reported_version = BUILD_INFO.semver_version();
    loop {
        interval.tick().await;

        let latest_version = match report_one(&config).await {
            Ok(latest_version) => latest_version,
            Err(e) => {
                debug!("failed to report telemetry: {}", e);
                continue;
            }
        };

        if latest_version > reported_version {
            // We assume users running development builds are sophisticated, and
            // may be intentionally not running the latest release, so downgrade
            // the message from warn to info level.
            //
            // TODO: avoid duplicating the message if tokio-rs/tracing#372
            // is resolved.
            match BUILD_INFO.semver_version().pre.as_str() {
                "dev" => {
                    debug!(
                        "a new version of materialized is available: {}",
                        latest_version
                    );
                }
                _ => {
                    warn!(
                        "a new version of materialized is available: {}",
                        latest_version
                    );
                }
            };
            reported_version = latest_version;
        }
    }
}

/// Generates the query used to gather telemetry data.
//
// If you add additional data to this query, please be sure to update the
// telemetry docs in doc/user/cli/_index.md#telemetry accordingly, and be sure
// the data is not identifiable.
fn make_telemetry_query(config: &Config) -> String {
    let architecture = std::env::consts::ARCH;
    let os = std::env::consts::OS;
    format!("
        SELECT jsonb_build_object(
            'version', mz_version(),
            'status', jsonb_build_object(
                'session_id', mz_internal.mz_session_id(),
                'uptime_seconds', extract(epoch FROM mz_uptime()),
                'num_workers', {workers},
                'architecture', '{architecture}',
                'os', '{os}',
                'sources', (
                    SELECT jsonb_object_agg(connector_type, jsonb_build_object('count', count))
                    FROM (SELECT connector_type, count(*) FROM mz_sources WHERE id LIKE 'u%' GROUP BY connector_type)
                ),
                'tables', jsonb_build_object('count', (SELECT count(*) FROM mz_tables WHERE id LIKE 'u%')),
                'views', jsonb_build_object('count', (SELECT count(*) FROM mz_views WHERE id LIKE 'u%')),
                'sinks', (
                    SELECT jsonb_object_agg(connector_type, jsonb_build_object('count', count))
                    FROM (SELECT connector_type, count(*) FROM mz_sinks WHERE id LIKE 'u%' GROUP BY connector_type)
                )
            )
        )",
        workers = config.workers
    )
}

/// The response returned by the telemetry server.
#[derive(Deserialize)]
struct V1VersionResponse {
    latest_release: String,
}

async fn report_one(config: &Config) -> Result<semver::Version, anyhow::Error> {
    let response: V1VersionResponse = Retry::default()
        .initial_backoff(Duration::from_secs(1))
        .max_duration(config.interval)
        .retry_async(|_state| async {
            let query_result = config
                .coord_client
                .system_execute_one(&make_telemetry_query(config))
                .await?;
            let response = mz_http_proxy::reqwest::client()
                .post(format!(
                    "https://{}/api/telemetry/{}",
                    config.domain, config.cluster_id
                ))
                .timeout(Duration::from_secs(10))
                .json(&query_result.rows[0][0])
                .send()
                .await?
                .error_for_status()?;
            Ok::<_, anyhow::Error>(response.json().await?)
        })
        .await?;
    Ok(response.latest_release.parse()?)
}
