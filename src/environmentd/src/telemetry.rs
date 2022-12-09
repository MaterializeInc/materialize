// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Telemetry collection.

// To test this module, you'll need to run environmentd with the following
// flags.
//
//   --tls-mode=disable
//   --segment-api-key=<REDACTED>
//   --frontegg-tenant=<REDACTED>
//   --frontegg-jwk=<REDACTED>
//   --frontegg-api-token-url=<REDACTED>
//   --frontegg-password-prefix=mzp_
//
// Use values from a personal Materialize Cloud stack for the values that
// are listed as <REDACTED>.
//
// You can then use the Segment debugger to watch the events emitted by your
// environment in real time:
// https://app.segment.com/materializeinc/sources/cloud_dev/debugger.

use serde::Serialize;
use serde_json::json;
use tokio::time::{self, Duration};
use tracing::warn;
use uuid::Uuid;

use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use mz_ore::task;

/// How frequently to send a summary to Segment.
const REPORT_INTERVAL: Duration = Duration::from_secs(3600);

/// Telemetry configuration.
#[derive(Clone)]
pub struct Config {
    /// The Segment client to report telemetry events to.
    pub segment_client: mz_segment::Client,
    /// A client to the adapter to introspect.
    pub adapter_client: mz_adapter::Client,
    /// The ID of the organization for which to report data.
    pub organization_id: Uuid,
}

/// Starts reporting telemetry events to Segment.
pub fn start_reporting(config: Config) {
    task::spawn(|| "telemetry_rollup", report_rollup_loop(config.clone()));
    task::spawn(|| "telemetry_traits", report_traits_loop(config));
}

async fn report_rollup_loop(
    Config {
        segment_client,
        adapter_client,
        organization_id,
    }: Config,
) {
    #[derive(Default)]
    struct Rollup {
        deletes: u64,
        inserts: u64,
        selects: u64,
        subscribes: u64,
        updates: u64,
    }

    let mut interval = time::interval(REPORT_INTERVAL);

    // The first tick always completes immediately. Ignore it.
    interval.tick().await;

    let mut last_rollup = Rollup::default();
    loop {
        interval.tick().await;

        let query_total = &adapter_client.metrics().query_total;
        let current_rollup = Rollup {
            deletes: query_total.with_label_values(&["user", "delete"]).get(),
            inserts: query_total.with_label_values(&["user", "insert"]).get(),
            updates: query_total.with_label_values(&["user", "update"]).get(),
            selects: query_total.with_label_values(&["user", "select"]).get(),
            subscribes: query_total.with_label_values(&["user", "subscribe"]).get(),
        };

        segment_client.track(
            // We use the organization ID as the user ID for events
            // that are not associated with a particular user.
            organization_id,
            "Environment Rolled Up",
            json!({
                "event_source": "environmentd",
                "deletes": current_rollup.deletes - last_rollup.deletes,
                "inserts": current_rollup.inserts - last_rollup.inserts,
                "updates": current_rollup.updates - last_rollup.updates,
                "selects": current_rollup.selects - last_rollup.selects,
                "subscribes": current_rollup.subscribes - last_rollup.subscribes,
            }),
            Some(json!({
                "groupId": organization_id,
            })),
        );

        last_rollup = current_rollup;
    }
}

async fn report_traits_loop(
    Config {
        segment_client,
        adapter_client,
        organization_id,
    }: Config,
) {
    const QUERY: &str = "SELECT
    (SELECT count(*) FROM mz_sources WHERE id LIKE 'u%') AS active_sources,
    (SELECT count(*) FROM mz_sinks WHERE id LIKE 'u%') AS active_sinks";

    #[derive(Debug, Serialize)]
    struct Traits {
        active_sources: i64,
        active_sinks: i64,
        active_subscribes: i64,
    }

    let mut interval = time::interval(REPORT_INTERVAL);
    loop {
        interval.tick().await;

        let traits = Retry::default()
            .initial_backoff(Duration::from_secs(1))
            .max_tries(5)
            .retry_async(|_state| async {
                let rows = adapter_client.introspection_execute_one(QUERY).await?;
                let row = rows.into_element();
                let mut row = row.iter();
                Ok::<_, anyhow::Error>(serde_json::to_value(Traits {
                    active_sources: row.next().unwrap().unwrap_int64(),
                    active_sinks: row.next().unwrap().unwrap_int64(),
                    active_subscribes: adapter_client
                        .metrics()
                        .active_subscribes
                        .with_label_values(&["user"])
                        .get(),
                })?)
            })
            .await;

        match traits {
            Ok(traits) => {
                segment_client.group(
                    // We use the organization ID as the user ID for events
                    // that are not associated with a particular user.
                    organization_id,
                    organization_id,
                    traits,
                );
            }
            Err(e) => warn!("unable to collect telemetry traits: {e}"),
        }
    }
}
