// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Telemetry collection.
//!
//! This report loop collects two types of telemetry data on a regular interval:
//!
//!   * Statistics, which represent aggregated activity since the last reporting
//!     interval. An example of a statistic is "number of SUBSCRIBE queries
//!     executed in the last reporting interval."
//!
//!   * Traits, which represent the properties of the environment at the time of
//!     reporting. An example of a trait is "number of currently active
//!     SUBSCRIBE queries."
//!
//! The reporting loop makes two Segment API calls each interval:
//!
//!   * A `group` API call [0] to report traits. The traits are scoped to the
//!     environment's cloud provider and region, as in:
//!
//!     ```json
//!     {
//!         "aws": {
//!             "us-east-1": {
//!                 "active_subscribes": 2,
//!                 ...
//!             }
//!         }
//!     }
//!     ```
//!
//!     Downstream tools often flatten these traits into, e.g.,
//!     `aws_us_east_1_active_subscribes`.
//!
//!  * A `track` API call [1] for the "Environment Rolled Up" event, containing
//!    both statistics and traits as the event properties, as in:
//!
//!    ```json
//!    {
//!        "cloud_provider": "aws",
//!        "cloud_provider_region": "us-east-1",
//!        "active_subscribes": 1,
//!        "subscribes": 23,
//!        ...
//!    }
//!    ```
//!
//!    This event is only emitted after the *first* reporting interval has
//!    completed, since at boot all statistics will be zero.
//!
//! The reason for including traits in both the `group` and `track` API calls is
//! because downstream tools want easy access to both of these questions:
//!
//!   1. What is the latest state of the environment?
//!   2. What was the state of this environment in this time window?
//!
//! Answering question 2 requires that we periodically report statistics and
//! traits in a `track` call. Strictly speaking, the `track` event could be used
//! to answer question 1 too (look for the latest "Environment Rolled Up"
//! event), but in practice it is often far more convenient to have the latest
//! state available as a property of the environment.
//!
//! [0]: https://segment.com/docs/connections/spec/group/
//! [1]: https://segment.com/docs/connections/spec/track/

// To test this module, you'll need to run environmentd with
// the --segment-api-key=<REDACTED> flag. Use the API key from your personal
// Materialize Cloud stack.
//
// You can then use the Segment debugger to watch the events emitted by your
// environment in real time:
// https://app.segment.com/materializeinc/sources/cloud_dev/debugger.

use serde_json::json;
use tokio::time::{self, Duration};
use tracing::warn;

use mz_adapter::telemetry::SegmentClientExt;
use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_repr::adt::jsonb::Jsonb;
use mz_sql::catalog::EnvironmentId;

/// How frequently to send a summary to Segment.
const REPORT_INTERVAL: Duration = Duration::from_secs(3600);

/// Telemetry configuration.
#[derive(Clone)]
pub struct Config {
    /// The Segment client to report telemetry events to.
    pub segment_client: mz_segment::Client,
    /// A client to the adapter to introspect.
    pub adapter_client: mz_adapter::Client,
    /// The ID of the environment for which to report data.
    pub environment_id: EnvironmentId,
}

/// Starts reporting telemetry events to Segment.
pub fn start_reporting(config: Config) {
    task::spawn(|| "telemetry", report_loop(config));
}

async fn report_loop(
    Config {
        segment_client,
        adapter_client,
        environment_id,
    }: Config,
) {
    struct Stats {
        deletes: u64,
        inserts: u64,
        selects: u64,
        subscribes: u64,
        updates: u64,
    }

    let mut last_stats: Option<Stats> = None;

    let mut interval = time::interval(REPORT_INTERVAL);
    loop {
        interval.tick().await;

        let traits = Retry::default()
            .initial_backoff(Duration::from_secs(1))
            .max_tries(5)
            .retry_async(|_state| async {
                let active_subscribes = adapter_client
                    .metrics()
                    .active_subscribes
                    .with_label_values(&["user"])
                    .get();
                let rows = adapter_client.introspection_execute_one(&format!("
                    SELECT jsonb_build_object(
                        'active_aws_privatelink_connections', (SELECT count(*) FROM mz_connections WHERE id LIKE 'u%' AND type = 'aws-privatelink')::int4,
                        'active_clusters', (SELECT count(*) FROM mz_clusters WHERE id LIKE 'u%')::int4,
                        'active_cluster_replicas', (
                            SELECT jsonb_object_agg(base.size, coalesce(count, 0))
                            FROM mz_internal.mz_cluster_replica_sizes base
                            LEFT JOIN (
                                SELECT size, count(*)::int4
                                FROM mz_cluster_replicas r
                                JOIN mz_clusters c ON c.id = r.cluster_id
                                WHERE c.id LIKE 'u%'
                                GROUP BY size
                            ) extant ON base.size = extant.size
                        ),
                        'active_confluent_schema_registry_connections', (SELECT count(*) FROM mz_connections WHERE id LIKE 'u%' AND type = 'confluent-schema-registry')::int4,
                        'active_materialized_views', (SELECT count(*) FROM mz_materialized_views WHERE id LIKE 'u%')::int4,
                        'active_sources', (SELECT count(*) FROM mz_sources WHERE id LIKE 'u%' AND type <> 'subsource')::int4,
                        'active_kafka_connections', (SELECT count(*) FROM mz_connections WHERE id LIKE 'u%' AND type = 'kafka')::int4,
                        'active_kafka_sources', (SELECT count(*) FROM mz_sources WHERE id LIKE 'u%' AND type = 'kafka')::int4,
                        'active_load_generator_sources', (SELECT count(*) FROM mz_sources WHERE id LIKE 'u%' AND type = 'load-generator')::int4,
                        'active_postgres_connections', (SELECT count(*) FROM mz_connections WHERE id LIKE 'u%' AND type = 'postgres')::int4,
                        'active_postgres_sources', (SELECT count(*) FROM mz_sources WHERE id LIKE 'u%' AND type = 'postgres')::int4,
                        'active_sinks', (SELECT count(*) FROM mz_sinks WHERE id LIKE 'u%')::int4,
                        'active_ssh_tunnel_connections', (SELECT count(*) FROM mz_connections WHERE id LIKE 'u%' AND type = 'ssh-tunnel')::int4,
                        'active_kafka_sinks', (SELECT count(*) FROM mz_sinks WHERE id LIKE 'u%' AND type = 'kafka')::int4,
                        'active_tables', (SELECT count(*) FROM mz_tables WHERE id LIKE 'u%')::int4,
                        'active_views', (SELECT count(*) FROM mz_views WHERE id LIKE 'u%')::int4,
                        'active_subscribes', {active_subscribes}
                    )",
                )).await?;
                let row = rows.into_element();
                let jsonb = Jsonb::from_row(row);
                Ok::<_, anyhow::Error>(jsonb.as_ref().to_serde_json())
            })
            .await;

        let traits = match traits {
            Ok(traits) => traits,
            Err(e) => {
                warn!("unable to collect telemetry traits: {e}");
                continue;
            }
        };

        segment_client.group(
            // We use the organization ID as the user ID for events
            // that are not associated with a particular user.
            environment_id.organization_id(),
            environment_id.organization_id(),
            json!({
                environment_id.cloud_provider().to_string(): {
                    environment_id.cloud_provider_region(): traits,
                }
            }),
        );

        let query_total = &adapter_client.metrics().query_total;
        let current_stats = Stats {
            deletes: query_total.with_label_values(&["user", "delete"]).get(),
            inserts: query_total.with_label_values(&["user", "insert"]).get(),
            updates: query_total.with_label_values(&["user", "update"]).get(),
            selects: query_total.with_label_values(&["user", "select"]).get(),
            subscribes: query_total.with_label_values(&["user", "subscribe"]).get(),
        };
        if let Some(last_stats) = &last_stats {
            let mut event = json!({
                "deletes": current_stats.deletes - last_stats.deletes,
                "inserts": current_stats.inserts - last_stats.inserts,
                "updates": current_stats.updates - last_stats.updates,
                "selects": current_stats.selects - last_stats.selects,
                "subscribes": current_stats.subscribes - last_stats.subscribes,
            });
            event
                .as_object_mut()
                .unwrap()
                .extend(traits.as_object().unwrap().clone());
            segment_client.environment_track(
                &environment_id,
                "environmentd",
                // We use the organization ID as the user ID for events
                // that are not associated with a particular user.
                environment_id.organization_id(),
                "Environment Rolled Up",
                event,
            );
        }
        last_stats = Some(current_stats);
    }
}
