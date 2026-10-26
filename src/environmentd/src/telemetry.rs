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

use std::collections::BTreeMap;

use anyhow::bail;
use chrono::Utc;
use futures::StreamExt;
use itertools::Itertools;
use mz_adapter::PeekResponseUnary;
use mz_adapter::telemetry::{EventDetails, SegmentClientExt};
use mz_build_info::BuildInfo;
use mz_license_keys::ValidatedLicenseKey;
use mz_ore::cast::CastLossy;
use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use mz_ore::{soft_panic_or_log, task};
use mz_repr::Row;
use mz_repr::adt::jsonb::Jsonb;
use mz_sql::catalog::EnvironmentId;
use mz_sql::feature_usage::statement_features;
use prometheus::IntCounterVec;
use prometheus::core::Collector;
use serde_json::json;
use tokio::time::{self, Duration};

use crate::BUILD_INFO;

/// Telemetry configuration.
#[derive(Clone)]
pub struct Config {
    /// The Segment client to report telemetry events to.
    pub segment_client: mz_segment::Client,
    /// A client to the adapter to introspect.
    pub adapter_client: mz_adapter::Client,
    /// The ID of the environment for which to report data.
    pub environment_id: EnvironmentId,
    /// The validated license key for this environment. Reported so downstream
    /// analytics can tell which entitlements are active and whether the key has
    /// expired.
    pub license_key: ValidatedLicenseKey,
    /// The version of the helm chart that deployed this environment, if
    /// self-managed. Folded into the reported `mz_version` trait.
    pub helm_chart_version: Option<String>,
    /// How frequently to send a summary to Segment.
    pub report_interval: Duration,
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
        license_key,
        helm_chart_version,
        report_interval,
    }: Config,
) {
    let mut last_stats: Option<BTreeMap<String, u64>> = None;

    let mut interval = time::interval(report_interval);
    loop {
        interval.tick().await;

        // Drained every tick, including the first, so that the first emitted
        // rollup covers exactly one interval.
        let feature_usage = adapter_client.metrics().feature_usage.drain();

        let traits = Retry::default()
            .initial_backoff(Duration::from_secs(1))
            .max_tries(5)
            .retry_async(|_state| async {
                let active_subscribes = adapter_client
                    .metrics()
                    .active_subscribes
                    .with_label_values(&["user"])
                    .get();
                let rows = support_query(&adapter_client, &format!("
                    SELECT jsonb_build_object(
                        'active_aws_privatelink_connections', (SELECT count(*) FROM mz_connections WHERE id LIKE 'u%' AND type = 'aws-privatelink')::int4,
                        'active_clusters', (SELECT count(*) FROM mz_clusters WHERE id LIKE 'u%')::int4,
                        'active_cluster_replicas', (
                            SELECT jsonb_object_agg(base.size, coalesce(count, 0))
                            FROM mz_catalog.mz_cluster_replica_sizes base
                            LEFT JOIN (
                                SELECT r.size, count(*)::int4
                                FROM mz_cluster_replicas r
                                JOIN mz_clusters c ON c.id = r.cluster_id
                                WHERE c.id LIKE 'u%'
                                GROUP BY r.size
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

                assert_eq!(1, rows.len(), "expected one row but got: {:?}", rows);
                let row = rows.into_first();

                let jsonb = Jsonb::from_row(row);
                Ok::<_, anyhow::Error>(jsonb.as_ref().to_serde_json())
            })
            .await;

        let mut traits = match traits {
            Ok(traits) => traits,
            Err(e) => {
                soft_panic_or_log!("unable to collect telemetry traits: {e}");
                continue;
            }
        };

        if let Some(traits) = traits.as_object_mut() {
            build_segment_traits(
                traits,
                &license_key,
                &BUILD_INFO,
                helm_chart_version.clone(),
            );
        }

        tracing::info!(?traits, "telemetry traits");

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

        let current_stats = user_statement_counts(&adapter_client.metrics().query_total);
        if let Some(last_stats) = &last_stats {
            let delta = |kind: &str| {
                let current = current_stats.get(kind).copied().unwrap_or(0);
                current - last_stats.get(kind).copied().unwrap_or(0)
            };
            let statements: serde_json::Map<_, _> = current_stats
                .keys()
                .map(|kind| (kind.clone(), json!(delta(kind))))
                .filter(|(_, count)| count != &json!(0))
                .collect();
            let mut properties = json!({
                "deletes": delta("delete"),
                "inserts": delta("insert"),
                "updates": delta("update"),
                "selects": delta("select"),
                "subscribes": delta("subscribe"),
                "statements": statements,
            });
            properties
                .as_object_mut()
                .unwrap()
                .extend(traits.as_object().unwrap().clone());
            segment_client.environment_track(
                &environment_id,
                "Environment Rolled Up",
                properties,
                EventDetails::default(),
            );

            let sets = feature_usage
                .counts
                .into_iter()
                .map(|((application_name, features), count)| {
                    json!({
                        "application_name": application_name,
                        "features": features.split(',').collect::<Vec<_>>(),
                        "count": count,
                    })
                })
                .collect();
            track_chunked(
                &segment_client,
                &environment_id,
                "SQL Feature Usage Rolled Up",
                json!({ "overflow": feature_usage.overflow }),
                "feature_sets",
                sets,
            );
        }
        last_stats = Some(current_stats);

        match object_feature_sets(&adapter_client).await {
            Ok(sets) => track_chunked(
                &segment_client,
                &environment_id,
                "SQL Object Features Snapshot",
                json!({}),
                "object_feature_sets",
                sets,
            ),
            Err(e) => soft_panic_or_log!("unable to collect object feature sets: {e}"),
        }
    }
}

/// Executes `sql` as the support user and returns all result rows.
async fn support_query(
    adapter_client: &mz_adapter::Client,
    sql: &str,
) -> Result<Vec<Row>, anyhow::Error> {
    let mut rows_stream = adapter_client.support_execute_one(sql).await?;
    let mut rows = Vec::new();
    while let Some(response) = rows_stream.next().await {
        match response {
            PeekResponseUnary::Rows(mut row_iter) => {
                while let Some(row) = row_iter.next() {
                    rows.push(row.to_owned());
                }
            }
            PeekResponseUnary::Canceled => bail!("query canceled"),
            PeekResponseUnary::Error(e) => bail!(e),
            PeekResponseUnary::DependencyDropped(dep) => {
                bail!("{}", dep.query_terminated_error())
            }
        }
    }
    Ok(rows)
}

/// Returns the user session counts of `mz_query_total`, by statement type.
fn user_statement_counts(query_total: &IntCounterVec) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for family in query_total.collect() {
        for metric in family.get_metric() {
            let label = |name: &str| {
                metric
                    .get_label()
                    .iter()
                    .find(|l| l.name() == name)
                    .map(|l| l.value().to_string())
            };
            if label("session_type").as_deref() != Some("user") {
                continue;
            }
            if let Some(kind) = label("statement_type") {
                // `IntCounter` values are integers below 2^53, so this is exact.
                counts.insert(kind, u64::cast_lossy(metric.get_counter().value()));
            }
        }
    }
    counts
}

/// Counts the live user objects by object type and the SQL feature set of
/// their definition.
async fn object_feature_sets(
    adapter_client: &mz_adapter::Client,
) -> Result<Vec<serde_json::Value>, anyhow::Error> {
    let rows = support_query(
        adapter_client,
        "SELECT o.type, d.create_sql
         FROM mz_objects o
         JOIN (
             SELECT id, create_sql FROM mz_views
             UNION ALL SELECT id, create_sql FROM mz_materialized_views
             UNION ALL SELECT id, create_sql FROM mz_indexes
             UNION ALL SELECT id, create_sql FROM mz_sources
             UNION ALL SELECT id, create_sql FROM mz_sinks
             UNION ALL SELECT id, create_sql FROM mz_tables
         ) d ON o.id = d.id
         WHERE o.id LIKE 'u%' AND d.create_sql IS NOT NULL",
    )
    .await?;

    let mut counts: BTreeMap<(String, String), u64> = BTreeMap::new();
    for row in rows {
        let mut datums = row.iter();
        let object_type = datums.next().expect("two columns").unwrap_str();
        let create_sql = datums.next().expect("two columns").unwrap_str();
        let features = match mz_sql_parser::parser::parse_statements(create_sql) {
            Ok(stmts) if stmts.len() == 1 => statement_features(&stmts[0].ast),
            // Stored definitions always parse with the running version's
            // parser, so this only fires on a bug.
            _ => {
                soft_panic_or_log!("unable to parse create_sql of a {object_type}");
                continue;
            }
        };
        let features = features.iter().join(",");
        *counts
            .entry((object_type.to_string(), features))
            .or_default() += 1;
    }
    Ok(counts
        .into_iter()
        .map(|((object_type, features), count)| {
            json!({
                "object_type": object_type,
                "features": features.split(',').collect::<Vec<_>>(),
                "count": count,
            })
        })
        .collect())
}

/// The serialized size budget for a chunked event's items. Segment rejects
/// events over 32KB, and the remainder covers the envelope and the properties
/// `environment_track` adds.
const SEGMENT_CHUNK_BYTES: usize = 24 * 1024;

/// Tracks `items` as one or more `event`s, each carrying `properties` plus a
/// chunk of `items` under `items_key` that fits [`SEGMENT_CHUNK_BYTES`].
///
/// Each chunk carries `report_timestamp`, `chunk_index`, and `chunk_count` so
/// downstream consumers can reassemble the chunks of one report. Nothing is
/// tracked when `items` is empty.
fn track_chunked(
    segment_client: &mz_segment::Client,
    environment_id: &EnvironmentId,
    event: &str,
    properties: serde_json::Value,
    items_key: &str,
    items: Vec<serde_json::Value>,
) {
    let chunks = chunk_by_size(items, SEGMENT_CHUNK_BYTES);
    let report_timestamp = Utc::now().timestamp_millis();
    let chunk_count = chunks.len();
    for (chunk_index, chunk) in chunks.into_iter().enumerate() {
        let mut properties = properties.clone();
        let map = properties.as_object_mut().expect("properties is an object");
        map.insert("report_timestamp".into(), json!(report_timestamp));
        map.insert("chunk_index".into(), json!(chunk_index));
        map.insert("chunk_count".into(), json!(chunk_count));
        map.insert(items_key.into(), json!(chunk));
        segment_client.environment_track(
            environment_id,
            event,
            properties,
            EventDetails::default(),
        );
    }
}

/// Splits `items` into chunks whose serialized size is at most `budget`. An
/// item larger than `budget` gets a chunk of its own.
fn chunk_by_size(items: Vec<serde_json::Value>, budget: usize) -> Vec<Vec<serde_json::Value>> {
    let mut chunks = Vec::new();
    let mut chunk = Vec::new();
    let mut size = 0;
    for item in items {
        let item_size = item.to_string().len();
        if !chunk.is_empty() && size + item_size > budget {
            chunks.push(std::mem::take(&mut chunk));
            size = 0;
        }
        size += item_size;
        chunk.push(item);
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

/// Merges the build version and the license key's identity and state into the
/// collected traits.
///
/// The license key is the authoritative source for the organization and
/// environment IDs. `EnvironmentId::organization_id` only matches the real
/// organization in cloud SaaS, so we report the license key's `sub`
/// (organization) and `aud` (environment) instead.
///
/// Expiry is recomputed against the wall clock on every call rather than
/// reusing the flag computed once at startup, so a key that lapses while
/// environmentd keeps running is reported as expired. The `expiration == 0`
/// guard exempts the sentinel disabled/emulator key, whose zero expiration is
/// not a real timestamp.
fn build_segment_traits(
    traits: &mut serde_json::Map<String, serde_json::Value>,
    license_key: &ValidatedLicenseKey,
    build_info: &BuildInfo,
    helm_chart_version: Option<String>,
) {
    let now_secs = u64::try_from(Utc::now().timestamp()).unwrap_or(0);
    let license_expired =
        license_key.expired || (license_key.expiration != 0 && now_secs >= license_key.expiration);
    traits.insert("organization_id".into(), json!(license_key.organization));
    traits.insert("environment_id".into(), json!(license_key.environment_id));
    traits.insert("license_key_id".into(), json!(license_key.id));
    traits.insert(
        "license_expiration_timestamp".into(),
        json!(license_key.expiration),
    );
    traits.insert("license_expired".into(), json!(license_expired));
    traits.insert(
        "license_expiration_behavior".into(),
        json!(license_key.expiration_behavior),
    );
    traits.insert(
        "mz_version".into(),
        json!(build_info.human_version(helm_chart_version)),
    );
}

#[cfg(test)]
mod tests {
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_license_keys::ValidatedLicenseKey;
    use serde_json::json;

    use super::{build_segment_traits, chunk_by_size};

    fn license_key(expiration: u64, expired: bool) -> ValidatedLicenseKey {
        ValidatedLicenseKey {
            id: "test-license-key-id".into(),
            organization: "test-organization-id".into(),
            environment_id: "test-environment-id".into(),
            expiration,
            expired,
            ..ValidatedLicenseKey::for_tests()
        }
    }

    // Expiry is computed against the real wall clock, so these tests use
    // expirations in the distant past (1, i.e. just after the Unix epoch) or
    // distant future (`u64::MAX`) to stay deterministic.

    #[mz_ore::test]
    fn reports_license_and_version_traits() {
        let mut traits = json!({"active_clusters": 1});
        build_segment_traits(
            traits.as_object_mut().unwrap(),
            &license_key(u64::MAX, false),
            &DUMMY_BUILD_INFO,
            Some("25.1.0".into()),
        );
        assert_eq!(
            traits,
            json!({
                "active_clusters": 1,
                "organization_id": "test-organization-id",
                "environment_id": "test-environment-id",
                "license_key_id": "test-license-key-id",
                "license_expiration_timestamp": u64::MAX,
                "license_expired": false,
                "license_expiration_behavior": "Warn",
                "mz_version": DUMMY_BUILD_INFO.human_version(Some("25.1.0".into()))
            })
        );
    }

    #[mz_ore::test]
    fn expiry_is_recomputed_from_wall_clock() {
        // The key was valid at startup (`expired: false`) but the clock has
        // since passed its expiration.
        let mut traits = json!({});
        build_segment_traits(
            traits.as_object_mut().unwrap(),
            &license_key(1, false),
            &DUMMY_BUILD_INFO,
            None,
        );
        assert_eq!(&traits["license_expired"], &json!(true));
    }

    #[mz_ore::test]
    fn startup_expired_flag_is_preserved() {
        // A key marked expired at startup stays expired even if the clock
        // reads before its expiration.
        let mut traits = json!({});
        build_segment_traits(
            traits.as_object_mut().unwrap(),
            &license_key(u64::MAX, true),
            &DUMMY_BUILD_INFO,
            None,
        );
        assert_eq!(&traits["license_expired"], &json!(true));
    }

    #[mz_ore::test]
    fn zero_expiration_sentinel_never_expires() {
        let mut traits = json!({});
        build_segment_traits(
            traits.as_object_mut().unwrap(),
            &license_key(0, false),
            &DUMMY_BUILD_INFO,
            None,
        );
        assert_eq!(&traits["license_expired"], &json!(false));
    }

    #[mz_ore::test]
    fn chunks_respect_budget() {
        // Each item serializes to 3 bytes, e.g. `"a"`.
        let items: Vec<_> = ["a", "b", "c", "d", "e"].map(|s| json!(s)).into();
        let chunks = chunk_by_size(items, 7);
        assert_eq!(
            chunks,
            vec![
                vec![json!("a"), json!("b")],
                vec![json!("c"), json!("d")],
                vec![json!("e")],
            ]
        );
        let oversized = chunk_by_size(vec![json!("toolong"), json!("a")], 3);
        assert_eq!(oversized, vec![vec![json!("toolong")], vec![json!("a")]]);
        assert!(chunk_by_size(vec![], 3).is_empty());
    }
}
