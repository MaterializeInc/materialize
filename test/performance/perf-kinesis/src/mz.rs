// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::Context;
use tokio_postgres::Client;

pub async fn create_source_and_views(
    client: &Client,
    stream_arn: String,
) -> Result<(), anyhow::Error> {
    let query = format!(
        "CREATE SOURCE foo
         FROM KINESIS ARN '{stream_arn}'
         FORMAT BYTES",
        stream_arn = stream_arn,
    );
    log::info!("creating source=> {}", query);
    client
        .batch_execute(&query)
        .await
        .context("Creating source")?;

    let query = "CREATE VIEW foo_view AS SELECT CONVERT_FROM(data, 'utf8') AS data FROM foo";
    log::info!("creating view=> {}", query);
    client
        .batch_execute(query)
        .await
        .context("Creating non-materialized view")?;

    // Only materialize the count.
    let query = "CREATE MATERIALIZED VIEW foo_count AS SELECT count(*) FROM foo";
    log::info!("creating materialized view=> {}", query);
    client
        .batch_execute(query)
        .await
        .context("Creating materialized view")?;

    Ok(())
}

pub async fn query_materialized_view_until(
    client: &Client,
    view_name: &str,
    expected_total_records: i64,
) -> Result<(), anyhow::Error> {
    let query = format!("SELECT * FROM {view_name};", view_name = view_name);
    log::info!("querying view=> {}", query);

    // 1QPS until caught up.
    loop {
        let timer = std::time::Instant::now();
        match client.query_one(&*query, &[]).await {
            Ok(row) => {
                let count: i64 = row.get("count");
                if count == expected_total_records {
                    log::info!(
                        "Found all {} records, done querying.",
                        expected_total_records
                    );
                    break;
                }
            }
            Err(e) => {
                // We will see this error if we query the view between the
                // time it is created and when it ingests its first data.
                // This error is transient and should be retried.
                if e.to_string()
                    .contains("At least one input has no complete timestamps yet.")
                {
                    log::debug!("Error querying, will try again... {}", e.to_string());
                } else {
                    return Err(e).context("Querying materialized view");
                }
            }
        }

        let elapsed = timer.elapsed();
        if elapsed < Duration::from_secs(1) {
            tokio::time::delay_for(Duration::from_secs(1) - elapsed).await;
        } else {
            log::info!(
                "Expected to query for records in 1s, took {}",
                elapsed
            );
        }
    }
    Ok(())
}
