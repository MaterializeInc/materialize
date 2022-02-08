// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use mz_test_util::mz_client;

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
    tracing::info!("creating source=> {}", query);
    client
        .batch_execute(&query)
        .await
        .context("Creating source")?;

    let query = "CREATE VIEW foo_view AS SELECT CONVERT_FROM(data, 'utf8') AS data FROM foo";
    tracing::info!("creating view=> {}", query);
    client
        .batch_execute(query)
        .await
        .context("Creating non-materialized view")?;

    // Only materialize the count.
    let query = "CREATE MATERIALIZED VIEW foo_count AS SELECT count(*) FROM foo";
    tracing::info!("creating materialized view=> {}", query);
    client
        .batch_execute(query)
        .await
        .context("Creating materialized view")?;

    Ok(())
}

pub async fn query_materialized_view_until(
    client: &Client,
    view_name: &str,
    expected_total_records: u64,
) -> Result<(), anyhow::Error> {
    let query = format!("SELECT * FROM {view_name};", view_name = view_name);
    tracing::info!("querying view=> {}", query);

    let row = mz_client::try_query_one(&client, &*query, Duration::from_secs(1)).await?;
    let count: i64 = row.get("count");
    if count as u64 == expected_total_records {
        tracing::info!(
            "Found all {} records, done querying.",
            expected_total_records
        );
    }
    Ok(())
}
