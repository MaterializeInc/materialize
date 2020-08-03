// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Error, Result};
use csv::Writer;
use ore::retry;
use rand::Rng;
use tokio_postgres::Client;

use test_util::mz_client;

pub async fn create_proto_source(
    mz_client: &Client,
    descriptor: &[u8],
    kafka_url: &impl std::fmt::Display,
    kafka_topic_name: &str,
    source_name: &str,
    message_name: &str,
    batch_size: Option<u64>,
) -> Result<()> {
    let encoded = hex::encode(descriptor);
    let query = if let Some(batch_size) = batch_size {
        format!(
            "CREATE SOURCE {source} FROM KAFKA BROKER '{kafka_url}' TOPIC '{topic}' \
             WITH (max_timestamp_batch_size={batch_size}) \
             FORMAT PROTOBUF MESSAGE '{message}' USING SCHEMA '\\x{descriptor}' \
             ",
            descriptor = encoded,
            kafka_url = kafka_url,
            topic = kafka_topic_name,
            source = source_name,
            message = message_name,
            batch_size = batch_size
        )
    } else {
        format!(
            "CREATE SOURCE {source} FROM KAFKA BROKER '{kafka_url}' TOPIC '{topic}' \
             FORMAT PROTOBUF MESSAGE '{message}' USING SCHEMA '\\x{descriptor}'",
            descriptor = encoded,
            kafka_url = kafka_url,
            topic = kafka_topic_name,
            source = source_name,
            message = message_name,
        )
    };

    log::debug!("creating source=> {}", query);

    mz_client::execute(&mz_client, &query).await?;
    Ok(())
}

pub async fn create_kafka_sink(
    mz_client: &Client,
    kafka_url: &impl std::fmt::Display,
    sink_topic_name: &str,
    sink_name: &str,
    schema_registry_url: &str,
) -> Result<String> {
    let query = format!(
            "CREATE SINK {sink} FROM billing_monthly_statement INTO KAFKA BROKER '{kafka_url}' TOPIC '{topic}' \
             WITH (consistency = true) FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '{schema_registry}'",
             sink = sink_name,
             kafka_url = kafka_url,
             topic = sink_topic_name,
             schema_registry = schema_registry_url
         );

    log::debug!("creating sink=> {}", query);
    mz_client::execute(&mz_client, &query).await?;

    // Get the topic for the newly-created sink.
    let row = mz_client
        .query_one(
            "SELECT topic FROM mz_kafka_sinks NATURAL JOIN mz_catalog_names \
                 WHERE name = 'materialize.public.' || $1",
            &[&sink_name],
        )
        .await?;
    Ok(row.get("topic"))
}

pub async fn create_csv_source(
    mz_client: &Client,
    file_name: &str,
    source_name: &str,
    num_clients: u32,
    seed: u64,
    batch_size: Option<u64>,
) -> Result<()> {
    let path = PathBuf::from(file_name);
    let mut writer = Writer::from_path(path.clone())
        .with_context(|| format!("failed to initialize csv writer for {}", path.display()))?;
    use rand::SeedableRng;
    let rng = &mut rand::rngs::StdRng::seed_from_u64(seed);

    for i in 1..num_clients {
        writer
            .write_record(&[
                i.to_string(),
                rng.gen_range(1, 10).to_string(),
                rng.gen_range(1, 10).to_string(),
            ])
            .with_context(|| format!("failed to write data to {}", path.display()))?;
    }

    writer
        .flush()
        .with_context(|| format!("failed to flush data to {}", path.display()))?;

    let absolute_path = if path.is_absolute() {
        path
    } else {
        path.canonicalize()
            .with_context(|| format!("failed to convert {} to an absolute path", path.display()))?
    };
    let query = if let Some(batch_size) = batch_size {
        format!(
                "CREATE SOURCE {source} FROM FILE '{file}'  WITH (max_timestamp_batch_size={batch_size}) FORMAT CSV WITH 3 COLUMNS \
               ",
                source = source_name,
                file = absolute_path.display(),
                batch_size = batch_size
            )
    } else {
        format!(
            "CREATE SOURCE {source} FROM FILE '{file}' FORMAT CSV WITH 3 COLUMNS",
            source = source_name,
            file = absolute_path.display(),
        )
    };

    log::debug!("creating csv source=> {}", query);
    mz_client::execute(&mz_client, &query).await?;
    Ok(())
}

pub async fn reingest_sink(
    mz_client: &Client,
    kafka_url: &str,
    schema_registry_url: &str,
    source_name: &str,
    topic_name: &str,
) -> Result<()> {
    let query = format!("CREATE MATERIALIZED SOURCE {source_name} FROM KAFKA BROKER '{kafka_url}' TOPIC '{topic_name}' \
                     FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '{schema_registry}' ENVELOPE DEBEZIUM",
                    source_name = source_name,
                    kafka_url = kafka_url,
                    topic_name = topic_name,
                    schema_registry = schema_registry_url);

    log::debug!("creating materialized source to reingest sink=> {}", query);
    mz_client::execute(&mz_client, &query).await?;

    Ok(())
}

pub async fn validate_sink(
    mz_client: &Client,
    check_sink_view: &str,
    input_view: &str,
    invalid_rows_view: &str,
) -> Result<()> {
    let count_check_sink_query = format!("SELECT count(*) from {}", check_sink_view);
    let count_input_view_query = format!("SELECT count(*) from {}", input_view);

    retry::retry_for::<_, _, _, Error>(Duration::from_secs(15), |_| async {
        let count_check_sink: i64 = mz_client
            .query_one(&*count_check_sink_query, &[])
            .await?
            .get(0);
        let count_input_view: i64 = mz_client
            .query_one(&*count_input_view_query, &[])
            .await?
            .get(0);

        if count_check_sink != count_input_view {
            bail!(
                "Expected check_sink view to have {} rows, found {}",
                count_input_view,
                count_check_sink
            );
        }

        Ok(())
    })
    .await?;

    let query = format!("SELECT * FROM {}", invalid_rows_view);
    log::debug!("validating sinks=> {}", query);
    let rows = mz_client.query(&*query, &[]).await?;

    if rows.len() != 0 {
        bail!(
            "Expected 0 invalid rows from check_sink, found {}",
            rows.len()
        );
    }

    Ok(())
}

fn billing_agg_view(unit: &str) -> String {
    format!("CREATE MATERIALIZED VIEW billing_agg_by_{unit} AS
SELECT date_trunc as {unit}, client_id, meter, cpu_num, memory_gb, disk_gb, sum(value)
FROM billing_records
GROUP BY date_trunc('{unit}', interval_start::timestamp), client_id, meter, cpu_num, memory_gb, disk_gb",
unit = unit)
}

pub async fn init_views(
    client: &Client,
    kafka_source_name: &str,
    csv_source_name: &str,
) -> Result<()> {
    let views = vec![
        format!(
            "CREATE MATERIALIZED VIEW billing_raw_data AS
SELECT
    *
FROM
    {}",
            kafka_source_name
        ),
        format!(
            "CREATE MATERIALIZED VIEW billing_prices AS
SELECT
column1::int AS client_id,
((column2::float) / 1000.0) AS price_per_cpu_ms,
((column3::float) / 1000.0) AS price_per_gb_ms
FROM
{}",
            csv_source_name
        ),
        "CREATE MATERIALIZED VIEW billing_batches AS
SELECT
    billing_raw_data.id,
    to_timestamp((billing_raw_data.interval_start->'seconds')::bigint) interval_start,
    to_timestamp((billing_raw_data.interval_end->'seconds')::bigint) interval_end
FROM
    billing_raw_data"
            .to_string(),
        "CREATE MATERIALIZED VIEW billing_records AS
SELECT
    r.value->>'id' id,
    billing_raw_data.id batch_id,
    to_timestamp((r.value->'interval_start'->'seconds')::bigint) interval_start,
    to_timestamp((r.value->'interval_end'->'seconds')::bigint)  interval_end,
    r.value->>'meter' meter,
    (r.value->'value')::int value,
    (r.value->'info'->'client_id')::int client_id,
    (r.value->'info'->'vm_id')::int vm_id,
    (r.value->'info'->'cpu_num')::int cpu_num,
    (r.value->'info'->'memory_gb')::int memory_gb,
    (r.value->'info'->'disk_gb')::int disk_gb
FROM
    billing_raw_data,
    jsonb_array_elements(records) AS r"
            .to_string(),
        billing_agg_view("minute"),
        billing_agg_view("hour"),
        billing_agg_view("day"),
        billing_agg_view("month"),
        "CREATE MATERIALIZED VIEW billing_monthly_statement AS
SELECT
    billing_agg_by_month.month,
    billing_agg_by_month.client_id,
    billing_agg_by_month.sum as execution_time_ms,
    billing_agg_by_month.cpu_num,
    billing_agg_by_month.memory_gb,
    floor((billing_agg_by_month.sum * ((billing_agg_by_month.cpu_num * billing_prices.price_per_cpu_ms) + (billing_agg_by_month.memory_gb * billing_prices.price_per_gb_ms)))) as monthly_bill
FROM
    billing_agg_by_month, billing_prices
WHERE
    billing_agg_by_month.client_id = billing_prices.client_id AND billing_agg_by_month.meter = 'execution_time_ms'"
        .to_string(),
    ];

    for v in views.iter() {
        mz_client::execute(&client, v).await?;
    }

    Ok(())
}

pub async fn drop_indexes(client: &Client) -> Result<()> {
    let views = vec!["billing_raw_data", "billing_records"];

    for v in views.iter() {
        let index_name = format!("{}_primary_idx", v);
        mz_client::drop_index(client, &index_name).await?;
    }

    Ok(())
}

pub async fn init_sink_views(client: &Client, reingested_sink_source_name: &str) -> Result<()> {
    let views = vec![
        format!(
            "CREATE MATERIALIZED VIEW check_sink AS
SELECT
    {src}.execution_time_ms AS t1,
    {src}.monthly_bill AS bill1,
    billing_monthly_statement.execution_time_ms AS t2,
    billing_monthly_statement.monthly_bill AS bill2
FROM
    {src}, billing_monthly_statement
WHERE
    to_timestamp(reingested_sink.month / 1000000) = billing_monthly_statement.month AND
    {src}.client_id = billing_monthly_statement.client_id AND
    {src}.cpu_num = billing_monthly_statement.cpu_num AND
    {src}.memory_gb = billing_monthly_statement.memory_gb",
            src = reingested_sink_source_name
        ),
        "CREATE MATERIALIZED VIEW invalid_sink_rows AS
    SELECT * FROM check_sink
WHERE
    t1 != t2 or bill1 != bill2"
            .to_string(),
    ];

    for v in views.iter() {
        mz_client::execute(&client, v).await?;
    }

    Ok(())
}
