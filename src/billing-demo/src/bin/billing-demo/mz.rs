// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::{bail, Result};
use mz_ore::retry::Retry;
use tokio_postgres::Client;
use tracing::debug;

use mz_test_util::mz_client;

pub async fn create_proto_source(
    mz_client: &Client,
    descriptor: &[u8],
    kafka_url: &impl std::fmt::Display,
    kafka_topic_name: &str,
    source_name: &str,
    message_name: &str,
    enable_persistence: bool,
) -> Result<()> {
    let encoded = hex::encode(descriptor);

    let enable_persistence_str = if enable_persistence {
        "WITH (persistence = true)"
    } else {
        ""
    };

    let query = format!(
        "CREATE SOURCE {source} FROM KAFKA BROKER '{kafka_url}' TOPIC '{topic}' \
         {persistence} FORMAT PROTOBUF MESSAGE '{message}' USING SCHEMA '\\x{descriptor}'",
        descriptor = encoded,
        kafka_url = kafka_url,
        topic = kafka_topic_name,
        persistence = enable_persistence_str,
        source = source_name,
        message = message_name,
    );

    debug!("creating source=> {}", query);

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
        "CREATE CONNECTION IF NOT EXISTS {sink}_kafka_conn
            FOR KAFKA BROKER '{kafka_url}'",
        sink = sink_name,
        kafka_url = kafka_url,
    );

    debug!("creating kafka connection=> {}", query);
    mz_client::execute(&mz_client, &query).await?;

    let query = format!(
        "CREATE CONNECTION IF NOT EXISTS {sink}_csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '{schema_registry_url}'",
        sink = sink_name,
        schema_registry_url = schema_registry_url,
    );

    debug!("creating confluent schema registry connection=> {}", query);
    mz_client::execute(&mz_client, &query).await?;

    let query = format!(
            "CREATE SINK {sink} FROM billing_monthly_statement INTO KAFKA CONNECTION {sink}_kafka_conn (TOPIC '{topic}') \
             CONSISTENCY (TOPIC '{topic}-consistency' )
             FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {sink}_csr_conn",
             sink = sink_name,
             topic = sink_topic_name,
         );

    debug!("creating sink=> {}", query);
    mz_client::execute(&mz_client, &query).await?;

    // Get the topic for the newly-created sink.
    let row = mz_client
        .query_one(
            "SELECT topic FROM mz_kafka_sinks JOIN mz_catalog_names ON sink_id = global_id \
                 WHERE name = 'materialize.public.' || $1",
            &[&sink_name],
        )
        .await?;
    Ok(row.get("topic"))
}

pub async fn create_price_table(
    mz_client: &Client,
    source_name: &str,
    seed: u64,
    num_clients: u32,
) -> Result<()> {
    let query = format!(
        "CREATE TABLE {source_name} (client_id text, price_per_cpu_ms text, price_per_gb_ms text)",
        source_name = source_name,
    );

    debug!("creating price table=> {}", query);
    mz_client::execute(&mz_client, &query).await?;

    use rand::{Rng, SeedableRng};
    let rng = &mut rand::rngs::StdRng::seed_from_u64(seed);
    debug!("filling price table");
    for i in 1..num_clients {
        mz_client::execute(
            &mz_client,
            &format!(
                "INSERT into {} VALUES ('{}', '{}', '{}')",
                source_name,
                i,
                rng.gen_range(1..10),
                rng.gen_range(1..10),
            ),
        )
        .await?;
    }
    Ok(())
}

pub async fn reingest_sink(
    mz_client: &Client,
    kafka_url: &str,
    schema_registry_url: &str,
    source_name: &str,
    topic_name: &str,
) -> Result<()> {
    let query = format!(
        "CREATE CONNECTION IF NOT EXISTS {source}_csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '{schema_registry_url}'",
        source = source_name,
        schema_registry_url = schema_registry_url,
    );

    debug!("creating confluent schema registry connection=> {}", query);
    mz_client::execute(&mz_client, &query).await?;

    let query = format!(
        "
        CREATE SOURCE {source_name} \
        FROM KAFKA BROKER '{kafka_url}' TOPIC '{topic_name}' \
        FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {source_name}_csr_conn \
        ENVELOPE DEBEZIUM",
        source_name = source_name,
        kafka_url = kafka_url,
        topic_name = topic_name
    );

    debug!("creating source to reingest sink=> {}", query);
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

    Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry_async(|_| async {
            let count_check_sink: i64 = mz_client
                .query_one(&*count_check_sink_query, &[])
                .await?
                .get(0);
            let count_input_view: i64 = mz_client
                .query_one(&*count_input_view_query, &[])
                .await?
                .get(0);

            if count_input_view == 0 {
                bail!("source is still loading");
            }

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
    debug!("validating sinks=> {}", query);
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
SELECT date_trunc('{unit}', interval_start::timestamp) AS {unit}, client_id, meter, cpu_num, memory_gb, disk_gb, sum(value)
FROM billing_records
GROUP BY {unit}, client_id, meter, cpu_num, memory_gb, disk_gb",
unit = unit)
}

pub async fn init_views(
    client: &Client,
    kafka_source_name: &str,
    price_table_name: &str,
) -> Result<()> {
    let billing_raw_data = format!(
        "CREATE MATERIALIZED VIEW billing_raw_data AS
        SELECT
        *
        FROM
        {}",
        kafka_source_name
    );

    let billing_prices = format!(
        "CREATE MATERIALIZED VIEW billing_prices AS
        SELECT
        client_id::int AS client_id,
        ((price_per_cpu_ms::float) / 1000.0) AS price_per_cpu_ms,
        ((price_per_gb_ms::float) / 1000.0) AS price_per_gb_ms
        FROM
        {}",
        price_table_name
    );

    let billing_batches = r#"CREATE MATERIALIZED VIEW billing_batches AS
        SELECT
            billing_raw_data.id,
            to_timestamp(((billing_raw_data.interval_start)."seconds")::bigint) interval_start,
            to_timestamp(((billing_raw_data.interval_end)."seconds")::bigint) interval_end
        FROM
            billing_raw_data"#
        .to_string();

    let billing_records = r#"CREATE MATERIALIZED VIEW billing_records AS
    SELECT
        (r).id id,
        billing_raw_data.id batch_id,
        to_timestamp((((r)."interval_start")."seconds")::bigint) interval_start,
        to_timestamp((((r)."interval_end")."seconds")::bigint) interval_end,
        (r).meter meter,
        ((r)."value")::int value,
        (((r).info).client_id)::int client_id,
        (((r).info).vm_id)::int vm_id,
        (((r).info).cpu_num)::int cpu_num,
        (((r).info).memory_gb)::int memory_gb,
        (((r).info).disk_gb)::int disk_gb
    FROM
        billing_raw_data,
        unnest(records) AS r"#
        .to_string();

    let billing_monthly_statement = "CREATE MATERIALIZED VIEW billing_monthly_statement AS
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
            .to_string();

    let billing_top_5_months_per_client =
        "CREATE MATERIALIZED VIEW billing_top_5_months_per_client AS
    SELECT
        client_id,
        month,
        execution_time_ms,
        monthly_bill,
        cpu_num,
        memory_gb
    FROM
        (SELECT DISTINCT client_id FROM billing_monthly_statement) grp,
        LATERAL
            (SELECT month, execution_time_ms, monthly_bill, cpu_num, memory_gb
                FROM
                    billing_monthly_statement
                WHERE client_id = grp.client_id
                ORDER BY monthly_bill DESC LIMIT 5)"
            .to_string();

    let views = vec![
        billing_raw_data,
        billing_prices,
        billing_batches,
        billing_records,
        billing_agg_view("minute"),
        billing_agg_view("hour"),
        billing_agg_view("day"),
        billing_agg_view("month"),
        billing_monthly_statement,
        billing_top_5_months_per_client,
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
    {src}.month = billing_monthly_statement.month AND
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
