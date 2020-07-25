// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::Sync;
use std::time::Duration;

use anyhow::{bail, Error, Result};
use csv::Writer;
use ore::retry;
use postgres_types::ToSql;
use rand::Rng;
use tokio_postgres::{Client, NoTls};

/// A materialized client with custom methods
pub struct MzClient(Client);

impl MzClient {
    /// Construct a new client talking to mz_url
    pub async fn new(mz_host: &str, mz_port: u16) -> Result<MzClient> {
        let (client, conn) = tokio_postgres::Config::new()
            .user("mzd")
            .host(mz_host)
            .port(mz_port)
            .connect(NoTls)
            .await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                log::error!("connection error: {}", e);
            }
        });

        Ok(MzClient(client))
    }

    pub async fn show_sources(&self) -> Result<Vec<String>> {
        let mut res = Vec::new();
        for row in self.0.query("SHOW SOURCES", &[]).await? {
            res.push(row.get(0))
        }

        Ok(res)
    }

    pub async fn drop_source(&self, name: &str) -> Result<()> {
        let q = format!("DROP SOURCE IF EXISTS {} CASCADE", name);
        log::debug!("deleting source=> {}", q);
        self.0.execute(&*q, &[]).await?;
        Ok(())
    }

    pub async fn create_proto_source(
        &self,
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

        self.0.execute(&*query, &[]).await?;
        Ok(())
    }

    pub async fn create_kafka_sink(
        &self,
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
        self.0.execute(&*query, &[]).await?;

        // Get the topic for the newly-created sink.
        let row = self
            .0
            .query_one(
                "SELECT topic FROM mz_kafka_sinks NATURAL JOIN mz_catalog_names \
                 WHERE name = 'materialize.public.' || $1",
                &[&sink_name],
            )
            .await?;
        Ok(row.get("topic"))
    }

    pub async fn create_csv_source(
        &self,
        file_name: &str,
        source_name: &str,
        num_clients: u32,
        seed: u64,
        batch_size: Option<u64>,
    ) -> Result<()> {
        let mut writer = Writer::from_path(file_name)?;
        use rand::SeedableRng;
        let rng = &mut rand::rngs::StdRng::seed_from_u64(seed);

        for i in 1..num_clients {
            writer.write_record(&[
                i.to_string(),
                rng.gen_range(1, 10).to_string(),
                rng.gen_range(1, 10).to_string(),
            ])?;
        }

        writer.flush()?;
        let query = if let Some(batch_size) = batch_size {
            format!(
                "CREATE SOURCE {source} FROM FILE '{file}'  WITH (max_timestamp_batch_size={batch_size}) FORMAT CSV WITH 3 COLUMNS \
               ",
                source = source_name,
                file = file_name,
                batch_size = batch_size
            )
        } else {
            format!(
                "CREATE SOURCE {source} FROM FILE '{file}' FORMAT CSV WITH 3 COLUMNS",
                source = source_name,
                file = file_name
            )
        };

        log::debug!("creating csv source=> {}", query);
        self.0.execute(&*query, &[]).await?;
        Ok(())
    }

    pub async fn execute(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        log::debug!("exec-> {} params={:?}", query, params);
        Ok(self.0.execute(query, params).await?)
    }

    pub async fn reingest_sink(
        &self,
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
        self.0.execute(&*query, &[]).await?;

        Ok(())
    }

    pub async fn validate_sink(
        &self,
        check_sink_view: &str,
        input_view: &str,
        invalid_rows_view: &str,
    ) -> Result<()> {
        let count_check_sink_query = format!("SELECT count(*) from {}", check_sink_view);
        let count_input_view_query = format!("SELECT count(*) from {}", input_view);

        retry::retry_for::<_, _, _, Error>(Duration::from_secs(15), |_| async {
            let count_check_sink: i64 = self
                .0
                .query_one(&*count_check_sink_query, &[])
                .await?
                .get(0);
            let count_input_view: i64 = self
                .0
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
        let rows = self.0.query(&*query, &[]).await?;

        if rows.len() != 0 {
            bail!(
                "Expected 0 invalid rows from check_sink, found {}",
                rows.len()
            );
        }

        Ok(())
    }
}
