// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::Sync;

use csv::Writer;
use postgres_types::ToSql;
use rand::Rng;
use tokio_postgres::{Client, NoTls};

use crate::error::Result;

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
        let q = format!("DROP SOURCE {} CASCADE", name);
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
    ) -> Result<()> {
        let encoded = hex::encode(descriptor);

        let query = format!(
            "CREATE SOURCE {source} FROM KAFKA BROKER '{kafka_url}' TOPIC '{topic}' \
             FORMAT PROTOBUF MESSAGE '{message}' USING SCHEMA '\\x{descriptor}'",
            descriptor = encoded,
            kafka_url = kafka_url,
            topic = kafka_topic_name,
            source = source_name,
            message = message_name,
        );
        log::debug!("creating source=> {}", query);

        self.0.execute(&*query, &[]).await?;
        Ok(())
    }

    pub async fn create_csv_source(
        &self,
        file_name: &str,
        source_name: &str,
        num_clients: u32,
    ) -> Result<()> {
        let mut writer = Writer::from_path(file_name)?;
        use rand::SeedableRng;
        let rng = &mut rand::rngs::StdRng::from_seed(rand::random());

        for i in 1..num_clients {
            writer.write_record(&[
                i.to_string(),
                rng.gen_range(1, 10).to_string(),
                rng.gen_range(1, 10).to_string(),
            ])?;
        }

        writer.flush()?;
        let query = format!(
            "CREATE SOURCE {source} FROM FILE '{file}' FORMAT CSV WITH 3 COLUMNS",
            source = source_name,
            file = file_name,
        );

        log::debug!("creating csv source=> {}", query);
        self.0.execute(&*query, &[]).await?;
        Ok(())
    }

    pub async fn execute(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        log::debug!("exec-> {} params={:?}", query, params);
        Ok(self.0.execute(query, params).await?)
    }
}
