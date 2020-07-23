// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
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

    pub async fn create_upsert_text_source(
        &self,
        kafka_url: &impl std::fmt::Display,
        kafka_topic_name: &str,
        source_name: &str,
    ) -> Result<()> {
        let query = format!(
            "CREATE MATERIALIZED SOURCE {source} FROM KAFKA BROKER '{kafka_url}' TOPIC '{topic}' \
             WITH (persistence = true) FORMAT TEXT",
            kafka_url = kafka_url,
            topic = kafka_topic_name,
            source = source_name,
        );
        log::debug!("creating source=> {}", query);

        self.0.execute(&*query, &[]).await?;
        Ok(())
    }
}
