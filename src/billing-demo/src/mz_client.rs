// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::marker::Sync;

use postgres_types::ToSql;
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
        let encoded = base64::encode(descriptor);

        let query = format!(
            "CREATE SOURCE {source} FROM 'kafka://{kafka_url}/{topic}' \
             USING SCHEMA '{descriptor}' \
             WITH (format='protobuf-descriptor', message_name='{message}')",
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

    pub async fn execute(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        log::debug!("exec-> {} params={:?}", query, params);
        Ok(self.0.execute(query, params).await?)
    }
}
