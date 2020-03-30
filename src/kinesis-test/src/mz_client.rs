// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::thread;
use std::time::Duration;

use tokio_postgres::{Client, NoTls, Row};

use crate::error::Result;

/// A Materialized client with custom methods to create, query, and drop
/// sources and views based on Kinesis Data Streams.
pub struct MzClient(Client);

impl MzClient {
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

    pub async fn create_kinesis_source(
        &self,
        source_name: &str,
        arn: &str,
        access_key: &str,
        secret_access_key: &str,
    ) -> Result<()> {
        let query = format!(
            "CREATE SOURCE {source_name}
             FROM KINESIS ARN '{arn}'
             WITH (access_key = '{access_key}',
                   secret_access_key = '{secret_access_key}')
             FORMAT BYTES;",
            source_name = source_name,
            arn = arn,
            access_key = access_key,
            secret_access_key = secret_access_key
        );
        log::debug!("creating source=> {}", query);

        self.0.execute(&*query, &[]).await?;
        Ok(())
    }

    pub async fn create_view(&self, source_name: &str) -> Result<()> {
        let query = format!(
            "CREATE MATERIALIZED VIEW {source_name}_view AS
            SELECT CONVERT_FROM(data, 'utf8') AS DATA FROM {source_name};",
            source_name = source_name
        );
        log::debug!("creating view=> {}", query);

        self.0.execute(&*query, &[]).await?;
        Ok(())
    }

    pub async fn query_view(&self, source_name: &str) -> Result<Vec<Row>> {
        let query = format!(
            "SELECT * FROM {source_name}_view;",
            source_name = source_name
        );
        log::debug!("querying view=> {}", query);

        let mut result = Vec::new();
        loop {
            match self.0.query(&*query, &[]).await {
                Ok(rows) => {
                    for r in rows {
                        result.push(r);
                    }
                    break;
                }
                Err(e) => {
                    // We will see this error if we query the view between the
                    // time it is created and when it ingests its first data.
                    // This error is transient and should be retried.
                    if e.to_string()
                        .contains("At least one input has no complete timestamps yet.")
                    {
                        log::debug!("Hit error querying, will try again... {}", e.to_string());
                        thread::sleep(Duration::from_secs(1));
                    } else {
                        panic!("hit error trying to query view: {}", e.to_string());
                    }
                }
            }
        }

        Ok(result)
    }

    pub async fn drop_kinesis_source_and_view(&self, source_name: &str) -> Result<()> {
        // DROP VIEW (dependent on source)
        let query = format!("DROP VIEW {source_name}_view;", source_name = source_name);
        log::debug!("dropping view=> {}", query);

        self.0.execute(&*query, &[]).await?;

        // DROP SOURCE
        let query = format!("DROP SOURCE {source_name}", source_name = source_name);
        log::debug!("dropping source=> {}", query);

        self.0.execute(&*query, &[]).await?;
        Ok(())
    }
}
