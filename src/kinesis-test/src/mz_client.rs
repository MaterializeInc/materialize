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

use tokio_postgres::{Client, Connection, NoTls, Row};

/// A Materialized client with custom methods to create, query, and drop
/// sources and views based on Kinesis Data Streams.
pub struct MzClient(Client);

impl MzClient {
    pub async fn new(mz_host: &str, mz_port: u16) -> Result<MzClient, String> {
        let (client, conn) = tokio_postgres::Config::new()
            .user("mzd")
            .host(mz_host)
            .port(mz_port)
            .connect(NoTls)
            .await
            .map_err(|e| format!("creating MzClient: {}", e))?;

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
        aws_region: &str,
        aws_account: &str,
        stream_name: &str,
        access_key: &str,
        secret_access_key: &str,
        endpoint: &str,
    ) -> Result<(), String> {
        let query = format!(
            "CREATE SOURCE {source_name}
                 FROM KINESIS ARN 'arn:aws:kinesis:{aws_region}:{aws_account}:stream/{stream_name}'
                 WITH (access_key = '{access_key}',
                       secret_access_key = '{secret_access_key}',
                       endpoint = '{endpoint}')
                 FORMAT BYTES;",
            source_name = source_name,
            aws_region = aws_region,
            aws_account = aws_account,
            stream_name = stream_name,
            access_key = access_key,
            secret_access_key = secret_access_key,
            endpoint = endpoint
        );
        //        log::debug!("creating source=> {}", query);
        println!("creating source=> {}", query);

        self.0
            .execute(&*query, &[])
            .await
            .map_err(|e| format!("creating Kinesis source: {}", e))?;
        Ok(())
    }

    pub async fn create_materialized_view(
        &self,
        source_name: &str,
        view_name: &str,
    ) -> Result<(), String> {
        let query = format!(
            "CREATE MATERIALIZED VIEW {view_name}
             AS SELECT CONVERT_FROM(data, 'utf8') AS DATA FROM {source_name}",
            view_name = view_name,
            source_name = source_name,
        );
        println!("creating materialized view=> {}", query);

        self.0
            .execute(&*query, &[])
            .await
            .map_err(|e| format!("creating materialized view: {}", e))?;
        Ok(())
    }
    
    pub async fn query_view(&self, view_name: &str) -> Result<Vec<Row>, String> {
        let query = format!("SELECT * FROM {view_name};", view_name = view_name);
        //        log::debug!("querying view=> {}", query);
        println!("querying view=> {}", query);

        let mut result = Vec::new();
        loop {
            match self.0.query(&*query, &[]).await {
                Ok(rows) => {
                    result = rows;
                    break;
                }
                Err(e) => {
                    // We will see this error if we query the view between the
                    // time it is created and when it ingests its first data.
                    // This error is transient and should be retried.
                    if e.to_string()
                        .contains("At least one input has no complete timestamps yet.")
                    {
                        println!("{}. Sleeping...", e);
                        thread::sleep(Duration::from_secs(1));
                    } else {
                        return Err(format!("trying to query view: {}", e));
                    }
                }
            }
        }
        Ok(result)
    }
}
