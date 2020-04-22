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

use tokio_postgres::{Client, NoTls};

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

    #[allow(clippy::too_many_arguments)]
    pub async fn query_materialize_for_kinesis_records(
        &self,
        aws_region: &str,
        aws_account: &str,
        stream_name: &str,
        access_key: &str,
        secret_access_key: &str,
        token: &Option<String>,
        num_records: i64,
    ) -> Result<(), String> {
        let timer = std::time::Instant::now();
        let source_name = String::from("foo");
        let view_name = format!("{}_view", source_name);
        self.create_kinesis_source(
            &source_name,
            aws_region,
            aws_account,
            stream_name,
            access_key,
            secret_access_key,
            token,
        )
        .await
        .map_err(|e| format!("creating kinesis source: {}", e))?;

        self.create_materialized_view(&source_name, &view_name)
            .await
            .map_err(|e| format!("creating materialized view: {}", e))?;
        self.query_view(&view_name, num_records)
            .await
            .map_err(|e| format!("querying view: {}", e))?;
        println!(
            "Read all {} records in Materialize from Kinesis source in {} milliseconds",
            num_records,
            timer.elapsed().as_millis()
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_kinesis_source(
        &self,
        source_name: &str,
        aws_region: &str,
        aws_account: &str,
        stream_name: &str,
        access_key: &str,
        secret_access_key: &str,
        token: &Option<String>,
    ) -> Result<(), String> {
        let query = format!(
            "CREATE SOURCE {source_name}
                 FROM KINESIS ARN 'arn:aws:kinesis:{aws_region}:{aws_account}:stream/{stream_name}'
                 WITH (access_key = '{access_key}',
                       secret_access_key = '{secret_access_key}',
                       token = '{token}')
                 FORMAT BYTES;",
            source_name = source_name,
            aws_region = aws_region,
            aws_account = aws_account,
            stream_name = stream_name,
            access_key = access_key,
            secret_access_key = secret_access_key,
            token = token.as_ref().unwrap_or(&String::new())
        );
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

    pub async fn query_view(&self, view_name: &str, num_records: i64) -> Result<(), String> {
        let query = format!(
            "SELECT COUNT(data) as count FROM {view_name};",
            view_name = view_name
        );
        println!("querying view=> {}", query);

        loop {
            match self.0.query(&*query, &[]).await {
                Ok(rows) => {
                    assert!(rows.len() == 1);
                    match rows.get(0) {
                        Some(row) => {
                            let count: i64 = row.get("count");
                            if count == num_records {
                                break;
                            }
                        }
                        None => return Err(String::from("Expected count, got None")),
                    }
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
                        return Err(format!("hit error trying to query view: {}", e.to_string()));
                    }
                }
            }
        }
        Ok(())
    }
}
