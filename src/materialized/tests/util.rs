// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub type TestResult = Result<(), Box<dyn Error>>;

#[derive(Default, Clone)]
pub struct Config {
    data_directory: Option<PathBuf>,
    bootstrap_sql: String,
}

impl Config {
    pub fn data_directory(mut self, data_directory: impl Into<PathBuf>) -> Self {
        self.data_directory = Some(data_directory.into());
        self
    }

    pub fn bootstrap_sql(mut self, bootstrap_sql: impl Into<String>) -> Self {
        self.bootstrap_sql = bootstrap_sql.into();
        self
    }
}

pub fn start_server(config: Config) -> Result<(Server, postgres::Client), Box<dyn Error>> {
    let server = Server(materialized::serve(materialized::Config {
        logging_granularity: Some(Duration::from_millis(10)),
        timestamp_frequency: Some(Duration::from_millis(50)),
        max_increment_ts_size: 1000,
        ts_source: None,
        threads: 1,
        process: 0,
        addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)],
        bootstrap_sql: config.bootstrap_sql,
        data_directory: config.data_directory,
        symbiosis_url: None,
        gather_metrics: false,
        start_time: Instant::now(),
    })?);
    let client = server.connect()?;
    Ok((server, client))
}

pub struct Server(materialized::Server);

impl Server {
    pub fn connect(&self) -> Result<postgres::Client, Box<dyn Error>> {
        let local_addr = self.0.local_addr();
        Ok(postgres::Config::new()
            .host(&local_addr.ip().to_string())
            .port(local_addr.port())
            .user("root")
            .connect(postgres::NoTls)?)
    }

    pub async fn connect_async(&self) -> Result<tokio_postgres::Client, Box<dyn Error>> {
        let local_addr = self.0.local_addr();
        let (client, conn) = tokio_postgres::Config::new()
            .host(&local_addr.ip().to_string())
            .port(local_addr.port())
            .user("root")
            .connect(tokio_postgres::NoTls)
            .await?;
        tokio::spawn(async move {
            if let Err(err) = conn.await {
                panic!("connection error: {}", err);
            }
        });
        Ok(client)
    }
}
