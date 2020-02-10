// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct Config {
    data_directory: Option<PathBuf>,
    logging_granularity: Option<Duration>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            data_directory: None,
            logging_granularity: Some(Duration::from_millis(10)),
        }
    }
}

impl Config {
    pub fn logging_granularity(mut self, granularity: Option<Duration>) -> Self {
        self.logging_granularity = granularity;
        self
    }

    pub fn data_directory(mut self, data_directory: impl Into<PathBuf>) -> Self {
        self.data_directory = Some(data_directory.into());
        self
    }
}

pub fn start_server(config: Config) -> Result<(Server, postgres::Client), Box<dyn Error>> {
    let server = Server(materialized::serve(materialized::Config {
        logging_granularity: config.logging_granularity,
        timestamp_frequency: None,
        max_increment_ts_size: 1000,
        threads: 1,
        process: 0,
        addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)],
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
    pub fn pg_config(&self) -> postgres::Config {
        let local_addr = self.0.local_addr();
        let mut config = postgres::Config::new();
        config
            .host(&local_addr.ip().to_string())
            .port(local_addr.port())
            .user("root");
        config
    }

    pub fn pg_config_async(&self) -> tokio_postgres::Config {
        let local_addr = self.0.local_addr();
        let mut config = tokio_postgres::Config::new();
        config
            .host(&local_addr.ip().to_string())
            .port(local_addr.port())
            .user("root");
        config
    }

    pub fn connect(&self) -> Result<postgres::Client, Box<dyn Error>> {
        Ok(self.pg_config().connect(postgres::NoTls)?)
    }

    pub async fn connect_async(&self) -> Result<tokio_postgres::Client, Box<dyn Error>> {
        let (client, conn) = self
            .pg_config_async()
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
