// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use postgres::{Client, Config as PgConfig, NoTls};

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

pub fn start_server(config: Config) -> Result<(materialized::Server, Client), Box<dyn Error>> {
    let server = materialized::serve(materialized::Config {
        logging_granularity: Some(Duration::from_millis(10)),
        threads: 1,
        process: 0,
        addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)],
        bootstrap_sql: config.bootstrap_sql,
        data_directory: config.data_directory,
        symbiosis_url: None,
        gather_metrics: false,
    })?;
    let local_addr = server.local_addr();
    let client = PgConfig::new()
        .host(&local_addr.ip().to_string())
        .port(local_addr.port())
        .user("root")
        .connect(NoTls)?;
    Ok((server, client))
}
