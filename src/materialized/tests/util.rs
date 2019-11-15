// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use postgres::params::{ConnectParams, Host};
use postgres::{Connection, TlsMode};

#[allow(dead_code)] // compilation of integration tests seems weird
pub type TestResult = Result<(), Box<dyn Error>>;

type TResult = Result<(materialized::Server, Connection), Box<dyn Error>>;

pub fn start_server(data_directory: Option<PathBuf>) -> TResult {
    start_server_inner(data_directory, None)
}

// since this is only used in some of the test crates it shows up as a dead code warning in the others
#[allow(dead_code)]
pub fn start_symbiosis_server(data_directory: Option<PathBuf>) -> TResult {
    start_server_inner(
        data_directory,
        std::env::var("MZ_SYMBIOSIS_URL")
            .ok()
            .or_else(|| Some("postgres://localhost:5432".into())),
    )
}

fn start_server_inner(data_directory: Option<PathBuf>, symbiosis_url: Option<String>) -> TResult {
    let server = materialized::serve(materialized::Config {
        logging_granularity: Some(Duration::from_millis(10)),
        threads: 1,
        process: 0,
        addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)],
        bootstrap_sql: "".into(),
        data_directory,
        symbiosis_url,
        gather_metrics: false,
    })?;
    let local_addr = server.local_addr();
    let conn = Connection::connect(
        ConnectParams::builder()
            .port(local_addr.port())
            .user("root", None)
            .build(Host::Tcp(local_addr.ip().to_string())),
        TlsMode::None,
    )?;
    Ok((server, conn))
}
