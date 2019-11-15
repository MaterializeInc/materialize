// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use postgres::params::{ConnectParams, Host};
use postgres::{Connection, TlsMode};

pub fn start_server(
    data_directory: Option<PathBuf>,
) -> Result<(materialized::Server, Connection), Box<dyn Error>> {
    let server = materialized::serve(materialized::Config {
        logging_granularity: None,
        threads: 1,
        process: 0,
        addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)],
        bootstrap_sql: "".into(),
        data_directory,
        symbiosis_url: None,
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
