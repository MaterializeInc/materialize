// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fivetran destination for Materialize.

use mz_fivetran_destination::{DestinationServer, MaterializeDestination};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use std::net::{Ipv6Addr, SocketAddr};
use std::process;
use tonic::transport::Server;

/// Fivetran destination for Materialize.
#[derive(clap::Parser)]
#[clap(name = "mz-fivetran-destination")]
struct Args {
    /// The port on which to listen.
    #[clap(long, env = "PORT", value_name = "PORT", default_value = "6874")]
    port: u16,
}

#[tokio::main]
async fn main() {
    let args = cli::parse_args(CliConfig {
        env_prefix: Some("MZ_FIVETRAN_DESTINATION_"),
        enable_version_flag: false,
    });
    if let Err(err) = run(args).await {
        eprintln!(
            "mz-fivetran-destination: fatal: {}",
            err.display_with_causes()
        );
        process::exit(1);
    }
}

async fn run(Args { port }: Args) -> Result<(), anyhow::Error> {
    let addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, port));
    println!("listening on {addr}...");
    Server::builder()
        .add_service(DestinationServer::new(MaterializeDestination))
        .serve(addr)
        .await?;
    Ok(())
}
