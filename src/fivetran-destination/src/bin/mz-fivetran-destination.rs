// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fivetran destination for Materialize.

use mz_fivetran_destination::logging::FivetranLoggingFormat;
use mz_fivetran_destination::{DestinationServer, MaterializeDestination};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;

use std::net::{Ipv6Addr, SocketAddr};
use std::process;

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
    // Start our tracing as early as possible so we can capture any issues with startup.
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing_core::Level::INFO)
        .event_format(FivetranLoggingFormat::destination())
        .init();

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

async fn run(Args { port }: Args) -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, port));

    // Fivetran requires us to listen on both IPv4 and IPv6, so we explicitly create a Socket which
    // sets `IPV6_V6ONLY` to false to make sure Linux dual stack kicks in.
    let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    // Disable Nagle's algorithm, maybe not needed but seems decent to start.
    socket.set_nodelay(true)?;
    // OpenBSD disables dual-stack, if we need to support OpenBSD we'll have to explicitly create
    // two sockets.
    //
    // See: <https://man.openbsd.org/cgi-bin/man.cgi/OpenBSD-current/man4/inet6.4#DESCRIPTION>
    socket.set_only_v6(false)?;
    socket.bind(&addr.into())?;
    // Generally OSes default to 128 as the maximum number of backlog connections, so this seems
    // like a good value to set.
    socket.listen(128)?;
    tracing::info!("listening on {addr}...");

    let tcp_listener: std::net::TcpListener = socket.into();
    let tcp_listener = TcpListenerStream::new(TcpListener::from_std(tcp_listener)?);

    let destination = DestinationServer::new(MaterializeDestination)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);
    Server::builder()
        .add_service(destination)
        .serve_with_incoming(tcp_listener)
        .await?;
    Ok(())
}
