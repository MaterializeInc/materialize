// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[derive(clap::Parser)]
struct Args {
    /// The address on which to listen for a connection from the coordinator.
    #[clap(
        long,
        env = "COORD_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6876"
    )]
    listen_addr_c: String,
    /// The address on which to listen for a connection from the coordinator.
    #[clap(
        long,
        env = "REPLICA_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6877"
    )]
    listen_addr_d: String,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run(mz_ore::cli::parse_args()).await {
        eprintln!("active_demo: {:#}", err);
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    use tokio::net::TcpListener;

    let listener_c = TcpListener::bind(args.listen_addr_c).await?;
    let listener_d = TcpListener::bind(args.listen_addr_d).await?;

    tracing::info!(
        "listening for coordinator connection on {}...",
        listener_c.local_addr()?
    );

    tracing::info!(
        "listening for replica connections on {}...",
        listener_d.local_addr()?
    );

    let (conn_c, addr) = listener_c.accept().await?;
    let mut coordinator = mz_dataflow_types::client::tcp::framed_server(conn_c);
    tracing::info!("coordinator connection accepted from: {:?}", addr);

    let mut active_replicas =
        mz_dataflow_types::client::replicated::ActiveReplication::new(Vec::new());

    loop {
        use futures::sink::SinkExt;
        use futures::stream::TryStreamExt;
        use mz_dataflow_types::client::Client;

        tokio::select! {
            // Accept a new dataflow replica.
            connection = listener_d.accept() => {
                if let Ok((conn_d, addr)) = connection {
                    tracing::info!("replica connection accepted from: {:?}", addr);
                    let client = mz_dataflow_types::client::tcp::TcpClient::new(conn_d);
                    active_replicas.add_replica(client).await;
                }
            }
            // Receive a new command to broadcast.
            cmd = coordinator.try_next() => match cmd? {
                None => return Ok(()),
                Some(cmd) => active_replicas.send(cmd).await?,
            },
            // Receive a new response to return.
            Some(response) = active_replicas.recv() => coordinator.send(response).await?,
        }
    }
}
