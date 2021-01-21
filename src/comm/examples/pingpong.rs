// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::net::Ipv4Addr;
use std::time::Duration;

use futures::sink::SinkExt;
use futures::stream::TryStreamExt;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::net::TcpListener;

use comm::{mpsc, Connection, Switchboard};
use ore::future::OreTryStreamExt;

/// Example of using comm channels.
#[derive(StructOpt)]
struct Args {
    /// Total number of processes.
    #[structopt(short = "n", default_value = "3")]
    processes: u16,
    /// ID of this process.
    #[structopt(short = "p", default_value = "0")]
    process: usize,
    #[structopt(default_value = "42")]
    magic_number: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Args = ore::cli::parse_args();

    let runtime = tokio::runtime::Runtime::new()?;
    let nodes: Vec<_> = (0..args.processes)
        .map(|i| (Ipv4Addr::LOCALHOST, 6876 + i))
        .collect();
    let listener = runtime.block_on(TcpListener::bind(&nodes[args.process]))?;
    println!("listening on {}...", listener.local_addr()?);

    let switchboard = Switchboard::new(nodes, args.process);
    runtime.spawn({
        let switchboard = switchboard.clone();
        async move {
            loop {
                let (conn, _addr) = listener.accept().await.expect("accept failed");
                switchboard
                    .handle_connection(conn)
                    .await
                    .expect("handle connection failed");
            }
        }
    });

    runtime.block_on(async {
        switchboard.rendezvous(Duration::from_secs(30)).await?;

        if args.process == 0 {
            leader(switchboard, args.magic_number).await
        } else {
            follower(switchboard).await
        }
    })
}

async fn leader<C>(switchboard: Switchboard<C>, magic_number: usize) -> Result<(), Box<dyn Error>>
where
    C: Connection,
{
    let mut broadcast_tx = switchboard.broadcast_tx(BroadcastToken);

    // Step 1. Send some typed data.
    broadcast_tx
        .send(BroadcastMessage::Number(magic_number))
        .await?;

    // Step 2. Send along the send half of an MPSC channel.
    let (resp_tx, mut resp_rx) = switchboard.mpsc();
    broadcast_tx
        .send(BroadcastMessage::ResponseChannel(resp_tx))
        .await?;

    // Step 3. Wait for every peer to respond.
    for _ in 1..switchboard.size() {
        resp_rx.try_recv().await?;
    }

    // Step 4. Send shutdown signal.
    broadcast_tx.send(BroadcastMessage::Shutdown).await?;

    Ok(())
}

async fn follower<C>(switchboard: Switchboard<C>) -> Result<(), Box<dyn Error>>
where
    C: Connection,
{
    let mut rx = switchboard.broadcast_rx(BroadcastToken);

    // Step 1. Read some typed data.
    if let Some(BroadcastMessage::Number(n)) = rx.try_next().await? {
        println!("magic number: {}", n);
    } else {
        panic!("did not receive magic number");
    }

    // Step 2. Receive the receive half of an MPSC channel.
    if let Some(BroadcastMessage::ResponseChannel(tx)) = rx.try_next().await? {
        // Step 3. Send acknowledgement of MPSC channel.
        tx.connect().await?.send(()).await?;
    } else {
        panic!("did not receive response channel");
    }

    // Step 4. Wait for shutdown signal.
    assert_eq!(rx.try_next().await?, Some(BroadcastMessage::Shutdown));

    Ok(())
}

struct BroadcastToken;

impl comm::broadcast::Token for BroadcastToken {
    type Item = BroadcastMessage;
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
enum BroadcastMessage {
    Number(usize),
    ResponseChannel(mpsc::Sender<()>),
    Shutdown,
}
