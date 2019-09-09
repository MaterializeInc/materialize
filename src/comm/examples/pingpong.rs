// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use comm::{mpsc, Connection, Switchboard};
use futures::{Future, Sink, Stream};
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().collect();

    let mut opts = getopts::Options::new();
    opts.optopt("n", "", "total number of processes", "NUM");
    opts.optopt("p", "", "id of this process", "NUM");

    let popts = opts.parse(&args[1..])?;
    let n = popts.opt_get_default("n", 3)?;
    let id = popts.opt_get_default("p", 0)?;
    let magic_number = popts.free.get(0).unwrap_or(&"42".into()).parse()?;

    let nodes: Vec<_> = (0..n).map(|i| format!("127.0.0.1:{}", 6876 + i)).collect();
    let addr: SocketAddr = nodes[id].parse()?;
    let listener = TcpListener::bind(&addr)?;
    println!("listening on {}...", listener.local_addr()?);

    let switchboard = Switchboard::new(nodes, id);
    let mut runtime = tokio::runtime::Runtime::new()?;
    {
        let switchboard = switchboard.clone();
        runtime.spawn(
            listener
                .incoming()
                .map_err(|err| panic!("switchboard: accept: {}", err))
                .for_each(move |conn| switchboard.handle_connection(conn))
                .map_err(|err| panic!("switchboard: handle connection: {}", err)),
        );
    }

    runtime.block_on(switchboard.rendezvous(Duration::from_secs(30)))?;

    if id == 0 {
        leader(switchboard, magic_number)
    } else {
        follower(switchboard)
    }
}

fn leader<C>(switchboard: Switchboard<C>, magic_number: usize) -> Result<(), Box<dyn Error>>
where
    C: Connection,
{
    let mut broadcast_tx = switchboard.broadcast_tx::<BroadcastToken>().wait();
    let mut send = |item| {
        broadcast_tx.send(item)?;
        broadcast_tx.flush()
    };

    // Step 1. Send some typed data.
    send(BroadcastMessage::Number(magic_number))?;

    // Step 2. Send along the send half of an MPSC channel.
    let (resp_tx, mut resp_rx) = switchboard.mpsc();
    send(BroadcastMessage::ResponseChannel(resp_tx))?;

    // Step 3. Wait for every peer to respond.
    for _ in 1..switchboard.size() {
        resp_rx = resp_rx
            .into_future()
            .map_err(|(err, _stream)| err)
            .wait()?
            .1;
    }

    // Step 4. Send shutdown signal.
    send(BroadcastMessage::Shutdown)?;

    Ok(())
}

fn follower<C>(switchboard: Switchboard<C>) -> Result<(), Box<dyn Error>>
where
    C: Connection,
{
    let mut rx = switchboard.broadcast_rx::<BroadcastToken>().wait();
    let mut recv = || rx.next().transpose().expect("rx error");

    // Step 1. Read some typed data.
    if let Some(BroadcastMessage::Number(n)) = recv() {
        println!("magic number: {}", n);
    } else {
        panic!("did not receive magic number");
    }

    // Step 2. Receive the receive half of an MPSC channel.
    if let Some(BroadcastMessage::ResponseChannel(tx)) = recv() {
        // Step 3. Send acknowledgement of MPSC channel.
        tx.connect().wait()?.send(()).wait()?;
    } else {
        panic!("did not receive response channel");
    }

    // Step 4. Wait for shutdown signal.
    assert_eq!(recv(), Some(BroadcastMessage::Shutdown));

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
