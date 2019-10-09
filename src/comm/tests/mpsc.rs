// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use comm::Switchboard;
use futures::{Future, Sink, Stream};
use ore::future::StreamExt;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

/// Verifies that MPSC channels receive messages from all streams
/// simultaneously. The original implementation had a bug in which streams were
/// exhausted in order, i.e., messages from the second stream were only
/// presented after the first stream was closed.
#[test]
fn test_mpsc_select() -> Result<(), Box<dyn Error>> {
    let (switchboard, _runtime) = Switchboard::local()?;

    let (tx, mut rx) = switchboard.mpsc();
    let mut tx1 = tx.clone().connect().wait()?;
    let mut tx2 = tx.connect().wait()?;

    tx1 = tx1.send(1).wait()?;
    tx2 = tx2.send(2).wait()?;
    let msgs = rx.by_ref().take(2).collect().wait()?;
    if msgs != &[1, 2] && msgs != [2, 1] {
        panic!("received unexpected messages: {:#?}", msgs);
    }

    tx1.send(3).wait()?;
    tx2.send(4).wait()?;
    let msgs = rx.by_ref().take(2).collect().wait()?;
    if msgs != &[3, 4] && msgs != [4, 3] {
        panic!("received unexpected messages: {:#?}", msgs);
    }

    Ok(())
}

/// Verifies that TCP connections are reused by repeatedly connecting the same
/// MPSC transmitter. Without connection reuse, the test should crash with an
/// AddrNotAvailable error because the OS will run out of outgoing TCP ports.
#[test]
fn test_mpsc_connection_reuse() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let listener = TcpListener::bind(&addr)?;
    let mut runtime = Runtime::new()?;
    let switchboard = Switchboard::new(vec![listener.local_addr()?], 0, runtime.executor());
    runtime.spawn({
        let switchboard = switchboard.clone();
        listener
            .incoming()
            .map_err(|err| panic!("switchboard: accept: {}", err))
            .for_each(move |conn| switchboard.handle_connection(conn))
            .map_err(|err| panic!("switchboard: handle connection: {}", err))
    });

    let (tx, rx) = switchboard.mpsc();
    let (result_tx, result_rx) = futures::sync::oneshot::channel();

    // This is the empirically-determined number that exhausts ports on macOS
    // without connection reuse.
    const N: u64 = 1 << 14;

    runtime.spawn(rx.take(N).drain().then(|res| {
        result_tx.send(res).unwrap();
        Ok(())
    }));

    for i in 0..N {
        let tx = tx.connect().wait()?;
        tx.send(i).wait()?;
    }

    result_rx.wait()??;
    Ok(())
}

/// Test that we can send a largeish frame (64MiB) over the channel. The true
/// limit is higher, but we don't test that here to minimize the runtime of this
/// test. The goal is just to make sure we're not using Tokio's default limit of
/// 8MiB, which is too low.
#[test]
fn test_mpsc_big_frame() -> Result<(), Box<dyn Error>> {
    const BIG_LEN: usize = 64 * 1 << 20; // 64MiB
    let (switchboard, mut runtime) = Switchboard::local()?;
    let (tx, rx) = switchboard.mpsc::<String>();

    runtime.spawn(
        rx.recv()
            .map(|(msg, _stream)| assert_eq!(msg.len(), BIG_LEN))
            .map_err(|err| panic!("{}", err)),
    );

    let msg = " ".repeat(BIG_LEN);
    let tx = tx.clone().connect().wait()?;
    tx.send(msg).wait()?;

    Ok(())
}
