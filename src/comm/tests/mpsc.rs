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

use futures::sink::SinkExt;
use futures::stream::{self, StreamExt, TryStreamExt};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

use comm::Switchboard;
use ore::future::{OreStreamExt, OreTryStreamExt};

/// Verifies that MPSC channels receive messages from all streams
/// simultaneously. The original implementation had a bug in which streams were
/// exhausted in order, i.e., messages from the second stream were only
/// presented after the first stream was closed.
#[test]
fn test_mpsc_select() -> Result<(), Box<dyn Error>> {
    let (switchboard, mut runtime) = Switchboard::local()?;
    runtime.block_on(async {
        let (tx, mut rx) = switchboard.mpsc();
        let mut tx1 = tx.clone().connect().await?;
        let mut tx2 = tx.connect().await?;

        tx1.send(1).await?;
        tx2.send(2).await?;
        let msgs = rx.by_ref().take(2).try_collect::<Vec<_>>().await?;
        if msgs != [1, 2] && msgs != [2, 1] {
            panic!("received unexpected messages: {:#?}", msgs);
        }

        tx1.send(3).await?;
        tx2.send(4).await?;
        let msgs = rx.take(2).try_collect::<Vec<_>>().await?;
        if msgs != [3, 4] && msgs != [4, 3] {
            panic!("received unexpected messages: {:#?}", msgs);
        }

        Ok(())
    })
}

/// Verifies that `mpsc_limited` will close the receiver after the expected
/// number of producers have connected and then disconnected.
#[test]
fn test_mpsc_limited_close() -> Result<(), Box<dyn Error>> {
    let (switchboard, mut runtime) = Switchboard::local()?;
    runtime.block_on(async {
        let (tx, rx) = switchboard.mpsc_limited(2);
        let mut tx1 = tx.clone().connect().await?;
        let mut tx2 = tx.clone().connect().await?;

        tx1.send_all(&mut stream::iter(vec![Ok(1), Ok(2)])).await?;
        tx2.send_all(&mut stream::iter(vec![Ok(3), Ok(4), Ok(5)]))
            .await?;
        drop(tx1);
        drop(tx2);

        // If had created an unlimited channel, or specified more than two
        // expected producers, we'd be here forever waiting for the channel to
        // close.
        let mut msgs = rx.try_collect::<Vec<_>>().await?;
        msgs.sort();
        assert_eq!(msgs, &[1, 2, 3, 4, 5]);

        Ok(())
    })
}

/// Verifies that TCP connections are reused by repeatedly connecting the same
/// MPSC transmitter. Without connection reuse, the test should crash with an
/// AddrNotAvailable error because the OS will run out of outgoing TCP ports.
#[test]
fn test_mpsc_connection_reuse() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let mut runtime = Runtime::new()?;
    let executor = runtime.handle().clone();
    runtime.block_on(async {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let mut listener = TcpListener::bind(&addr).await?;

        let switchboard = Switchboard::new(vec![listener.local_addr()?], 0, executor.clone());
        executor.spawn({
            let switchboard = switchboard.clone();
            async move {
                let mut incoming = listener.incoming();
                while let Some(conn) = incoming.next().await {
                    let conn = conn.expect("test switchboard: accept failed");
                    switchboard
                        .handle_connection(conn)
                        .await
                        .expect("test switchboard: handle connection failed");
                }
            }
        });

        let (tx, rx) = switchboard.mpsc();
        let (result_tx, result_rx) = futures::channel::oneshot::channel();

        // This is the empirically-determined number that exhausts ports on macOS
        // without connection reuse.
        const N: usize = 1 << 14;

        executor.spawn(async {
            rx.take(N).drain().await;
            result_tx.send(()).unwrap();
        });

        for i in 0..N {
            let mut tx = tx.connect().await?;
            tx.send(i).await?;
        }

        result_rx.await?;
        Ok(())
    })
}

/// Test that we can send a largeish frame (64MiB) over the channel. The true
/// limit is higher, but we don't test that here to minimize the runtime of this
/// test. The goal is just to make sure we're not using Tokio's default limit of
/// 8MiB, which is too low.
#[test]
fn test_mpsc_big_frame() -> Result<(), Box<dyn Error>> {
    const BIG_LEN: usize = 64 * (1 << 20); // 64MiB
    let (switchboard, mut runtime) = Switchboard::local()?;
    runtime.block_on(async {
        let (tx, mut rx) = switchboard.mpsc::<String>();

        tokio::spawn(async move {
            let msg = rx.try_recv().await.expect("recv failed");
            assert_eq!(msg.len(), BIG_LEN);
        });

        let msg = " ".repeat(BIG_LEN);
        let mut tx = tx.clone().connect().await?;
        tx.send(msg).await?;

        Ok(())
    })
}
