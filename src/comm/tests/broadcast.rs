// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::thread;

use futures::sink::SinkExt;
use futures::stream::{StreamExt, TryStreamExt};
use futures::TryFutureExt;
use tokio::runtime::Handle;
use uuid::Uuid;

use comm::{broadcast, Switchboard};
use ore::future::channel::mpsc::ReceiverExt;
use ore::future::OreTryStreamExt;

/// Verifies that broadcast tokens can allocate channels dynamically by
/// overriding the `Token::uuid` method.
#[tokio::test]
async fn test_broadcast_dynamic() -> Result<(), Box<dyn Error>> {
    #[derive(Clone, Copy)]
    struct TestToken(Uuid);

    impl broadcast::Token for TestToken {
        type Item = usize;

        fn loopback(&self) -> bool {
            true
        }

        fn uuid(&self) -> Uuid {
            self.0
        }
    }

    let switchboard = Switchboard::local()?;
    let token1 = TestToken(Uuid::new_v4());
    let token2 = TestToken(Uuid::new_v4());

    switchboard.broadcast_tx(token1).send(1).await?;
    switchboard.broadcast_tx(token2).send(2).await?;

    let msg1 = switchboard.broadcast_rx(token1).try_recv().await?;
    let msg2 = switchboard.broadcast_rx(token2).try_recv().await?;
    assert_eq!(msg1, 1);
    assert_eq!(msg2, 2);
    Ok(())
}

/// Verifies that a pathological interleaving of broadcast transmitter and
/// receiver creation does not result in dropped messages. We previously had
/// a bug where the second broadcast transmitter would not be connected to
/// the broadcast receiver.
#[tokio::test]
async fn test_broadcast_interleaving() -> Result<(), Box<dyn Error>> {
    struct TestToken;

    impl broadcast::Token for TestToken {
        type Item = usize;

        fn loopback(&self) -> bool {
            true
        }
    }

    let switchboard = Switchboard::local()?;
    // Create a transmitter and send a message before the receiver is created.
    switchboard.broadcast_tx(TestToken).send(42).await?;

    // Create the receiver.
    let rx = switchboard.broadcast_rx(TestToken);

    // Create a new transmitter and send another message.
    switchboard.broadcast_tx(TestToken).send(42).await?;

    // Verify that the receiver sees both messages.
    assert_eq!(rx.take(2).try_collect::<Vec<_>>().await?, &[42, 42]);

    Ok(())
}

#[tokio::test(threaded_scheduler)]
async fn test_broadcast_fanout() -> Result<(), Box<dyn Error>> {
    struct TestToken;

    impl broadcast::Token for TestToken {
        type Item = usize;

        fn loopback(&self) -> bool {
            true
        }
    }

    async fn test(
        f: impl Fn(futures::channel::mpsc::UnboundedReceiver<usize>) -> usize + Send + Copy + 'static,
    ) -> Result<(), Box<dyn Error>> {
        let switchboard = Switchboard::local()?;
        let mut tx = switchboard.broadcast_tx(TestToken);
        let mut rx = switchboard.broadcast_rx(TestToken).fanout();

        let threads: Vec<_> = (0..3)
            .map(|_| {
                let rx = rx.attach();
                let executor = Handle::current();
                thread::spawn(move || executor.enter(|| f(rx)))
            })
            .collect();

        tokio::spawn(rx.shuttle().map_err(|err| panic!("{}", err)));

        tx.send(42).await?;
        assert_eq!(
            threads
                .into_iter()
                .map(|t| t.join().unwrap())
                .collect::<Vec<_>>(),
            vec![42, 42, 42]
        );

        Ok(())
    }

    test(|mut rx| loop {
        if let Ok(Some(n)) = rx.try_next() {
            break n;
        }
    })
    .await?;

    test(|rx| {
        let mut rx = rx.request_unparks();
        loop {
            thread::park();
            if let Ok(Some(n)) = rx.try_next() {
                break n;
            }
        }
    })
    .await?;

    Ok(())
}

/// Test that non-loopback broadcasting in a cluster size of one is a no-op.
/// This is a bit silly, but it can happen, and the original implementation of
/// broadcast panicked in this case.
#[tokio::test]
async fn test_broadcast_empty() -> Result<(), Box<dyn Error>> {
    struct TestToken;

    impl broadcast::Token for TestToken {
        type Item = usize;

        fn loopback(&self) -> bool {
            false
        }
    }

    let switchboard = Switchboard::local()?;
    let mut tx = switchboard.broadcast_tx(TestToken);
    tx.send(42).await?;
    Ok(())
}
