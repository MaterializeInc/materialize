// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use comm::{broadcast, Switchboard};
use futures::{Future, Sink};
use ore::future::sync::mpsc::ReceiverExt;
use std::error::Error;
use std::thread;
use tokio::runtime::TaskExecutor;

#[test]
fn test_broadcast_fanout() -> Result<(), Box<dyn Error>> {
    struct TestToken;

    impl broadcast::Token for TestToken {
        type Item = usize;

        fn loopback() -> bool {
            true
        }
    }

    fn test(
        f: impl Fn(TaskExecutor, futures::sync::mpsc::UnboundedReceiver<usize>) -> usize
            + Send
            + Copy
            + 'static,
    ) -> Result<(), Box<dyn Error>> {
        let (switchboard, mut runtime) = Switchboard::local()?;

        let tx = switchboard.broadcast_tx::<TestToken>();
        let mut rx = switchboard.broadcast_rx::<TestToken>().fanout();

        let threads: Vec<_> = (0..3)
            .map(|_| {
                let rx = rx.attach();
                let executor = runtime.executor();
                thread::spawn(move || f(executor, rx))
            })
            .collect();

        runtime.spawn(rx.shuttle().map_err(|err| panic!("{}", err)));

        tx.send(42).wait()?;
        assert_eq!(
            threads
                .into_iter()
                .map(|t| t.join().unwrap())
                .collect::<Vec<_>>(),
            vec![42, 42, 42]
        );

        Ok(())
    }

    test(|_executor, mut rx| loop {
        if let Ok(Some(n)) = rx.try_next() {
            break n;
        }
    })?;

    test(|executor, rx| {
        let mut rx = rx.request_unparks(executor).unwrap();
        loop {
            thread::park();
            if let Ok(Some(n)) = rx.try_next() {
                break n;
            }
        }
    })?;

    Ok(())
}

/// Test that non-loopback broadcasting in a cluster size of one is a no-op.
/// This is a bit silly, but it can happen, and the original implementation of
/// broadcast panicked in this case.
#[test]
fn test_broadcast_empty() -> Result<(), Box<dyn Error>> {
    struct TestToken;

    impl broadcast::Token for TestToken {
        type Item = usize;

        fn loopback() -> bool {
            false
        }
    }

    let (switchboard, _runtime) = Switchboard::local()?;
    let tx = switchboard.broadcast_tx::<TestToken>();
    tx.send(42).wait()?;

    Ok(())
}
