// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use comm::Switchboard;
use futures::{Future, Sink, Stream};
use std::error::Error;
use tokio::net::UnixStream;

/// Verifies that MPSC channels receive messages from all streams
/// simultaneously. The original implementation had a bug in which streams were
/// exhausted in order, i.e., messages from the second stream were only
/// presented after the first stream was closed.
#[test]
fn test_mpsc_select() -> Result<(), Box<dyn Error>> {
    let (switchboard, _runtime) = Switchboard::local()?;

    let (tx, mut rx) = switchboard.mpsc();
    let mut tx1 = tx.clone().connect::<UnixStream>().wait()?;
    let mut tx2 = tx.connect::<UnixStream>().wait()?;

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
