// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unit tests for CTP internals. Protocol-level tests live in `tests/transport.rs`.

use std::io::Cursor;

use prometheus::IntCounter;
use tokio::sync::mpsc;

use super::{CONNECTION_CLOSED, Connection, Metrics, error_channel, metrics};

#[mz_ore::test(tokio::test)]
async fn send_error_counts_only_complete_frames() {
    #[derive(Clone)]
    struct Counts([IntCounter; 2]);

    impl Metrics<u8, ()> for Counts {
        fn bytes_sent(&mut self, _: usize) {}
        fn bytes_received(&mut self, _: usize) {}
        fn message_received(&mut self, _: &()) {}
        fn sent_message_counter(&self, msg: &u8) -> Option<&IntCounter> {
            Some(&self.0[usize::from(*msg)])
        }
    }

    let counts = Counts(["first", "second"].map(|name| IntCounter::new(name, name).unwrap()));
    // Each frame has an eight-byte prefix and a one-byte payload.
    let mut bytes = [0; 13];
    let writer = metrics::Writer::new(Cursor::new(bytes.as_mut_slice()), counts.clone());
    let (tx, rx) = mpsc::unbounded_channel();
    tx.send(0).unwrap();
    tx.send(1).unwrap();
    tx.send(1).unwrap();
    drop(tx);

    let (error_tx, mut error_rx) = error_channel();
    Connection::<u8, ()>::run_send_task(writer, rx, error_tx).await;
    assert!(error_rx.collect().await.contains("send error"));
    assert_eq!(counts.0[0].get(), 1);
    assert_eq!(counts.0[1].get(), 0);
}

#[mz_ore::test(tokio::test)]
async fn error_channel_keeps_first_error() {
    let (tx, mut rx) = error_channel();

    tx.report("first".into());
    tx.report("second".into());

    assert_eq!(rx.collect().await, "first");
    // Repeated collection must not observe the discarded error either.
    assert_eq!(rx.collect().await, "first");
}

#[mz_ore::test(tokio::test)]
async fn error_channel_keeps_first_error_reported_late() {
    let (tx, mut rx) = error_channel();

    tx.report("first".into());
    assert_eq!(rx.collect().await, "first");

    // An error reported after the first one was collected is still discarded.
    tx.report("second".into());
    assert_eq!(rx.collect().await, "first");
}

#[mz_ore::test(tokio::test)]
async fn error_channel_reports_closure_without_error() {
    let (tx, mut rx) = error_channel();

    drop(tx);

    assert_eq!(rx.collect().await, CONNECTION_CLOSED);
}

#[mz_ore::test(tokio::test)]
async fn error_channel_waits_for_error() {
    let (tx, mut rx) = error_channel();

    let tx2 = tx.clone();
    let handle = mz_ore::task::spawn(|| "report", async move {
        tx2.report("first".into());
    });

    assert_eq!(rx.collect().await, "first");
    handle.await;
}
