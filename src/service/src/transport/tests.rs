// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unit tests for CTP internals. Protocol-level tests live in `tests/transport.rs`.

use super::{CONNECTION_CLOSED, Connection, error_channel};

#[mz_ore::test(tokio::test(start_paused = true))]
async fn heartbeat_waits_for_first_tick_skips_missed_ticks_and_stops_with_receiver() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let task = mz_ore::task::spawn(
        || "heartbeat-test",
        Connection::<(), ()>::run_heartbeat_task(tx),
    );
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());

    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert_eq!(rx.recv().await, Some(None));
    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    assert_eq!(rx.recv().await, Some(None));
    assert!(rx.try_recv().is_err());

    drop(rx);
    task.await;
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
