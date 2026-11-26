// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unit tests for CTP internals. Protocol-level tests live in `tests/transport.rs`.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use super::{CONNECTION_CLOSED, Connection, NoopMetrics, error_channel, read_message};

/// An `AsyncWrite` that records the bytes of every write it receives.
#[derive(Clone, Default)]
struct RecordingWriter(Arc<Mutex<Vec<Vec<u8>>>>);

impl AsyncWrite for RecordingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.lock().expect("lock poisoned").push(buf.to_vec());
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[mz_ore::test(tokio::test)]
async fn send_task_writes_queued_messages_at_once() {
    let msgs = [1_u64, 2, 3];
    let (msg_tx, msg_rx) = mpsc::unbounded_channel();
    for msg in msgs {
        msg_tx.send(msg).expect("receiver alive");
    }
    drop(msg_tx);

    let writer = RecordingWriter::default();
    let (error_tx, _error_rx) = error_channel();
    Connection::<u64, u64>::run_send_task(writer.clone(), msg_rx, error_tx, NoopMetrics).await;

    let writes = writer.0.lock().expect("lock poisoned").clone();
    assert_eq!(writes.len(), 1, "queued messages must share one write");

    let mut reader = &writes[0][..];
    for expected in msgs {
        let msg: u64 = read_message(&mut reader).await.expect("valid frame");
        assert_eq!(msg, expected);
    }
    assert!(reader.is_empty(), "no bytes beyond the queued messages");
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
