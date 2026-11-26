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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc;

use super::{CONNECTION_CLOSED, Connection, NoopMetrics, error_channel, write_message};

/// An `AsyncRead` that hands out as much of its bytes as a read asks for, then reports the end
/// of the stream. Counts the reads that returned bytes.
struct CountingReader {
    bytes: Vec<u8>,
    reads: Arc<AtomicUsize>,
}

impl AsyncRead for CountingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let len = buf.remaining().min(self.bytes.len());
        if len > 0 {
            buf.put_slice(&self.bytes[..len]);
            self.bytes.drain(..len);
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        Poll::Ready(Ok(()))
    }
}

#[mz_ore::test(tokio::test)]
async fn recv_task_reads_queued_messages_at_once() {
    let msgs = [1_u64, 2, 3];
    let mut bytes = Vec::new();
    for msg in &msgs {
        write_message(&mut bytes, Some(msg))
            .await
            .expect("write to a `Vec`");
    }

    let reads = Arc::new(AtomicUsize::new(0));
    let reader = CountingReader {
        bytes,
        reads: Arc::clone(&reads),
    };
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
    let (error_tx, _error_rx) = error_channel();
    // Returns when it reaches the end of the stream.
    Connection::<u64, u64>::run_recv_task(reader, msg_tx, error_tx, NoopMetrics).await;

    for expected in msgs {
        assert_eq!(msg_rx.recv().await, Some(expected));
    }
    assert_eq!(
        reads.load(Ordering::SeqCst),
        1,
        "queued messages must share one read"
    );
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
