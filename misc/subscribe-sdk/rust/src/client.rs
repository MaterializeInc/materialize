// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The pgwire transport: connect to Materialize and drive a subscription.
//!
//! The transport is deliberately thin. It runs the `DECLARE`/`FETCH` loop and
//! reads columns in their text encoding via the simple-query protocol, then
//! hands every row to the tested [`Decoder`]. All protocol judgement lives in
//! the decoder and the consistency engine, not here.
//!
//! A background task drains the cursor continuously into a *bounded* buffer,
//! rather than fetching only when the consumer asks. This is deliberate:
//! Materialize buffers a subscription's unread output in `environmentd` without
//! bound, so a consumer that stops fetching pushes an unbounded cost onto the
//! server. Draining continuously keeps that buffer on the client, where it is
//! bounded and fails loud ([`SubscribeError::BufferOverflow`]) if the consumer
//! cannot keep up. The client falls over, never the server.
//!
//! TLS is not yet wired, so this connects over plaintext (suitable for the
//! emulator or a `sslmode=disable` local server). A TLS connector is the next
//! step for this module.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use crate::batch::{Batcher, ConsistentBatch};
use crate::envelope::{Datum, Decoder, Envelope, StreamMessage};
use crate::error::SubscribeError;
use crate::statement::Subscribe;
use crate::token::ResumeToken;

/// The cursor name used for the subscription. One subscription owns its
/// connection, so a fixed name is safe.
const CURSOR: &str = "mz_subscribe_cursor";

/// Default client-side buffer capacity, in decoded messages. A slow consumer
/// that falls this far behind trips [`SubscribeError::BufferOverflow`].
///
/// TODO: make this configurable per subscription once we have real workloads to
/// size it against.
pub(crate) const DEFAULT_BUFFER_CAPACITY: usize = 1 << 16;

/// A connection to Materialize for consuming subscriptions.
///
/// One connection backs one subscription at a time. Transaction-mode connection
/// poolers (such as PgBouncer) break the `DECLARE`/`FETCH` cursor and are not
/// supported.
#[derive(Debug)]
pub struct SubscribeClient {
    client: Arc<tokio_postgres::Client>,
    buffer_capacity: usize,
}

impl SubscribeClient {
    /// Connects to Materialize using a libpq-style connection string.
    ///
    /// Connects over plaintext for now (see the module docs). The background
    /// connection task is spawned onto the current Tokio runtime.
    pub async fn connect(conninfo: &str) -> Result<Self, SubscribeError> {
        let (client, connection) = tokio_postgres::connect(conninfo, NoTls)
            .await
            .map_err(classify_error)?;
        // The connection must be driven for the client to make progress.
        tokio::spawn(async move {
            // A connection error here surfaces on the next client call as a
            // closed connection, which `classify_error` maps to `Transient`.
            let _ = connection.await;
        });
        Ok(SubscribeClient {
            client: Arc::new(client),
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
        })
    }

    /// Overrides the client-side buffer capacity, in decoded messages. Larger
    /// buffers tolerate burstier consumers at the cost of memory.
    pub fn buffer_capacity(mut self, messages: usize) -> Self {
        self.buffer_capacity = messages.max(1);
        self
    }

    /// Starts a new subscription, taking the initial snapshot.
    pub async fn subscribe(&self, subscribe: Subscribe) -> Result<BatchStream, SubscribeError> {
        let batcher = Batcher::new(
            subscribe.fingerprint(),
            subscribe.envelope().clone(),
            /* with_snapshot */ true,
        );
        let raw = self
            .open_raw(&subscribe, subscribe.to_sql_initial())
            .await?;
        Ok(BatchStream { raw, batcher })
    }

    /// Resumes a subscription from `token`, skipping the snapshot and starting
    /// exactly where the token left off.
    ///
    /// Fails with [`SubscribeError::SchemaMismatch`] if `subscribe` does not
    /// match the query the token was taken against, since resuming would
    /// otherwise mix incompatible results.
    pub async fn resume(
        &self,
        subscribe: Subscribe,
        token: &ResumeToken,
    ) -> Result<BatchStream, SubscribeError> {
        check_fingerprint(&subscribe, token)?;
        let batcher = Batcher::new(
            subscribe.fingerprint(),
            subscribe.envelope().clone(),
            /* with_snapshot */ false,
        );
        let raw = self
            .open_raw(&subscribe, subscribe.to_sql_resume(token))
            .await?;
        Ok(BatchStream { raw, batcher })
    }

    /// Starts a subscription and hands back the *raw* decoded stream: the
    /// timestamped changes and progress markers, before any batching.
    ///
    /// This is the composable substrate. Most callers want [`subscribe`], which
    /// layers consistent batching on top. Reach for the raw stream to build a
    /// different consistency policy, or to feed a [`crate::Cohort`]-style
    /// multi-view engine of your own.
    ///
    /// [`subscribe`]: SubscribeClient::subscribe
    pub async fn subscribe_raw(&self, subscribe: Subscribe) -> Result<RawStream, SubscribeError> {
        self.open_raw(&subscribe, subscribe.to_sql_initial()).await
    }

    /// Resumes a raw stream from `token`. See [`subscribe_raw`] and [`resume`].
    ///
    /// [`subscribe_raw`]: SubscribeClient::subscribe_raw
    /// [`resume`]: SubscribeClient::resume
    pub async fn resume_raw(
        &self,
        subscribe: Subscribe,
        token: &ResumeToken,
    ) -> Result<RawStream, SubscribeError> {
        check_fingerprint(&subscribe, token)?;
        self.open_raw(&subscribe, subscribe.to_sql_resume(token))
            .await
    }

    async fn open_raw(
        &self,
        subscribe: &Subscribe,
        subscribe_sql: String,
    ) -> Result<RawStream, SubscribeError> {
        start_cursor(&self.client, &subscribe_sql).await?;
        let (tx, rx) = mpsc::channel(self.buffer_capacity);
        let status = Arc::new(Mutex::new(None));
        let cancel = Arc::new(AtomicBool::new(false));
        tokio::spawn(run_drain(
            Arc::clone(&self.client),
            subscribe.envelope().clone(),
            subscribe.is_bounded(),
            tx,
            |message| message,
            cancel,
            Arc::clone(&status),
        ));
        Ok(RawStream { rx, status })
    }
}

/// Rejects a resume whose query shape no longer matches the checkpoint.
fn check_fingerprint(subscribe: &Subscribe, token: &ResumeToken) -> Result<(), SubscribeError> {
    let fingerprint = subscribe.fingerprint();
    if fingerprint != token.fingerprint() {
        return Err(SubscribeError::SchemaMismatch {
            expected: token.fingerprint().to_string(),
            actual: fingerprint,
        });
    }
    Ok(())
}

/// The raw decoded stream for one subscription: [`StreamMessage`]s in arrival
/// order, before any batching or consistency policy is applied.
///
/// This is layer one, the composable substrate the batcher and cohort are built
/// on. Pull messages with [`RawStream::next`].
#[derive(Debug)]
pub struct RawStream {
    rx: mpsc::Receiver<StreamMessage>,
    status: Arc<Mutex<Option<SubscribeError>>>,
}

impl RawStream {
    /// Returns the next decoded message, or `None` when the stream ends (only a
    /// bounded subscription ends on its own). Surfaces the terminal error if the
    /// drain failed.
    pub async fn next(&mut self) -> Result<Option<StreamMessage>, SubscribeError> {
        match self.rx.recv().await {
            Some(message) => Ok(Some(message)),
            None => take_terminal_status(&self.status),
        }
    }
}

/// A stream of [`ConsistentBatch`]es for one subscription.
///
/// Pull batches with [`BatchStream::next`]. Each returned batch is complete: a
/// consumer that applies it and persists its `resume_token` atomically achieves
/// exactly-once state. This is the raw stream plus the [`Batcher`] consistency
/// engine.
#[derive(Debug)]
pub struct BatchStream {
    raw: RawStream,
    batcher: Batcher,
}

impl BatchStream {
    /// Returns the next consistent batch, or `None` when a bounded subscription
    /// (one with `UP TO`) has terminated.
    ///
    /// Consumes raw messages until a timestamp closes. Idle periods still yield
    /// empty batches as the frontier advances.
    pub async fn next(&mut self) -> Result<Option<ConsistentBatch>, SubscribeError> {
        loop {
            match self.raw.next().await? {
                Some(message) => {
                    if let Some(batch) = self.batcher.push(message)? {
                        return Ok(Some(batch));
                    }
                }
                None => return Ok(None),
            }
        }
    }
}

/// Reads and clears the terminal status of a finished drain: an error if it
/// failed, or a clean end otherwise.
fn take_terminal_status(
    status: &Mutex<Option<SubscribeError>>,
) -> Result<Option<StreamMessage>, SubscribeError> {
    match status.lock().expect("status mutex poisoned").take() {
        Some(error) => Err(error),
        None => Ok(None),
    }
}

/// The `FETCH` statement, with a short timeout so an idle unbounded subscription
/// still returns periodically to check for cancellation and emit progress.
pub(crate) fn fetch_sql() -> String {
    format!("FETCH ALL {CURSOR} WITH (timeout = '1s')")
}

/// Connects a fresh connection and opens the subscription cursor on it. Used for
/// cohort members, which each own a connection.
pub(crate) async fn connect_cursor(
    conninfo: &str,
    subscribe_sql: &str,
) -> Result<Arc<tokio_postgres::Client>, SubscribeError> {
    let (client, connection) = tokio_postgres::connect(conninfo, NoTls)
        .await
        .map_err(classify_error)?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let client = Arc::new(client);
    start_cursor(&client, subscribe_sql).await?;
    Ok(client)
}

/// Opens the subscription cursor inside a transaction on an existing connection.
///
/// A cursor must live inside a transaction, which also holds the read so the
/// frontier does not advance out from under a slow reader mid-batch. If
/// `DECLARE` fails after `BEGIN`, this rolls back so the connection is not left
/// in an open transaction.
async fn start_cursor(
    client: &tokio_postgres::Client,
    subscribe_sql: &str,
) -> Result<(), SubscribeError> {
    client.simple_query("BEGIN").await.map_err(classify_error)?;
    if let Err(error) = client
        .simple_query(&format!("DECLARE {CURSOR} CURSOR FOR {subscribe_sql}"))
        .await
    {
        let _ = client.simple_query("ROLLBACK").await;
        return Err(classify_error(error));
    }
    Ok(())
}

/// The background drain: fetch, decode, and forward messages into `tx` until the
/// subscription ends, the consumer leaves, or a sibling drain fails.
///
/// On any terminal error it records the first error in `status` and sets
/// `cancel` so sibling drains (in a cohort) stop too. It always tries to
/// `ROLLBACK` on the way out, releasing the cursor so the server stops buffering
/// output no one will read.
pub(crate) async fn run_drain<T, W>(
    client: Arc<tokio_postgres::Client>,
    envelope: Envelope,
    bounded: bool,
    tx: mpsc::Sender<T>,
    wrap: W,
    cancel: Arc<AtomicBool>,
    status: Arc<Mutex<Option<SubscribeError>>>,
) where
    T: Send + 'static,
    W: Fn(StreamMessage) -> T + Send + 'static,
{
    if let Err(error) = drain_loop(&client, &envelope, bounded, &tx, &wrap, &cancel).await {
        let mut slot = status.lock().expect("status mutex poisoned");
        if slot.is_none() {
            *slot = Some(error);
        }
        cancel.store(true, Ordering::SeqCst);
    }
    // Best-effort cursor release. If the connection is already broken this is a
    // no-op; if it is healthy it stops the server buffering for a dead reader.
    let _ = client.simple_query("ROLLBACK").await;
}

async fn drain_loop<T, W>(
    client: &tokio_postgres::Client,
    envelope: &Envelope,
    bounded: bool,
    tx: &mpsc::Sender<T>,
    wrap: &W,
    cancel: &AtomicBool,
) -> Result<(), SubscribeError>
where
    W: Fn(StreamMessage) -> T,
{
    let fetch = fetch_sql();
    let mut decoder: Option<Decoder> = None;
    loop {
        // Stop promptly when a sibling failed or the consumer walked away. The
        // fetch timeout bounds how long either takes to notice.
        if cancel.load(Ordering::SeqCst) || tx.is_closed() {
            return Ok(());
        }

        let messages = client.simple_query(&fetch).await.map_err(classify_error)?;
        let mut saw_row = false;
        for message in messages {
            if let SimpleQueryMessage::Row(row) = message {
                saw_row = true;
                if decoder.is_none() {
                    let columns: Vec<String> =
                        row.columns().iter().map(|c| c.name().to_string()).collect();
                    decoder = Some(Decoder::new(&columns, envelope)?);
                }
                let decoder = decoder.as_ref().expect("decoder set above");
                let raw: Vec<Datum> = (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect();
                let decoded = decoder.decode(&raw)?;
                match tx.try_send(wrap(decoded)) {
                    Ok(()) => {}
                    // The consumer dropped the receiver: nothing more to do.
                    Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                    // The buffer is full: the consumer fell behind. Fail loud
                    // rather than block the drain (which would backpressure the
                    // server into unbounded buffering).
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        return Err(SubscribeError::BufferOverflow);
                    }
                }
            }
        }

        // A fetch that returned no rows on a bounded subscription means it has
        // drained. An unbounded one just idled; loop and fetch again.
        if !saw_row && bounded {
            return Ok(());
        }
    }
}

/// Classifies a `tokio_postgres` error into the SDK's taxonomy.
///
/// A server error carries a DB payload (a SQLSTATE and message); anything
/// without one is a connection-level failure and therefore retryable. This
/// discriminator mirrors the Python SDK, so the two agree on what is
/// retryable.
fn classify_error(error: tokio_postgres::Error) -> SubscribeError {
    match error.as_db_error() {
        Some(db) => classify_server_message(db.message()),
        // No DB payload means a connection-level failure: retryable.
        None => SubscribeError::Transient(error.to_string()),
    }
}

/// Maps a server error message to a typed error. Pure, so it is unit-tested and
/// kept in lockstep with the Python SDK.
///
/// The compaction-horizon case is matched by message text because Materialize
/// does not yet expose a dedicated SQLSTATE for it. That match lives *only*
/// here, and both the current and the historical wording are handled so the
/// classification survives server upgrades.
fn classify_server_message(message: &str) -> SubscribeError {
    if is_compaction_horizon(message) {
        SubscribeError::CompactionHorizon
    } else if message.contains("dropped") && message.contains("dependenc") {
        SubscribeError::DependencyDropped(message.to_string())
    } else {
        SubscribeError::Fatal(message.to_string())
    }
}

fn is_compaction_horizon(message: &str) -> bool {
    // Current wording followed by the pre-#34712 wording.
    message.contains("could not find a valid timestamp")
        || message.contains("not valid for all inputs")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_compaction_horizon_wordings() {
        assert!(is_compaction_horizon(
            "could not find a valid timestamp for the query"
        ));
        assert!(is_compaction_horizon(
            "Timestamp (123) is not valid for all inputs"
        ));
        assert!(!is_compaction_horizon("some unrelated error"));
    }

    #[test]
    fn classifies_server_messages() {
        assert!(matches!(
            classify_server_message("could not find a valid timestamp for the query"),
            SubscribeError::CompactionHorizon
        ));
        assert!(matches!(
            classify_server_message("dependency winning_bids was dropped"),
            SubscribeError::DependencyDropped(_)
        ));
        assert!(matches!(
            classify_server_message("column \"x\" does not exist"),
            SubscribeError::Fatal(_)
        ));
    }

    /// End-to-end smoke test against a real Materialize. Ignored by default;
    /// run with `MZ_SUBSCRIBE_TEST_DSN` set to exercise the transport.
    #[tokio::test]
    #[ignore = "requires a running Materialize; set MZ_SUBSCRIBE_TEST_DSN"]
    async fn live_subscribe_smoke() {
        let dsn = std::env::var("MZ_SUBSCRIBE_TEST_DSN").expect("MZ_SUBSCRIBE_TEST_DSN");
        let client = SubscribeClient::connect(&dsn).await.expect("connect");
        let mut stream = client
            .subscribe(Subscribe::query("SELECT 1 AS n"))
            .await
            .expect("subscribe");
        // The first non-empty batch should carry the snapshot.
        loop {
            let batch = stream.next().await.expect("batch").expect("not terminated");
            if !batch.is_empty() {
                assert!(batch.is_snapshot);
                break;
            }
        }
    }
}
