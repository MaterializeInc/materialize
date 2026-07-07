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
//! hands every row to the tested [`Decoder`] and [`Batcher`]. All protocol
//! judgement lives in those, not here.
//!
//! TLS is not yet wired, so this connects over plaintext (suitable for the
//! emulator or a `sslmode=disable` local server). A TLS connector is the next
//! step for this module.

use tokio_postgres::{NoTls, SimpleQueryMessage};

use crate::batch::{Batcher, ConsistentBatch};
use crate::envelope::{Datum, Decoder};
use crate::error::SubscribeError;
use crate::statement::Subscribe;
use crate::token::ResumeToken;

/// The cursor name used for the subscription. One subscription owns its
/// connection, so a fixed name is safe.
const CURSOR: &str = "mz_subscribe_cursor";

/// A connection to Materialize for consuming subscriptions.
///
/// One connection backs one subscription at a time. Transaction-mode connection
/// poolers (such as PgBouncer) break the `DECLARE`/`FETCH` cursor and are not
/// supported.
#[derive(Debug)]
pub struct SubscribeClient {
    client: tokio_postgres::Client,
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
        Ok(SubscribeClient { client })
    }

    /// Starts a new subscription, taking the initial snapshot.
    pub async fn subscribe(&self, subscribe: Subscribe) -> Result<BatchStream<'_>, SubscribeError> {
        let sql = subscribe.to_sql_initial();
        self.open(subscribe, sql, /* with_snapshot */ true).await
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
    ) -> Result<BatchStream<'_>, SubscribeError> {
        let fingerprint = subscribe.fingerprint();
        if fingerprint != token.fingerprint() {
            return Err(SubscribeError::SchemaMismatch {
                expected: token.fingerprint().to_string(),
                actual: fingerprint,
            });
        }
        let sql = subscribe.to_sql_resume(token);
        self.open(subscribe, sql, /* with_snapshot */ false).await
    }

    async fn open(
        &self,
        subscribe: Subscribe,
        subscribe_sql: String,
        with_snapshot: bool,
    ) -> Result<BatchStream<'_>, SubscribeError> {
        // A cursor must live inside a transaction, which also holds the read so
        // the frontier does not advance out from under a slow reader mid-batch.
        //
        // NOTE: one subscription owns its connection for its lifetime. If
        // `DECLARE` fails after `BEGIN`, roll back so the connection is not left
        // in an open transaction.
        self.client
            .simple_query("BEGIN")
            .await
            .map_err(classify_error)?;
        if let Err(error) = self
            .client
            .simple_query(&format!("DECLARE {CURSOR} CURSOR FOR {subscribe_sql}"))
            .await
        {
            let _ = self.client.simple_query("ROLLBACK").await;
            return Err(classify_error(error));
        }

        Ok(BatchStream {
            client: &self.client,
            fetch_sql: format!("FETCH ALL {CURSOR} WITH (timeout = '1s')"),
            decoder: None,
            batcher: Batcher::new(subscribe.fingerprint(), with_snapshot),
            envelope: subscribe.envelope().clone(),
            bounded: subscribe.is_bounded(),
        })
    }
}

/// A stream of [`ConsistentBatch`]es for one subscription.
///
/// Pull batches with [`BatchStream::next`]. Each returned batch is complete: a
/// consumer that applies it and persists its `resume_token` atomically achieves
/// exactly-once state.
#[derive(Debug)]
pub struct BatchStream<'a> {
    client: &'a tokio_postgres::Client,
    fetch_sql: String,
    decoder: Option<Decoder>,
    batcher: Batcher,
    envelope: crate::envelope::Envelope,
    bounded: bool,
}

impl BatchStream<'_> {
    /// Returns the next consistent batch, or `None` when a bounded subscription
    /// (one with `UP TO`) has terminated.
    ///
    /// Fetches from the server until a timestamp closes, so it may issue
    /// several `FETCH`es before returning. Idle periods still yield empty
    /// batches as the frontier advances.
    pub async fn next(&mut self) -> Result<Option<ConsistentBatch>, SubscribeError> {
        loop {
            let messages = self
                .client
                .simple_query(&self.fetch_sql)
                .await
                .map_err(classify_error)?;

            let mut terminated = true;
            for message in messages {
                match message {
                    SimpleQueryMessage::Row(row) => {
                        // A row means the cursor is still live.
                        terminated = false;
                        if self.decoder.is_none() {
                            let columns: Vec<String> =
                                row.columns().iter().map(|c| c.name().to_string()).collect();
                            self.decoder = Some(Decoder::new(&columns, &self.envelope)?);
                        }
                        let decoder = self.decoder.as_ref().expect("decoder set above");
                        let raw: Vec<Datum> = (0..row.len())
                            .map(|i| row.get(i).map(str::to_string))
                            .collect();
                        let message = decoder.decode(&raw)?;
                        if let Some(batch) = self.batcher.push(message)? {
                            return Ok(Some(batch));
                        }
                    }
                    SimpleQueryMessage::CommandComplete(_) => {}
                    // Newer message variants (added behind `#[non_exhaustive]`)
                    // carry no rows and can be ignored.
                    _ => {}
                }
            }

            // A `FETCH` that returns only a command tag and no rows means the
            // bounded subscription has drained.
            if terminated && self.is_bounded() {
                return Ok(None);
            }
        }
    }

    /// Whether this is a bounded (`UP TO`) subscription. Only a bounded cursor
    /// terminates with an empty fetch; an unbounded one blocks on the fetch
    /// timeout and keeps advancing, so an empty fetch there just means "idle,
    /// poll again".
    fn is_bounded(&self) -> bool {
        self.bounded
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
