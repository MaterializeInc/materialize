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

//! Live subscriptions: changes streamed to the caller as they happen, with no
//! checkpoint and no resume.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use materialize_sdk_core::statement::fetch_sql;
use materialize_sdk_core::{Event, PushError, Row, Start, Subscribe, Subscription};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{Client, Config, SimpleQueryMessage, Socket};

use crate::error::Error;

/// How a live subscription fetches and buffers.
#[derive(Clone, Debug)]
pub struct LiveOptions {
    max_rows: u64,
    fetch_timeout: Duration,
    buffer: usize,
    max_consumer_wait: Duration,
    max_pending_bytes: Option<usize>,
    max_timestamp_bytes: Option<usize>,
}

impl Default for LiveOptions {
    fn default() -> LiveOptions {
        LiveOptions {
            max_rows: 1000,
            fetch_timeout: Duration::from_secs(1),
            buffer: 64,
            max_consumer_wait: Duration::from_secs(30),
            // 64 MiB.
            max_pending_bytes: Some(67_108_864),
            // 1 GiB.
            max_timestamp_bytes: Some(1_073_741_824),
        }
    }
}

impl LiveOptions {
    /// The most rows one `FETCH` returns. 1000 by default.
    pub fn max_rows(mut self, max_rows: u64) -> LiveOptions {
        self.max_rows = max_rows;
        self
    }

    /// How long a `FETCH` waits for rows before it comes back empty, at most
    /// one day. One second by default. A `FETCH` that checks for the end of
    /// the stream has no timeout, see [`Subscription::fetch`].
    pub fn fetch_timeout(mut self, timeout: Duration) -> LiveOptions {
        self.fetch_timeout = timeout;
        self
    }

    /// How many events can wait for the consumer. 64 by default.
    ///
    /// Each event holds at most the memory limit of
    /// [`LiveOptions::max_pending_bytes`] plus one timestamp's updates, which
    /// [`LiveOptions::max_timestamp_bytes`] bounds, so the buffer bounds memory
    /// only together with those limits. Outside the buffer, the fetch loop
    /// also holds the updates not released yet and the events waiting for
    /// room, which the same limits bound, and one `FETCH`'s rows, which
    /// [`LiveOptions::max_rows`] bounds by count and not by bytes.
    pub fn buffer(mut self, events: usize) -> LiveOptions {
        self.buffer = events;
        self
    }

    /// How long the fetch loop waits, after each `FETCH`, for the consumer to
    /// take that `FETCH`'s events before the subscription fails with
    /// [`Error::ConsumerFellBehind`]. 30 seconds by default.
    ///
    /// While it waits, the fetch loop does not fetch, so the backlog grows in
    /// Materialize and the session sits idle in its transaction. Keep it well
    /// below `idle_in_transaction_session_timeout`, two minutes by default.
    pub fn max_consumer_wait(mut self, wait: Duration) -> LiveOptions {
        self.max_consumer_wait = wait;
        self
    }

    /// See [`Subscription::with_max_pending_bytes`]. 64 MiB by default.
    pub fn max_pending_bytes(mut self, limit: Option<usize>) -> LiveOptions {
        self.max_pending_bytes = limit;
        self
    }

    /// See [`Subscription::with_max_timestamp_bytes`]. 1 GiB by default.
    ///
    /// The initial snapshot counts as one timestamp, and one larger than this
    /// limit passes only in chunks, so [`LiveOptions::max_pending_bytes`] must
    /// be below this limit when both are set.
    pub fn max_timestamp_bytes(mut self, limit: Option<usize>) -> LiveOptions {
        self.max_timestamp_bytes = limit;
        self
    }

    fn validate(&self) -> Result<(), Error> {
        fetch_sql("c", self.max_rows, Some(self.fetch_timeout))
            .map_err(|error| Error::InvalidOption(error.to_string()))?;
        if self.buffer == 0 || self.buffer > Semaphore::MAX_PERMITS {
            return Err(Error::InvalidOption(format!(
                "the buffer must hold between 1 and {} events",
                Semaphore::MAX_PERMITS
            )));
        }
        if let (Some(pending), Some(timestamp)) = (self.max_pending_bytes, self.max_timestamp_bytes)
            && pending >= timestamp
        {
            return Err(Error::InvalidOption(
                "max_pending_bytes must be below max_timestamp_bytes".into(),
            ));
        }
        Ok(())
    }
}

/// A live subscription: a snapshot, then every change, as [`Event`]s.
///
/// A task fetches from the cursor without waiting on the consumer, except for
/// up to [`LiveOptions::max_consumer_wait`] when the buffer is full, and then
/// fails with [`Error::ConsumerFellBehind`]. A fetch loop that waited longer
/// would leave the backlog in Materialize until the server retired the
/// subscribe.
#[derive(Debug)]
pub struct LiveSubscription {
    events: mpsc::Receiver<Event>,
    end: Option<oneshot::Receiver<Result<(), Error>>>,
    columns: Arc<OnceLock<Vec<String>>>,
    _task: AbortOnDrop,
    _connection: AbortOnDrop,
}

/// Aborts the task when dropped, so that dropping a [`LiveSubscription`], or
/// the future of [`LiveSubscription::connect`], ends the session.
#[derive(Debug)]
pub(crate) struct AbortOnDrop(pub(crate) JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl LiveSubscription {
    /// Connects to Materialize with `config` and `tls`, and starts `subscribe`
    /// with a snapshot.
    ///
    /// The subscription owns the session: its cursor lives in a transaction
    /// that stays open until the stream ends. Session settings, such as the
    /// cluster the subscribe runs on, come from `config`'s `options`.
    /// Dropping the subscription closes the connection, which ends the
    /// session and the subscribe on the server, even while a `FETCH` waits.
    ///
    /// Must run in a Tokio runtime with IO and time enabled.
    pub async fn connect<T>(
        config: &Config,
        tls: T,
        subscribe: Subscribe,
        options: LiveOptions,
    ) -> Result<LiveSubscription, Error>
    where
        T: MakeTlsConnect<Socket>,
        T::Stream: Send + 'static,
    {
        options.validate()?;
        let mut sub = Subscription::new(subscribe, Start::Snapshot { as_of: None })?;
        if let Some(limit) = options.max_pending_bytes {
            sub = sub.with_max_pending_bytes(limit);
        }
        if let Some(limit) = options.max_timestamp_bytes {
            sub = sub.with_max_timestamp_bytes(limit);
        }
        let (client, connection) = config
            .connect(tls)
            .await
            .map_err(|e| server_error(&sub, &e))?;
        // Aborting this task drops the socket, which ends the session even
        // while a request is outstanding. Dropping the client alone waits for
        // the outstanding request to finish first.
        let (failure_tx, mut failure) = oneshot::channel();
        let connection = AbortOnDrop(tokio::spawn(async move {
            // The server can end the session with an error that no request
            // receives, such as an idle timeout, so it surfaces here.
            if let Err(error) = connection.await {
                let _ = failure_tx.send(error);
            }
        }));
        start(&client, &mut sub, &mut failure).await?;
        let capacity = options.buffer;
        let (events_tx, events) = mpsc::channel(capacity);
        let (end_tx, end) = oneshot::channel();
        let columns = Arc::new(OnceLock::new());
        let task_columns = Arc::clone(&columns);
        let task = AbortOnDrop(tokio::spawn(async move {
            let result = fetch_loop(
                &client,
                sub,
                &options,
                &events_tx,
                &task_columns,
                &mut failure,
            )
            .await;
            // The consumer may be gone, and then nobody needs the result.
            let _ = end_tx.send(result);
        }));
        Ok(LiveSubscription {
            events,
            end: Some(end),
            columns,
            _task: task,
            _connection: connection,
        })
    }

    /// The next event, or `None` once the stream has ended or failed. A
    /// failure is returned once, after the events delivered before it. A
    /// `FETCH` whose rows break the protocol or pass
    /// [`LiveOptions::max_timestamp_bytes`] delivers none of its events.
    ///
    /// Every batch carries its frontier, and an advance without changes is an
    /// empty batch, so the latest batch's upper tells how current the stream
    /// is.
    pub async fn next(&mut self) -> Option<Result<Event, Error>> {
        if let Some(event) = self.events.recv().await {
            return Some(Ok(event));
        }
        match self.end.take()?.await {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Err(error)),
            Err(_) => Some(Err(Error::TaskEnded)),
        }
    }

    /// The names of the columns of each update's row, or of its key followed
    /// by its value in the upsert envelope. Set before the first event
    /// arrives.
    pub fn columns(&self) -> Option<&[String]> {
        self.columns.get().map(Vec::as_slice)
    }
}

/// The server's error on the connection, if any, arrives on this channel.
type Failure = oneshot::Receiver<tokio_postgres::Error>;

/// Opens the cursor's transaction and declares the cursor.
async fn start(
    client: &Client,
    sub: &mut Subscription,
    failure: &mut Failure,
) -> Result<(), Error> {
    if let Err(error) = client.batch_execute("BEGIN").await {
        return Err(failed(sub, error, failure).await);
    }
    // The extended protocol refuses a statement that holds more than one
    // command, which a query's text could otherwise smuggle in.
    let declare = sub.declare_sql()?;
    if let Err(error) = client.execute(declare.as_str(), &[]).await {
        return Err(failed(sub, error, failure).await);
    }
    Ok(())
}

async fn fetch_loop(
    client: &Client,
    mut sub: Subscription,
    options: &LiveOptions,
    events: &mpsc::Sender<Event>,
    columns: &OnceLock<Vec<String>>,
    failure: &mut Failure,
) -> Result<(), Error> {
    while !sub.is_finished() {
        let fetch = sub.fetch(options.max_rows, Some(options.fetch_timeout))?;
        let (names, rows) = match query(client, fetch.sql()).await {
            Ok(result) => result,
            Err(error) => return Err(failed(&sub, error, failure).await),
        };
        let released = sub.push(fetch, &names, rows).map_err(|error| match error {
            PushError::Protocol(error) => Error::from(error),
            PushError::TimestampTooLarge(error) => Error::from(error),
            PushError::Usage(error) => Error::from(error),
        })?;
        if let Some(payload) = sub.columns() {
            columns.get_or_init(|| payload.to_vec());
        }
        // One deadline for all of this FETCH's events, so a consumer that
        // drains slowly cannot keep the session idle for longer.
        let released_at = Instant::now();
        for event in released {
            let permit = match events.try_reserve() {
                Ok(permit) => permit,
                Err(TrySendError::Closed(())) => return Ok(()),
                Err(TrySendError::Full(())) => {
                    let remaining = options
                        .max_consumer_wait
                        .saturating_sub(released_at.elapsed());
                    match tokio::time::timeout(remaining, events.reserve()).await {
                        Ok(Ok(permit)) => permit,
                        Ok(Err(_)) => return Ok(()),
                        Err(_) => {
                            return Err(Error::ConsumerFellBehind {
                                capacity: options.buffer,
                                waited: released_at.elapsed(),
                            });
                        }
                    }
                }
            };
            permit.send(event);
        }
    }
    // The stream has ended, so a failure to end the transaction changes
    // nothing the consumer can see.
    let _ = client.batch_execute("ROLLBACK").await;
    Ok(())
}

/// Runs `sql` with the simple query protocol, which returns every cell in its
/// text format, and returns the column names and the rows.
async fn query(
    client: &Client,
    sql: &str,
) -> Result<(Vec<String>, Vec<Row>), tokio_postgres::Error> {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for message in client.simple_query(sql).await? {
        if let SimpleQueryMessage::Row(row) = message {
            if columns.is_empty() {
                columns = row.columns().iter().map(|c| c.name().to_string()).collect();
            }
            rows.push(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            );
        }
    }
    Ok((columns, rows))
}

/// The error for a failed request: the server's own error if the connection
/// closed because of one, and otherwise the request's.
async fn failed(sub: &Subscription, error: tokio_postgres::Error, failure: &mut Failure) -> Error {
    if error.is_closed() {
        // The connection task can still be delivering the server's error.
        if let Ok(Ok(cause)) = tokio::time::timeout(Duration::from_secs(1), failure).await {
            return server_error(sub, &cause);
        }
    }
    server_error(sub, &error)
}

fn server_error(sub: &Subscription, error: &tokio_postgres::Error) -> Error {
    let (sqlstate, message, detail, hint) = match error.as_db_error() {
        Some(db) => (
            Some(db.code().code().to_string()),
            db.message().to_string(),
            db.detail().map(str::to_string),
            db.hint().map(str::to_string),
        ),
        None => (None, message_with_sources(error), None, None),
    };
    let classification = sub.classify(sqlstate.as_deref(), &message);
    Error::Server {
        sqlstate,
        message,
        detail,
        hint,
        classification,
    }
}

/// The driver's message with its causes, such as "connection refused", which
/// its own message omits.
pub(crate) fn message_with_sources(error: &tokio_postgres::Error) -> String {
    let mut message = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}
