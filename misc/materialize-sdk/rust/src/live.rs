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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use materialize_sdk_core::statement::fetch_sql;
use materialize_sdk_core::{Classification, Event, PushError, Row, Start, Subscribe, Subscription};
use tokio::sync::{Notify, oneshot};
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
    max_batch_bytes: usize,
    max_buffered_bytes: usize,
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
            // 64 MiB.
            max_batch_bytes: 67_108_864,
            // 4 GiB, about 64 snapshot chunks of the default
            // `max_pending_bytes`.
            max_buffered_bytes: 1usize.checked_shl(32).unwrap_or(usize::MAX),
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

    /// How many events can wait for the consumer before each new batch is
    /// appended to the last batch waiting, so that a slow consumer gets
    /// fewer, larger batches. An event that cannot be appended still waits,
    /// within [`LiveOptions::max_buffered_bytes`]. 64 by default.
    ///
    /// Snapshot chunks are never appended, and neither is anything to the
    /// batch that completes the snapshot, so that changes after the snapshot
    /// stay apart from it.
    pub fn buffer(mut self, events: usize) -> LiveOptions {
        self.buffer = events;
        self
    }

    /// The most bytes a batch grows to by appending, in the estimate
    /// [`Event::approx_bytes`] gives, so that a slow consumer's writes stay
    /// bounded. 64 MiB by default.
    pub fn max_batch_bytes(mut self, bytes: usize) -> LiveOptions {
        self.max_batch_bytes = bytes;
        self
    }

    /// The most bytes of updates that can wait for the consumer, in the
    /// estimate [`Event::approx_bytes`] gives. An event that alone is larger
    /// still passes once nothing else waits, so zero passes one event at a
    /// time. 4 GiB by default.
    ///
    /// Outside the buffer, the fetch loop also holds one `FETCH`'s events
    /// while they wait for room, the updates not released yet, which
    /// [`LiveOptions::max_pending_bytes`] and
    /// [`LiveOptions::max_timestamp_bytes`] bound, and one `FETCH`'s rows,
    /// which [`LiveOptions::max_rows`] bounds by count and not by bytes.
    pub fn max_buffered_bytes(mut self, bytes: usize) -> LiveOptions {
        self.max_buffered_bytes = bytes;
        self
    }

    /// How long the fetch loop waits, after each `FETCH`, for room for that
    /// `FETCH`'s events before the subscription fails with
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

    pub(crate) fn validate(&self) -> Result<(), Error> {
        fetch_sql("c", self.max_rows, Some(self.fetch_timeout))
            .map_err(|error| Error::InvalidOption(error.to_string()))?;
        if self.buffer == 0 {
            return Err(Error::InvalidOption(
                "the buffer must hold at least one event".into(),
            ));
        }
        materialize_sdk_core::batch::check_limits(self.max_pending_bytes, self.max_timestamp_bytes)
            .map_err(|error| Error::InvalidOption(error.to_string()))
    }
}

/// A live subscription: a snapshot, then every change, as [`Event`]s.
///
/// A task fetches from the cursor without waiting on the consumer, except for
/// up to [`LiveOptions::max_consumer_wait`] when an event finds no room in the
/// buffer, and then fails with [`Error::ConsumerFellBehind`]. A fetch loop
/// that waited longer would leave the backlog in Materialize until the server
/// retired the subscribe.
#[derive(Debug)]
pub struct LiveSubscription {
    queue: Arc<Queue>,
    end: Option<oneshot::Receiver<Result<(), Error>>>,
    learned: Arc<Learned>,
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
        LiveSubscription::connect_at(
            config,
            tls,
            subscribe,
            Start::Snapshot { as_of: None },
            options,
        )
        .await
    }

    /// [`LiveSubscription::connect`], starting at `start`.
    pub(crate) async fn connect_at<T>(
        config: &Config,
        tls: T,
        subscribe: Subscribe,
        start: Start,
        options: LiveOptions,
    ) -> Result<LiveSubscription, Error>
    where
        T: MakeTlsConnect<Socket>,
        T::Stream: Send + 'static,
    {
        options.validate()?;
        let mut sub = Subscription::new(subscribe, start)?;
        if let Some(limit) = options.max_pending_bytes {
            sub = sub.with_max_pending_bytes(limit);
        }
        if let Some(limit) = options.max_timestamp_bytes {
            sub = sub.with_max_timestamp_bytes(limit);
        }
        let (client, connection) = with_network_defaults(config)
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
        declare(&client, &mut sub, &mut failure).await?;
        let queue = Arc::new(Queue::default());
        // Made before the task, so that a task dropped before it first runs
        // still closes the queue.
        let task_queue = CloseOnDrop(Arc::clone(&queue));
        let (end_tx, end) = oneshot::channel();
        let learned = Arc::new(Learned::default());
        let task_learned = Arc::clone(&learned);
        let task = AbortOnDrop(tokio::spawn(async move {
            let queue = task_queue;
            let result = fetch_loop(
                &client,
                sub,
                &options,
                &queue.0,
                &task_learned,
                &mut failure,
            )
            .await;
            // The consumer may be gone, and then nobody needs the result.
            let _ = end_tx.send(result);
        }));
        Ok(LiveSubscription {
            queue,
            end: Some(end),
            learned,
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
    /// empty batch, or raises the upper of a batch still waiting, so the
    /// latest batch's upper tells how current the stream is.
    pub async fn next(&mut self) -> Option<Result<Event, Error>> {
        if let Some(event) = self.queue.take().await {
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
        self.learned.columns.get().map(Vec::as_slice)
    }

    /// The stream's `AS OF`, at which a snapshot's updates are. Set before the
    /// first event arrives.
    pub fn as_of(&self) -> Option<u64> {
        self.learned.as_of.get().copied()
    }
}

/// The events between the fetch loop and the consumer.
#[derive(Debug, Default)]
struct Queue {
    state: Mutex<Queued>,
    /// Wakes the consumer after an event arrives or the stream ends.
    arrived: Notify,
    /// Wakes the fetch loop after the consumer takes an event.
    taken: Notify,
}

#[derive(Debug, Default)]
struct Queued {
    /// Each event with its [`Event::approx_bytes`].
    events: VecDeque<(Event, usize)>,
    bytes: usize,
    closed: bool,
}

impl Queue {
    /// Queues `event` as [`LiveOptions::buffer`] describes, waiting for room
    /// until `max_consumer_wait` after `released_at`.
    async fn put(
        &self,
        event: Event,
        options: &LiveOptions,
        released_at: Instant,
    ) -> Result<(), Error> {
        let bytes = event.approx_bytes();
        let mut event = Some(event);
        loop {
            // Each notification stores a permit while nobody waits, so one
            // sent between this check and the wait below is not lost.
            let taken = self.taken.notified();
            let (events, queued) = {
                let mut state = self.lock();
                let fits = state.events.is_empty()
                    || state.bytes.saturating_add(bytes) <= options.max_buffered_bytes;
                if fits && let Some(next) = event.take() {
                    let full = state.events.len() >= options.buffer;
                    let left = match (state.events.back_mut(), next) {
                        (Some((Event::Batch(last), last_bytes)), Event::Batch(next))
                            if full
                                && last_bytes.saturating_add(bytes) <= options.max_batch_bytes =>
                        {
                            match last.append(next) {
                                Ok(()) => {
                                    *last_bytes = last_bytes.saturating_add(bytes);
                                    None
                                }
                                Err(next) => Some(Event::Batch(next)),
                            }
                        }
                        (_, next) => Some(next),
                    };
                    if let Some(next) = left {
                        state.events.push_back((next, bytes));
                    }
                    state.bytes = state.bytes.saturating_add(bytes);
                    drop(state);
                    self.arrived.notify_one();
                    return Ok(());
                }
                (state.events.len(), state.bytes)
            };
            let remaining = options
                .max_consumer_wait
                .saturating_sub(released_at.elapsed());
            if tokio::time::timeout(remaining, taken).await.is_err() {
                return Err(Error::ConsumerFellBehind {
                    events,
                    bytes: queued,
                    waited: released_at.elapsed(),
                });
            }
        }
    }

    /// The next event, or `None` once the stream ended and every event was
    /// taken.
    async fn take(&self) -> Option<Event> {
        loop {
            let arrived = self.arrived.notified();
            {
                let mut state = self.lock();
                if let Some((event, bytes)) = state.events.pop_front() {
                    state.bytes = state.bytes.saturating_sub(bytes);
                    drop(state);
                    self.taken.notify_one();
                    return Some(event);
                }
                if state.closed {
                    return None;
                }
            }
            arrived.await;
        }
    }

    fn close(&self) {
        self.lock().closed = true;
        self.arrived.notify_one();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Queued> {
        // Nothing that can panic runs while the lock is held, other than an
        // allocation failure, which aborts.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Closes the queue when the fetch task ends, also by a panic or an abort, so
/// that a waiting consumer moves on to the task's result.
struct CloseOnDrop(Arc<Queue>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// What the fetch loop learns from the stream, set before it hands over the
/// first event that depends on it.
#[derive(Debug, Default)]
struct Learned {
    columns: OnceLock<Vec<String>>,
    as_of: OnceLock<u64>,
}

/// The server's error on the connection, if any, arrives on this channel.
type Failure = oneshot::Receiver<tokio_postgres::Error>;

/// Opens the cursor's transaction and declares the cursor.
async fn declare(
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
    queue: &Queue,
    learned: &Learned,
    failure: &mut Failure,
) -> Result<(), Error> {
    while !sub.is_finished() {
        let fetch = sub.fetch(options.max_rows, Some(options.fetch_timeout))?;
        // A timed FETCH returns by its timeout, so a longer wait means the
        // connection was lost without a word, which TCP may not notice for
        // minutes.
        let wait = fetch
            .timeout()
            .map(|timeout| timeout.saturating_add(FETCH_GRACE));
        let fetched = match wait {
            Some(wait) => match tokio::time::timeout(wait, query(client, fetch.sql())).await {
                Ok(fetched) => fetched,
                Err(_) => return Err(silent(&sub, wait)),
            },
            None => query(client, fetch.sql()).await,
        };
        let (names, rows) = match fetched {
            Ok(result) => result,
            Err(error) => return Err(failed(&sub, error, failure).await),
        };
        let released = sub.push(fetch, &names, rows).map_err(|error| match error {
            PushError::Protocol(error) => Error::from(error),
            PushError::TimestampTooLarge(error) => Error::from(error),
            PushError::Usage(error) => Error::from(error),
        })?;
        if let Some(payload) = sub.columns() {
            learned.columns.get_or_init(|| payload.to_vec());
        }
        if let Some(as_of) = sub.as_of() {
            learned.as_of.get_or_init(|| as_of);
        }
        // One deadline for all of this FETCH's events, so a consumer that
        // drains slowly cannot keep the session idle for longer.
        let released_at = Instant::now();
        for event in released {
            queue.put(event, options, released_at).await?;
        }
    }
    // The stream has ended, so a failure to end the transaction changes
    // nothing the consumer can see.
    let _ = client.batch_execute("ROLLBACK").await;
    Ok(())
}

/// Runs `sql` with the simple query protocol, which returns every cell in its
/// text format, and returns the column names and the rows.
pub(crate) async fn query(
    client: &Client,
    sql: &str,
) -> Result<(Vec<String>, Vec<Row>), tokio_postgres::Error> {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for message in client.simple_query(sql).await? {
        match message {
            // The description arrives even for a result without rows, which
            // the core needs to tell an empty result from a missing column.
            SimpleQueryMessage::RowDescription(described) if columns.is_empty() => {
                columns = described.iter().map(|c| c.name().to_string()).collect();
            }
            SimpleQueryMessage::Row(row) => {
                if columns.is_empty() {
                    columns = row.columns().iter().map(|c| c.name().to_string()).collect();
                }
                rows.push(
                    (0..row.len())
                        .map(|i| row.get(i).map(str::to_string))
                        .collect(),
                );
            }
            _ => {}
        }
    }
    Ok((columns, rows))
}

/// How long past its timeout a timed `FETCH` may take before the connection
/// counts as lost.
const FETCH_GRACE: Duration = Duration::from_secs(30);

/// The error for a timed `FETCH` with no response within `wait`, which the
/// core classifies like any failure without a server error.
fn silent(sub: &Subscription, wait: Duration) -> Error {
    let message =
        format!("no response to a FETCH within {wait:?}, so the connection counts as lost");
    Error::Server {
        sqlstate: None,
        classification: sub.classify(None, &message),
        message,
        detail: None,
        hint: None,
    }
}

/// `config` with the network settings the SDK relies on, where the caller
/// set none: a connect timeout, and TCP keepalives that notice a peer gone
/// silent within about a minute. tokio-postgres starts keepalives only after
/// two hours idle, so an untimed `FETCH` on a dropped connection would wait
/// that long.
pub(crate) fn with_network_defaults(config: &Config) -> Config {
    let mut config = config.clone();
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(Duration::from_secs(30));
    }
    // tokio-postgres's default, which the caller left unchanged.
    if config.get_keepalives_idle() == Duration::from_secs(2 * 60 * 60) {
        config.keepalives_idle(Duration::from_secs(30));
    }
    if config.get_keepalives_interval().is_none() {
        config.keepalives_interval(Duration::from_secs(10));
    }
    if config.get_keepalives_retries().is_none() {
        config.keepalives_retries(3);
    }
    config
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
    server_error_with(error, |sqlstate, message| sub.classify(sqlstate, message))
}

/// The error for a failed request, classified by `classify`.
pub(crate) fn server_error_with(
    error: &tokio_postgres::Error,
    classify: impl FnOnce(Option<&str>, &str) -> Classification,
) -> Error {
    let (sqlstate, message, detail, hint) = match error.as_db_error() {
        Some(db) => (
            Some(db.code().code().to_string()),
            db.message().to_string(),
            db.detail().map(str::to_string),
            db.hint().map(str::to_string),
        ),
        None => (None, message_with_sources(error), None, None),
    };
    let classification = classify(sqlstate.as_deref(), &message);
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

#[cfg(test)]
mod tests {
    use materialize_sdk_core::{Batcher, Change, Frontier, Message, Update};

    use super::*;

    #[test]
    fn network_defaults_fill_only_what_the_caller_left_unset() {
        let defaulted = with_network_defaults(&Config::new());
        assert_eq!(
            defaulted.get_connect_timeout(),
            Some(&Duration::from_secs(30))
        );
        assert_eq!(defaulted.get_keepalives_idle(), Duration::from_secs(30));
        assert_eq!(
            defaulted.get_keepalives_interval(),
            Some(Duration::from_secs(10))
        );
        assert_eq!(defaulted.get_keepalives_retries(), Some(3));
        let mut chosen = Config::new();
        chosen
            .connect_timeout(Duration::from_secs(5))
            .keepalives_idle(Duration::from_secs(600))
            .keepalives_interval(Duration::from_secs(60))
            .keepalives_retries(9);
        let kept = with_network_defaults(&chosen);
        assert_eq!(kept.get_connect_timeout(), Some(&Duration::from_secs(5)));
        assert_eq!(kept.get_keepalives_idle(), Duration::from_secs(600));
        assert_eq!(
            kept.get_keepalives_interval(),
            Some(Duration::from_secs(60))
        );
        assert_eq!(kept.get_keepalives_retries(), Some(9));
    }

    /// `count` contiguous batches with one insert each, at timestamps from 1.
    fn batches(count: u64) -> Vec<Event> {
        let start = Start::Resume {
            frontier: Frontier::new(1).unwrap(),
        };
        let mut batcher = Batcher::new(start, None);
        // The server opens a resumed stream with a progress row at its `AS OF`,
        // one below the frontier.
        assert!(
            batcher
                .push(Message::Progress { frontier: 0 })
                .unwrap()
                .is_none()
        );
        let mut events = Vec::new();
        for t in 1..=count {
            let update = Update {
                timestamp: t,
                change: Change::Diff {
                    row: vec![Some(t.to_string())],
                    diff: 1,
                },
            };
            assert!(batcher.push(Message::Update(update)).unwrap().is_none());
            events.extend(batcher.push(Message::Progress { frontier: t + 1 }).unwrap());
        }
        events
    }

    fn options() -> LiveOptions {
        LiveOptions::default()
            .buffer(1)
            .max_consumer_wait(Duration::from_secs(1))
    }

    #[tokio::test(start_paused = true)]
    async fn batches_are_appended_while_the_buffer_is_full() {
        let queue = Queue::default();
        for event in batches(5) {
            queue.put(event, &options(), Instant::now()).await.unwrap();
        }
        let Some(Event::Batch(batch)) = queue.take().await else {
            panic!("expected a batch");
        };
        assert_eq!(batch.lower(), Some(Frontier::new(1).unwrap()));
        assert_eq!(batch.upper(), Frontier::new(6).unwrap());
        let times: Vec<_> = batch.updates().iter().map(|u| u.timestamp).collect();
        assert_eq!(times, [1, 2, 3, 4, 5]);
        assert_eq!(queue.lock().bytes, 0);
        assert!(queue.lock().events.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_chunk_waits_for_room_and_then_falls_behind() {
        let queue = Queue::default();
        let [batch] = batches(1).try_into().unwrap();
        queue.put(batch, &options(), Instant::now()).await.unwrap();
        let chunk = Event::SnapshotChunk {
            updates: Vec::new(),
        };
        let failed = queue
            .put(chunk, &options().max_buffered_bytes(0), Instant::now())
            .await;
        assert!(
            matches!(failed, Err(Error::ConsumerFellBehind { events: 1, .. })),
            "{failed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn appending_stops_at_the_batch_limit() {
        let queue = Queue::default();
        let [first, second, third] = batches(3).try_into().unwrap();
        let options = options().max_batch_bytes(first.approx_bytes() + second.approx_bytes());
        for event in [first, second, third] {
            queue.put(event, &options, Instant::now()).await.unwrap();
        }
        let uppers: Vec<_> = std::iter::from_fn(|| queue.lock().events.pop_front())
            .map(|(event, _)| match event {
                Event::Batch(batch) => batch.upper(),
                other @ Event::SnapshotChunk { .. } => panic!("expected a batch, got {other:?}"),
            })
            .collect();
        let frontier = |t| Frontier::new(t).unwrap();
        assert_eq!(uppers, [frontier(3), frontier(4)]);
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_is_appended_to_the_batch_that_completes_the_snapshot() {
        let mut batcher = Batcher::new(Start::Snapshot { as_of: None }, None);
        let insert = |t: u64| Update {
            timestamp: t,
            change: Change::Diff {
                row: vec![Some(t.to_string())],
                diff: 1,
            },
        };
        let mut events = Vec::new();
        events.extend(batcher.push(Message::Progress { frontier: 10 }).unwrap());
        for t in [10, 11, 12] {
            events.extend(batcher.push(Message::Update(insert(t))).unwrap());
            events.extend(batcher.push(Message::Progress { frontier: t + 1 }).unwrap());
        }
        let queue = Queue::default();
        for event in events {
            queue.put(event, &options(), Instant::now()).await.unwrap();
        }
        let Some(Event::Batch(snapshot)) = queue.take().await else {
            panic!("expected the snapshot");
        };
        assert!(snapshot.is_snapshot());
        assert_eq!(snapshot.upper(), Frontier::new(11).unwrap());
        let Some(Event::Batch(later)) = queue.take().await else {
            panic!("expected the later changes");
        };
        assert_eq!(later.upper(), Frontier::new(13).unwrap());
        assert_eq!(later.updates().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn appending_stops_at_the_byte_limit() {
        let queue = Queue::default();
        let [first, second] = batches(2).try_into().unwrap();
        let options = options().max_buffered_bytes(first.approx_bytes());
        queue.put(first, &options, Instant::now()).await.unwrap();
        let failed = queue.put(second, &options, Instant::now()).await;
        assert!(
            matches!(failed, Err(Error::ConsumerFellBehind { .. })),
            "{failed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiting_producer_continues_once_the_consumer_takes() {
        let queue = Arc::new(Queue::default());
        let [batch] = batches(1).try_into().unwrap();
        queue.put(batch, &options(), Instant::now()).await.unwrap();
        let producer = Arc::clone(&queue);
        let chunk = Event::SnapshotChunk {
            updates: Vec::new(),
        };
        let put = tokio::spawn(async move {
            let options = options().max_buffered_bytes(0);
            producer.put(chunk, &options, Instant::now()).await
        });
        tokio::task::yield_now().await;
        assert!(matches!(queue.take().await, Some(Event::Batch(_))));
        put.await.unwrap().unwrap();
        assert!(matches!(
            queue.take().await,
            Some(Event::SnapshotChunk { .. })
        ));
    }

    #[tokio::test]
    async fn the_consumer_stops_waiting_when_the_producer_task_ends_in_any_way() {
        let queue = Arc::new(Queue::default());
        let producer = CloseOnDrop(Arc::clone(&queue));
        // Aborted before it first runs, as when a runtime shuts down.
        let task = tokio::spawn(async move {
            let _producer = producer;
            std::future::pending::<()>().await;
        });
        task.abort();
        let consumer = Arc::clone(&queue);
        let taken = tokio::spawn(async move { consumer.take().await });
        let taken = tokio::time::timeout(Duration::from_secs(10), taken).await;
        assert!(taken.expect("the consumer kept waiting").unwrap().is_none());
    }
}
