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

//! One cursor of a subscription, as a transport drives it.
//!
//! The transport owns a session and runs, in order: `BEGIN`,
//! [`Subscription::declare_sql`], then [`Subscription::fetch`] and
//! [`Subscription::push`] in a loop until [`Subscription::is_finished`]. Each
//! subscription needs its own session, because the cursor name is fixed and a
//! waiting `FETCH` holds the session.
//!
//! The transport's fetch loop should bound how long it waits for the
//! consumer, well below `idle_in_transaction_session_timeout` (two minutes by
//! default), and fail once that runs out. A session that sits in its
//! transaction for longer is ended with `25P03`, which
//! [`crate::classify::classify`] treats as transient.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::batch::{BatchError, Batcher, Event, Start};
use crate::classify::{Classification, Phase, classify};
use crate::decode::{Decoder, Row};
use crate::error::{ProtocolError, TimestampTooLarge, UsageError};
use crate::statement::{self, Subscribe};

/// The name of the cursor every subscription declares.
pub const CURSOR: &str = "materialize_sdk";

/// A `FETCH` to run, which goes back to [`Subscription::push`] with its
/// result.
#[derive(Debug, PartialEq, Eq)]
pub struct Fetch {
    sql: String,
    timeout: Option<Duration>,
    /// The subscription that issued it, so another one refuses it.
    owner: u64,
    id: u64,
}

impl Fetch {
    /// The statement.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// How long the statement waits for rows, or `None` for a `FETCH` that
    /// waits until rows arrive or the stream ends. See [`Subscription::fetch`].
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }
}

/// Why [`Subscription::push`] failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PushError {
    /// The server broke the subscribe protocol, and the subscription failed.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    /// One timestamp exceeded [`Subscription::with_max_timestamp_bytes`], and
    /// the subscription failed.
    #[error(transparent)]
    TimestampTooLarge(#[from] TimestampTooLarge),
    /// The caller passed a `FETCH` the subscription is not waiting on.
    #[error(transparent)]
    Usage(#[from] UsageError),
}

impl From<BatchError> for PushError {
    fn from(error: BatchError) -> PushError {
        match error {
            BatchError::Protocol(error) => PushError::Protocol(error),
            BatchError::TimestampTooLarge(error) => PushError::TimestampTooLarge(error),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Outstanding {
    id: u64,
    timed: bool,
}

/// Numbers every subscription in the process, for [`Fetch`]'s owner.
static NEXT_OWNER: AtomicU64 = AtomicU64::new(0);

/// One cursor of a subscription.
#[derive(Debug)]
pub struct Subscription {
    owner: u64,
    subscribe: Subscribe,
    sql: String,
    declared: bool,
    batcher: Batcher,
    /// The output columns, known from the first non-empty `FETCH`.
    columns: Option<(Vec<String>, Decoder)>,
    next_fetch: u64,
    outstanding: Option<Outstanding>,
    probe: bool,
    /// The first error from the stream. The batcher's state has moved past
    /// what the caller received, so every later call fails with it.
    failed: Option<BatchError>,
}

impl Subscription {
    /// Returns the subscription `subscribe`, starting at `start`.
    pub fn new(subscribe: Subscribe, start: Start) -> Result<Subscription, UsageError> {
        let sql = subscribe.sql(start)?;
        let batcher = Batcher::new(start, subscribe.up_to());
        Ok(Subscription {
            owner: NEXT_OWNER.fetch_add(1, Ordering::Relaxed),
            subscribe,
            sql,
            declared: false,
            batcher,
            columns: None,
            next_fetch: 0,
            outstanding: None,
            probe: false,
            failed: None,
        })
    }

    /// Bounds the memory held between progress rows. See
    /// [`Batcher::with_max_pending_bytes`].
    pub fn with_max_pending_bytes(mut self, limit: usize) -> Subscription {
        self.batcher = self.batcher.with_max_pending_bytes(limit);
        self
    }

    /// Fails the subscription when one timestamp holds more than `limit`
    /// bytes. See [`Batcher::with_max_timestamp_bytes`].
    pub fn with_max_timestamp_bytes(mut self, limit: usize) -> Subscription {
        self.batcher = self.batcher.with_max_timestamp_bytes(limit);
        self
    }

    /// The `SUBSCRIBE` statement.
    pub fn subscribe_sql(&self) -> &str {
        &self.sql
    }

    /// The statement that declares the cursor, to run after `BEGIN`.
    ///
    /// A subscription reads one cursor, so a second call fails. A second cursor
    /// starts its own stream, with a new snapshot this subscription would take
    /// for updates. To read again, start a new subscription from the upper of
    /// the last batch received.
    pub fn declare_sql(&mut self) -> Result<String, UsageError> {
        if self.declared {
            return Err(UsageError::new(
                "the cursor was already declared; start a new subscription to read again",
            ));
        }
        self.declared = true;
        Ok(statement::declare_sql(CURSOR, &self.sql))
    }

    /// The next `FETCH` to run. Only one can be outstanding.
    ///
    /// The statement waits at most `timeout`, except for a probe. A `FETCH`
    /// with a timeout returns nothing both when the timeout expires and when
    /// the stream has ended (`src/pgwire/src/protocol.rs`), so after a timed
    /// `FETCH` comes back empty, the next one has no timeout. That one returns
    /// with the first rows to arrive, which on a live object is the next
    /// progress row, or with nothing once the stream has ended. While the
    /// frontier stands still it keeps waiting, which on a `REFRESH EVERY`
    /// view lasts until the next refresh, so a transport that must stop
    /// sooner cancels the statement.
    ///
    /// After the subscription failed, this fails with a usage error that names
    /// the failure.
    pub fn fetch(&mut self, max_rows: u64, timeout: Option<Duration>) -> Result<Fetch, UsageError> {
        if let Some(error) = &self.failed {
            return Err(UsageError::new(format!("the subscription failed: {error}")));
        }
        if !self.declared {
            return Err(UsageError::new("the cursor is not declared"));
        }
        if self.batcher.is_finished() {
            return Err(UsageError::new("the stream has ended"));
        }
        if self.outstanding.is_some() {
            return Err(UsageError::new(
                "a FETCH is outstanding; pass its result to push first",
            ));
        }
        let timeout = timeout.filter(|_| !self.probe);
        let sql = statement::fetch_sql(CURSOR, max_rows, timeout)?;
        let id = self.next_fetch;
        self.next_fetch = id.wrapping_add(1);
        self.outstanding = Some(Outstanding {
            id,
            timed: timeout.is_some(),
        });
        Ok(Fetch {
            sql,
            timeout,
            owner: self.owner,
            id,
        })
    }

    /// Accepts the result of `fetch`: its column names and rows.
    ///
    /// Returns the events the rows release, in order. An empty result of a
    /// `FETCH` without a timeout ends the stream and releases what remains.
    /// A [`PushError::Protocol`] or [`PushError::TimestampTooLarge`] fails the
    /// subscription. A `fetch` from another subscription is refused and lost,
    /// so the subscription that issued it can no longer fetch.
    pub fn push<S: AsRef<str>>(
        &mut self,
        fetch: Fetch,
        columns: &[S],
        rows: Vec<Row>,
    ) -> Result<Vec<Event>, PushError> {
        if fetch.owner != self.owner {
            return Err(UsageError::new("this FETCH is from another subscription").into());
        }
        let outstanding = match self.outstanding {
            Some(outstanding) if outstanding.id == fetch.id => outstanding,
            _ => {
                return Err(UsageError::new("this FETCH is not the outstanding one").into());
            }
        };
        self.outstanding = None;
        let result = self.accept(outstanding.timed, columns, rows);
        if let Err(error) = &result {
            self.failed = Some(error.clone());
        }
        Ok(result?)
    }

    fn accept<S: AsRef<str>>(
        &mut self,
        timed: bool,
        columns: &[S],
        rows: Vec<Row>,
    ) -> Result<Vec<Event>, BatchError> {
        if rows.is_empty() {
            if timed {
                self.probe = true;
                return Ok(Vec::new());
            }
            self.probe = false;
            return Ok(self
                .batcher
                .finish()?
                .map(Event::Batch)
                .into_iter()
                .collect());
        }
        self.probe = false;
        let decoder = match &self.columns {
            Some((known, decoder)) => {
                if !known
                    .iter()
                    .map(String::as_str)
                    .eq(columns.iter().map(AsRef::as_ref))
                {
                    return Err(ProtocolError::new(format!(
                        "the cursor's columns changed from {known:?}"
                    ))
                    .into());
                }
                decoder
            }
            None => {
                let decoder = Decoder::new(self.subscribe.envelope(), columns)?;
                let known = columns.iter().map(|c| c.as_ref().to_string()).collect();
                &self.columns.insert((known, decoder)).1
            }
        };
        let mut events = Vec::new();
        for row in rows {
            let message = decoder.decode(row)?;
            events.extend(self.batcher.push(message)?);
        }
        Ok(events)
    }

    /// Classifies an error from any statement of this subscription.
    pub fn classify(&self, sqlstate: Option<&str>, message: &str) -> Classification {
        // An error before the first progress row comes from starting the
        // subscription, not from running it.
        let phase = if self.batcher.progress().is_some() {
            Phase::Streaming
        } else {
            Phase::Starting
        };
        classify(sqlstate, message, phase)
    }

    /// Whether the stream has ended. Once it has, the transport closes the
    /// cursor.
    pub fn is_finished(&self) -> bool {
        self.batcher.is_finished()
    }

    /// Whether a protocol error or [`PushError::TimestampTooLarge`] ended the
    /// subscription.
    pub fn is_failed(&self) -> bool {
        self.failed.is_some()
    }

    /// The payload column names, once the first rows have arrived. See
    /// [`Decoder::columns`].
    pub fn columns(&self) -> Option<&[String]> {
        self.columns.as_ref().map(|(_, decoder)| decoder.columns())
    }

    /// The frontier of the latest progress row, for lag metrics.
    pub fn progress(&self) -> Option<u64> {
        self.batcher.progress()
    }

    /// The stream's `AS OF`. See [`Batcher::as_of`].
    pub fn as_of(&self) -> Option<u64> {
        self.batcher.as_of()
    }

    /// The memory held for updates not yet released. See
    /// [`Batcher::with_max_pending_bytes`].
    pub fn pending_bytes(&self) -> usize {
        self.batcher.pending_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::Batch;
    use crate::classify::ErrorKind;
    use crate::decode::{Change, Update};
    use crate::frontier::Frontier;
    use crate::statement::ObjectRef;

    const COLUMNS: [&str; 4] = ["mz_timestamp", "mz_progressed", "mz_diff", "id"];
    const TIMEOUT: Option<Duration> = Some(Duration::from_secs(1));

    fn orders() -> Subscribe {
        Subscribe::object(ObjectRef {
            id: "u1".into(),
            database: Some("db".into()),
            schema: "public".into(),
            name: "orders".into(),
        })
    }

    fn declared(subscribe: Subscribe, start: Start) -> Subscription {
        let mut sub = Subscription::new(subscribe, start).unwrap();
        sub.declare_sql().unwrap();
        sub
    }

    fn progress(t: u64) -> Row {
        vec![Some(t.to_string()), Some("t".into()), None, None]
    }

    fn insert(t: u64, id: &str) -> Row {
        vec![
            Some(t.to_string()),
            Some("f".into()),
            Some("1".into()),
            Some(id.into()),
        ]
    }

    fn inserted(t: u64, id: &str) -> Update {
        Update {
            timestamp: t,
            change: Change::Diff {
                row: vec![Some(id.into())],
                diff: 1,
            },
        }
    }

    /// Runs one `FETCH` that returns `rows`, and returns the batches.
    fn round(sub: &mut Subscription, timeout: Option<Duration>, rows: Vec<Row>) -> Vec<Batch> {
        let fetch = sub.fetch(100, timeout).unwrap();
        sub.push(fetch, &COLUMNS, rows)
            .unwrap()
            .into_iter()
            .map(|event| match event {
                Event::Batch(batch) => batch,
                other @ Event::SnapshotChunk { .. } => panic!("expected a batch, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn snapshot_then_stream() {
        let mut sub = Subscription::new(orders(), Start::Snapshot { as_of: None }).unwrap();
        assert!(
            sub.fetch(100, TIMEOUT).is_err(),
            "the cursor is not declared"
        );
        assert_eq!(
            sub.declare_sql().unwrap(),
            r#"DECLARE "materialize_sdk" CURSOR FOR SUBSCRIBE ["u1" AS "db"."public"."orders"] WITH (PROGRESS, SNAPSHOT = true)"#
        );
        assert!(
            sub.declare_sql().is_err(),
            "a subscription reads one cursor"
        );
        let fetch = sub.fetch(100, TIMEOUT).unwrap();
        assert_eq!(
            fetch.sql(),
            r#"FETCH 100 "materialize_sdk" WITH (TIMEOUT = '1000 milliseconds')"#
        );
        let events = sub
            .push(
                fetch,
                &COLUMNS,
                vec![progress(5), insert(5, "a"), progress(6)],
            )
            .unwrap();
        let [Event::Batch(snapshot)] = &events[..] else {
            panic!("expected one batch, got {events:?}");
        };
        assert!(snapshot.is_snapshot());
        assert_eq!(snapshot.updates(), [inserted(5, "a")]);
        assert_eq!(sub.columns(), Some(&["id".to_string()][..]));
        assert_eq!(sub.progress(), Some(6));
    }

    #[test]
    fn an_empty_timed_fetch_is_followed_by_a_probe() {
        let mut sub = declared(orders(), Start::Snapshot { as_of: None });
        round(&mut sub, TIMEOUT, vec![progress(5), insert(5, "a")]);
        assert!(round(&mut sub, TIMEOUT, vec![]).is_empty());
        let probe = sub.fetch(100, TIMEOUT).unwrap();
        assert_eq!(probe.sql(), r#"FETCH 100 "materialize_sdk""#);
        assert_eq!(probe.timeout(), None);
        sub.push(probe, &COLUMNS, vec![progress(6)]).unwrap();
        assert_eq!(
            sub.fetch(100, TIMEOUT).unwrap().timeout(),
            TIMEOUT,
            "rows end the probe"
        );
    }

    #[test]
    fn an_empty_probe_ends_the_stream() {
        let start = Start::Resume {
            frontier: Frontier::new(10).unwrap(),
        };
        let mut sub = declared(orders().with_up_to(20), start);
        round(&mut sub, TIMEOUT, vec![progress(9), insert(12, "a")]);
        round(&mut sub, TIMEOUT, vec![]);
        let [last] = &round(&mut sub, TIMEOUT, vec![])[..] else {
            panic!("expected one batch");
        };
        assert_eq!(last.upper(), Frontier::new(20).unwrap());
        assert_eq!(last.updates(), [inserted(12, "a")]);
        assert!(sub.is_finished());
        assert!(
            sub.fetch(100, TIMEOUT).is_err(),
            "nothing to fetch after the end"
        );
    }

    #[test]
    fn a_stream_without_up_to_ends_on_an_empty_probe() {
        let mut sub = declared(orders(), Start::Snapshot { as_of: None });
        round(&mut sub, TIMEOUT, vec![progress(5), insert(5, "a")]);
        round(&mut sub, TIMEOUT, vec![]);
        let [last] = &round(&mut sub, TIMEOUT, vec![])[..] else {
            panic!("expected one batch");
        };
        assert!(last.is_snapshot());
        assert_eq!(last.upper(), Frontier::new(6).unwrap());
        assert!(sub.is_finished());
    }

    #[test]
    fn one_fetch_is_outstanding_at_a_time() {
        let mut sub = declared(orders(), Start::Snapshot { as_of: None });
        let first = sub.fetch(100, TIMEOUT).unwrap();
        assert!(sub.fetch(100, None).is_err());
        sub.push(first, &COLUMNS, vec![progress(5)]).unwrap();

        // Another subscription's FETCH, issued at the same point in its stream.
        let mut sub = declared(orders(), Start::Snapshot { as_of: None });
        let mut other = declared(orders(), Start::Snapshot { as_of: None });
        let _ours = sub.fetch(100, TIMEOUT).unwrap();
        let foreign = other.fetch(100, TIMEOUT).unwrap();
        assert!(matches!(
            sub.push(foreign, &COLUMNS, vec![progress(5)]),
            Err(PushError::Usage(_))
        ));
    }

    #[test]
    fn a_timestamp_over_the_hard_limit_fails_the_subscription() {
        let limit = 2 * inserted(5, "a").approx_bytes();
        let mut sub =
            declared(orders(), Start::Snapshot { as_of: None }).with_max_timestamp_bytes(limit);
        round(&mut sub, TIMEOUT, vec![progress(5), insert(5, "a")]);
        let fetch = sub.fetch(100, TIMEOUT).unwrap();
        let rows = vec![insert(6, "b"), insert(6, "c"), insert(6, "d"), progress(7)];
        let too_large = PushError::TimestampTooLarge(TimestampTooLarge {
            timestamp: 6,
            limit,
        });
        assert_eq!(sub.push(fetch, &COLUMNS, rows), Err(too_large));
        assert!(sub.is_failed());
        assert!(sub.fetch(100, TIMEOUT).is_err());
    }

    #[test]
    fn an_error_fails_every_later_call() {
        let mut sub = declared(orders(), Start::Snapshot { as_of: None });
        let zero_diff = vec![
            Some("6".into()),
            Some("f".into()),
            Some("0".into()),
            Some("b".into()),
        ];
        let fetch = sub.fetch(100, TIMEOUT).unwrap();
        let rows = vec![progress(5), insert(5, "a"), progress(6), zero_diff];
        assert!(matches!(
            sub.push(fetch, &COLUMNS, rows),
            Err(PushError::Protocol(_))
        ));
        assert!(sub.is_failed());
        assert!(
            sub.fetch(100, TIMEOUT).is_err(),
            "the snapshot batch released before the error was lost, so nothing may follow"
        );
        let mut other = declared(orders(), Start::Snapshot { as_of: None });
        let foreign = other.fetch(100, TIMEOUT).unwrap();
        assert!(
            matches!(
                sub.push(foreign, &COLUMNS, vec![progress(5)]),
                Err(PushError::Usage(_))
            ),
            "a FETCH from another subscription is misuse, whatever this one's state"
        );
    }

    #[test]
    fn columns_must_match_the_envelope_and_stay_fixed() {
        let mut sub = declared(orders(), Start::Snapshot { as_of: None });
        let upsert_columns = ["mz_timestamp", "mz_progressed", "mz_state", "id"];
        let fetch = sub.fetch(100, TIMEOUT).unwrap();
        assert!(sub.push(fetch, &upsert_columns, vec![progress(5)]).is_err());

        let mut sub = declared(orders(), Start::Snapshot { as_of: None });
        round(&mut sub, TIMEOUT, vec![progress(5)]);
        let renamed = ["mz_timestamp", "mz_progressed", "mz_diff", "other"];
        let fetch = sub.fetch(100, TIMEOUT).unwrap();
        assert!(sub.push(fetch, &renamed, vec![progress(6)]).is_err());
    }

    #[test]
    fn errors_are_classified_by_phase() {
        let mut sub = declared(orders(), Start::Snapshot { as_of: None });
        assert_eq!(
            sub.classify(Some("XX000"), "invalid id u1"),
            Classification::CheckObject {
                otherwise: ErrorKind::Fatal
            }
        );
        round(&mut sub, TIMEOUT, vec![progress(5)]);
        assert_eq!(
            sub.classify(Some("XX000"), "division by zero"),
            Classification::Kind {
                kind: ErrorKind::StreamPoisoned
            }
        );
    }
}
